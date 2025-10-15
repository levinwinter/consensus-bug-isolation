"""
Main Analysis Pipeline for Consensus Bug Isolation

This module orchestrates the two-phase analysis pipeline:

Phase 1 - Cache Generation:
  - Processes validator logs from 7-node consensus network
  - Evaluates predicates on message pairs per node
  - Caches results to predicates-cache-{node_id}.txt files

Phase 2 - Statistical Fault Localization:
  - Loads cached predicate evaluations
  - Aggregates observations across successful and failed runs
  - Uses statistical fault localization (Tarantula-based) to identify suspicious predicates
  - Recursively isolates failure-causing predicates by importance score

Network topology:
- Left partition: validators 0-4
- Right partition: validators 2-6
- Overlap: validators 2-4
"""

import itertools as it
import os
import pickle
import sys
from multiprocessing import Pool
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tqdm import tqdm

from src.analysis.cache import generate_predicate_cache, load_predicate_cache
from src.analysis.fault_localization import Aggregation, Report, isolate
from src.utils.config import SET_LOW, SET_HIGH

# Import stats function if needed for analysis
try:
    from scripts.analyze import stats
except ImportError:
    stats = None

# Network partition definitions (validators in each partition)
_SET_LOW = SET_LOW   # Left partition
_SET_HIGH = SET_HIGH  # Right partition

# Cache file paths for aggregation results
AGGREGATION_CACHE_FILE = 'cache/aggregation_cache.pickle'
REPORTS_CACHE_FILE = 'cache/reports_cache.pickle'


def process_run_to_report(path):
    """
    Process a single consensus run and convert to a Report object.

    This function:
    1. Loads cached predicate evaluations from all 7 validator nodes
    2. Aggregates predicate observations across nodes
    3. Checks if predicate was observed in left/right network partitions
    4. Determines if run succeeded or failed based on results.txt

    Args:
        path: Path to run directory (e.g., data/buggy-7.../timestamp/)

    Returns:
        Report object with success status and predicate observations,
        or None if cache files are missing
    """
    # Load cached predicates from all 7 validator nodes
    results = list(map(load_predicate_cache, zip(it.repeat(path), [0, 1, 2, 3, 4, 5, 6])))

    # If any node is missing cache, skip this run
    if None in results:
        return None

    # Aggregate predicate observations across nodes
    # Key: predicate string, Value: set of node IDs where predicate was observed true
    aggregated = {}
    for node_id, results_per_node in enumerate(results):
        for result in results_per_node:
            # Initialize entry even if not observed (needed for tracking)
            aggregated.setdefault(str(result), set())
            if result.observed_true:
                aggregated[str(result)].add(node_id)

    # For each predicate, check if it was observed in network partitions
    # Generate observations for different threshold levels (> 0, > 1, > 2, > 3, > 4)
    partition_observations = {}
    for pred, observed_nodes in aggregated.items():
        # Count how many nodes in each partition observed this predicate
        nodes_low = len(observed_nodes.intersection(_SET_LOW))
        nodes_high = len(observed_nodes.intersection(_SET_HIGH))

        # Check each threshold: predicate observed in > i nodes in at least one partition
        for threshold in range(0, 5):
            predicate_key = f'"{pred}" > {threshold}'
            partition_observations[predicate_key] = (nodes_low > threshold or nodes_high > threshold)

    # Determine if this run succeeded or failed
    with open(os.path.join(path, "results.txt"), "r") as f:
        successful = "reason: all committed" in f.read()

    # Extract run name (timestamp) from path
    run_name = path.split("/")[-1]

    return Report(successful, partition_observations, run_name)


def aggregate_predicate_observations(reports: list[Report], aggregation: dict[str, Aggregation] = {}):
    """
    Aggregate predicate observations across multiple runs.

    Builds a statistical model of each predicate's behavior by categorizing
    observations into four categories:
    - successful_true: Predicate observed in successful run
    - successful_false: Predicate not observed in successful run
    - failure_true: Predicate observed in failed run
    - failure_false: Predicate not observed in failed run

    This aggregation enables statistical fault localization to identify
    predicates correlated with failures.

    Args:
        reports: List of Report objects from consensus runs
        aggregation: Existing aggregation dict to update (for incremental processing)

    Returns:
        Dictionary mapping predicate IDs to Aggregation objects with statistics
    """
    for report in reports:
        for (predicate_id, observed_true) in report.observations.items():
            # Initialize aggregation for this predicate if needed
            if predicate_id not in aggregation:
                aggregation[predicate_id] = Aggregation()

            # Categorize this observation based on success and observation status
            if report.successful and observed_true:
                aggregation[predicate_id].successful_true.add(report.name)
            elif report.successful and not observed_true:
                aggregation[predicate_id].successful_false.add(report.name)
            elif not report.successful and observed_true:
                aggregation[predicate_id].failure_true.add(report.name)
            elif not report.successful and not observed_true:
                aggregation[predicate_id].failure_false.add(report.name)

    return aggregation


class LightweightReport:
    """
    Lightweight version of Report containing only success status and run name.

    Used for memory efficiency during iterative fault isolation, where we don't
    need the full observation data after initial aggregation.
    """

    def __init__(self, successful, name) -> None:
        self.successful = successful
        self.name = name

if __name__ == '__main__':
    # Discover all run directories in data folder
    paths = []
    for dirpath, _, filenames in os.walk("data"):
        if len(filenames) == 0:
            continue
        paths.append(dirpath)

    paths = sorted(paths)

    # Initialize multiprocessing pool for parallel processing
    pool = Pool()

    reports = []
    aggregation = {}

    # Control whether to regenerate cache or load from pickle
    # Set to False to load cached aggregation (much faster)
    REGENERATE_CACHE = True

    if REGENERATE_CACHE:
        # Phase 1: Generate predicate caches for all runs
        print("Phase 1: Generating predicate caches...")

        # Prepare cache generation tasks: (path, node_id) for all 7 nodes per run
        cache_tasks = [(path, node_id) for path in paths for node_id in range(7)]

        # Parallelize cache generation across all nodes and runs
        list(tqdm(
            pool.imap_unordered(generate_predicate_cache, cache_tasks),
            total=len(cache_tasks),
            desc="Generating cache"
        ))

        # Phase 2: Load caches and aggregate observations
        print("\nPhase 2: Aggregating observations...")

        for report in tqdm(
            pool.imap_unordered(process_run_to_report, paths),
            total=len(paths),
            desc="Generating reports"
        ):
            if report is not None:
                # Create lightweight report for memory efficiency
                reports.append(LightweightReport(report.successful, report.name))
                # Aggregate observations incrementally
                aggregate_predicate_observations([report], aggregation)

        # Save aggregation to disk for fast re-loading
        os.makedirs(os.path.dirname(AGGREGATION_CACHE_FILE), exist_ok=True)
        with open(AGGREGATION_CACHE_FILE, 'wb') as handle:
            pickle.dump(aggregation, handle, protocol=pickle.HIGHEST_PROTOCOL)
        with open(REPORTS_CACHE_FILE, 'wb') as handle:
            pickle.dump(reports, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        # Load pre-computed aggregation from cache
        print("Loading cached aggregation...")
        with open(AGGREGATION_CACHE_FILE, 'rb') as handle:
            aggregation = pickle.load(handle)
        with open(REPORTS_CACHE_FILE, 'rb') as handle:
            reports = pickle.load(handle)

    # Filter out predicates involving consensus_hash
    # (These tend to produce less useful fault localization results)
    aggregation = {x: y for x, y in aggregation.items() if 'consensus_hash' not in x}

    pool.close()
    pool.terminate()

    # Phase 3: Perform statistical fault localization
    print("\nPhase 3: Isolating failure-causing predicates...")
    isolate(reports, aggregations=aggregation, stats_fn=stats)
