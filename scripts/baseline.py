#!/usr/bin/env python3
"""
Baseline PRED-based Analysis Pipeline

This module implements the baseline approach that reads predicate observations
directly from PRED annotations in validator logs (as opposed to the message-based
approach that evaluates predicates on consensus messages).

Both approaches are equal, first-class approaches for consensus bug isolation:
- Baseline (this file): Uses pre-annotated PRED observations
- Message-based (main.py): Evaluates predicates on message pairs

This baseline uses the same statistical fault localization algorithm from
src/analysis/fault_localization.py but gets observations from PRED markers.
"""

import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.analysis.fault_localization import isolate, Report
from scripts.analyze import stats


def run_baseline_analysis():
    """
    Run baseline PRED-based analysis pipeline.

    Reads PRED annotations from validator logs and performs statistical
    fault localization to identify failure-correlated predicates.
    """
    reports = []

    # Walk through all data directories and process validator logs
    for (dirpath, _, filenames) in os.walk('data'):
        if (len(filenames) == 0):
            continue
        print(dirpath)

        # Determine if this run succeeded or failed
        with open(os.path.join(dirpath, 'results.txt'), 'r') as f:
            correct = 'reason: all committed' in f.read()

        all_observations = []
        observations = {}

        # Read PRED annotations from all 7 validator logs
        for i in range(0, 7):
            with open(os.path.join(dirpath, f'validator_{i}.txt'), 'r') as f:
                for line in f.readlines():
                    # Look for lines starting with PRED
                    if not line.startswith('PRED'):
                        continue

                    # Parse PRED line format: "PRED <predicate> <0|1>"
                    line = line.strip()[5:]  # Remove "PRED " prefix
                    id = " ".join(line.split(" ")[:-1])  # Extract predicate ID

                    # Skip malformed PRED lines
                    if 'PRED' in id:
                        continue

                    # Parse observation value (1 = true, 0 = false)
                    observation = line.split(" ")[-1] == "1"

                    # Track both "is true" and "is false" observations
                    if id not in observations:
                        observations[id + ' is true'] = observation
                        observations[id + ' is false'] = not observation
                    else:
                        # Aggregate observations across nodes (OR logic)
                        observations[id + ' is true'] = observation or observations[id + ' is true']
                        observations[id + ' is false'] = (not observation) or observations[id + ' is false']

            all_observations.append(observations)

        # Create report for this run
        run_name = dirpath.split('/')[-1]
        reports.append(Report(correct, observations, run_name))

    # Perform statistical fault localization using shared algorithm
    print(f"\nProcessed {len(reports)} runs from validator logs with PRED annotations")
    print("Starting statistical fault localization...\n")
    isolate(reports, stats_fn=stats)


if __name__ == '__main__':
    run_baseline_analysis()
