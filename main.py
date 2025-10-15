import os
import itertools as it
from analyze import stats
from final import do_magic_skip_cache, do_magic_from_cache
from multiprocessing import Pool

from isolate.isolate import Aggregation, Report, isolate

from tqdm import tqdm

import pickle

from runs import runs as all_runs

# Pre-compute sets to avoid repeated creation in hot loop
_SET_LOW = frozenset([0, 1, 2, 3, 4])
_SET_HIGH = frozenset([2, 3, 4, 5, 6])

def to_report(path):
    results = list(map(do_magic_from_cache, zip(it.repeat(path), [0,1,2,3,4,5,6])))
    # print(results)
    if None in results:
        return None
    aggregated = {}
    for i, results_per_node in enumerate(results):
        for result in results_per_node:
            if result.observed_true:
                result_str = str(result)
                aggregated.setdefault(result_str, set()).add(i)
    per_unl = {}
    for pred, peers in aggregated.items():
        # Use pre-computed frozensets instead of creating lists
        peers_low = len(peers.intersection(_SET_LOW))
        peers_high = len(peers.intersection(_SET_HIGH))
        for i in range(0, 5):
            per_unl[f'"{pred}" > {i}'] = (peers_low > i or peers_high > i)

    with open(os.path.join(path, "results.txt"), "r") as f:
        correct = "reason: all committed" in f.read()
        # print(correct)

    return Report(correct, per_unl, path.split("/")[-1])


def aggregate(reports: list[Report], aggregation: dict[str, Aggregation] = {}):

    for report in reports:
        for (id, observed_true) in report.observations.items():
            if id not in aggregation:
                aggregation[id] = Aggregation()
            if report.successful and observed_true:
                aggregation[id].successful_true.add(report.name)
            elif report.successful and not observed_true:
                aggregation[id].successful_false.add(report.name)
            elif not report.successful and observed_true:
                aggregation[id].failure_true.add(report.name)
            elif not report.successful and not observed_true:
                aggregation[id].failure_false.add(report.name)

    return aggregation

class StubReport:

    def __init__(self, successful, name) -> None:
        self.successful = successful
        self.name = name

if __name__ == '__main__':
    paths = []
    for dirpath, _, filenames in os.walk("data"):
        if len(filenames) == 0:
            continue
        paths.append(dirpath)

    paths = sorted(paths)
    paths = paths[:]

    pool = Pool()

    reports = []
    aggregation = {}

    if True:
        # Convert all_runs to a set for O(1) lookup instead of O(n)
        all_runs_set = set(all_runs)
        paths = [path for path in paths if int(path.split('/')[-1]) in all_runs_set]
        
        # Prepare all cache generation tasks - use list comprehension
        cache_tasks = [(path, node_id) for path in paths for node_id in range(7)]
        
        # Parallelize cache generation
        list(tqdm(pool.imap_unordered(do_magic_skip_cache, cache_tasks), total=len(cache_tasks), desc="Generating cache"))
        
        # Parallelize report generation
        for out in tqdm(pool.imap_unordered(to_report, paths), total=len(paths), desc="Generating reports"):
            if out is not None:
                reports.append(StubReport(out.successful, out.name))
                aggregate([out], aggregation)
        with open('1200filename.pickle', 'wb') as handle:
            pickle.dump(aggregation, handle, protocol=pickle.HIGHEST_PROTOCOL)
        with open('1200filename2.pickle', 'wb') as handle:
            pickle.dump(reports, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open('1200filename.pickle', 'rb') as handle:
            aggregation = pickle.load(handle)
        with open('1200filename2.pickle', 'rb') as handle:
            reports = pickle.load(handle)

    # Use dict comprehension for better performance
    aggregation = {x: y for x, y in aggregation.items() if 'consensus_hash' not in x}

    pool.close()
    pool.terminate()

    # for key, agg in aggregation.items():
    #     if 'Validation -> Validation where peers isdisjoint peers, ledger_hash eq ledger_hash, ledger_sequence lt ledger_sequence' in key:
    #         print(key)
    #         print(agg)
    #         print(agg.failure_true)
    #         stats(agg.failure_true)

    isolate(reports, aggregations=aggregation)
