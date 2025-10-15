#! /usr/bin/python3

import os
import re
import sys

from runs import runs as runs_all

from rich.table import Table
from rich.console import Console

version = ''

cap = 1000000
if len(sys.argv) == 3 and sys.argv[1] == 'cap':
    cap = int(sys.argv[2])

if len(sys.argv) == 3:
    if sys.argv[1] == 'show':
        for config in os.listdir(f'data/{version}'):
            if os.path.exists(f'data/{version}' + config + '/' + sys.argv[2]):
                print(open(f'data/{version}' + config + '/' + sys.argv[2] + '/results.txt').read(), end = '')
                exit(0)

def f_or(x, y):
    return lambda z: x(z) or y(z)

def f_not(x):
    return lambda y: not x(y)

def f_insufficient(line):
    return bool(re.search(r'\[flag\] Ledger \d+ diverged and has insufficient support\n', line))

def f_timeout(line):
    return bool(re.search(r'\[flag\] Timeout after \d+ messages\n', line))

def f_incompatible(line):
    return bool(re.search(r'\[flag\] Incompatible ledger <.+>\n', line))

def count(f, flags):
    return len(list(filter(f, flags)))

built_ledger = re.compile(r'Built ledger #(\d+): (.*)')
def new_insufficient_support(path):
    hashes: dict[int, dict[int, str]] = {}
    for i in range(0, 7):
        with open(os.path.join(path, f'validator_{i}.txt'), 'r') as f:
            for line in f.readlines():
                if match := built_ledger.search(line.strip()):
                    if match.groups()[0] not in hashes:
                        hashes[match.groups()[0]] = {}
                    hashes[match.groups()[0]][i] = match.groups()[1]
    for ledger, vs in hashes.items():
        l, r = {}, {}
        lc, rc = 0, 0
        if 0 in vs:
            l.setdefault(vs[0], set())
            l[vs[0]].add(0)
            lc += 1
        if 1 in vs:
            l.setdefault(vs[1], set())
            l[vs[1]].add(1)
            lc += 1
        if 2 in vs:
            l.setdefault(vs[2], set())
            l[vs[2]].add(2)
            r.setdefault(vs[2], set())
            r[vs[2]].add(2)
            lc += 1
        if 4 in vs:
            l.setdefault(vs[4], set())
            l[vs[4]].add(4)
            r.setdefault(vs[4], set())
            r[vs[4]].add(4)
            rc += 1
        if 5 in vs:
            r.setdefault(vs[5], set())
            r[vs[5]].add(5)
            rc += 1
        if 6 in vs:
            r.setdefault(vs[6], set())
            r[vs[6]].add(6)
            rc += 1
        # if len(vs) < 7 and vs[0] == vs[1] and vs[1] == vs[2] and vs[2] != vs[4] and vs[4] == vs[5] and vs[5] == vs[6]:
        #     return True
        for lLedger in l:
            for rLedger in r:
                if len(l[lLedger]) > 2 and len(r[rLedger]) > 2:
                    if lLedger != rLedger:
                        return True
    return False

def new_incompatible_ledger(path):
    nodes = set()
    for i in range(0, 7):
        with open(os.path.join(path, f'validator_{i}.txt'), 'r') as f:
            for line in f.readlines():
                if 'Not validating incompatible' in line:
                    nodes.add(i)
    return len(nodes.intersection([0, 1, 2, 3, 4])) > 1 or len(nodes.intersection([2, 3, 4, 5, 6])) > 1


def stats(filters):
    all_runs = []
    filters = list(map(str, filters))
    all_timeout = []

    counter = 0
    uncategorized_count = 0
    table = Table()
    scores = Table()

    table.add_column("Configuration", justify="right", style="cyan", no_wrap=True)
    table.add_column("Total", justify='right')
    table.add_column("Correct", style='green', justify='right')
    table.add_column("Insufficient", justify='right')
    table.add_column("Incompatible", justify='right')
    table.add_column("Timeout", justify='right')
    table.add_column("Incomplete", justify='right')
    table.add_column("Uncategorized", justify='right')
    table.add_column("II", justify='right')
    table.add_column("UInc", justify='right')
    table.add_column("UII", justify='right')
    table.add_column("UIns", justify='right')

    scores.add_column('Metric')
    scores.add_column('Incompatible', justify='right')
    scores.add_column('Insufficient', justify='right')
    scores.add_column('Agreement', justify='right')

    agg = {
        'Incompatible': {
            'TP': set(),
            'FP': set(),
            'TN': set(),
            'FN': set(),
        },
        'Insufficient': {
            'TP': set(),
            'FP': set(),
            'TN': set(),
            'FN': set(),
        },
        'Agreement': {
            'TP': set(),
            'FP': set(),
            'TN': set(),
            'FN': set(),
        },
    }

    for config in sorted(os.listdir(f'data/{version}')):
        match = re.search(r'buggy-7-(\d)-(\d)-\d-(.*)', config)
        if match is None:
            continue
        (c, d, scope) = match.groups()
        if scope == 'any-scope':
            scope = 'as'
        elif scope == 'baseline':
            scope = 'bs'
        else:
            scope = 'ss'
        # print(f'd={d} c={c} {scope}|', end = '')
        runs = os.listdir(f'data/{version}' + config)
        runs = list(filter(lambda run: run in list(map(str, runs_all)), runs))
        print('- found', len(runs), 'runs')
        correct = []
        incomplete = []
        uncategorized = []
        insufficient_support = []
        insufficient_incompatible = []
        incompatible = []
        U_insufficient_support = []
        U_insufficient_incompatible = []
        U_incompatible = []
        timeout = []
        runs = list(filter(lambda run: run not in ['1687147966', '1687007386', '1687175769', '1687181772', '1687239494', '1687026943'], runs))[:300]
        all_runs += runs
        for run in runs[:cap]:
            p = True
            if len(filters) > 0 and run not in filters:
                p = False
            # if run in map(str, [1687147966, 1687142445, 1687214998, 1687175769, 1687026943]):
            #     continue # filter timeout
            results = open(f'data/{version}' + config + '/' + run + '/results.txt').readlines()
            # print(results[4].strip())
            if results[-1] != 'done!\n':
                incomplete.append(run)
            elif results[4] == 'reason: all committed\n':
                correct.append(run)
            elif results[4] == 'reason: flags\n':
                flags = results[5:-1]
                if count(f_not(f_timeout), flags) == 0:
                    a = new_insufficient_support(f'data/{version}' + config + '/' + run)
                    b = new_incompatible_ledger(f'data/{version}' + config + '/' + run)
                    if a and b:
                        if p:
                            insufficient_incompatible.append(run)
                        agg['Incompatible']['TP' if p else 'FN'].add(run)
                        agg['Insufficient']['TP' if p else 'FN'].add(run)
                        agg['Agreement']['FP' if p else 'TN'].add(run)
                    elif a:
                        if p:
                            insufficient_support.append(run)
                        agg['Incompatible']['FP' if p else 'TN'].add(run)
                        agg['Insufficient']['TP' if p else 'FN'].add(run)
                        agg['Agreement']['FP' if p else 'TN'].add(run)
                    elif b:
                        if p:
                            incompatible.append(run)
                        agg['Incompatible']['TP' if p else 'FN'].add(run)
                        agg['Insufficient']['FP' if p else 'TN'].add(run)
                        agg['Agreement']['FP' if p else 'TN'].add(run)
                    else:
                        raise Exception('timeout')
                elif count(f_not(f_or(f_insufficient, f_timeout)), flags) == 0 and count(f_insufficient, flags) > 0:
                    if p:
                        insufficient_support.append(run)
                    agg['Incompatible']['FP' if p else 'TN'].add(run)
                    agg['Insufficient']['TP' if p else 'FN'].add(run)
                    agg['Agreement']['FP' if p else 'TN'].add(run)
                elif count(f_not(f_or(f_incompatible, f_timeout)), flags) == 0 and count(f_incompatible, flags) > 0:
                    a = new_insufficient_support(f'data/{version}' + config + '/' + run)
                    b = new_incompatible_ledger(f'data/{version}' + config + '/' + run)
                    if a and b:
                        if p:
                            insufficient_incompatible.append(run)
                        agg['Incompatible']['TP' if p else 'FN'].add(run)
                        agg['Insufficient']['TP' if p else 'FN'].add(run)
                        agg['Agreement']['FP' if p else 'TN'].add(run)
                    elif a:
                        if p:
                            insufficient_support.append(run)
                        agg['Incompatible']['FP' if p else 'TN'].add(run)
                        agg['Insufficient']['TP' if p else 'FN'].add(run)
                        agg['Agreement']['FP' if p else 'TN'].add(run)
                    elif b:
                        if p:
                            incompatible.append(run)
                        agg['Incompatible']['TP' if p else 'FN'].add(run)
                        agg['Insufficient']['FP' if p else 'TN'].add(run)
                        agg['Agreement']['FP' if p else 'TN'].add(run)
                    else:
                        raise Exception('timeout')
                elif count(f_not(f_or(f_or(f_incompatible, f_insufficient), f_timeout)), flags) == 0 and count(f_incompatible, flags) > 0 and count(f_insufficient, flags) > 0:
                    # insufficient_support.append(run)
                    # incompatible.append(run)
                    a = new_insufficient_support(f'data/{version}' + config + '/' + run)
                    b = new_incompatible_ledger(f'data/{version}' + config + '/' + run)
                    if a and b:
                        if p:
                            insufficient_incompatible.append(run)
                        agg['Incompatible']['TP' if p else 'FN'].add(run)
                        agg['Insufficient']['TP' if p else 'FN'].add(run)
                        agg['Agreement']['FP' if p else 'TN'].add(run)
                    elif a:
                        if p:
                            insufficient_support.append(run)
                        agg['Incompatible']['FP' if p else 'TN'].add(run)
                        agg['Insufficient']['TP' if p else 'FN'].add(run)
                        agg['Agreement']['FP' if p else 'TN'].add(run)
                    elif b:
                        if p:
                            incompatible.append(run)
                        agg['Incompatible']['TP' if p else 'FN'].add(run)
                        agg['Insufficient']['FP' if p else 'TN'].add(run)
                        agg['Agreement']['FP' if p else 'TN'].add(run)
                    else:
                        raise Exception('timeout')
                    counter += 1
                else:
                    a = new_insufficient_support(f'data/{version}' + config + '/' + run)
                    b = new_incompatible_ledger(f'data/{version}' + config + '/' + run)
                    if a and b:
                        if p:
                            U_insufficient_incompatible.append(run)
                        agg['Incompatible']['TP' if p else 'FN'].add(run)
                        agg['Insufficient']['TP' if p else 'FN'].add(run)
                        agg['Agreement']['TP' if p else 'FN'].add(run)
                    elif a:
                        if p:
                            U_insufficient_support.append(run)
                        agg['Incompatible']['FP' if p else 'TN'].add(run)
                        agg['Insufficient']['TP' if p else 'FN'].add(run)
                        agg['Agreement']['TP' if p else 'FN'].add(run)
                    elif b:
                        if p:
                            U_incompatible.append(run)
                        agg['Incompatible']['TP' if p else 'FN'].add(run)
                        agg['Insufficient']['FP' if p else 'TN'].add(run)
                        agg['Agreement']['TP' if p else 'FN'].add(run)
                    else:
                        if p:
                            uncategorized.append(run)
                        agg['Incompatible']['FP' if p else 'TN'].add(run)
                        agg['Insufficient']['FP' if p else 'TN'].add(run)
                        agg['Agreement']['TP' if p else 'FN'].add(run)
                        if p:
                            uncategorized_count += 1
            else:
                raise Exception("unknown")
        # print(f'{len(runs[:cap]):5d}', end = '|')
        # print(f'{len(correct):7d}', end = '|')
        # print(f'{len(insufficient_support):12d}', end = '|')
        # print(f'{len(incompatible):12d}', end = '|')
        # print(f'{len(timeout):7d}', end = '|')
        # print(f'{len(incomplete):10d}', end = '|')
        # print(f'{len(uncategorized):13d}', uncategorized)

        table.add_row(f'd={d} c={c} {scope}', str(len(runs[:cap])), str(len(correct)), str(len(insufficient_support)), str(len(incompatible)), str(len(timeout)), str(len(incomplete)), str(len(uncategorized)), str(len(insufficient_incompatible)), str(len(U_incompatible)), str(len(U_insufficient_incompatible)), str(len(U_insufficient_support)))

    # print('multiple bugs', counter)

    # print(uncategorized_count, 'uncategorized')
    if len(sys.argv) == 3 and sys.argv[1] == 'list' and sys.argv[2] == 'timeouts':
        print('timeouts:', all_timeout)

    # print('\n\n')

    console = Console()
    console.print(table)

    sensitivity = lambda l: len(agg[l]['TP']) / (len(agg[l]['TP']) + len(agg[l]['FN'])) # recall
    specifity = lambda l: len(agg[l]['TN']) / (len(agg[l]['TN']) + len(agg[l]['FP']))
    precision = lambda l: len(agg[l]['TP']) / (len(agg[l]['TP']) + len(agg[l]['FP']))
    f1 = lambda l: 0 if precision(l) + sensitivity(l) == 0 else 2 * (precision(l) * sensitivity(l)) / (precision(l) + sensitivity(l))
    f0_5 = lambda l: 0 if precision(l) + sensitivity(l) == 0 else 1.25 * (precision(l) * sensitivity(l)) / (0.25 * precision(l) + sensitivity(l))

    a_incompatible = (len(agg['Incompatible']['TP']) + len(agg['Incompatible']['TN'])) / (len(agg['Incompatible']['TP']) + len(agg['Incompatible']['TN']) + len(agg['Incompatible']['FP']) + len(agg['Incompatible']['FN']))
    p_incompatible = len(agg['Incompatible']['TP']) / (len(agg['Incompatible']['TP']) + len(agg['Incompatible']['FP']))
    a_insufficient = (len(agg['Insufficient']['TP']) + len(agg['Insufficient']['TN'])) / (len(agg['Insufficient']['TP']) + len(agg['Insufficient']['TN']) + len(agg['Insufficient']['FP']) + len(agg['Insufficient']['FN']))
    p_insufficient = len(agg['Insufficient']['TP']) / (len(agg['Insufficient']['TP']) + len(agg['Insufficient']['FP']))
    a_agreement = (len(agg['Agreement']['TP']) + len(agg['Agreement']['TN'])) / (len(agg['Agreement']['TP']) + len(agg['Agreement']['TN']) + len(agg['Agreement']['FP']) + len(agg['Agreement']['FN']))
    p_agreement = len(agg['Agreement']['TP']) / (len(agg['Agreement']['TP']) + len(agg['Agreement']['FP']))
    scores.add_row('Precision', '{:.1%}'.format(p_incompatible), '{:.1%}'.format(p_insufficient), '{:.1%}'.format(p_agreement))
    scores.add_row('Recall', '{:.1%}'.format(sensitivity('Incompatible')), '{:.1%}'.format(sensitivity('Insufficient')), '{:.1%}'.format(sensitivity('Agreement')))
    scores.add_row('F1', '{:.1%}'.format(f1('Incompatible')), '{:.1%}'.format(f1('Insufficient')), '{:.1%}'.format(f1('Agreement')))
    scores.add_row('F0.5', '{:.1%}'.format(f0_5('Incompatible')), '{:.1%}'.format(f0_5('Insufficient')), '{:.1%}'.format(f0_5('Agreement')))
    scores.add_row('Specifity', '{:.1%}'.format(specifity('Incompatible')), '{:.1%}'.format(specifity('Insufficient')), '{:.1%}'.format(specifity('Agreement')))
    scores.add_row('Accuracy', '{:.1%}'.format(a_incompatible), '{:.1%}'.format(a_insufficient), '{:.1%}'.format(a_agreement))

    # print(list(map(int, all_runs)))

    console.print(scores)

if __name__ == '__main__':
    stats([])
