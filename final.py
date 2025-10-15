from copy import copy, deepcopy
import itertools
import os
import json
from predicates import Assertion, Predicate
import operator as op
from tqdm import tqdm
from eval import eval
import re

class CachedPredicate:

    def __init__(self, observed: bool, observed_true: bool, name: str) -> None:
        self.observed = observed
        self.observed_true = observed_true
        self.name = name
    
    def __str__(self) -> str:
        return self.name

cache_re = re.compile(r'^(True|False) (True|False) (.+)')

from validation import Proposal, Validation

def read_json(file):
    lines = []
    while line := file.readline():
        lines.append(line)
        if line == '}\n':
            break
    return json.loads(''.join(lines))

def read_messages(path):
    messages = []
    with open(path, 'r') as f:
        while line := f.readline():
            line = line.strip()
            if 'Received ProposeSet' in line:
                messages.append(Proposal(read_json(f)))
            if 'Received Validation' in line:
                messages.append(Validation(read_json(f)))
    return messages

fields = [
    (Proposal, 'peers', set[int]),
    (Proposal, 'close_time', 'time'),
    (Proposal, 'previous_ledger', 'hash_l'),
    (Proposal, 'propose_seq', 'seq_prp'),
    (Proposal, 'transaction_hash', 'hash_tx'),
    (Validation, 'peers', set[int]),
    (Validation, 'consensus_hash', 'hash_tx'),
    (Validation, 'flags', 'flags'),
    (Validation, 'ledger_hash', 'hash_l'),
    (Validation, 'ledger_sequence', 'ledger_seq'),
    (Validation, 'signing_time', 'time')
]

predicates: list[Predicate] = []
for i in range(1, 5):
    for (l, r) in itertools.product([Proposal, Validation], repeat=2):
        possible_fields = []
        for lField, rField in itertools.product(filter(lambda x: x[0] == l, fields), filter(lambda x: x[0] == r, fields)):
            if lField[2] != rField[2]:
                continue
            possible_fields.append((lField, rField))
        for max_assertions in range(0, 4):
            for using_fields in itertools.combinations(possible_fields, max_assertions):
                field_and_op = []
                for lField, rField in using_fields:
                    inner = []
                    for operator in {
                            'seq_prp': [op.eq, op.ne, op.lt, op.gt],
                            'flags': [op.eq, op.ne],
                            'ledger_seq': [op.eq, op.ne, op.lt, op.gt],
                            set[int]: [op.eq, op.ne, set.isdisjoint, set.issubset, set.issuperset],
                            'hash_l': [op.eq, op.ne],
                            'hash_tx': [op.eq, op.ne],
                            'time': [op.eq, op.ne],
                        }[lField[2]]:
                        inner.append((lField, operator, rField))
                    field_and_op.append(inner)
                for using_fields_with_ops in itertools.product(*field_and_op):
                    assertions = []
                    for lField, operator, rField in using_fields_with_ops:
                            assertions.append(Assertion(lField[1], rField[1], operator))
                    predicates.append(Predicate(l, r, i, assertions))

# predicates = predicates[:500]
print(len(predicates), 'predicates')

def do_magic_from_cache(x):
    path, id = x[0], x[1]
    cache_path = os.path.join(path, f'predicates-cache-{id}.txt')
    if os.path.exists(cache_path):
        with open(cache_path, 'r') as f:
            predicates_c = []
            for line in f.readlines():
                # f'Predicate(at least {self.threshold} {self.typeL.__name__} -> {self.typeR.__name__} where {", ".join(map(str, self.assertions))})'
                result = cache_re.search(line).groups()
                predicates_c.append(CachedPredicate(result[0] == 'True', result[1] == 'True', result[2]))
            return predicates_c
    else:
        print(path, 'not cached')
        return None

# Pre-compute filter sets to avoid repeated set creation
_FILTER_SET_LOW = frozenset([0, 1, 2, 3, 4])
_FILTER_SET_HIGH = frozenset([2, 3, 4, 5, 6])

def do_magic_skip_cache(x):
    path, id = x[0], x[1]
    cache_path = os.path.join(path, f'predicates-cache-{id}.txt')
    if os.path.exists(cache_path):
        return
    messages = read_messages(os.path.join(path, f'validator_{id}.txt'))

    # Use pre-computed frozensets instead of creating new sets each time
    filter_set = _FILTER_SET_LOW if id < 4 else _FILTER_SET_HIGH
    messages = [m for m in messages if not m.peers.isdisjoint(filter_set)]

    local_predicates = deepcopy(predicates)
    seen: dict[Proposal | Validation, set] = {}
    for message in messages:
        if message in seen:
            message.peers.update(seen[message])
        for other, peers in seen.items():
            other.peers = peers
            for predicate in local_predicates:
                predicate.eval(other, message)
        seen.setdefault(message, set())
        seen[message] = message.peers
    
    with open(cache_path, 'w') as f:
        for predicate in local_predicates:
            f.write(f'{predicate.observed} {predicate.observed_true} {str(predicate)}\n')

    return local_predicates
