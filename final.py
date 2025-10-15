"""
Predicate Cache Generation Module

This module is responsible for generating and loading predicate evaluation caches
for the consensus bug isolation tool. It processes validator logs to evaluate
~360,000 predicates on message pairs and caches results for fast aggregation.

Key responsibilities:
1. Parse validator logs to extract Proposals and Validations
2. Generate exhaustive predicates for message field comparisons
3. Evaluate predicates on message pairs from each validator node
4. Cache results to avoid recomputation during statistical analysis
"""

# Standard library imports
from copy import deepcopy
import itertools
import json
import operator as op
import os
import re

# Local imports
from predicates import Assertion, Predicate
from validation import Proposal, Validation

class CachedPredicate:
    """
    Represents a predicate that has been evaluated and loaded from cache.

    Attributes:
        observed: Whether the predicate's message pattern was observed
        observed_true: Whether the predicate matched when observed
        name: String representation of the predicate
    """

    def __init__(self, observed: bool, observed_true: bool, name: str) -> None:
        self.observed = observed
        self.observed_true = observed_true
        self.name = name

    def __str__(self) -> str:
        return self.name


class PredicateState:
    """
    Lightweight wrapper that tracks per-run state for a predicate without copying.

    Instead of deep copying predicates for each run, we share the immutable
    predicate logic and only track mutable state (observed, observed_true) here.
    This significantly reduces memory usage and initialization overhead.
    """
    __slots__ = ['predicate', 'observed', 'observed_true']

    def __init__(self, predicate: Predicate) -> None:
        self.predicate = predicate
        self.observed = False
        self.observed_true = False

    def eval(self, l, r) -> bool:
        """Evaluate predicate with this instance's state tracking."""
        if self.observed_true or not (isinstance(l, self.predicate.typeL) and
                                       isinstance(r, self.predicate.typeR) and
                                       self.predicate.threshold <= len(l.peers)):
            return False
        self.observed = True
        for assertion in self.predicate.assertions:
            if not assertion.eval(l, r):
                return False
        self.observed_true = True
        return True

    def __str__(self) -> str:
        return str(self.predicate)


# Regex for parsing cached predicate lines: "True/False True/False predicate_description"
cache_re = re.compile(r'^(True|False) (True|False) (.+)')

def read_multiline_json(file):
    """
    Read a multi-line JSON object from a file stream.

    Validator logs contain JSON objects spread across multiple lines.
    This reads until it finds a closing brace on its own line.

    Args:
        file: Open file handle positioned at start of JSON object

    Returns:
        Parsed JSON object as a dictionary
    """
    lines = []
    while line := file.readline():
        lines.append(line)
        if line == '}\n':
            break
    return json.loads(''.join(lines))


def parse_validator_log(path):
    """
    Parse a validator log file to extract consensus messages.

    Reads through a validator log searching for "Received ProposeSet" and
    "Received Validation" markers, then parses the JSON message that follows.

    Args:
        path: Path to validator log file (e.g., data/.../validator_0.txt)

    Returns:
        List of Proposal and Validation message objects
    """
    messages = []
    with open(path, 'r') as f:
        while line := f.readline():
            line = line.strip()
            if 'Received ProposeSet' in line:
                messages.append(Proposal(read_multiline_json(f)))
            if 'Received Validation' in line:
                messages.append(Validation(read_multiline_json(f)))
    return messages

# Message field definitions for predicate generation
# Format: (MessageType, field_name, type_category)
# Type categories group fields that can be meaningfully compared
fields = [
    # Proposal fields
    (Proposal, 'peers', set[int]),           # Set of validator IDs that sent this message
    (Proposal, 'close_time', 'time'),        # Ledger close time
    (Proposal, 'previous_ledger', 'hash_l'), # Hash of previous ledger
    (Proposal, 'propose_seq', 'seq_prp'),    # Proposal sequence number
    (Proposal, 'transaction_hash', 'hash_tx'),# Hash of transaction set

    # Validation fields
    (Validation, 'peers', set[int]),          # Set of validator IDs that sent this message
    (Validation, 'consensus_hash', 'hash_tx'),# Hash of agreed-upon transaction set
    (Validation, 'flags', 'flags'),           # Validation flags (bitfield)
    (Validation, 'ledger_hash', 'hash_l'),    # Hash of validated ledger
    (Validation, 'ledger_sequence', 'ledger_seq'), # Ledger sequence number
    (Validation, 'signing_time', 'time')      # Time validation was signed
]

"""
Generate all possible predicates exhaustively.

This generates predicates in the form:
"At least N messages of TypeL -> TypeR where <assertions>"

Parameters explored:
- Threshold (N): 1-4 messages minimum
- Message type pairs: Proposal->Proposal, Proposal->Validation, etc.
- Assertions: 0-3 field comparisons
- Operators: Depends on field type (eq/ne for hashes, lt/gt for sequences, etc.)

This generates approximately 360,000 unique predicates to test.
"""
predicates: list[Predicate] = []

# Mapping of type categories to valid comparison operators
OPERATORS_BY_TYPE = {
    'seq_prp': [op.eq, op.ne, op.lt, op.gt],         # Proposal sequence numbers
    'flags': [op.eq, op.ne],                          # Validation flags
    'ledger_seq': [op.eq, op.ne, op.lt, op.gt],      # Ledger sequence numbers
    set[int]: [op.eq, op.ne, set.isdisjoint, set.issubset, set.issuperset],  # Peer sets
    'hash_l': [op.eq, op.ne],                         # Ledger hashes
    'hash_tx': [op.eq, op.ne],                        # Transaction hashes
    'time': [op.eq, op.ne],                           # Timestamps
}

# Generate predicates for each threshold level (1-4 messages)
for threshold in range(1, 5):
    # Generate for all message type combinations (Proposal/Validation pairs)
    for (left_type, right_type) in itertools.product([Proposal, Validation], repeat=2):
        # Find compatible field pairs (same type category)
        compatible_field_pairs = []
        left_fields = filter(lambda x: x[0] == left_type, fields)
        right_fields = filter(lambda x: x[0] == right_type, fields)

        for left_field, right_field in itertools.product(left_fields, right_fields):
            if left_field[2] != right_field[2]:  # Skip incompatible type categories
                continue
            compatible_field_pairs.append((left_field, right_field))

        # Generate predicates with 0-3 assertions
        for assertion_count in range(0, 4):
            for field_combination in itertools.combinations(compatible_field_pairs, assertion_count):
                # For each field pair, try all valid operators
                operators_per_field = []
                for left_field, right_field in field_combination:
                    operators_for_this_field = []
                    for operator in OPERATORS_BY_TYPE[left_field[2]]:
                        operators_for_this_field.append((left_field, operator, right_field))
                    operators_per_field.append(operators_for_this_field)

                # Generate all combinations of operator choices
                for operator_combination in itertools.product(*operators_per_field):
                    assertions = []
                    for left_field, operator, right_field in operator_combination:
                        assertions.append(Assertion(left_field[1], right_field[1], operator))
                    predicates.append(Predicate(left_type, right_type, threshold, assertions))

print(len(predicates), 'predicates')

# Partition predicates by message type pairs for faster lookup
# This allows us to only evaluate relevant predicates for each message pair
predicates_by_type: dict[tuple[type, type], list[Predicate]] = {}
for predicate in predicates:
    key = (predicate.typeL, predicate.typeR)
    if key not in predicates_by_type:
        predicates_by_type[key] = []
    predicates_by_type[key].append(predicate)

print(f"Predicates partitioned: {len(predicates_by_type)} type pairs")
for key, pred_list in predicates_by_type.items():
    print(f"  {key[0].__name__}->{key[1].__name__}: {len(pred_list)} predicates")

# Network partition definitions
# Left partition: validators 0-4, Right partition: validators 2-6
# Overlap: validators 2-4 are in both partitions
_FILTER_SET_LOW = frozenset([0, 1, 2, 3, 4])   # Left partition
_FILTER_SET_HIGH = frozenset([2, 3, 4, 5, 6])  # Right partition


def load_predicate_cache(args):
    """
    Load cached predicate evaluation results from disk.

    Reads a cache file containing predicate evaluation results for a single
    validator node's run. Each line contains: observed observed_true predicate_name

    Args:
        args: Tuple of (run_path, node_id)
              run_path: Path to run directory (e.g., data/buggy-7.../timestamp/)
              node_id: Validator ID (0-6)

    Returns:
        List of CachedPredicate objects, or None if cache doesn't exist
    """
    path, node_id = args
    cache_path = os.path.join(path, f'predicates-cache-{node_id}.txt')

    if not os.path.exists(cache_path):
        print(path, 'not cached')
        return None

    with open(cache_path, 'r') as f:
        cached_predicates = []
        for line in f.readlines():
            # Parse line format: "True/False True/False predicate_description"
            match = cache_re.search(line)
            if match:
                observed, observed_true, name = match.groups()
                cached_predicates.append(
                    CachedPredicate(observed == 'True', observed_true == 'True', name)
                )
        return cached_predicates


def generate_predicate_cache(args):
    """
    Generate and cache predicate evaluation results for a validator node's run.

    This function:
    1. Parses the validator log to extract messages
    2. Filters messages by network partition overlap
    3. Evaluates all predicates on message pairs
    4. Caches results to disk for fast re-loading

    The algorithm evaluates each predicate against all pairs of messages,
    deduplicating messages by content and tracking peer sets.

    Optimizations:
    - Partitions predicates by message type to only evaluate relevant ones
    - Removes predicates from active set after they match (observed_true)
    - Uses message type to select appropriate predicate subset

    Args:
        args: Tuple of (run_path, node_id)
              run_path: Path to run directory (e.g., data/buggy-7.../timestamp/)
              node_id: Validator ID (0-6)

    Returns:
        None (writes cache file as side effect)
    """
    path, node_id = args
    cache_path = os.path.join(path, f'predicates-cache-{node_id}.txt')

    # Skip if already cached
    if os.path.exists(cache_path):
        return

    # Parse validator log to extract Proposals and Validations
    messages = parse_validator_log(os.path.join(path, f'validator_{node_id}.txt'))

    # Filter messages to only include those from our network partition
    # Validators 0-3 use left partition, validators 4-6 use right partition
    filter_set = _FILTER_SET_LOW if node_id < 4 else _FILTER_SET_HIGH
    messages = [m for m in messages if not m.peers.isdisjoint(filter_set)]

    # Create lightweight state wrappers for predicates (avoids expensive deepcopy)
    # Each type pair maintains its own list of predicate states that haven't matched yet
    active_predicates_by_type: dict[tuple[type, type], list[PredicateState]] = {}
    all_predicate_states = []  # Keep reference to all predicate states for final output

    for key, pred_list in predicates_by_type.items():
        # Wrap each predicate in a lightweight state tracker (much faster than deepcopy)
        state_wrappers = [PredicateState(pred) for pred in pred_list]
        active_predicates_by_type[key] = state_wrappers
        all_predicate_states.extend(state_wrappers)

    # Deduplicate messages by content, aggregating peer sets
    # Key: message content, Value: set of peers that sent this message
    message_deduplication_map: dict[Proposal | Validation, set] = {}

    for message in messages:
        # If we've seen this message before, merge peer sets
        if message in message_deduplication_map:
            message.peers.update(message_deduplication_map[message])

        # Evaluate predicates on this message paired with all previous messages
        for other_message, peers in message_deduplication_map.items():
            other_message.peers = peers

            # Only evaluate predicates matching the message type pair
            type_key = (type(other_message), type(message))
            if type_key not in active_predicates_by_type:
                continue

            active_predicates = active_predicates_by_type[type_key]

            # Track which predicates to remove (those that become observed_true)
            to_remove = []
            for i, predicate in enumerate(active_predicates):
                if predicate.eval(other_message, message):
                    # Predicate matched, mark for removal from active set
                    to_remove.append(i)

            # Remove matched predicates from active set (iterate backwards to maintain indices)
            for i in reversed(to_remove):
                active_predicates.pop(i)

        # Add this message to deduplication map
        message_deduplication_map.setdefault(message, set())
        message_deduplication_map[message] = message.peers

    # Write results to cache file (batch into string buffer first)
    output_lines = []
    for pred_state in all_predicate_states:
        output_lines.append(f'{pred_state.observed} {pred_state.observed_true} {str(pred_state)}\n')

    with open(cache_path, 'w') as f:
        f.writelines(output_lines)
