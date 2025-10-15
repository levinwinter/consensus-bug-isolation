import operator as op

from validation import Proposal, Validation

Message = Proposal | Validation
# Assertion = (Callable[[Message, set[int], Message, set[int]], bool], str)

# senders_equal: Assertion = (lambda l, ls, r, rs: ls == rs, 'senders equal')
# senders_disjoint: Assertion = (lambda l, ls, r, rs: ls.isdisjoint(rs), 'senders disjoint')
# senders_subset: Assertion = (lambda l, ls, r, rs: ls.issubset(rs), 'senders subset')
# senders_superset: Assertion = (lambda l, ls, r, rs: ls.issuperset(rs), 'senders superset')

# p_p_txs_eq: Assertion = (lambda l, ls, r, rs: l.transaction_hash == r.transaction_hash , 'txs hash equal')
# p_p_seq_eq: Assertion = (lambda l, l)

class Assertion():
    """
    Represents a comparison assertion between two message fields.

    Uses __slots__ for memory efficiency and faster attribute access.
    """
    __slots__ = ['fieldL', 'fieldR', 'op']

    def __init__(self, fieldL: str, fieldR: str, op) -> None:
        self.fieldL = fieldL
        self.fieldR = fieldR
        self.op = op

    def eval(self, l: Message, r: Message) -> bool:
        return self.op(getattr(l, self.fieldL), getattr(r, self.fieldR))

    def __str__(self) -> str:
        return f'{self.fieldL} {self.op.__name__} {self.fieldR}'

class Predicate:
    """
    Represents a predicate that checks for message patterns in consensus logs.

    Format: "At least N messages of TypeL -> TypeR where <assertions>"

    Uses __slots__ for memory efficiency since we create ~4,500 of these.
    Note: observed and observed_true are now tracked externally via PredicateState
    wrapper to avoid deep copying overhead.
    """
    __slots__ = ['typeL', 'typeR', 'threshold', 'assertions', 'observed', 'observed_true']

    def __init__(self, typeL: type, typeR: type, threshold: int, assertions: list[Assertion]) -> None:
        self.typeL = typeL
        self.typeR = typeR
        self.threshold = threshold
        self.assertions = assertions
        self.observed = False
        self.observed_true = False

    def eval(self, l: Message, r: Message):
        if self.observed_true or not (isinstance(l, self.typeL) and isinstance(r, self.typeR) and self.threshold <= len(l.peers)):
            return False
        self.observed = True
        for assertion in self.assertions:
            if not assertion.eval(l, r):
                return False
        self.observed_true = True
        return True

    def __str__(self) -> str:
        return f'Predicate(at least {self.threshold} {self.typeL.__name__} -> {self.typeR.__name__} where {", ".join(map(str, self.assertions))})'
