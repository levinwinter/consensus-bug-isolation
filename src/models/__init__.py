"""
Data models for consensus messages and predicates.
"""

from .messages import Proposal, Validation
from .predicates import Assertion, Predicate

__all__ = ['Proposal', 'Validation', 'Assertion', 'Predicate']
