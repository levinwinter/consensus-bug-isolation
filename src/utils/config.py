"""
Network topology and configuration constants.

Defines the network partition structure for the 7-node consensus network:
- Left partition (SET_LOW): validators 0-4
- Right partition (SET_HIGH): validators 2-6
- Overlap: validators 2-4 are in both partitions
"""

# Network partition definitions (validators in each partition)
SET_LOW = frozenset([0, 1, 2, 3, 4])   # Left partition
SET_HIGH = frozenset([2, 3, 4, 5, 6])  # Right partition

# Filter sets for message filtering by partition
FILTER_SET_LOW = frozenset([0, 1, 2, 3, 4])   # Left partition
FILTER_SET_HIGH = frozenset([2, 3, 4, 5, 6])  # Right partition
