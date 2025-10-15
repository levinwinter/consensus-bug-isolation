# Pre-create peer sets for better performance
_PEER_SETS = {
    'n9KkgT2SFxpQGic7peyokvkXcAmNLFob1AZXeErMFHxJ71q5MGaK': frozenset([0]),
    'n9M6ouZU7cLwRHPiVZjgJdEgrVyx2uv9euZzepdb34wDoj1RP5uS': frozenset([1]),
    'n9LJhBqLGTjPQa2KJtJmkHUubaHs1Y1ENYKZVmzZYhNb7GXh9m4j': frozenset([2]),
    'n9KgN4axJo1WC3fjFoUSkJ4gtZX4Pk2jPZzGR5CE9ddo16ewAPjN': frozenset([3]),
    'n9MsRMobdfpGvpXeGb3F6bm7WZbCiPrxzc1qBPP7wQox3NJzs5j2': frozenset([4]),
    'n9JFX46v3d3WgQW8DJQeBwqTk8vaCR7LufApEy65J1eK4X7dZbR3': frozenset([5]),
    'n9LFueHyYVJSyDArog2qtR42NixmeGxpaqFEFFp1xjxGU9aYRDZc': frozenset([6]),
}

_SIGNING_KEY_PEER_SETS = {
    "029F5BA6DE5151829B4BDEE7EF08646AE682416C55D13339A9E240717918BB27AE": frozenset([0]),
    "03A03ADBA690303EE808E709F1CFB0ED57D0B81830E0D6D3BF8FC7BE1464EC5307": frozenset([1]),
    "02E69913587A02971E53BE6DA2AF3E0EE93344BFE929791F19D3242427B15F0DEB": frozenset([2]),
    "02954103E420DA5361F00815929207B36559492B6C37C62CB2FE152CCC6F3C11C5": frozenset([3]),
    "03491045ADCC8ED22918AC94868E4F0A923CC098B1F2D486772B2B547F29642F12": frozenset([4]),
    "0224638749C987804AB2405DD2C91E495D201C964F948E03B0E6B63976A8FB337B": frozenset([5]),
    "032CA71B9FF55090AD3830FB52617EFB3703EA5CC6A2EA159C8D849FB9D281955B": frozenset([6]),
}

class Proposal:

    def __init__(self, message) -> None:
        self.close_time = int(message['close_time'])
        # Use pre-created frozensets and convert to regular set only once
        self.peers = set(_PEER_SETS.get(message['peer_id'], set()))
        self.previous_ledger = bytes.fromhex(message['previous_ledger'])
        try:
            self.propose_seq = message['propose_seq']
            self.transaction_hash = bytes.fromhex(message['transaction_hash'])
        except KeyError:
            self.propose_seq = 0
            self.transaction_hash = bytes()
    
    def __str__(self) -> str:
        peer = self.peers
        txs = bytes.hex(self.transaction_hash[:3])
        seq = self.propose_seq
        prev = bytes.hex(self.previous_ledger[:3])
        closed_at = self.close_time
        return f'Proposal(peers={peer}, txs={txs}, seq={seq}, prev={prev}, closed_at={closed_at})'
    
    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Proposal):
            return False
        other: Proposal = __value
        return self.transaction_hash == other.transaction_hash and \
               self.propose_seq == other.propose_seq and \
               self.previous_ledger == other.previous_ledger and \
               self.close_time == other.close_time
    
    def __hash__(self) -> int:
        return hash((self.transaction_hash, self.propose_seq, self.previous_ledger, self.close_time))


class Validation:

    def __init__(self, message) -> None:
        self.consensus_hash = bytes.fromhex(message["ConsensusHash"])
        self.flags = message["Flags"]
        self.ledger_hash = bytes.fromhex(message["LedgerHash"])
        self.ledger_sequence = message["LedgerSequence"]
        # Use pre-created frozensets and convert to regular set only once
        self.peers = set(_SIGNING_KEY_PEER_SETS.get(message["SigningPubKey"], set()))
        self.signing_time = message["SigningTime"]
    
    def __str__(self) -> str:
        peer = self.peers
        seq = self.ledger_sequence
        hash = bytes.hex(self.ledger_hash[:3])
        consensus = bytes.hex(self.consensus_hash[:3])
        signing_time = self.signing_time
        flags = self.flags
        return f'Validation(peers={peer}, seq={seq}, hash={hash}, consensus={consensus}, signed_at={signing_time}, flags={flags})'
    
    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Validation):
            return False
        other: Validation = __value
        return self.ledger_sequence == other.ledger_sequence and \
               self.ledger_hash == other.ledger_hash and \
               self.consensus_hash == other.consensus_hash and \
               self.signing_time == other.signing_time and \
               self.flags == other.flags
    
    def __hash__(self) -> int:
        return hash((self.ledger_sequence, self.ledger_hash, self.consensus_hash, self.signing_time, self.flags))
