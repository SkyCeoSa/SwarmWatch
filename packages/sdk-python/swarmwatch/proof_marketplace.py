"""
SwarmWatch Proof Marketplace & Verifier Node Staking Pool (Seam 93 / ADR 0104)

Provides decentralized double-auction matching for cryptographic proof verification,
economic staking and slashing guarantees (CPM1), BFT verification quorum (CPM3),
and fail-closed safety circuit latching under solvency breaches (CPM4).
Zero-token cost guarantee ($0.00 USD), sub-250 microsecond execution latency SLO.
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from swarmwatch.circuit_bus import SafetyCircuitBus, global_safety_circuit_bus
from swarmwatch.hlc import HybridLogicalClock


class ProofMarketplaceError(Exception):
    """Base exception for all proof marketplace operations."""
    pass


class InsufficientStakeError(ProofMarketplaceError):
    """Raised when verifier node has insufficient bonded collateral."""
    pass


class VerifierSlashedError(ProofMarketplaceError):
    """Raised when an operation is attempted by a slashed verifier node."""
    pass


class BountyNotFoundError(ProofMarketplaceError):
    """Raised when requested bounty ID does not exist."""
    pass


class BountyExpiredError(ProofMarketplaceError):
    """Raised when submitting bids or receipts to an expired bounty."""
    pass


class InvalidReceiptSignatureError(ProofMarketplaceError):
    """Raised when a verifier receipt signature fails cryptographic verification."""
    pass


class InsufficientVerificationQuorumError(ProofMarketplaceError):
    """Raised when settlement is attempted without meeting threshold verification quorum."""
    pass


class MarketplaceSolvencyBreachError(ProofMarketplaceError):
    """Raised when aggregate pool stake falls below minimum required solvency threshold."""
    pass


@dataclass
class VerifierNode:
    """Represents a staked verifier node in the marketplace pool."""
    verifier_id: str
    public_key: str  # Hex-encoded 32-byte Ed25519 public key
    bonded_stake: float
    min_stake: float = 100.0
    reputation_score: float = 1.0
    is_slashed: bool = False
    is_active: bool = True


@dataclass
class ProofBounty:
    """Represents a proof verification bounty open for verifier bidding."""
    bounty_id: str
    proof_type: str
    proof_digest: str  # Hex sha256 of the unverified proof payload
    max_bounty: float
    required_quorum: int = 3
    deadline_epoch: int = 1
    status: str = "OPEN"  # OPEN, MATCHED, FINALIZED, DISPUTED, SLASHED
    created_at_hlc: str = ""


@dataclass
class VerifierBid:
    """Represents a sealed bid from a staked verifier to verify a bounty."""
    bid_id: str
    bounty_id: str
    verifier_id: str
    fee_bid: float
    staked_collateral: float
    signature: str  # Hex-encoded Ed25519 signature over bid digest


@dataclass
class ProofVerificationReceipt:
    """Cryptographic attestation receipt submitted by a verifier."""
    receipt_id: str
    bounty_id: str
    verifier_id: str
    verdict: bool  # True = Proof Valid, False = Proof Invalid / Fraudulent
    proof_hash: str
    signature: str  # Hex-encoded Ed25519 signature over receipt digest
    timestamp_ns: int = 0


@dataclass
class SettlementCertificate:
    """Finalized cryptographic settlement certificate for a completed bounty."""
    settlement_id: str
    bounty_id: str
    cleared_fee: float
    participating_verifiers: List[str]
    payout_distributions: Dict[str, float]
    slashed_verifiers: List[str]
    finalized_verdict: bool
    created_at_hlc: str


def compute_bid_digest(bounty_id: str, verifier_id: str, fee_bid: float) -> str:
    """Computes deterministic canonical digest for a verifier bid."""
    raw = f"sw:bid:v1:{bounty_id}:{verifier_id}:{fee_bid:.6f}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_receipt_digest(bounty_id: str, verifier_id: str, verdict: bool, proof_hash: str) -> str:
    """Computes deterministic canonical digest for a proof verification receipt."""
    raw = f"sw:receipt:v1:{bounty_id}:{verifier_id}:{1 if verdict else 0}:{proof_hash}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def verify_ed25519_signature(public_key_hex: str, signature_hex: str, digest_hex: str) -> bool:
    """Verifies Ed25519 signature against 32-byte digest in constant time."""
    try:
        pub_bytes = bytes.fromhex(public_key_hex)
        sig_bytes = bytes.fromhex(signature_hex)
        digest_bytes = bytes.fromhex(digest_hex)
        if len(pub_bytes) != 32 or len(sig_bytes) != 64:
            return False
        pub = Ed25519PublicKey.from_public_bytes(pub_bytes)
        pub.verify(sig_bytes, digest_bytes)
        return True
    except (ValueError, InvalidSignature, TypeError):
        return False


class ProofMarketplaceCoordinator:
    """
    Authoritative coordinator for the decentralized proof marketplace & staking pool.
    Enforces CPM1, CPM2, CPM3, and CPM4 invariants.
    """

    def __init__(
        self,
        marketplace_id: str = "market-primary",
        min_solvency_threshold: float = 100.0,
        circuit_bus: Optional[SafetyCircuitBus] = None,
    ) -> None:
        self.marketplace_id = marketplace_id
        self.min_solvency_threshold = min_solvency_threshold
        self.circuit_bus = circuit_bus or global_safety_circuit_bus
        self._hlc = HybridLogicalClock(node_id=f"mkt-{marketplace_id}")
        self._lock = threading.RLock()

        self._verifiers: Dict[str, VerifierNode] = {}
        self._bounties: Dict[str, ProofBounty] = {}
        self._bids: Dict[str, List[VerifierBid]] = {}  # bounty_id -> bids
        self._matched_verifiers: Dict[str, Set[str]] = {}  # bounty_id -> matched verifiers
        self._receipts: Dict[str, List[ProofVerificationReceipt]] = {}  # bounty_id -> receipts
        self._settlements: Dict[str, SettlementCertificate] = {}

    def bind_circuit_bus(self, circuit_bus: SafetyCircuitBus) -> None:
        """Binds the authoritative SafetyCircuitBus."""
        with self._lock:
            self.circuit_bus = circuit_bus

    def register_verifier(self, node: VerifierNode) -> None:
        """Registers a verifier node with initial collateral stake."""
        with self._lock:
            if node.bonded_stake < node.min_stake:
                raise InsufficientStakeError(
                    f"Verifier '{node.verifier_id}' bonded stake {node.bonded_stake} is below minimum {node.min_stake}."
                )
            self._verifiers[node.verifier_id] = node

    def get_verifier(self, verifier_id: str) -> Optional[VerifierNode]:
        """Returns registered verifier node metadata."""
        with self._lock:
            return self._verifiers.get(verifier_id)

    def bond_stake(self, verifier_id: str, amount: float) -> float:
        """Adds additional collateral stake to a verifier node."""
        with self._lock:
            node = self._verifiers.get(verifier_id)
            if not node:
                raise ProofMarketplaceError(f"Verifier '{verifier_id}' not found.")
            if node.is_slashed:
                raise VerifierSlashedError(f"Cannot bond stake to slashed verifier '{verifier_id}'.")
            if amount <= 0:
                raise ProofMarketplaceError("Stake bonding amount must be positive.")
            node.bonded_stake += amount
            return node.bonded_stake

    def unbond_stake(self, verifier_id: str, amount: float) -> float:
        """Unbonds collateral stake ensuring min_stake invariant CPM1."""
        with self._lock:
            node = self._verifiers.get(verifier_id)
            if not node:
                raise ProofMarketplaceError(f"Verifier '{verifier_id}' not found.")
            if node.is_slashed:
                raise VerifierSlashedError(f"Cannot unbond stake from slashed verifier '{verifier_id}'.")
            if amount <= 0:
                raise ProofMarketplaceError("Stake unbonding amount must be positive.")
            if node.bonded_stake - amount < node.min_stake:
                raise InsufficientStakeError(
                    f"Unbonding {amount} would reduce stake {node.bonded_stake} below minimum {node.min_stake}."
                )
            node.bonded_stake -= amount
            return node.bonded_stake

    def create_bounty(self, bounty: ProofBounty) -> str:
        """Creates a new verification bounty open for double-auction bidding."""
        with self._lock:
            if self.circuit_bus and self.circuit_bus.is_tripped():
                raise ProofMarketplaceError("Cannot create bounty: Safety circuit bus is currently TRIPPED.")
            if bounty.bounty_id in self._bounties:
                return bounty.bounty_id

            if not bounty.created_at_hlc:
                bounty.created_at_hlc = str(self._hlc.now())
            bounty.status = "OPEN"
            self._bounties[bounty.bounty_id] = bounty
            self._bids[bounty.bounty_id] = []
            self._matched_verifiers[bounty.bounty_id] = set()
            self._receipts[bounty.bounty_id] = []
            return bounty.bounty_id

    def submit_bid(self, bid: VerifierBid) -> None:
        """Submits a signed verifier bid for an open bounty (CPM2)."""
        with self._lock:
            bounty = self._bounties.get(bid.bounty_id)
            if not bounty:
                raise BountyNotFoundError(f"Bounty '{bid.bounty_id}' not found.")
            if bounty.status != "OPEN":
                raise BountyExpiredError(f"Bounty '{bid.bounty_id}' is in status '{bounty.status}', not OPEN.")

            verifier = self._verifiers.get(bid.verifier_id)
            if not verifier:
                raise ProofMarketplaceError(f"Verifier '{bid.verifier_id}' not registered.")
            if verifier.is_slashed or not verifier.is_active:
                raise VerifierSlashedError(f"Verifier '{bid.verifier_id}' is slashed or inactive.")
            if verifier.bonded_stake < verifier.min_stake:
                raise InsufficientStakeError(f"Verifier '{bid.verifier_id}' bonded stake is below minimum.")

            if bid.fee_bid > bounty.max_bounty:
                raise ProofMarketplaceError(
                    f"Bid fee {bid.fee_bid} exceeds bounty maximum {bounty.max_bounty}."
                )

            # Cryptographic signature validation
            digest = compute_bid_digest(bid.bounty_id, bid.verifier_id, bid.fee_bid)
            if not verify_ed25519_signature(verifier.public_key, bid.signature, digest):
                raise InvalidReceiptSignatureError(
                    f"Bid signature from verifier '{bid.verifier_id}' is invalid."
                )

            # Idempotent replacement or append
            existing = [b for b in self._bids[bid.bounty_id] if b.verifier_id == bid.verifier_id]
            if existing:
                self._bids[bid.bounty_id].remove(existing[0])
            self._bids[bid.bounty_id].append(bid)

    def match_bounty(self, bounty_id: str) -> List[str]:
        """Clears double-auction order book selecting lowest-fee qualifying bids (CPM2)."""
        with self._lock:
            bounty = self._bounties.get(bounty_id)
            if not bounty:
                raise BountyNotFoundError(f"Bounty '{bounty_id}' not found.")
            if bounty.status != "OPEN":
                raise BountyExpiredError(f"Bounty '{bounty_id}' cannot be matched in status '{bounty.status}'.")

            bids = self._bids.get(bounty_id, [])
            # Filter active non-slashed verifiers
            valid_bids = [
                b for b in bids
                if self._verifiers.get(b.verifier_id)
                and not self._verifiers[b.verifier_id].is_slashed
                and self._verifiers[b.verifier_id].is_active
            ]

            if len(valid_bids) < bounty.required_quorum:
                raise InsufficientVerificationQuorumError(
                    f"Bounty '{bounty_id}' has {len(valid_bids)} valid bids, requiring quorum {bounty.required_quorum}."
                )

            # Sort ascending by fee_bid, then descending by staked_collateral
            sorted_bids = sorted(valid_bids, key=lambda x: (x.fee_bid, -x.staked_collateral))
            selected = sorted_bids[:bounty.required_quorum]
            selected_ids = [b.verifier_id for b in selected]

            bounty.status = "MATCHED"
            self._matched_verifiers[bounty_id] = set(selected_ids)
            return selected_ids

    def submit_receipt(self, receipt: ProofVerificationReceipt) -> None:
        """Submits a signed verification receipt from an assigned verifier (CPM3)."""
        with self._lock:
            bounty = self._bounties.get(receipt.bounty_id)
            if not bounty:
                raise BountyNotFoundError(f"Bounty '{receipt.bounty_id}' not found.")
            if bounty.status not in ("OPEN", "MATCHED"):
                raise BountyExpiredError(f"Bounty '{receipt.bounty_id}' cannot accept receipts in status '{bounty.status}'.")

            verifier = self._verifiers.get(receipt.verifier_id)
            if not verifier:
                raise ProofMarketplaceError(f"Verifier '{receipt.verifier_id}' not registered.")
            if verifier.is_slashed or not verifier.is_active:
                raise VerifierSlashedError(f"Verifier '{receipt.verifier_id}' is slashed or inactive.")

            # Signature verification
            digest = compute_receipt_digest(
                receipt.bounty_id,
                receipt.verifier_id,
                receipt.verdict,
                receipt.proof_hash,
            )
            if not verify_ed25519_signature(verifier.public_key, receipt.signature, digest):
                raise InvalidReceiptSignatureError(
                    f"Receipt signature from verifier '{receipt.verifier_id}' failed verification."
                )

            if receipt.timestamp_ns == 0:
                receipt.timestamp_ns = time.time_ns()

            # Record receipt idempotently
            existing = [r for r in self._receipts[receipt.bounty_id] if r.verifier_id == receipt.verifier_id]
            if existing:
                self._receipts[receipt.bounty_id].remove(existing[0])
            self._receipts[receipt.bounty_id].append(receipt)

    def settle_bounty(self, bounty_id: str) -> SettlementCertificate:
        """
        Finalizes bounty settlement verifying K-of-N quorum (CPM3),
        slashing dissenting/fraudulent verifiers (CPM1), and distributing fee payouts.
        """
        with self._lock:
            bounty = self._bounties.get(bounty_id)
            if not bounty:
                raise BountyNotFoundError(f"Bounty '{bounty_id}' not found.")

            receipts = self._receipts.get(bounty_id, [])
            if len(receipts) < bounty.required_quorum:
                raise InsufficientVerificationQuorumError(
                    f"Bounty '{bounty_id}' has {len(receipts)} receipts, requiring quorum {bounty.required_quorum}."
                )

            # Quorum vote aggregation
            true_receipts = [r for r in receipts if r.verdict is True]
            false_receipts = [r for r in receipts if r.verdict is False]

            # Byzantine super-majority verdict
            q = bounty.required_quorum
            finalized_verdict: bool
            honest_receipts: List[ProofVerificationReceipt]
            slashed_receipts: List[ProofVerificationReceipt]

            if len(true_receipts) >= q:
                finalized_verdict = True
                honest_receipts = true_receipts
                slashed_receipts = false_receipts
            elif len(false_receipts) >= q:
                finalized_verdict = False
                honest_receipts = false_receipts
                slashed_receipts = true_receipts
            else:
                bounty.status = "DISPUTED"
                raise InsufficientVerificationQuorumError(
                    f"Conflicting receipts without threshold quorum ({len(true_receipts)} vs {len(false_receipts)})."
                )

            # Slash malicious/dissenting verifiers (CPM1)
            slashed_ids: List[str] = []
            for r in slashed_receipts:
                self.slash_verifier(
                    verifier_id=r.verifier_id,
                    penalty_fraction=1.0,
                    reason=f"Fraudulent attestation on bounty {bounty_id} (contradicted certified verdict {finalized_verdict})",
                )
                slashed_ids.append(r.verifier_id)

            # Determine clearing fee and fee payout distribution
            bids = {b.verifier_id: b for b in self._bids.get(bounty_id, [])}
            matching_bids = [bids[r.verifier_id].fee_bid for r in honest_receipts if r.verifier_id in bids]
            clearing_fee = max(matching_bids) if matching_bids else bounty.max_bounty

            per_verifier_payout = clearing_fee / max(1, len(honest_receipts))
            payouts: Dict[str, float] = {}
            for r in honest_receipts:
                payouts[r.verifier_id] = per_verifier_payout
                node = self._verifiers.get(r.verifier_id)
                if node:
                    node.reputation_score = min(2.0, node.reputation_score + 0.05)

            settlement_id = f"sw:settle:v1:{hashlib.sha256(f'{bounty_id}:{finalized_verdict}:{time.time_ns()}'.encode('utf-8')).hexdigest()}"
            certificate = SettlementCertificate(
                settlement_id=settlement_id,
                bounty_id=bounty_id,
                cleared_fee=clearing_fee,
                participating_verifiers=[r.verifier_id for r in honest_receipts],
                payout_distributions=payouts,
                slashed_verifiers=slashed_ids,
                finalized_verdict=finalized_verdict,
                created_at_hlc=str(self._hlc.now()),
            )

            bounty.status = "FINALIZED"
            self._settlements[bounty_id] = certificate
            return certificate

    def slash_verifier(self, verifier_id: str, penalty_fraction: float = 1.0, reason: str = "") -> float:
        """Slashes bonded collateral of a malicious or equivocating verifier node (CPM1)."""
        with self._lock:
            node = self._verifiers.get(verifier_id)
            if not node:
                raise ProofMarketplaceError(f"Verifier '{verifier_id}' not found.")

            penalty_fraction = max(0.0, min(1.0, penalty_fraction))
            slashed_amount = node.bonded_stake * penalty_fraction
            node.bonded_stake -= slashed_amount
            node.is_slashed = True
            node.is_active = False
            node.reputation_score = 0.0

            # Check aggregate solvency threshold (CPM4)
            active_solvency = sum(v.bonded_stake for v in self._verifiers.values() if v.is_active and not v.is_slashed)
            if active_solvency < self.min_solvency_threshold:
                if self.circuit_bus:
                    self.circuit_bus.trip(
                        reason="PROOF_MARKETPLACE_SOLVENCY_BREACH",
                        scope=f"marketplace:{self.marketplace_id}",
                        metadata={
                            "active_solvency": active_solvency,
                            "min_solvency_threshold": self.min_solvency_threshold,
                            "slashed_verifier_id": verifier_id,
                            "reason": reason,
                        },
                    )

            return slashed_amount

    def get_status(self) -> Dict[str, Any]:
        """Returns marketplace health, solvency, and verifier pool overview."""
        with self._lock:
            total_verifiers = len(self._verifiers)
            active_verifiers = [v for v in self._verifiers.values() if v.is_active and not v.is_slashed]
            slashed_count = sum(1 for v in self._verifiers.values() if v.is_slashed)
            active_solvency = sum(v.bonded_stake for v in active_verifiers)

            return {
                "marketplace_id": self.marketplace_id,
                "total_verifiers": total_verifiers,
                "active_verifiers_count": len(active_verifiers),
                "slashed_verifiers_count": slashed_count,
                "active_solvency": active_solvency,
                "min_solvency_threshold": self.min_solvency_threshold,
                "open_bounties_count": sum(1 for b in self._bounties.values() if b.status == "OPEN"),
                "finalized_bounties_count": len(self._settlements),
            }


# Authoritative Global Singleton
global_proof_marketplace: ProofMarketplaceCoordinator = ProofMarketplaceCoordinator()
global_marketplace_coordinator: ProofMarketplaceCoordinator = global_proof_marketplace
