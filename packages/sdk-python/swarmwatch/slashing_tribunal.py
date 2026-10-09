"""
SwarmWatch Real-Time Epistemic Dispute Resolution & Dynamic Slashing Escalation Tribunal (Seam 97 / ADR 0108)

Provides multi-tiered dispute escalation (TRB1), severity-graded dynamic slashing (TRB2),
super-majority arbiter quorum finality (TRB3), and fail-closed safety circuit latching under systemic surges (TRB4).
Zero-token cost guarantee ($0.00 USD), sub-250 microsecond execution latency SLO.
"""

from __future__ import annotations

import hashlib
import hmac
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from swarmwatch.circuit_bus import SafetyCircuitBus, global_safety_circuit_bus
from swarmwatch.hlc import HybridLogicalClock
from swarmwatch.byzantine_oracle import verify_bft_signature


class TribunalError(Exception):
    """Base exception for slashing tribunal and dispute escalation operations."""
    pass


class DisputeNotFoundError(TribunalError):
    """Raised when a referenced dispute claim does not exist."""
    pass


class InvalidDisputeTierError(TribunalError):
    """Raised when attempting an invalid dispute tier transition (TRB1)."""
    pass


class InsufficientArbiterQuorumError(TribunalError):
    """Raised when an appellate ruling lacks the super-majority signature quorum (TRB3)."""
    pass


class TribunalArbiterQuarantinedError(TribunalError):
    """Raised when an inactive or quarantined arbiter attempts to cast a vote."""
    pass


class TribunalCircuitTrippedError(TribunalError):
    """Raised when tribunal operations are blocked due to a tripped safety circuit bus."""
    pass


@dataclass
class DisputeClaim:
    """Formal claim filed in the multi-tiered dispute escalation pipeline."""
    dispute_id: str
    claimant_id: str
    defendant_id: str
    severity: str  # LOW, MEDIUM, CRITICAL
    tier: int = 1  # 1: Local, 2: Committee, 3: Tribunal Appellate
    dispute_bond: float = 10.0
    status: str = "OPEN"  # OPEN, ESCALATED, RESOLVED_CLAIMANT, RESOLVED_DEFENDANT, DISMISSED
    evidence_digest: str = ""
    created_at_hlc: str = ""


@dataclass
class TribunalArbiter:
    """Registered arbiter eligible to participate in tribunal appellate quorums."""
    arbiter_id: str
    public_key: str
    bonded_stake: float
    is_active: bool = True


@dataclass
class TribunalVerdictReceipt:
    """Irreversible on-ledger appellate ruling attesting final dispute adjudication."""
    verdict_id: str
    dispute_id: str
    final_tier: int
    verdict: str  # RESOLVED_CLAIMANT, RESOLVED_DEFENDANT, DISMISSED
    slashed_amount: float
    recipient_id: str
    arbiter_signatures: Dict[str, str]
    finalized_at_hlc: str


@dataclass
class TribunalEscalationRecord:
    """Record of formal tier escalation accompanied by additional bond deposit."""
    dispute_id: str
    from_tier: int
    to_tier: int
    added_bond: float
    escalated_at_hlc: str


def compute_verdict_digest(
    dispute_id: str,
    tier: int,
    verdict: str,
    slashed_amount: float,
    recipient_id: str,
    hlc_ts: str,
) -> str:
    """Computes canonical digest over tribunal appellate verdict."""
    raw = f"sw:trb:verdict:v1:{dispute_id}:{tier}:{verdict}:{slashed_amount:.6f}:{recipient_id}:{hlc_ts}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def verify_arbiter_signature(pubkey: str, digest: str, signature: str) -> bool:
    """Verifies cryptographic Ed25519 arbiter signature. Zero HMAC/test-vector fallback."""
    if not signature or not pubkey or not digest:
        return False
    return verify_bft_signature(pubkey, digest, signature)


class SlashingEscalationTribunal:
    """
    Authoritative coordinator for real-time epistemic dispute resolution and
    dynamic slashing escalation (ADR 0108).
    Guarantees TRB1, TRB2, TRB3, and TRB4 invariants.
    """

    def __init__(
        self,
        tribunal_id: str = "slashing-tribunal-primary",
        surge_threshold: int = 5,  # Max concurrent critical disputes before safety trip (TRB4)
        circuit_bus: Optional[SafetyCircuitBus] = None,
    ) -> None:
        self.tribunal_id = tribunal_id
        self.surge_threshold = surge_threshold
        self.circuit_bus = circuit_bus or global_safety_circuit_bus
        self._hlc = HybridLogicalClock(node_id=f"trb-{tribunal_id}")
        self._lock = threading.RLock()

        self._arbiters: Dict[str, TribunalArbiter] = {}
        self._disputes: Dict[str, DisputeClaim] = {}
        self._verdicts: Dict[str, TribunalVerdictReceipt] = {}
        self._escalations: Dict[str, List[TribunalEscalationRecord]] = {}

    def bind_circuit_bus(self, circuit_bus: SafetyCircuitBus) -> None:
        """Binds authoritative safety circuit bus."""
        with self._lock:
            self.circuit_bus = circuit_bus

    def register_arbiter(self, arbiter: TribunalArbiter) -> None:
        """Registers an active arbiter into the tribunal roster."""
        with self._lock:
            if len(arbiter.public_key) != 64 or any(c not in "0123456789abcdefABCDEF" for c in arbiter.public_key):
                raise TribunalError("Arbiter public_key must be a valid 64-character hex Ed25519 key.")
            self._arbiters[arbiter.arbiter_id] = arbiter

    def file_dispute(
        self,
        claimant_id: str,
        defendant_id: str,
        severity: str = "MEDIUM",
        evidence_digest: str = "",
        dispute_bond: float = 10.0,
        dispute_id: Optional[str] = None,
    ) -> DisputeClaim:
        """
        Files a new dispute claim at Tier 1 (TRB1).
        Monitors critical surge count against safety threshold (TRB4).
        """
        with self._lock:
            if self.circuit_bus and self.circuit_bus.is_tripped():
                raise TribunalCircuitTrippedError("Cannot file dispute: Safety circuit bus is TRIPPED.")

            if dispute_bond <= 0:
                raise TribunalError("Dispute challenge bond must be strictly positive.")

            did = dispute_id or f"dsp-{hashlib.sha256(f'{claimant_id}:{defendant_id}:{time.time_ns()}'.encode('utf-8')).hexdigest()[:16]}"
            claim = DisputeClaim(
                dispute_id=did,
                claimant_id=claimant_id,
                defendant_id=defendant_id,
                severity=severity,
                tier=1,
                dispute_bond=dispute_bond,
                status="OPEN",
                evidence_digest=evidence_digest or ("0" * 64),
                created_at_hlc=str(self._hlc.now()),
            )
            self._disputes[did] = claim
            self._escalations[did] = []

            # Check critical dispute surge (TRB4)
            critical_open = sum(
                1 for d in self._disputes.values()
                if d.status in ("OPEN", "ESCALATED") and d.severity == "CRITICAL"
            )
            if critical_open >= self.surge_threshold:
                if self.circuit_bus:
                    self.circuit_bus.trip(
                        reason="TRIBUNAL_SYSTEMIC_DISPUTE_SURGE",
                        scope=f"tribunal:{self.tribunal_id}",
                        metadata={
                            "critical_open_disputes": critical_open,
                            "surge_threshold": self.surge_threshold,
                        },
                    )

            return claim

    def escalate_dispute(
        self,
        dispute_id: str,
        additional_bond: float,
    ) -> DisputeClaim:
        """
        Escalates dispute monotonically: Tier 1 -> Tier 2 -> Tier 3 (TRB1).
        Requires increasing challenge bond.
        """
        with self._lock:
            if self.circuit_bus and self.circuit_bus.is_tripped():
                raise TribunalCircuitTrippedError("Cannot escalate dispute: Safety circuit bus is TRIPPED.")

            claim = self._disputes.get(dispute_id)
            if not claim:
                raise DisputeNotFoundError(f"Dispute '{dispute_id}' not found.")

            if claim.status not in ("OPEN", "ESCALATED"):
                raise TribunalError(f"Dispute '{dispute_id}' is already finalized (status={claim.status}).")

            if claim.tier >= 3:
                raise InvalidDisputeTierError("Dispute is already at highest tier (Tier 3 Tribunal Appellate).")

            required_bond = claim.dispute_bond  # Must at least match or increase previous bond
            if additional_bond < required_bond:
                raise TribunalError(
                    f"Additional bond {additional_bond:.2f} is insufficient (requires at least {required_bond:.2f})."
                )

            old_tier = claim.tier
            claim.tier += 1
            claim.dispute_bond += additional_bond
            claim.status = "ESCALATED"

            esc_rec = TribunalEscalationRecord(
                dispute_id=dispute_id,
                from_tier=old_tier,
                to_tier=claim.tier,
                added_bond=additional_bond,
                escalated_at_hlc=str(self._hlc.now()),
            )
            self._escalations[dispute_id].append(esc_rec)
            return claim

    def adjudicate_dispute(
        self,
        dispute_id: str,
        ruling: str,  # RESOLVED_CLAIMANT, RESOLVED_DEFENDANT, DISMISSED
        defendant_collateral: float,
        arbiter_votes: Dict[str, str],  # arbiter_id -> signature
        hlc_timestamp: Optional[str] = None,
    ) -> TribunalVerdictReceipt:
        """
        Synthesizes appellate ruling with super-majority quorum and dynamic slashing (TRB2, TRB3).
        """
        with self._lock:
            claim = self._disputes.get(dispute_id)
            if not claim:
                raise DisputeNotFoundError(f"Dispute '{dispute_id}' not found.")

            if claim.status not in ("OPEN", "ESCALATED"):
                raise TribunalError(f"Dispute '{dispute_id}' is already finalized.")

            # 1. Super-Majority Arbiter Quorum (TRB3)
            active_arbiters = [a for a in self._arbiters.values() if a.is_active]
            k = len(active_arbiters)
            required_quorum = (2 * k // 3) + 1 if k > 0 else 1

            valid_signatures: Dict[str, str] = {}
            hlc_ts = hlc_timestamp or str(self._hlc.now())

            # 2. Dynamic Slashing Penalty Calculation (TRB2)
            if ruling == "RESOLVED_CLAIMANT":
                if claim.severity == "LOW":
                    slash_rate = 0.05
                elif claim.severity == "MEDIUM":
                    slash_rate = 0.25
                else:  # CRITICAL
                    slash_rate = 1.00
                slashed_amount = round(defendant_collateral * slash_rate, 6)
                recipient_id = claim.claimant_id
            else:
                slashed_amount = 0.0
                recipient_id = claim.defendant_id

            digest = compute_verdict_digest(
                dispute_id=dispute_id,
                tier=claim.tier,
                verdict=ruling,
                slashed_amount=slashed_amount,
                recipient_id=recipient_id,
                hlc_ts=hlc_ts,
            )

            for arb_id, sig in arbiter_votes.items():
                arb = self._arbiters.get(arb_id)
                if not arb or not arb.is_active:
                    continue
                if verify_arbiter_signature(arb.public_key, digest, sig):
                    valid_signatures[arb_id] = sig

            if len(valid_signatures) < required_quorum:
                raise InsufficientArbiterQuorumError(
                    f"Insufficient arbiter quorum: received {len(valid_signatures)}, required {required_quorum} (K={k})."
                )

            # Finalize dispute state
            claim.status = ruling
            verdict_id = f"vrd-{hashlib.sha256(f'{dispute_id}:{ruling}:{hlc_ts}'.encode('utf-8')).hexdigest()[:16]}"
            receipt = TribunalVerdictReceipt(
                verdict_id=verdict_id,
                dispute_id=dispute_id,
                final_tier=claim.tier,
                verdict=ruling,
                slashed_amount=slashed_amount,
                recipient_id=recipient_id,
                arbiter_signatures=valid_signatures,
                finalized_at_hlc=hlc_ts,
            )
            self._verdicts[verdict_id] = receipt
            return receipt

    def get_dispute(self, dispute_id: str) -> Optional[DisputeClaim]:
        """Returns dispute claim by ID."""
        with self._lock:
            return self._disputes.get(dispute_id)

    def get_status(self) -> Dict[str, Any]:
        """Returns point-in-time metrics for slashing tribunal."""
        with self._lock:
            open_disputes = sum(1 for d in self._disputes.values() if d.status in ("OPEN", "ESCALATED"))
            resolved_disputes = sum(1 for d in self._disputes.values() if d.status not in ("OPEN", "ESCALATED"))
            total_slashed = sum(v.slashed_amount for v in self._verdicts.values())
            active_arbiters = sum(1 for a in self._arbiters.values() if a.is_active)
            return {
                "tribunal_id": self.tribunal_id,
                "total_disputes_count": len(self._disputes),
                "open_disputes_count": open_disputes,
                "resolved_disputes_count": resolved_disputes,
                "active_arbiters_count": active_arbiters,
                "total_verdicts_count": len(self._verdicts),
                "total_slashed_collateral": round(total_slashed, 6),
                "circuit_tripped": self.circuit_bus.is_tripped() if self.circuit_bus else False,
            }


# Authoritative Global Singleton
global_slashing_tribunal: SlashingEscalationTribunal = SlashingEscalationTribunal()
global_tribunal: SlashingEscalationTribunal = global_slashing_tribunal
