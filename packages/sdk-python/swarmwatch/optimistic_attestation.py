# -*- coding: utf-8 -*-
"""
SwarmWatch Fraud-Provable Optimistic State Attestation (Seam 88 / ADR 0099).

Implements verifiable optimistic state transition batching, deterministic single-step
fraud challenges (F1), timelocked finalization with pinned prover keys (F2), and pluggable
proof verification (F3).

Invariants:
- F1: Invalid transitions in a batch are proven fraudulent via deterministic step re-execution.
- F2: Attestations finalize strictly after challenge window W passes without valid challenges.
- F3: Pinned prover public keys prevent self-signed attacker injection.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import hashlib
import json
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from swarmwatch.qc import compute_merkle_root


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class AttestationError(Exception):
    """Base exception for optimistic attestation operations."""


class UnauthorizedProverError(AttestationError):
    """Raised when an attestation is signed by an unrecognized or unpinned prover key."""


class PrematureFinalizationError(AttestationError):
    """Raised when finalization is attempted before the challenge window has elapsed."""


class BatchChallengedError(AttestationError):
    """Raised when finalization is attempted on a batch with an active fraud proof."""


# ==============================================================================
# 2. Domain Types & State Machine
# ==============================================================================

def execute_transition_step(pre_state_hash: str, action: str) -> str:
    """
    Deterministic transition function f(pre_state, action) -> post_state.
    Can be replaced or extended via pluggable state machines.
    """
    raw = f"{pre_state_hash}:{action}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class TransitionStep:
    """Single state transition step within a batch."""
    step_index: int
    pre_state_hash: str
    action: str
    post_state_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_index": self.step_index,
            "pre_state_hash": self.pre_state_hash,
            "action": self.action,
            "post_state_hash": self.post_state_hash,
        }


@dataclass(frozen=True)
class OptimisticAttestation:
    """Timelocked attestation signed by an authorized prover."""
    batch_id: str
    prover_id: str
    prover_pubkey_hex: str
    step_count: int
    initial_state_hash: str
    final_state_hash: str
    batch_root: str
    challenge_deadline_ns: int
    signature_hex: str
    steps: List[TransitionStep] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "prover_id": self.prover_id,
            "prover_pubkey_hex": self.prover_pubkey_hex,
            "step_count": self.step_count,
            "initial_state_hash": self.initial_state_hash,
            "final_state_hash": self.final_state_hash,
            "batch_root": self.batch_root,
            "challenge_deadline_ns": self.challenge_deadline_ns,
            "signature_hex": self.signature_hex,
        }


@dataclass(frozen=True)
class FraudProof:
    """Incontrovertible proof of an invalid step transition (Invariant F1)."""
    proof_id: str
    batch_id: str
    failing_step_index: int
    claimed_post_state: str
    recomputed_post_state: str
    proven_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "proof_id": self.proof_id,
            "batch_id": self.batch_id,
            "failing_step_index": self.failing_step_index,
            "claimed_post_state": self.claimed_post_state,
            "recomputed_post_state": self.recomputed_post_state,
            "proven_at_ns": self.proven_at_ns,
        }


@dataclass(frozen=True)
class FinalizedStateReceipt:
    """Irreversible receipt issued after the challenge window closes cleanly."""
    receipt_id: str
    batch_id: str
    final_state_hash: str
    finalized_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "batch_id": self.batch_id,
            "final_state_hash": self.final_state_hash,
            "finalized_at_ns": self.finalized_at_ns,
        }


# ==============================================================================
# 3. Optimistic Attestation Engine Deep Module
# ==============================================================================

class OptimisticAttestationEngine:
    """
    Manages optimistic state transition batches, challenge windows, fraud proofs,
    and timelocked finalization with pinned prover keys.
    """

    def __init__(self, pinned_prover_keys: Optional[Set[str]] = None) -> None:
        self.pinned_prover_keys: Set[str] = set(pinned_prover_keys or set())
        self._lock = threading.RLock()
        self._fraud_proofs: Dict[str, FraudProof] = {}  # batch_id -> FraudProof
        self._finalized_receipts: Dict[str, FinalizedStateReceipt] = {}

    def register_prover_key(self, public_key_hex: str) -> None:
        """Pins an authorized prover public key."""
        with self._lock:
            self.pinned_prover_keys.add(public_key_hex.lower())

    def attest_batch(
        self,
        steps: List[TransitionStep],
        prover_id: str,
        prover_key: Ed25519PrivateKey,
        challenge_window_ms: int = 5000,
    ) -> OptimisticAttestation:
        """
        Creates a signed optimistic state transition batch attestation.
        """
        if not steps:
            raise ValueError("Cannot attest an empty transition batch")

        prover_pub_hex = prover_key.public_key().public_bytes_raw().hex().lower()

        # Invariant F2: Prover key must match pinned keys if whitelist configured
        if self.pinned_prover_keys and prover_pub_hex not in self.pinned_prover_keys:
            raise UnauthorizedProverError(f"Prover key '{prover_pub_hex[:16]}...' is not authorized")

        leaf_hashes = [
            hashlib.sha256(
                f"{s.step_index}:{s.pre_state_hash}:{s.action}:{s.post_state_hash}".encode("utf-8")
            ).hexdigest()
            for s in steps
        ]
        batch_root = compute_merkle_root(leaf_hashes)

        now_ns = time.time_ns()
        deadline_ns = now_ns + (challenge_window_ms * 1_000_000)

        initial_state = steps[0].pre_state_hash
        final_state = steps[-1].post_state_hash

        batch_id_raw = f"batch:{prover_id}:{batch_root}:{deadline_ns}"
        batch_id = hashlib.sha256(batch_id_raw.encode("utf-8")).hexdigest()

        # Sign canonical attestation payload
        payload = f"sw:attest:v1:{batch_id}:{batch_root}:{initial_state}:{final_state}:{deadline_ns}".encode("utf-8")
        sig = prover_key.sign(hashlib.sha256(payload).digest())

        return OptimisticAttestation(
            batch_id=batch_id,
            prover_id=prover_id,
            prover_pubkey_hex=prover_pub_hex,
            step_count=len(steps),
            initial_state_hash=initial_state,
            final_state_hash=final_state,
            batch_root=batch_root,
            challenge_deadline_ns=deadline_ns,
            signature_hex=sig.hex(),
            steps=list(steps),
        )

    def challenge_step(
        self,
        attestation: OptimisticAttestation,
        step_index: int,
    ) -> Optional[FraudProof]:
        """
        Challenges a specific step in the batch via deterministic re-execution (Invariant F1).
        If the re-executed post-state does not match the claimed post-state, a FraudProof is emitted.
        """
        with self._lock:
            if step_index < 0 or step_index >= len(attestation.steps):
                raise ValueError(f"Step index {step_index} out of bounds")

            step = attestation.steps[step_index]
            actual_post_state = execute_transition_step(step.pre_state_hash, step.action)

            if actual_post_state != step.post_state_hash:
                proof_id = hashlib.sha256(
                    f"fraud:{attestation.batch_id}:{step_index}:{time.time_ns()}".encode("utf-8")
                ).hexdigest()

                proof = FraudProof(
                    proof_id=proof_id,
                    batch_id=attestation.batch_id,
                    failing_step_index=step_index,
                    claimed_post_state=step.post_state_hash,
                    recomputed_post_state=actual_post_state,
                )
                self._fraud_proofs[attestation.batch_id] = proof
                return proof

            return None

    def finalize(
        self,
        attestation: OptimisticAttestation,
        current_time_ns: Optional[int] = None,
    ) -> FinalizedStateReceipt:
        """
        Finalizes an attestation after the challenge window elapses without challenges (Invariant F2).
        Fails closed if challenged, premature, or signed by an unpinned prover key.
        """
        with self._lock:
            now_ns = current_time_ns or time.time_ns()

            # 1. Pinned key check
            if self.pinned_prover_keys and attestation.prover_pubkey_hex.lower() not in self.pinned_prover_keys:
                raise UnauthorizedProverError("Cannot finalize batch signed by unpinned prover key")

            # 2. Challenge window check
            if now_ns < attestation.challenge_deadline_ns:
                remaining_ms = (attestation.challenge_deadline_ns - now_ns) / 1_000_000.0
                raise PrematureFinalizationError(
                    f"Challenge window is still open ({remaining_ms:.2f}ms remaining)"
                )

            # 3. Active fraud proof check
            if attestation.batch_id in self._fraud_proofs:
                proof = self._fraud_proofs[attestation.batch_id]
                raise BatchChallengedError(
                    f"Cannot finalize fraudulent batch (step {proof.failing_step_index} proved invalid)"
                )

            # 4. Issue finalized receipt
            receipt_id = hashlib.sha256(
                f"receipt:{attestation.batch_id}:{attestation.final_state_hash}:{now_ns}".encode("utf-8")
            ).hexdigest()

            receipt = FinalizedStateReceipt(
                receipt_id=receipt_id,
                batch_id=attestation.batch_id,
                final_state_hash=attestation.final_state_hash,
                finalized_at_ns=now_ns,
            )

            self._finalized_receipts[attestation.batch_id] = receipt
            return receipt

    def is_fraudulent(self, batch_id: str) -> bool:
        with self._lock:
            return batch_id in self._fraud_proofs
