# -*- coding: utf-8 -*-
"""
Fenced Effect Ledger & Monotonic Idempotency Barrier (Seam 52 / ADR 0063).
Guarantees that distributed multi-agent side-effects and external tool interventions
execute and commit at most once under a monotonic fencing epoch, preventing stale leaders
and duplicate actions from executing during retries, failovers, or partitions.
"""

from __future__ import annotations

import secrets
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from swarmwatch.canonical import CanonicalCodec, CanonicalDomainTag


class ClaimStatus:
    """Discrete lifecycle states of an effect claim."""
    CLAIMED = "CLAIMED"
    AUTHORIZED = "AUTHORIZED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"
    EXPIRED = "EXPIRED"


class EffectReceipt:
    """
    Cryptographically verifiable receipt certifying that an external side-effect
    was executed and committed exactly once under an authoritative epoch.
    """

    def __init__(
        self,
        receipt_id: str,
        effect_key: str,
        idempotency_key: str,
        epoch: int,
        tenant_id: str,
        committed_at: float,
        result_digest: str,
        receipt_hash: Optional[str] = None,
    ) -> None:
        self.receipt_id = receipt_id
        self.effect_key = effect_key
        self.idempotency_key = idempotency_key
        self.epoch = epoch
        self.tenant_id = tenant_id
        self.committed_at = committed_at
        self.result_digest = result_digest
        self.receipt_hash = receipt_hash or self.compute_receipt_hash()

    def compute_receipt_hash(self) -> str:
        payload = {
            "committed_at": round(self.committed_at, 6),
            "effect_key": self.effect_key,
            "epoch": self.epoch,
            "idempotency_key": self.idempotency_key,
            "receipt_id": self.receipt_id,
            "result_digest": self.result_digest,
            "tenant_id": self.tenant_id,
        }
        return CanonicalCodec.hash(payload, CanonicalDomainTag.EFFECT)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "effect_key": self.effect_key,
            "idempotency_key": self.idempotency_key,
            "epoch": self.epoch,
            "tenant_id": self.tenant_id,
            "committed_at": self.committed_at,
            "result_digest": self.result_digest,
            "receipt_hash": self.receipt_hash,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EffectReceipt:
        return cls(
            receipt_id=str(data["receipt_id"]),
            effect_key=str(data["effect_key"]),
            idempotency_key=str(data["idempotency_key"]),
            epoch=int(data["epoch"]),
            tenant_id=str(data["tenant_id"]),
            committed_at=float(data["committed_at"]),
            result_digest=str(data["result_digest"]),
            receipt_hash=str(data.get("receipt_hash", "")),
        )


class EffectRecord:
    """Internal durable record of an effect claim in the ledger."""

    def __init__(
        self,
        effect_key: str,
        claim_id: str,
        idempotency_key: str,
        epoch: int,
        tenant_id: str = "default",
        action: str = "external_tool",
        status: str = ClaimStatus.CLAIMED,
        permit_hash: Optional[str] = None,
        claimed_at: Optional[float] = None,
        expires_at: Optional[float] = None,
        execution_result: Optional[Any] = None,
        receipt: Optional[EffectReceipt] = None,
    ) -> None:
        self.effect_key = effect_key
        self.claim_id = claim_id
        self.idempotency_key = idempotency_key
        self.epoch = epoch
        self.tenant_id = tenant_id
        self.action = action
        self.status = status
        self.permit_hash = permit_hash
        now = time.time()
        self.claimed_at = claimed_at if claimed_at is not None else now
        self.expires_at = expires_at if expires_at is not None else (self.claimed_at + 5.0)
        self.execution_result = execution_result
        self.receipt = receipt

    def is_expired(self, current_time: Optional[float] = None) -> bool:
        now = current_time if current_time is not None else time.time()
        return now > self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "effect_key": self.effect_key,
            "claim_id": self.claim_id,
            "idempotency_key": self.idempotency_key,
            "epoch": self.epoch,
            "tenant_id": self.tenant_id,
            "action": self.action,
            "status": self.status,
            "permit_hash": self.permit_hash,
            "claimed_at": self.claimed_at,
            "expires_at": self.expires_at,
            "execution_result": self.execution_result,
            "receipt": self.receipt.to_dict() if self.receipt else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EffectRecord:
        receipt = EffectReceipt.from_dict(data["receipt"]) if data.get("receipt") else None
        return cls(
            effect_key=str(data["effect_key"]),
            claim_id=str(data["claim_id"]),
            idempotency_key=str(data["idempotency_key"]),
            epoch=int(data["epoch"]),
            tenant_id=str(data.get("tenant_id", "default")),
            action=str(data.get("action", "external_tool")),
            status=str(data.get("status", ClaimStatus.CLAIMED)),
            permit_hash=str(data["permit_hash"]) if data.get("permit_hash") else None,
            claimed_at=float(data["claimed_at"]) if data.get("claimed_at") is not None else None,
            expires_at=float(data["expires_at"]) if data.get("expires_at") is not None else None,
            execution_result=data.get("execution_result"),
            receipt=receipt,
        )


class ClaimResult:
    """Result returned by EffectLedger.claim()."""

    def __init__(
        self,
        status: str,
        claim_id: Optional[str] = None,
        epoch: int = 1,
        record: Optional[EffectRecord] = None,
        reason: Optional[str] = None,
        receipt: Optional[EffectReceipt] = None,
    ) -> None:
        self.status = status
        self.claim_id = claim_id
        self.epoch = epoch
        self.record = record
        self.reason = reason
        self.receipt = receipt

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "claim_id": self.claim_id,
            "epoch": self.epoch,
            "record": self.record.to_dict() if self.record else None,
            "reason": self.reason,
            "receipt": self.receipt.to_dict() if self.receipt else None,
        }


class EffectLedger:
    """
    Linearizable, thread-safe Fenced Effect Ledger with Compare-And-Swap (CAS)
    semantics, monotonic fencing epochs, and idempotent exactly-once execution barriers.
    """

    def __init__(self, initial_epoch: int = 1) -> None:
        self._lock = threading.RLock()
        self._current_epoch = initial_epoch
        self._records: Dict[str, EffectRecord] = {}  # effect_key -> EffectRecord
        self._claims: Dict[str, EffectRecord] = {}   # claim_id -> EffectRecord
        self._committed_history: List[EffectRecord] = []

    @property
    def current_epoch(self) -> int:
        with self._lock:
            return self._current_epoch

    def set_current_epoch(self, epoch: int) -> None:
        with self._lock:
            if epoch >= self._current_epoch:
                self._current_epoch = epoch

    def trip_epoch(self, reason: str = "safety_trip") -> int:
        """Increments the fencing epoch, immediately obsoleting in-flight claims."""
        with self._lock:
            self._current_epoch += 1
            # Invalidate all active (uncommitted) claims under older epochs
            for claim_id, record in list(self._claims.items()):
                if record.status in (ClaimStatus.CLAIMED, ClaimStatus.AUTHORIZED):
                    if record.epoch < self._current_epoch:
                        record.status = ClaimStatus.ABORTED
            return self._current_epoch

    def claim(
        self,
        effect_key: str,
        tenant_id: str = "default",
        epoch: int = 1,
        idempotency_key: Optional[str] = None,
        action: str = "external_tool",
        timeout_ms: float = 5000.0,
    ) -> ClaimResult:
        """
        Attempts to claim an effect key under compare-and-swap semantics.
        Enforces monotonic fencing epoch validation and idempotency barrier.
        """
        with self._lock:
            # 1. Monotonic fencing epoch check
            if epoch < self._current_epoch:
                return ClaimResult(
                    status="REJECTED",
                    epoch=self._current_epoch,
                    reason=f"Stale epoch {epoch} < {self._current_epoch} (STALE_EPOCH)",
                )

            # Generate default idempotency key if omitted
            effective_idempotency_key = idempotency_key or f"idemp-{secrets.token_hex(8)}"

            # 2. Check if effect_key exists
            existing = self._records.get(effect_key)
            if existing:
                # Case A: Already COMMITTED
                if existing.status == ClaimStatus.COMMITTED:
                    if existing.idempotency_key == effective_idempotency_key:
                        return ClaimResult(
                            status="IDEMPOTENT_COMMITTED",
                            claim_id=existing.claim_id,
                            epoch=existing.epoch,
                            record=existing,
                            receipt=existing.receipt,
                            reason="Effect already committed under identical idempotency key",
                        )
                    else:
                        return ClaimResult(
                            status="REJECTED",
                            epoch=self._current_epoch,
                            reason=f"Effect {effect_key} already committed under different idempotency key",
                        )

                # Case B: In-flight claim (CLAIMED or AUTHORIZED)
                if existing.status in (ClaimStatus.CLAIMED, ClaimStatus.AUTHORIZED):
                    now = time.time()
                    if existing.is_expired(now):
                        # Lease expired! CAS takeover allowed
                        existing.status = ClaimStatus.EXPIRED
                    else:
                        # Still valid lease
                        if existing.idempotency_key == effective_idempotency_key:
                            # Re-attach to identical in-flight claim
                            return ClaimResult(
                                status="ACCEPTED",
                                claim_id=existing.claim_id,
                                epoch=existing.epoch,
                                record=existing,
                                reason="Re-attached to active in-flight claim",
                            )
                        else:
                            return ClaimResult(
                                status="REJECTED",
                                epoch=self._current_epoch,
                                reason=f"Effect {effect_key} actively locked by claim {existing.claim_id}",
                            )

            # 3. Create fresh claim
            claim_id = f"clm-{secrets.token_hex(6)}"
            now = time.time()
            expires_at = now + (max(100.0, timeout_ms) / 1000.0)

            record = EffectRecord(
                effect_key=effect_key,
                claim_id=claim_id,
                idempotency_key=effective_idempotency_key,
                epoch=epoch,
                tenant_id=tenant_id,
                action=action,
                status=ClaimStatus.CLAIMED,
                claimed_at=now,
                expires_at=expires_at,
            )

            self._records[effect_key] = record
            self._claims[claim_id] = record

            return ClaimResult(
                status="ACCEPTED",
                claim_id=claim_id,
                epoch=epoch,
                record=record,
                reason="Effect claim accepted",
            )

    def authorize(self, claim_id: str, permit_hash: str) -> Tuple[bool, str]:
        """
        Validates the claim against an active SafetyPermit hash.
        Transitions state from CLAIMED to AUTHORIZED.
        """
        with self._lock:
            record = self._claims.get(claim_id)
            if not record:
                return False, f"Claim {claim_id} not found"

            # Fencing check
            if record.epoch < self._current_epoch:
                record.status = ClaimStatus.ABORTED
                return False, f"Claim invalidated by epoch transition ({record.epoch} < {self._current_epoch})"

            # Expiry check
            now = time.time()
            if record.is_expired(now):
                record.status = ClaimStatus.EXPIRED
                return False, f"Claim {claim_id} expired before authorization"

            if record.status != ClaimStatus.CLAIMED:
                return False, f"Cannot authorize claim in status {record.status}"

            if not permit_hash or len(permit_hash) != 64:
                return False, "Invalid permit hash format (must be 64-char hex SHA-256)"

            record.permit_hash = permit_hash
            record.status = ClaimStatus.AUTHORIZED
            return True, "Claim authorized successfully"

    def complete(
        self,
        claim_id: str,
        execution_result: Optional[Any] = None,
    ) -> Tuple[bool, Optional[EffectReceipt], str]:
        """
        Commits an authorized effect, generating an immutable EffectReceipt.
        Guarantees that the side-effect commits at most once.
        """
        with self._lock:
            record = self._claims.get(claim_id)
            if not record:
                return False, None, f"Claim {claim_id} not found"

            # If already committed, return existing receipt idempotently
            if record.status == ClaimStatus.COMMITTED and record.receipt:
                return True, record.receipt, "Effect already committed (idempotent)"

            # Fencing check
            if record.epoch < self._current_epoch:
                record.status = ClaimStatus.ABORTED
                return False, None, f"Claim invalidated by epoch transition ({record.epoch} < {self._current_epoch})"

            # Expiry check
            now = time.time()
            if record.is_expired(now):
                record.status = ClaimStatus.EXPIRED
                return False, None, f"Claim {claim_id} expired before completion"

            # Must be CLAIMED or AUTHORIZED
            if record.status not in (ClaimStatus.CLAIMED, ClaimStatus.AUTHORIZED):
                return False, None, f"Cannot complete claim in status {record.status}"

            # Calculate result digest
            result_payload = execution_result if execution_result is not None else {}
            try:
                result_digest = CanonicalCodec.hash(result_payload)
            except Exception:
                result_digest = CanonicalCodec.hash({"raw": str(result_payload)})

            # Generate EffectReceipt
            receipt_id = f"rcpt-eff-{secrets.token_hex(6)}"
            receipt = EffectReceipt(
                receipt_id=receipt_id,
                effect_key=record.effect_key,
                idempotency_key=record.idempotency_key,
                epoch=record.epoch,
                tenant_id=record.tenant_id,
                committed_at=now,
                result_digest=result_digest,
            )

            record.status = ClaimStatus.COMMITTED
            record.execution_result = execution_result
            record.receipt = receipt
            self._committed_history.append(record)

            return True, receipt, "Effect completed and committed successfully"

    def abort(self, claim_id: str, reason: str = "canceled") -> Tuple[bool, str]:
        """Aborts an active in-flight claim, freeing the effect key for subsequent retry."""
        with self._lock:
            record = self._claims.get(claim_id)
            if not record:
                return False, f"Claim {claim_id} not found"

            if record.status == ClaimStatus.COMMITTED:
                return False, "Cannot abort already committed effect"

            record.status = ClaimStatus.ABORTED
            return True, f"Claim {claim_id} aborted: {reason}"

    def reconcile(
        self,
        since_epoch: int = 1,
        tenant_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Reconciles committed effect records across workers since a specific epoch."""
        with self._lock:
            results = []
            for record in self._committed_history:
                if record.epoch >= since_epoch:
                    if tenant_id is None or record.tenant_id == tenant_id:
                        results.append(record.to_dict())
            return results

    def status(self, tenant_id: str = "default") -> Dict[str, Any]:
        """Returns diagnostic ledger metrics."""
        with self._lock:
            active_claims = sum(
                1 for r in self._claims.values()
                if r.status in (ClaimStatus.CLAIMED, ClaimStatus.AUTHORIZED)
                and (tenant_id == "default" or r.tenant_id == tenant_id)
            )
            committed_count = sum(
                1 for r in self._committed_history
                if tenant_id == "default" or r.tenant_id == tenant_id
            )
            return {
                "current_epoch": self._current_epoch,
                "total_records": len(self._records),
                "active_claims_count": active_claims,
                "committed_effects_count": committed_count,
                "tenant_id": tenant_id,
            }

    def reset(self) -> None:
        """Resets ledger state for isolated test execution."""
        with self._lock:
            self._current_epoch = 1
            self._records.clear()
            self._claims.clear()
            self._committed_history.clear()


# Global singleton instance
global_effect_ledger = EffectLedger()
