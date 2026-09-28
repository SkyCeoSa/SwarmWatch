# -*- coding: utf-8 -*-
"""
Downstream Effect Protocol & 2PC Outcome-Unknown Reconciliation (Seam 62 / ADR 0073).
Enforces explicit two-phase commit semantics across external downstream network boundaries,
eliminating false exactly-once claims by failing closed into OUTCOME_UNKNOWN during timeouts
and requiring explicit downstream-enforced idempotency or active reconciliation probes.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import threading
import time
from typing import Any, Callable, Dict, Optional

from swarmwatch.canonical import CanonicalCodec, CanonicalDomainTag


class EffectPhase:
    """Discrete lifecycle states of a downstream effect."""
    PREPARED = "PREPARED"
    DISPATCHED = "DISPATCHED"
    COMMITTED = "COMMITTED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    ABORTED = "ABORTED"


class DownstreamIdempotencyContract:
    """Declared idempotency capability of the downstream service."""
    DOWNSTREAM_FENCED = "DOWNSTREAM_FENCED"
    UNCOORDINATED = "UNCOORDINATED"


class UncoordinatedRetryBlockedError(Exception):
    """Raised when a retry is attempted on an uncoordinated downstream service in OUTCOME_UNKNOWN state."""
    pass


class StaleEpochError(Exception):
    """Raised when an effect dispatch or acknowledgment carries an obsolete fencing epoch."""
    pass


class EffectNotFoundError(Exception):
    """Raised when the specified effect_id is not tracked in the boundary ledger."""
    pass


class DownstreamReceipt:
    """Cryptographically verifiable confirmation from a downstream service."""

    def __init__(
        self,
        downstream_confirmation_id: str,
        result_digest: str,
        latency_ms: float = 0.0,
        received_at: Optional[float] = None,
        receipt_hash: Optional[str] = None,
    ) -> None:
        self.downstream_confirmation_id = downstream_confirmation_id
        self.result_digest = result_digest
        self.latency_ms = latency_ms
        self.received_at = received_at if received_at is not None else time.time()
        self.receipt_hash = receipt_hash or self.compute_receipt_hash()

    def compute_receipt_hash(self) -> str:
        payload = {
            "downstream_confirmation_id": self.downstream_confirmation_id,
            "latency_ms": round(self.latency_ms, 3),
            "received_at": round(self.received_at, 6),
            "result_digest": self.result_digest,
        }
        return CanonicalCodec.hash(payload, CanonicalDomainTag.EFFECT)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "downstream_confirmation_id": self.downstream_confirmation_id,
            "result_digest": self.result_digest,
            "latency_ms": self.latency_ms,
            "received_at": self.received_at,
            "receipt_hash": self.receipt_hash,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DownstreamReceipt:
        return cls(
            downstream_confirmation_id=str(data["downstream_confirmation_id"]),
            result_digest=str(data["result_digest"]),
            latency_ms=float(data.get("latency_ms", 0.0)),
            received_at=float(data["received_at"]),
            receipt_hash=str(data.get("receipt_hash", "")),
        )


class EffectPreparation:
    """Internal durable representation of a prepared downstream effect."""

    def __init__(
        self,
        effect_id: str,
        tenant_id: str,
        effect_key: str,
        target_uri: str,
        payload: Dict[str, Any],
        payload_digest: str,
        idempotency_key: str,
        epoch: int,
        contract: str = DownstreamIdempotencyContract.DOWNSTREAM_FENCED,
        status: str = EffectPhase.PREPARED,
        prepared_at: Optional[float] = None,
        dispatched_at: Optional[float] = None,
        committed_at: Optional[float] = None,
        reconciled: bool = False,
        reconciled_at: Optional[float] = None,
        receipt: Optional[DownstreamReceipt] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        probe_attempts: int = 0,
    ) -> None:
        self.effect_id = effect_id
        self.tenant_id = tenant_id
        self.effect_key = effect_key
        self.target_uri = target_uri
        self.payload = payload
        self.payload_digest = payload_digest
        self.idempotency_key = idempotency_key
        self.epoch = epoch
        self.contract = contract
        self.status = status
        now = time.time()
        self.prepared_at = prepared_at if prepared_at is not None else now
        self.dispatched_at = dispatched_at
        self.committed_at = committed_at
        self.reconciled = reconciled
        self.reconciled_at = reconciled_at
        self.receipt = receipt
        self.error_message = error_message
        self.retry_count = retry_count
        self.probe_attempts = probe_attempts

    def to_dict(self) -> Dict[str, Any]:
        return {
            "effect_id": self.effect_id,
            "tenant_id": self.tenant_id,
            "effect_key": self.effect_key,
            "target_uri": self.target_uri,
            "payload": self.payload,
            "payload_digest": self.payload_digest,
            "idempotency_key": self.idempotency_key,
            "epoch": self.epoch,
            "contract": self.contract,
            "status": self.status,
            "prepared_at": self.prepared_at,
            "dispatched_at": self.dispatched_at,
            "committed_at": self.committed_at,
            "reconciled": self.reconciled,
            "reconciled_at": self.reconciled_at,
            "receipt": self.receipt.to_dict() if self.receipt else None,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "probe_attempts": self.probe_attempts,
            "is_outcome_unknown": self.status == EffectPhase.OUTCOME_UNKNOWN,
        }


class DispatchResult:
    """Outcome of dispatching an effect across the downstream network boundary."""

    def __init__(
        self,
        effect_id: str,
        status: str,
        is_outcome_unknown: bool,
        dispatch_output: Optional[Any] = None,
        error_message: Optional[str] = None,
    ) -> None:
        self.effect_id = effect_id
        self.status = status
        self.is_outcome_unknown = is_outcome_unknown
        self.dispatch_output = dispatch_output
        self.error_message = error_message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "effect_id": self.effect_id,
            "status": self.status,
            "is_outcome_unknown": self.is_outcome_unknown,
            "dispatch_output": self.dispatch_output,
            "error_message": self.error_message,
        }


class AcknowledgeResult:
    """Outcome of acknowledging a downstream receipt."""

    def __init__(
        self,
        effect_id: str,
        status: str,
        is_outcome_unknown: bool,
        receipt: Optional[DownstreamReceipt] = None,
    ) -> None:
        self.effect_id = effect_id
        self.status = status
        self.is_outcome_unknown = is_outcome_unknown
        self.receipt = receipt

    def to_dict(self) -> Dict[str, Any]:
        return {
            "effect_id": self.effect_id,
            "status": self.status,
            "is_outcome_unknown": self.is_outcome_unknown,
            "receipt": self.receipt.to_dict() if self.receipt else None,
        }


class ReconciliationResult:
    """Outcome of an active downstream status probe."""

    def __init__(
        self,
        effect_id: str,
        status: str,
        reconciled: bool,
        is_outcome_unknown: bool,
        downstream_confirmation_id: Optional[str] = None,
        probe_attempts: int = 1,
        error_message: Optional[str] = None,
    ) -> None:
        self.effect_id = effect_id
        self.status = status
        self.reconciled = reconciled
        self.is_outcome_unknown = is_outcome_unknown
        self.downstream_confirmation_id = downstream_confirmation_id
        self.probe_attempts = probe_attempts
        self.error_message = error_message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "effect_id": self.effect_id,
            "status": self.status,
            "reconciled": self.reconciled,
            "is_outcome_unknown": self.is_outcome_unknown,
            "downstream_confirmation_id": self.downstream_confirmation_id,
            "probe_attempts": self.probe_attempts,
            "error_message": self.error_message,
        }


class DownstreamEffectBoundary:
    """
    Deep Module: Coordinates downstream effect dispatch, timeout fail-closed
    outcome-unknown isolation, and active reconciliation probes across network boundaries.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._effects: Dict[str, EffectPreparation] = {}
        self._tenant_fences: Dict[str, int] = {}

    def set_tenant_epoch_fence(self, tenant_id: str, current_epoch: int) -> None:
        """Sets or advances the authoritative fencing epoch for a tenant."""
        with self._lock:
            existing = self._tenant_fences.get(tenant_id, 0)
            if current_epoch > existing:
                self._tenant_fences[tenant_id] = current_epoch

    def prepare(
        self,
        tenant_id: str,
        effect_key: str,
        target_uri: str,
        payload: Dict[str, Any],
        epoch: int = 1,
        contract: str = DownstreamIdempotencyContract.DOWNSTREAM_FENCED,
        idempotency_key: Optional[str] = None,
    ) -> EffectPreparation:
        """Prepares a downstream effect with deterministic idempotency key and epoch."""
        with self._lock:
            fence = self._tenant_fences.get(tenant_id, 0)
            if epoch < fence:
                raise StaleEpochError(
                    f"Provided epoch {epoch} is stale compared to tenant fence {fence}"
                )

            effect_id = f"eff_{secrets.token_hex(12)}"
            if not idempotency_key:
                raw_token = f"{tenant_id}:{effect_key}:{epoch}:{secrets.token_hex(8)}"
                idempotency_key = f"idem_{hashlib.sha256(raw_token.encode('utf-8')).hexdigest()[:24]}"

            payload_digest = CanonicalCodec.hash(payload, CanonicalDomainTag.EFFECT)

            prep = EffectPreparation(
                effect_id=effect_id,
                tenant_id=tenant_id,
                effect_key=effect_key,
                target_uri=target_uri,
                payload=payload,
                payload_digest=payload_digest,
                idempotency_key=idempotency_key,
                epoch=epoch,
                contract=contract,
                status=EffectPhase.PREPARED,
            )
            self._effects[effect_id] = prep
            return prep

    def dispatch(
        self,
        effect_id: str,
        dispatch_fn: Callable[[EffectPreparation], Any],
    ) -> DispatchResult:
        """
        Dispatches the prepared effect across the downstream boundary.
        If a network error or timeout occurs, fails closed to OUTCOME_UNKNOWN.
        """
        with self._lock:
            effect = self._effects.get(effect_id)
            if not effect:
                raise EffectNotFoundError(f"Effect {effect_id} not found")

            # Check for uncoordinated retry block
            if effect.status == EffectPhase.OUTCOME_UNKNOWN:
                if effect.contract == DownstreamIdempotencyContract.UNCOORDINATED:
                    raise UncoordinatedRetryBlockedError(
                        f"Effect {effect_id} is in OUTCOME_UNKNOWN and downstream service is UNCOORDINATED. "
                        "Blind retry blocked to prevent duplicate side-effects."
                    )
                effect.retry_count += 1

            fence = self._tenant_fences.get(effect.tenant_id, 0)
            if effect.epoch < fence:
                raise StaleEpochError(
                    f"Effect epoch {effect.epoch} is stale compared to tenant fence {fence}"
                )

        # Execute dispatch outside lock to allow concurrent execution
        try:
            output = dispatch_fn(effect)
            with self._lock:
                effect.status = EffectPhase.DISPATCHED
                effect.dispatched_at = time.time()
                effect.error_message = None
            return DispatchResult(
                effect_id=effect_id,
                status=EffectPhase.DISPATCHED,
                is_outcome_unknown=False,
                dispatch_output=output,
            )
        except Exception as e:
            with self._lock:
                effect.status = EffectPhase.OUTCOME_UNKNOWN
                effect.error_message = f"{type(e).__name__}: {str(e)}"
            return DispatchResult(
                effect_id=effect_id,
                status=EffectPhase.OUTCOME_UNKNOWN,
                is_outcome_unknown=True,
                error_message=effect.error_message,
            )

    def acknowledge(
        self,
        effect_id: str,
        receipt: DownstreamReceipt,
        epoch: Optional[int] = None,
    ) -> AcknowledgeResult:
        """Acknowledges explicit downstream receipt, committing the effect."""
        with self._lock:
            effect = self._effects.get(effect_id)
            if not effect:
                raise EffectNotFoundError(f"Effect {effect_id} not found")

            check_epoch = epoch if epoch is not None else effect.epoch
            fence = self._tenant_fences.get(effect.tenant_id, 0)
            if check_epoch < fence:
                raise StaleEpochError(
                    f"Acknowledge epoch {check_epoch} is stale compared to tenant fence {fence}"
                )

            effect.status = EffectPhase.COMMITTED
            effect.receipt = receipt
            effect.committed_at = time.time()
            effect.error_message = None

            return AcknowledgeResult(
                effect_id=effect_id,
                status=EffectPhase.COMMITTED,
                is_outcome_unknown=False,
                receipt=receipt,
            )

    def reconcile(
        self,
        effect_id: str,
        query_fn: Callable[[str, str], Dict[str, Any]],
    ) -> ReconciliationResult:
        """
        Executes an active reconciliation probe against the downstream target.
        Resolves OUTCOME_UNKNOWN into COMMITTED or ABORTED.
        """
        with self._lock:
            effect = self._effects.get(effect_id)
            if not effect:
                raise EffectNotFoundError(f"Effect {effect_id} not found")

            idem_key = effect.idempotency_key

        try:
            probe_data = query_fn(effect_id, idem_key)
            with self._lock:
                effect.probe_attempts += 1
                found = probe_data.get("found", False)
                if found:
                    effect.status = EffectPhase.COMMITTED
                    effect.reconciled = True
                    effect.reconciled_at = time.time()
                    downstream_id = str(probe_data.get("downstream_id", f"recon_{effect_id}"))
                    digest = str(probe_data.get("digest", effect.payload_digest))
                    effect.receipt = DownstreamReceipt(
                        downstream_confirmation_id=downstream_id,
                        result_digest=digest,
                    )
                    effect.committed_at = effect.reconciled_at
                    return ReconciliationResult(
                        effect_id=effect_id,
                        status=EffectPhase.COMMITTED,
                        reconciled=True,
                        is_outcome_unknown=False,
                        downstream_confirmation_id=downstream_id,
                        probe_attempts=effect.probe_attempts,
                    )
                else:
                    effect.status = EffectPhase.ABORTED
                    effect.reconciled = True
                    effect.reconciled_at = time.time()
                    return ReconciliationResult(
                        effect_id=effect_id,
                        status=EffectPhase.ABORTED,
                        reconciled=True,
                        is_outcome_unknown=False,
                        probe_attempts=effect.probe_attempts,
                    )
        except Exception as e:
            with self._lock:
                effect.probe_attempts += 1
                err = f"{type(e).__name__}: {str(e)}"
                return ReconciliationResult(
                    effect_id=effect_id,
                    status=EffectPhase.OUTCOME_UNKNOWN,
                    reconciled=False,
                    is_outcome_unknown=True,
                    probe_attempts=effect.probe_attempts,
                    error_message=err,
                )

    def get_effect(self, effect_id: str) -> Optional[EffectPreparation]:
        """Retrieves an effect record by id."""
        with self._lock:
            return self._effects.get(effect_id)

    def get_status(self) -> Dict[str, Any]:
        """Summarizes downstream effect boundary status."""
        with self._lock:
            total = len(self._effects)
            outcome_unknown = sum(1 for e in self._effects.values() if e.status == EffectPhase.OUTCOME_UNKNOWN)
            committed = sum(1 for e in self._effects.values() if e.status == EffectPhase.COMMITTED)
            dispatched = sum(1 for e in self._effects.values() if e.status == EffectPhase.DISPATCHED)
            aborted = sum(1 for e in self._effects.values() if e.status == EffectPhase.ABORTED)
            prepared = sum(1 for e in self._effects.values() if e.status == EffectPhase.PREPARED)

            return {
                "total_effects": total,
                "prepared_count": prepared,
                "dispatched_count": dispatched,
                "committed_count": committed,
                "outcome_unknown_count": outcome_unknown,
                "aborted_count": aborted,
                "epoch_fences": dict(self._tenant_fences),
            }


global_downstream_effect_boundary = DownstreamEffectBoundary()
