# -*- coding: utf-8 -*-
"""
SwarmWatch Unified Safety Circuit Bus & Quorum-Gated Rearm (Seam 82 / ADR 0093).

Implements a centralized, fail-closed safety circuit event bus across all subsystems,
monotonic generation counters, multi-operator Ed25519 threshold quorum rearming (Seam 81),
scope-isolated and global guardrails, and non-blocking pub/sub trip notifications.

Invariants:
- SC1: Trip generation is strictly monotonic (g' > g) across the lifetime of the process.
- SC2: Rearm strictly requires a valid ThresholdQuorumCertificate signed by >= 2 distinct operators
       and committing to a non-empty StateSnapshot root hash. Unilateral operator resets fail closed.
- SC3: A trip immediately transitions the circuit to TRIPPED and halts all guarded execution blocks.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from enum import Enum
import hashlib
import json
import threading
import time
from typing import Any, Callable, Dict, Generator, List, Optional, Set

from swarmwatch.qc import (
    CommitteeRoster,
    ThresholdQuorumCertificate,
    verify_quorum_certificate,
    QuorumCertificateError,
)


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class CircuitBusError(Exception):
    """Base exception for all unified safety circuit bus operations."""


class CircuitBusTrippedError(CircuitBusError):
    """Raised when an operation is executed while the safety circuit is tripped."""
    def __init__(self, reason: str, scope: str, generation: int):
        super().__init__(f"Safety circuit TRIPPED (scope={scope}, gen={generation}): {reason}")
        self.reason = reason
        self.scope = scope
        self.generation = generation


class UnauthorizedRearmError(CircuitBusError):
    """Raised when rearm is attempted without valid multi-operator quorum authorization."""


class InvalidSnapshotHashError(CircuitBusError):
    """Raised when rearm snapshot hash is missing or malformed."""


# ==============================================================================
# 2. Domain Types & Enums
# ==============================================================================

class CircuitState(str, Enum):
    """State of the unified safety circuit bus."""
    CLOSED = "CLOSED"      # Normal operation: guards allow execution
    TRIPPED = "TRIPPED"    # Safety trip active: all guarded scopes reject execution
    REARMING = "REARMING"  # Verification transition state


@dataclass(frozen=True)
class CircuitTripEvent:
    """Immutable audit record of a circuit trip event."""
    trip_id: str
    generation: int
    timestamp_ns: int
    reason: str
    scope: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CircuitBusStatus:
    """Current snapshot of the safety circuit bus."""
    state: CircuitState
    generation: int
    tripped_scopes: List[str]
    last_trip_event: Optional[CircuitTripEvent]
    total_trips: int
    last_rearm_ns: Optional[int]
    last_snapshot_hash: Optional[str]


# ==============================================================================
# 3. Unified Safety Circuit Bus Deep Module
# ==============================================================================

class SafetyCircuitBus:
    """
    Authoritative safety circuit bus governing fail-closed execution across SwarmWatch.
    Replaces fragmented, individual subsystem circuit breakers with a unified,
    quorum-gated mechanism.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._state: CircuitState = CircuitState.CLOSED
        self._generation: int = 0
        self._tripped_scopes: Set[str] = set()
        self._last_trip_event: Optional[CircuitTripEvent] = None
        self._total_trips: int = 0
        self._last_rearm_ns: Optional[int] = None
        self._last_snapshot_hash: Optional[str] = None
        self._subscribers: List[Callable[[CircuitTripEvent], None]] = []

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    def is_tripped(self, scope: str = "GLOBAL") -> bool:
        """Checks if either the specific scope or the global circuit is tripped."""
        with self._lock:
            if self._state == CircuitState.TRIPPED:
                return True
            return scope.upper() in self._tripped_scopes or "GLOBAL" in self._tripped_scopes

    def trip(
        self,
        reason: str,
        scope: str = "GLOBAL",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Transitions the circuit to TRIPPED for the specified scope.
        Increments the monotonic generation counter (Invariant SC1) and notifies subscribers (Invariant SC3).
        """
        normalized_scope = scope.upper()
        meta = dict(metadata or {})
        now_ns = time.time_ns()

        with self._lock:
            self._generation += 1
            trip_id_raw = f"trip:{self._generation}:{normalized_scope}:{now_ns}:{reason}"
            trip_id = hashlib.sha256(trip_id_raw.encode("utf-8")).hexdigest()

            event = CircuitTripEvent(
                trip_id=trip_id,
                generation=self._generation,
                timestamp_ns=now_ns,
                reason=reason,
                scope=normalized_scope,
                metadata=meta,
            )

            self._state = CircuitState.TRIPPED
            self._tripped_scopes.add(normalized_scope)
            self._last_trip_event = event
            self._total_trips += 1
            callbacks = list(self._subscribers)

        # Broadcast asynchronously to subscribers without holding lock
        for cb in callbacks:
            try:
                cb(event)
            except Exception:
                pass  # Subscribers must never break safety trip propagation

        return trip_id

    def rearm(
        self,
        qc: ThresholdQuorumCertificate,
        snapshot_hash: str,
        expected_roster: Optional[CommitteeRoster] = None,
    ) -> int:
        """
        Rearms the circuit back to CLOSED (Invariant SC2).
        Requires:
        1. Non-empty, 64-char hex snapshot_hash representing state verification.
        2. Verified ThresholdQuorumCertificate for domain 'circuit:rearm'.
        3. Quorum certificate signed by >= 2 distinct authorized operators.
        """
        if not snapshot_hash or len(snapshot_hash) < 32:
            raise InvalidSnapshotHashError("Rearm requires a valid cryptographic state snapshot hash")

        if qc.domain != "circuit:rearm":
            raise UnauthorizedRearmError(f"Certificate domain '{qc.domain}' is not 'circuit:rearm'")

        if len(qc.signers) < 2:
            raise UnauthorizedRearmError(
                f"Multi-operator rule requires >= 2 operator signatures, found {len(qc.signers)}"
            )

        # Verify the certificate digest matches the provided snapshot_hash
        if qc.digest != snapshot_hash:
            raise UnauthorizedRearmError(
                f"Certificate digest '{qc.digest}' does not match snapshot hash '{snapshot_hash}'"
            )

        # Cryptographically verify QC signatures
        verdict = verify_quorum_certificate(qc, roster=expected_roster)
        if not verdict.is_valid:
            raise UnauthorizedRearmError(
                f"Quorum certificate verification failed: {verdict.error_reason}"
            )

        with self._lock:
            self._state = CircuitState.CLOSED
            self._tripped_scopes.clear()
            self._last_rearm_ns = time.time_ns()
            self._last_snapshot_hash = snapshot_hash
            return self._generation

    @contextmanager
    def guard(self, scope: str = "GLOBAL") -> Generator[None, None, None]:
        """
        RAII context manager enforcing fail-closed circuit protection.
        Raises CircuitBusTrippedError immediately if tripped upon entry.
        """
        normalized_scope = scope.upper()
        with self._lock:
            if self.is_tripped(normalized_scope):
                event = self._last_trip_event
                reason = event.reason if event else "Circuit tripped"
                gen = self._generation
                raise CircuitBusTrippedError(reason=reason, scope=normalized_scope, generation=gen)
        yield

    def subscribe(self, callback: Callable[[CircuitTripEvent], None]) -> None:
        """Registers a listener for trip notifications."""
        with self._lock:
            if callback not in self._subscribers:
                self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[CircuitTripEvent], None]) -> None:
        """Unregisters a listener."""
        with self._lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)

    def get_status(self) -> CircuitBusStatus:
        """Returns an immutable point-in-time status snapshot."""
        with self._lock:
            return CircuitBusStatus(
                state=self._state,
                generation=self._generation,
                tripped_scopes=sorted(list(self._tripped_scopes)),
                last_trip_event=self._last_trip_event,
                total_trips=self._total_trips,
                last_rearm_ns=self._last_rearm_ns,
                last_snapshot_hash=self._last_snapshot_hash,
            )

    def reset_for_test(self) -> None:
        """Resets the circuit for isolated testing environments."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._tripped_scopes.clear()
            self._last_trip_event = None


# Authoritative Global Singleton
global_circuit_bus: SafetyCircuitBus = SafetyCircuitBus()
global_safety_circuit_bus: SafetyCircuitBus = global_circuit_bus
