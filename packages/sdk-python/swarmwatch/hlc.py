# -*- coding: utf-8 -*-
"""
SwarmWatch Hybrid Logical Clocks & Bounded-Skew Causal Timestamps (Seam 83 / ADR 0094).

Implements Kulkarni et al. Hybrid Logical Clocks (HLC) for distributed multi-cluster agent systems,
guaranteeing causal monotonicity (H1), bounded physical drift protection (H2), and deterministic
tie-breaking (H3).

Invariants:
- H1: If e -> f, then hlc(e) < hlc(f).
- H2: |hlc.l - physical| <= epsilon. Remote timestamps exceeding physical + epsilon are rejected.
- H3: Total ordering via (physical_ns, logical_counter, node_id).
"""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class HlcError(Exception):
    """Base exception for Hybrid Logical Clock operations."""


class BoundedSkewViolationError(HlcError):
    """Raised when an incoming timestamp exceeds the physical clock + maximum tolerated drift."""
    def __init__(self, remote_physical_ns: int, local_physical_ns: int, max_drift_ns: int):
        skew_ms = (remote_physical_ns - local_physical_ns) / 1_000_000.0
        max_ms = max_drift_ns / 1_000_000.0
        super().__init__(
            f"Bounded skew violation: remote clock leads local clock by {skew_ms:.2f}ms (tolerated max={max_ms:.2f}ms)"
        )
        self.remote_physical_ns = remote_physical_ns
        self.local_physical_ns = local_physical_ns
        self.max_drift_ns = max_drift_ns


# ==============================================================================
# 2. Domain Types
# ==============================================================================

@dataclass(frozen=True, order=False)
class HybridLogicalTimestamp:
    """
    Immutable distributed timestamp combining physical wall clock and logical event sequence.
    Strict total ordering: (physical_ns, logical_counter, node_id).
    """
    physical_ns: int
    logical_counter: int
    node_id: str

    def __lt__(self, other: HybridLogicalTimestamp) -> bool:
        if self.physical_ns != other.physical_ns:
            return self.physical_ns < other.physical_ns
        if self.logical_counter != other.logical_counter:
            return self.logical_counter < other.logical_counter
        return self.node_id < other.node_id

    def __le__(self, other: HybridLogicalTimestamp) -> bool:
        return self == other or self < other

    def __gt__(self, other: HybridLogicalTimestamp) -> bool:
        return not (self <= other)

    def __ge__(self, other: HybridLogicalTimestamp) -> bool:
        return not (self < other)

    def to_string(self) -> str:
        return f"{self.physical_ns}:{self.logical_counter}:{self.node_id}"

    @classmethod
    def from_string(cls, s: str) -> HybridLogicalTimestamp:
        parts = s.split(":")
        if len(parts) != 3:
            raise ValueError(f"Malformed HLC timestamp string: {s}")
        return cls(physical_ns=int(parts[0]), logical_counter=int(parts[1]), node_id=parts[2])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "physical_ns": self.physical_ns,
            "logical_counter": self.logical_counter,
            "node_id": self.node_id,
            "encoded": self.to_string(),
        }


# ==============================================================================
# 3. Hybrid Logical Clock Engine
# ==============================================================================

class HybridLogicalClock:
    """
    Hybrid Logical Clock implementation providing sub-microsecond monotonically increasing
    causal timestamps across distributed clusters.
    """

    def __init__(
        self,
        node_id: str,
        max_drift_ns: int = 500_000_000,  # 500ms default maximum clock drift
        physical_clock: Optional[Callable[[], int]] = None,
    ) -> None:
        self.node_id = str(node_id)
        self.max_drift_ns = int(max_drift_ns)
        self._physical_clock = physical_clock or time.time_ns
        self._lock = threading.Lock()

        # Internal state (l, c)
        self._l: int = -1
        self._c: int = 0

    @property
    def current_state(self) -> Tuple[int, int]:
        with self._lock:
            return self._l, self._c

    def now(self) -> HybridLogicalTimestamp:
        """
        Emits a strictly monotonic local event timestamp (Invariant H1).
        l' = max(l_prev, physical_now)
        c' = c_prev + 1 if l' == l_prev else 0
        """
        with self._lock:
            pt = self._physical_clock()
            l_old = self._l
            self._l = max(l_old, pt)

            if self._l == l_old:
                self._c += 1
            else:
                self._c = 0

            return HybridLogicalTimestamp(
                physical_ns=self._l,
                logical_counter=self._c,
                node_id=self.node_id,
            )

    def observe(self, remote: HybridLogicalTimestamp) -> HybridLogicalTimestamp:
        """
        Updates the local HLC upon message arrival from a remote peer (Invariant H1 & H2).
        Fails closed with BoundedSkewViolationError if remote clock exceeds physical_now + max_drift_ns.
        """
        with self._lock:
            pt = self._physical_clock()

            # Invariant H2: Bounded physical drift protection
            if remote.physical_ns > pt + self.max_drift_ns:
                raise BoundedSkewViolationError(
                    remote_physical_ns=remote.physical_ns,
                    local_physical_ns=pt,
                    max_drift_ns=self.max_drift_ns,
                )

            l_old = self._l
            c_old = self._c

            self._l = max(l_old, remote.physical_ns, pt)

            if self._l == l_old and self._l == remote.physical_ns:
                self._c = max(c_old, remote.logical_counter) + 1
            elif self._l == l_old:
                self._c = c_old + 1
            elif self._l == remote.physical_ns:
                self._c = remote.logical_counter + 1
            else:
                self._c = 0

            return HybridLogicalTimestamp(
                physical_ns=self._l,
                logical_counter=self._c,
                node_id=self.node_id,
            )

    @staticmethod
    def compare(a: HybridLogicalTimestamp, b: HybridLogicalTimestamp) -> int:
        """Compares two timestamps: returns -1 if a < b, 1 if a > b, 0 if a == b."""
        if a < b:
            return -1
        if a > b:
            return 1
        return 0


# Process-wide default instance
global_hlc: HybridLogicalClock = HybridLogicalClock(node_id="cluster-default")
