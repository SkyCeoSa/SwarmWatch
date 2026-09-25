# -*- coding: utf-8 -*-
"""
Loss-Aware Causal Ingestion & Evidentiary Completeness Rule (Seam 53 / ADR 0064).
Eliminates forensic blindspots and silent span dropping under queue saturation
by enforcing monotonic sequence allocation, explicit gap accounting, prioritized headroom,
and evidentiary completeness contracts.
"""

from __future__ import annotations

import collections
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union


class IngressPriority:
    """Telemetry ingestion priority tiers."""
    NORMAL = "NORMAL"      # Conversational turns, general agent messages
    HIGH = "HIGH"          # State snapshots, causal branch forks
    CRITICAL = "CRITICAL"  # Circuit breaker trips, safety interventions, honeypot alerts


class CompletenessStatus:
    """Evidentiary trace completeness states."""
    COMPLETE = "COMPLETE"        # Contiguous sequence, all parents accounted for
    INCOMPLETE = "INCOMPLETE"    # One or more sequence gaps or unanchored parents
    SATURATED = "SATURATED"      # Experiencing active packet drop under backpressure


class SequenceGap:
    """Immutable record of a detected sequence discontinuity."""

    def __init__(
        self,
        swarm_id: str,
        stream_id: str,
        expected_seq: int,
        received_seq: int,
        missing_count: int,
        detected_at: Optional[float] = None,
    ) -> None:
        self.swarm_id = swarm_id
        self.stream_id = stream_id
        self.expected_seq = expected_seq
        self.received_seq = received_seq
        self.missing_count = missing_count
        self.detected_at = detected_at if detected_at is not None else time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "swarm_id": self.swarm_id,
            "stream_id": self.stream_id,
            "expected_seq": self.expected_seq,
            "received_seq": self.received_seq,
            "missing_count": self.missing_count,
            "detected_at": self.detected_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SequenceGap:
        return cls(
            swarm_id=str(data["swarm_id"]),
            stream_id=str(data["stream_id"]),
            expected_seq=int(data["expected_seq"]),
            received_seq=int(data["received_seq"]),
            missing_count=int(data["missing_count"]),
            detected_at=float(data.get("detected_at", time.time())),
        )


class IngressResult:
    """Outcome of a span submission to CausalIngress."""

    def __init__(
        self,
        accepted: bool,
        seq_num: int,
        swarm_id: str,
        span_id: str,
        priority: str,
        status: str,
        gap_detected: bool = False,
        gap: Optional[SequenceGap] = None,
        reason: Optional[str] = None,
    ) -> None:
        self.accepted = accepted
        self.seq_num = seq_num
        self.swarm_id = swarm_id
        self.span_id = span_id
        self.priority = priority
        self.status = status
        self.gap_detected = gap_detected
        self.gap = gap
        self.reason = reason

    def to_dict(self) -> Dict[str, Any]:
        return {
            "accepted": self.accepted,
            "seq_num": self.seq_num,
            "swarm_id": self.swarm_id,
            "span_id": self.span_id,
            "priority": self.priority,
            "status": self.status,
            "gap_detected": self.gap_detected,
            "gap": self.gap.to_dict() if self.gap else None,
            "reason": self.reason,
        }


class CompletenessReport:
    """Comprehensive forensic report on trace completeness."""

    def __init__(
        self,
        swarm_id: str,
        status: str,
        total_submitted: int,
        total_accepted: int,
        total_dropped: int,
        critical_submitted: int,
        critical_accepted: int,
        critical_delivery_ratio: float,
        loss_ratio: float,
        sequence_gaps: List[Dict[str, Any]],
        missing_causal_parents: List[str],
    ) -> None:
        self.swarm_id = swarm_id
        self.status = status
        self.total_submitted = total_submitted
        self.total_accepted = total_accepted
        self.total_dropped = total_dropped
        self.critical_submitted = critical_submitted
        self.critical_accepted = critical_accepted
        self.critical_delivery_ratio = critical_delivery_ratio
        self.loss_ratio = loss_ratio
        self.sequence_gaps = sequence_gaps
        self.missing_causal_parents = missing_causal_parents

    def to_dict(self) -> Dict[str, Any]:
        return {
            "swarm_id": self.swarm_id,
            "status": self.status,
            "total_submitted": self.total_submitted,
            "total_accepted": self.total_accepted,
            "total_dropped": self.total_dropped,
            "critical_submitted": self.critical_submitted,
            "critical_accepted": self.critical_accepted,
            "critical_delivery_ratio": self.critical_delivery_ratio,
            "loss_ratio": self.loss_ratio,
            "sequence_gaps": self.sequence_gaps,
            "missing_causal_parents": self.missing_causal_parents,
        }


class CausalIngress:
    """
    Prioritized, bounded, and loss-aware causal ingestion coordinator.
    Assigns monotonic sequence numbers, tracks sequence gaps, and guarantees
    100% preservation of critical safety interventions under queue saturation.
    """

    def __init__(self, max_capacity: int = 1000, reserve_ratio: float = 0.2) -> None:
        self._lock = threading.RLock()
        self._max_capacity = max_capacity
        self._reserve_capacity = int(max_capacity * reserve_ratio)
        self._normal_threshold = max_capacity - self._reserve_capacity

        # Queue of accepted items: deque of (priority, span_dict)
        self._queue: collections.deque = collections.deque()

        # Stream counters: (swarm_id, stream_id) -> sequence generator
        self._stream_seq: Dict[Tuple[str, str], int] = {}
        # Received sequence: (swarm_id, stream_id) -> last accepted sequence
        self._stream_received: Dict[Tuple[str, str], int] = {}

        # Gaps by swarm: swarm_id -> list of SequenceGap
        self._gaps: Dict[str, List[SequenceGap]] = collections.defaultdict(list)

        # Causal lineage tracking: swarm_id -> set of span_ids accepted
        self._known_spans: Dict[str, Set[str]] = collections.defaultdict(set)
        # Referenced parents: swarm_id -> set of parent_span_ids referenced
        self._referenced_parents: Dict[str, Set[str]] = collections.defaultdict(set)

        # Metrics accounting: swarm_id -> stats dict
        self._stats: Dict[str, Dict[str, int]] = collections.defaultdict(
            lambda: {
                "submitted": 0,
                "accepted": 0,
                "dropped": 0,
                "critical_submitted": 0,
                "critical_accepted": 0,
            }
        )

    def submit(
        self,
        span: Union[Dict[str, Any], Any],
        priority: str = IngressPriority.NORMAL,
        stream_id: str = "main",
    ) -> IngressResult:
        """
        Submits a span for prioritized, loss-aware causal ingestion.
        Enforces monotonic sequence numbering, headroom reservation, and gap detection.
        """
        with self._lock:
            # Extract span fields
            if hasattr(span, "to_dict"):
                span_dict = span.to_dict()
            elif isinstance(span, dict):
                span_dict = dict(span)
            else:
                span_dict = {"payload": str(span)}

            swarm_id = str(span_dict.get("swarm_id", "default_swarm"))
            span_id = str(span_dict.get("span_id", f"spn-{time.time_ns()}"))
            parent_span_ids = span_dict.get("parent_span_ids") or []
            if isinstance(parent_span_ids, str):
                parent_span_ids = [parent_span_ids]

            # 1. Monotonic sequence allocation per stream
            key = (swarm_id, stream_id)
            current_seq = self._stream_seq.get(key, 0) + 1
            self._stream_seq[key] = current_seq

            # Update submission counters
            stats = self._stats[swarm_id]
            stats["submitted"] += 1
            if priority == IngressPriority.CRITICAL:
                stats["critical_submitted"] += 1

            # 2. Capacity & prioritization check
            current_q_len = len(self._queue)
            accepted = False
            drop_reason: Optional[str] = None

            if priority == IngressPriority.CRITICAL:
                # Critical spans are ALWAYS accepted via dedicated headroom
                accepted = True
            elif priority == IngressPriority.HIGH:
                # High priority allowed up to full max_capacity
                if current_q_len < self._max_capacity:
                    accepted = True
                else:
                    drop_reason = f"Ingestion queue saturated at max capacity ({self._max_capacity})"
            else:
                # Normal priority shed once normal_threshold is reached
                if current_q_len < self._normal_threshold:
                    accepted = True
                else:
                    drop_reason = f"Normal priority shed: queue ({current_q_len}) >= threshold ({self._normal_threshold})"

            if not accepted:
                stats["dropped"] += 1
                return IngressResult(
                    accepted=False,
                    seq_num=current_seq,
                    swarm_id=swarm_id,
                    span_id=span_id,
                    priority=priority,
                    status="DROPPED_SATURATION",
                    reason=drop_reason,
                )

            # 3. Accepted: Check for Sequence Gap on arrival
            last_received = self._stream_received.get(key)
            gap_detected = False
            gap_record: Optional[SequenceGap] = None

            if last_received is not None and current_seq > (last_received + 1):
                missing_count = current_seq - (last_received + 1)
                gap_record = SequenceGap(
                    swarm_id=swarm_id,
                    stream_id=stream_id,
                    expected_seq=last_received + 1,
                    received_seq=current_seq,
                    missing_count=missing_count,
                )
                self._gaps[swarm_id].append(gap_record)
                gap_detected = True

            self._stream_received[key] = current_seq
            stats["accepted"] += 1
            if priority == IngressPriority.CRITICAL:
                stats["critical_accepted"] += 1

            # 4. Record span & parent lineage
            self._known_spans[swarm_id].add(span_id)
            for p in parent_span_ids:
                if p:
                    self._referenced_parents[swarm_id].add(p)

            # Annotate span with ingestion metadata
            span_dict["_seq_num"] = current_seq
            span_dict["_priority"] = priority
            span_dict["_stream_id"] = stream_id

            self._queue.append((priority, span_dict))

            return IngressResult(
                accepted=True,
                seq_num=current_seq,
                swarm_id=swarm_id,
                span_id=span_id,
                priority=priority,
                status="GAP_RECORDED" if gap_detected else "ACCEPTED",
                gap_detected=gap_detected,
                gap=gap_record,
                reason="Span accepted for ingestion" if not gap_detected else f"Gap recorded ({gap_record.missing_count} missing)",
            )

    def drain(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """Drains up to batch_size spans from the buffer in FIFO order."""
        with self._lock:
            items: List[Dict[str, Any]] = []
            while self._queue and len(items) < batch_size:
                _, span_dict = self._queue.popleft()
                items.append(span_dict)
            return items

    def completeness(self, swarm_id: str) -> CompletenessReport:
        """
        Calculates the evidentiary completeness report for a given swarm.
        Identifies sequence gaps, missing causal parent references, and calculates loss ratio.
        """
        with self._lock:
            stats = self._stats[swarm_id]
            total_submitted = stats["submitted"]
            total_accepted = stats["accepted"]
            total_dropped = stats["dropped"]
            crit_sub = stats["critical_submitted"]
            crit_acc = stats["critical_accepted"]

            crit_ratio = (crit_acc / crit_sub) if crit_sub > 0 else 1.0
            loss_ratio = (total_dropped / total_submitted) if total_submitted > 0 else 0.0

            gaps = [g.to_dict() for g in self._gaps.get(swarm_id, [])]

            # Missing causal parents check
            known = self._known_spans.get(swarm_id, set())
            referenced = self._referenced_parents.get(swarm_id, set())
            missing_parents = sorted(list(referenced - known))

            # Evidentiary completeness status determination
            if total_dropped > 0:
                status = CompletenessStatus.SATURATED
            elif len(gaps) > 0 or len(missing_parents) > 0:
                status = CompletenessStatus.INCOMPLETE
            else:
                status = CompletenessStatus.COMPLETE

            return CompletenessReport(
                swarm_id=swarm_id,
                status=status,
                total_submitted=total_submitted,
                total_accepted=total_accepted,
                total_dropped=total_dropped,
                critical_submitted=crit_sub,
                critical_accepted=crit_acc,
                critical_delivery_ratio=crit_ratio,
                loss_ratio=round(loss_ratio, 6),
                sequence_gaps=gaps,
                missing_causal_parents=missing_parents,
            )

    def status(self, swarm_id: str = "default") -> Dict[str, Any]:
        """Returns diagnostic buffer metrics."""
        with self._lock:
            comp = self.completeness(swarm_id)
            return {
                "queue_length": len(self._queue),
                "max_capacity": self._max_capacity,
                "normal_threshold": self._normal_threshold,
                "reserve_capacity": self._reserve_capacity,
                "swarm_id": swarm_id,
                "completeness_status": comp.status,
                "total_dropped": comp.total_dropped,
                "total_gaps": len(comp.sequence_gaps),
                "critical_delivery_ratio": comp.critical_delivery_ratio,
            }

    def reset(self) -> None:
        """Resets all queues, sequences, and metrics."""
        with self._lock:
            self._queue.clear()
            self._stream_seq.clear()
            self._stream_received.clear()
            self._gaps.clear()
            self._known_spans.clear()
            self._referenced_parents.clear()
            self._stats.clear()


# Global singleton instance
global_causal_ingress = CausalIngress()
