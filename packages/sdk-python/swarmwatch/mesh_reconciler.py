# -*- coding: utf-8 -*-
"""
Partition Reconciliation & Conflict Lattice Engine (Seam 57 / ADR 0068).
Safely rejoins partitioned gossip mesh clusters and shard replicas without silent safety trip clearance.
Enforces:
1. Safety Trip Monotonicity Invariant: TRIPPED state is an absorbing barrier across partitions (cannot silently clear).
2. Explicit Conflict Lattice: Concurrent spans (V_A || V_B) preserve provenance metadata rather than silently dropping.
3. Monotonic Epoch Fencing: Reconciled authority epoch = max(E_local, E_remote) + (1 if conflict).
4. Zero external LLM API calls (<25us local execution).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, field
from enum import Enum
import hashlib
import secrets
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from swarmwatch.canonical import CanonicalCodec, CanonicalDomainTag


class LatticeVerdict(str, Enum):
    """
    Causal lattice ordering verdicts between two vector-clocked states.
    """
    DOMINATES = "DOMINATES"        # Local causally dominates Remote (V_L > V_R)
    DOMINATED = "DOMINATED"        # Remote causally dominates Local (V_L < V_R)
    IDENTICAL = "IDENTICAL"        # Local and Remote are identical (V_L == V_R)
    CONCURRENT = "CONCURRENT"      # Split-brain concurrent divergence (V_L || V_R)


def compare_vector_clocks(vc_a: Dict[str, int], vc_b: Dict[str, int]) -> LatticeVerdict:
    """
    Compares two vector clocks under standard partial order semantics.
    """
    all_keys = set(vc_a.keys()).union(set(vc_b.keys()))
    a_greater = False
    b_greater = False

    for k in all_keys:
        va = vc_a.get(k, 0)
        vb = vc_b.get(k, 0)
        if va > vb:
            a_greater = True
        elif vb > va:
            b_greater = True

    if a_greater and not b_greater:
        return LatticeVerdict.DOMINATES
    elif b_greater and not a_greater:
        return LatticeVerdict.DOMINATED
    elif not a_greater and not b_greater:
        return LatticeVerdict.IDENTICAL
    else:
        return LatticeVerdict.CONCURRENT


def merge_vector_clocks(vc_a: Dict[str, int], vc_b: Dict[str, int]) -> Dict[str, int]:
    """
    Computes the least upper bound (join) of two vector clocks.
    """
    all_keys = set(vc_a.keys()).union(set(vc_b.keys()))
    return {k: max(vc_a.get(k, 0), vc_b.get(k, 0)) for k in sorted(all_keys)}


@dataclass
class ReconciliationPlan:
    """
    Structured delta specification enumerating missing sequences and conflict lattice analysis.
    """
    plan_id: str
    tenant_id: str
    verdict: LatticeVerdict
    divergent_keys: List[str]
    missing_local_events: int
    missing_remote_events: int
    conflict_detected: bool
    merged_vector_clock: Dict[str, int]
    action_items: List[str]
    created_at: float = field(default_factory=time.time)
    plan_hash: Optional[str] = None

    def __post_init__(self) -> None:
        if isinstance(self.verdict, str):
            self.verdict = LatticeVerdict(self.verdict)
        if not self.plan_hash:
            self.plan_hash = self.compute_hash()

    def compute_hash(self) -> str:
        payload = {
            "conflict_detected": self.conflict_detected,
            "divergent_keys": sorted(self.divergent_keys),
            "merged_vector_clock": self.merged_vector_clock,
            "missing_local_events": self.missing_local_events,
            "missing_remote_events": self.missing_remote_events,
            "tenant_id": self.tenant_id,
            "verdict": self.verdict.value,
        }
        return CanonicalCodec.hash(payload, CanonicalDomainTag.RECONCILER)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "tenant_id": self.tenant_id,
            "verdict": self.verdict.value,
            "divergent_keys": list(self.divergent_keys),
            "missing_local_events": self.missing_local_events,
            "missing_remote_events": self.missing_remote_events,
            "conflict_detected": self.conflict_detected,
            "merged_vector_clock": dict(self.merged_vector_clock),
            "action_items": list(self.action_items),
            "created_at": self.created_at,
            "plan_hash": self.plan_hash,
        }


@dataclass
class MergeResult:
    """
    Deduplicated, causally ordered telemetry stream result binding converged spans.
    """
    merged_spans: List[Dict[str, Any]]
    total_spans: int
    deduplicated_count: int
    concurrent_conflicts_retained: int
    merged_vector_clock: Dict[str, int]
    convergence_hash: str
    execution_latency_us: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "merged_spans": self.merged_spans,
            "total_spans": self.total_spans,
            "deduplicated_count": self.deduplicated_count,
            "concurrent_conflicts_retained": self.concurrent_conflicts_retained,
            "merged_vector_clock": dict(self.merged_vector_clock),
            "convergence_hash": self.convergence_hash,
            "execution_latency_us": self.execution_latency_us,
        }


@dataclass
class AuthorityReconciliationResult:
    """
    Authoritative safety state outcome strictly enforcing trip monotonicity across partitions.
    """
    tenant_id: str
    converged_state: str
    trip_monotonicity_enforced: bool
    previous_local_epoch: int
    previous_remote_epoch: int
    reconciled_epoch: int
    active_permits_revoked: int
    reason: str
    reconciled_at: float = field(default_factory=time.time)
    reconciliation_hash: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.reconciliation_hash:
            self.reconciliation_hash = self.compute_hash()

    def compute_hash(self) -> str:
        payload = {
            "active_permits_revoked": self.active_permits_revoked,
            "converged_state": self.converged_state,
            "previous_local_epoch": self.previous_local_epoch,
            "previous_remote_epoch": self.previous_remote_epoch,
            "reconciled_epoch": self.reconciled_epoch,
            "tenant_id": self.tenant_id,
            "trip_monotonicity_enforced": self.trip_monotonicity_enforced,
        }
        return CanonicalCodec.hash(payload, CanonicalDomainTag.RECONCILER)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "converged_state": self.converged_state,
            "trip_monotonicity_enforced": self.trip_monotonicity_enforced,
            "previous_local_epoch": self.previous_local_epoch,
            "previous_remote_epoch": self.previous_remote_epoch,
            "reconciled_epoch": self.reconciled_epoch,
            "active_permits_revoked": self.active_permits_revoked,
            "reason": self.reason,
            "reconciled_at": self.reconciled_at,
            "reconciliation_hash": self.reconciliation_hash,
        }


class MeshReconciler:
    """
    Deterministic reconciliation coordinator resolving divergence between partitioned gossip
    mesh clusters and shard replicas.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._reconciliations: List[Dict[str, Any]] = []
        self._trips_preserved_count: int = 0
        self._concurrent_spans_retained: int = 0
        self._active_conflicts: int = 0

    def diff(
        self,
        local_state: Dict[str, Any],
        remote_state: Dict[str, Any],
        tenant_id: str = "default",
    ) -> ReconciliationPlan:
        """
        Analyzes vector clocks, divergent keys, and sequence intervals between two replicas.
        """
        with self._lock:
            vc_local = local_state.get("vector_clock", {})
            vc_remote = remote_state.get("vector_clock", {})

            verdict = compare_vector_clocks(vc_local, vc_remote)
            merged_vc = merge_vector_clocks(vc_local, vc_remote)

            # Discover divergent keys
            keys_local = set(local_state.get("keys", {}).keys()) if isinstance(local_state.get("keys"), dict) else set(local_state.keys())
            keys_remote = set(remote_state.get("keys", {}).keys()) if isinstance(remote_state.get("keys"), dict) else set(remote_state.keys())
            divergent = sorted(keys_local.symmetric_difference(keys_remote))

            # Missing events estimate
            max_seq_local = local_state.get("max_sequence", 0)
            max_seq_remote = remote_state.get("max_sequence", 0)
            missing_local = max(0, max_seq_remote - max_seq_local)
            missing_remote = max(0, max_seq_local - max_seq_remote)

            conflict = (verdict == LatticeVerdict.CONCURRENT) or bool(divergent)

            actions: List[str] = []
            if verdict == LatticeVerdict.DOMINATES:
                actions.append("PULL_REMOTE_FROM_LOCAL")
            elif verdict == LatticeVerdict.DOMINATED:
                actions.append("FAST_FORWARD_LOCAL_TO_REMOTE")
            elif verdict == LatticeVerdict.CONCURRENT:
                actions.append("RESOLVE_CONCURRENT_LATTICE_PRESERVE_PROVENANCE")
                self._active_conflicts += 1
            else:
                actions.append("NO_OP_IN_SYNC")

            pid = f"plan-{secrets.token_hex(6)}"
            plan = ReconciliationPlan(
                plan_id=pid,
                tenant_id=tenant_id,
                verdict=verdict,
                divergent_keys=divergent,
                missing_local_events=missing_local,
                missing_remote_events=missing_remote,
                conflict_detected=conflict,
                merged_vector_clock=merged_vc,
                action_items=actions,
            )
            return plan

    def merge_evidence(
        self,
        local_events: List[Dict[str, Any]],
        remote_events: List[Dict[str, Any]],
        tenant_id: str = "default",
    ) -> MergeResult:
        """
        Merges telemetry event streams, deduplicating identical events and preserving
        provenance metadata for concurrent conflict events.
        """
        start_ns = time.perf_counter_ns()
        with self._lock:
            seen_hashes: Set[str] = set()
            merged: List[Dict[str, Any]] = []
            dedup_count = 0
            concurrent_retained = 0
            cumulative_vc: Dict[str, int] = {}

            # Process all events
            all_raw = list(local_events) + list(remote_events)

            for evt in all_raw:
                # Canonical representation for deduplication
                evt_id = evt.get("span_id") or evt.get("id") or secrets.token_hex(4)
                turn = evt.get("turn_idx", evt.get("turn", 0))
                agent = evt.get("agent_id") or evt.get("agent", "unknown")
                payload = evt.get("payload", {})
                vc = evt.get("vector_clock", {})

                # Merge into cumulative vector clock
                if isinstance(vc, dict):
                    cumulative_vc = merge_vector_clocks(cumulative_vc, vc)

                # Compute content digest
                evt_digest = evt.get("digest") or CanonicalCodec.hash({
                    "agent": agent,
                    "id": evt_id,
                    "payload": payload,
                    "turn": turn,
                }, CanonicalDomainTag.RECONCILER)

                if evt_digest in seen_hashes:
                    dedup_count += 1
                    continue

                seen_hashes.add(evt_digest)

                # Check if this span was part of a concurrent branch
                is_concurrent = evt.get("is_concurrent_fork", False)
                origin_peer = evt.get("origin_peer", "peer_local")

                if is_concurrent:
                    concurrent_retained += 1
                    self._concurrent_spans_retained += 1

                retained_evt = dict(evt)
                retained_evt["digest"] = evt_digest
                retained_evt["provenance_peer"] = origin_peer
                retained_evt["is_concurrent_fork"] = is_concurrent
                merged.append(retained_evt)

            # Sort deterministically by turn_idx, then timestamp, then digest
            merged.sort(key=lambda x: (
                x.get("turn_idx", x.get("turn", 0)),
                x.get("timestamp", 0.0),
                x.get("digest", "")
            ))

            convergence_hash = CanonicalCodec.hash({
                "count": len(merged),
                "cumulative_vc": cumulative_vc,
                "first_digest": merged[0]["digest"] if merged else "",
                "last_digest": merged[-1]["digest"] if merged else "",
                "tenant_id": tenant_id,
            }, CanonicalDomainTag.RECONCILER)

            elapsed_us = (time.perf_counter_ns() - start_ns) / 1000.0

            return MergeResult(
                merged_spans=merged,
                total_spans=len(merged),
                deduplicated_count=dedup_count,
                concurrent_conflicts_retained=concurrent_retained,
                merged_vector_clock=cumulative_vc,
                convergence_hash=convergence_hash,
                execution_latency_us=elapsed_us,
            )

    def reconcile_authority(
        self,
        local_authority: Dict[str, Any],
        remote_authority: Dict[str, Any],
        tenant_id: str = "default",
    ) -> AuthorityReconciliationResult:
        """
        Reconciles safety authority states across network partitions.
        Enforces Trip Monotonicity: if ANY partition tripped, converged state MUST be TRIPPED.
        Advances authoritative epoch monotonically.
        """
        with self._lock:
            local_state = str(local_authority.get("state", "NOMINAL")).upper()
            remote_state = str(remote_authority.get("state", "NOMINAL")).upper()

            epoch_l = int(local_authority.get("epoch", 1))
            epoch_r = int(remote_authority.get("epoch", 1))

            trip_enforced = False
            converged_state = "NOMINAL"
            reason = "Authority states in agreement"

            # Trip Monotonicity Invariant: TRIPPED dominates all other states
            if local_state == "TRIPPED" or remote_state == "TRIPPED":
                converged_state = "TRIPPED"
                trip_enforced = True
                self._trips_preserved_count += 1
                if local_state != remote_state:
                    reason = f"Trip monotonicity enforced: preserved TRIPPED state across partition divergence (Local={local_state}, Remote={remote_state})"
                else:
                    reason = "Both partitions agreed on TRIPPED state"
            elif local_state == "DEGRADED" or remote_state == "DEGRADED":
                converged_state = "DEGRADED"
                reason = "Partition converged to DEGRADED state"
            else:
                converged_state = "NOMINAL"

            # Monotonic epoch calculation
            # If states diverged or either had conflict, step up epoch by 1 to fence out stale pre-partition permits
            states_differ = (local_state != remote_state)
            reconciled_epoch = max(epoch_l, epoch_r) + (1 if states_differ else 0)

            # Invalidate/revoke permits from stale epochs
            revoked_count = 0
            if states_differ:
                revoked_count = int(local_authority.get("active_permits", 0)) + int(remote_authority.get("active_permits", 0))

            res = AuthorityReconciliationResult(
                tenant_id=tenant_id,
                converged_state=converged_state,
                trip_monotonicity_enforced=trip_enforced,
                previous_local_epoch=epoch_l,
                previous_remote_epoch=epoch_r,
                reconciled_epoch=reconciled_epoch,
                active_permits_revoked=revoked_count,
                reason=reason,
            )

            self._reconciliations.append(res.to_dict())
            return res

    def status(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Returns diagnostic statistics of partition reconciliations.
        """
        with self._lock:
            reconciliations = [
                r for r in self._reconciliations
                if not tenant_id or r.get("tenant_id") == tenant_id
            ]

            return {
                "total_reconciliations": len(reconciliations),
                "trips_preserved_count": self._trips_preserved_count,
                "concurrent_spans_retained": self._concurrent_spans_retained,
                "active_conflicts": self._active_conflicts,
                "last_reconciliation": reconciliations[-1] if reconciliations else None,
                "status": "CONVERGED" if self._active_conflicts == 0 else "RESOLVING_CONFLICTS",
            }

    def reset(self) -> None:
        """
        Resets all in-memory reconciliation history.
        """
        with self._lock:
            self._reconciliations.clear()
            self._trips_preserved_count = 0
            self._concurrent_spans_retained = 0
            self._active_conflicts = 0


# Global singleton instance
global_mesh_reconciler = MeshReconciler()
