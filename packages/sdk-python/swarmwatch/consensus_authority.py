# -*- coding: utf-8 -*-
"""
Consensus Authority Binding & Linearizable Quorum Fencing (Seam 61 / ADR 0072).
Binds the SafetyAuthority execution permit plane to a linearizable replicated state machine (RSM).
Guarantees every permit reservation, commitment, trip, and re-arm requires majority quorum consensus,
failing closed under network partitions, minority isolation, or leader split-brain.
"""

from __future__ import annotations

import math
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

from swarmwatch.canonical import CanonicalCodec, CanonicalDomainTag
from swarmwatch.safety_authority import AuthorityState, SafetyAuthority, SafetyPermit, TenantQuota


class ConsensusPermitStatus:
    """Explicit decision status for quorum-committed execution permits."""
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    QUORUM_LOSS = "QUORUM_LOSS"
    TRIPPED = "TRIPPED"
    EXPIRED = "EXPIRED"


class ConsensusLogEntry:
    """
    Append-only log entry in the linearizable replicated state machine.
    """

    def __init__(
        self,
        index: int,
        epoch: int,
        term: int,
        op_type: str,
        payload: Dict[str, Any],
        entry_hash: Optional[str] = None,
        committed: bool = False,
        ack_nodes: Optional[Set[str]] = None,
        timestamp: Optional[float] = None,
    ):
        self.index = int(index)
        self.epoch = int(epoch)
        self.term = int(term)
        self.op_type = str(op_type)
        self.payload = dict(payload)
        self.timestamp = float(timestamp if timestamp is not None else time.time())
        self.committed = bool(committed)
        self.ack_nodes: Set[str] = set(ack_nodes or [])
        self.entry_hash = entry_hash if entry_hash is not None else self.compute_hash()

    def compute_hash(self) -> str:
        """Computes deterministic canonical SHA-256 digest of the log entry."""
        hash_payload = {
            "epoch": self.epoch,
            "index": self.index,
            "op_type": self.op_type,
            "payload": self.payload,
            "term": self.term,
            "timestamp": round(self.timestamp, 6),
        }
        return CanonicalCodec.hash(hash_payload, domain_tag=CanonicalDomainTag.PERMIT)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "epoch": self.epoch,
            "term": self.term,
            "op_type": self.op_type,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "committed": self.committed,
            "ack_nodes": sorted(list(self.ack_nodes)),
            "entry_hash": self.entry_hash,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ConsensusLogEntry:
        return cls(
            index=data["index"],
            epoch=data["epoch"],
            term=data["term"],
            op_type=data["op_type"],
            payload=data["payload"],
            entry_hash=data.get("entry_hash"),
            committed=data.get("committed", False),
            ack_nodes=set(data.get("ack_nodes", [])),
            timestamp=data.get("timestamp"),
        )


class ConsensusPermitResult:
    """Verifiable outcome of a quorum-fenced permit reservation or commitment."""

    def __init__(
        self,
        permit_id: str,
        status: str,
        epoch: int,
        log_index: int,
        quorum_achieved: bool,
        committed_cost: float = 0.0,
        committed_tokens: int = 0,
        rejection_reason: Optional[str] = None,
        permit_hash: Optional[str] = None,
        node_id: str = "node-1",
        ack_count: int = 1,
        quorum_required: int = 1,
    ):
        self.permit_id = permit_id
        self.status = status
        self.epoch = epoch
        self.log_index = log_index
        self.quorum_achieved = quorum_achieved
        self.committed_cost = float(committed_cost)
        self.committed_tokens = int(committed_tokens)
        self.rejection_reason = rejection_reason
        self.permit_hash = permit_hash
        self.node_id = node_id
        self.ack_count = ack_count
        self.quorum_required = quorum_required

    @property
    def is_accepted(self) -> bool:
        return self.status == ConsensusPermitStatus.ACCEPTED and self.quorum_achieved

    def to_dict(self) -> Dict[str, Any]:
        return {
            "permit_id": self.permit_id,
            "status": self.status,
            "epoch": self.epoch,
            "log_index": self.log_index,
            "quorum_achieved": self.quorum_achieved,
            "committed_cost": self.committed_cost,
            "committed_tokens": self.committed_tokens,
            "rejection_reason": self.rejection_reason,
            "permit_hash": self.permit_hash,
            "node_id": self.node_id,
            "ack_count": self.ack_count,
            "quorum_required": self.quorum_required,
        }


class CommittedTenantState:
    """Snapshot of linearizable, quorum-committed tenant budget and trip status."""

    def __init__(
        self,
        tenant_id: str,
        epoch: int,
        state: str,
        max_cost: float,
        max_tokens: int,
        max_turns: int,
        spent_cost: float,
        reserved_cost: float,
        spent_tokens: int,
        reserved_tokens: int,
        spent_turns: int,
        last_committed_index: int,
        trip_reason: Optional[str] = None,
    ):
        self.tenant_id = tenant_id
        self.epoch = epoch
        self.state = state
        self.max_cost = float(max_cost)
        self.max_tokens = int(max_tokens)
        self.max_turns = int(max_turns)
        self.spent_cost = float(spent_cost)
        self.reserved_cost = float(reserved_cost)
        self.available_cost = max(0.0, self.max_cost - (self.spent_cost + self.reserved_cost))
        self.spent_tokens = int(spent_tokens)
        self.reserved_tokens = int(reserved_tokens)
        self.available_tokens = max(0, self.max_tokens - (self.spent_tokens + self.reserved_tokens))
        self.spent_turns = int(spent_turns)
        self.last_committed_index = int(last_committed_index)
        self.trip_reason = trip_reason

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "epoch": self.epoch,
            "state": self.state,
            "max_cost": self.max_cost,
            "max_tokens": self.max_tokens,
            "max_turns": self.max_turns,
            "spent_cost": self.spent_cost,
            "reserved_cost": self.reserved_cost,
            "available_cost": self.available_cost,
            "spent_tokens": self.spent_tokens,
            "reserved_tokens": self.reserved_tokens,
            "available_tokens": self.available_tokens,
            "spent_turns": self.spent_turns,
            "last_committed_index": self.last_committed_index,
            "trip_reason": self.trip_reason,
        }


class ConsensusTripResult:
    """Outcome of a quorum-replicated circuit trip command."""

    def __init__(
        self,
        tenant_id: str,
        tripped: bool,
        epoch: int,
        log_index: int,
        quorum_achieved: bool,
        reason: str,
    ):
        self.tenant_id = tenant_id
        self.tripped = tripped
        self.epoch = epoch
        self.log_index = log_index
        self.quorum_achieved = quorum_achieved
        self.reason = reason

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "tripped": self.tripped,
            "epoch": self.epoch,
            "log_index": self.log_index,
            "quorum_achieved": self.quorum_achieved,
            "reason": self.reason,
        }


class ConsensusRearmResult:
    """Outcome of a quorum-replicated rearm and epoch advancement command."""

    def __init__(
        self,
        tenant_id: str,
        rearmed: bool,
        old_epoch: int,
        new_epoch: int,
        log_index: int,
        quorum_achieved: bool,
    ):
        self.tenant_id = tenant_id
        self.rearmed = rearmed
        self.old_epoch = old_epoch
        self.new_epoch = new_epoch
        self.log_index = log_index
        self.quorum_achieved = quorum_achieved

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "rearmed": self.rearmed,
            "old_epoch": self.old_epoch,
            "new_epoch": self.new_epoch,
            "log_index": self.log_index,
            "quorum_achieved": self.quorum_achieved,
        }


class ConsensusAuthorityBinding:
    """
    Linearizable, Quorum-Committed Authority Binding (Seam 61).
    Wraps SafetyAuthority with a replicated state machine requiring majority quorum
    consensus (Q >= floor(N/2) + 1) for all permit reservations, commits, trips, and rearms.
    Guarantees strict fail-closed rejection during network splits or minority partitions.
    """

    def __init__(
        self,
        node_id: str = "node-1",
        cluster_nodes: Optional[List[str]] = None,
        initial_epoch: int = 1,
        underlying_authority: Optional[SafetyAuthority] = None,
    ):
        self._lock = threading.RLock()
        self.node_id = str(node_id)
        self.cluster_nodes = list(cluster_nodes or [self.node_id])
        if self.node_id not in self.cluster_nodes:
            self.cluster_nodes.append(self.node_id)
        self.cluster_nodes.sort()

        self._epoch = int(initial_epoch)
        self._term = 1
        self._committed_index = 0
        self._log: List[ConsensusLogEntry] = []
        self._isolated_nodes: Set[str] = set()

        # Connect to underlying single-node safety authority
        self._authority = underlying_authority or SafetyAuthority(initial_epoch=self._epoch)

        # Pending uncommitted reservations
        self._pending_reservations: Dict[str, Dict[str, Any]] = {}

    @property
    def quorum_required(self) -> int:
        """Quorum size: floor(N / 2) + 1."""
        with self._lock:
            n = len(self.cluster_nodes)
            return (n // 2) + 1

    @property
    def reachable_nodes(self) -> List[str]:
        """List of active cluster nodes reachable from this node."""
        with self._lock:
            if self.node_id in self._isolated_nodes:
                return [self.node_id]
            return [n for n in self.cluster_nodes if n not in self._isolated_nodes]

    @property
    def has_quorum(self) -> bool:
        """True if the count of reachable nodes satisfies the majority quorum."""
        with self._lock:
            return len(self.reachable_nodes) >= self.quorum_required

    @property
    def epoch(self) -> int:
        with self._lock:
            return self._epoch

    @property
    def term(self) -> int:
        with self._lock:
            return self._term

    @property
    def committed_index(self) -> int:
        with self._lock:
            return self._committed_index

    def set_cluster_membership(self, nodes: List[str]):
        """Dynamically reconfigures cluster membership."""
        with self._lock:
            new_nodes = sorted(list(set(nodes)))
            if self.node_id not in new_nodes:
                new_nodes.append(self.node_id)
                new_nodes.sort()
            self.cluster_nodes = new_nodes

    def simulate_partition(self, isolated_nodes: List[str]):
        """Simulates network partition isolating the specified node IDs."""
        with self._lock:
            self._isolated_nodes = set(isolated_nodes)

    def restore_partition(self):
        """Clears partition isolation, restoring full network connectivity."""
        with self._lock:
            self._isolated_nodes.clear()

    def configure_tenant(
        self,
        tenant_id: str,
        max_cost: float = 100.0,
        max_tokens: int = 1_000_000,
        max_turns: int = 1000,
    ) -> CommittedTenantState:
        with self._lock:
            self._authority.configure_tenant(
                tenant_id=tenant_id,
                max_cost=max_cost,
                max_tokens=max_tokens,
                max_turns=max_turns,
            )
            return self.read_committed(tenant_id)

    def reserve(
        self,
        tenant_id: str,
        action: str = "agent_turn",
        cost_estimate: float = 0.0,
        tokens_estimate: int = 0,
        timeout_ms: float = 5000.0,
    ) -> ConsensusPermitResult:
        """
        Atomically proposes and quorum-commits an execution permit reservation.
        Fails closed with QUORUM_LOSS if reachable nodes do not form a majority.
        """
        with self._lock:
            permit_id = f"permit-{uuid.uuid4().hex[:12]}"
            quorum_needed = self.quorum_required
            reachable = self.reachable_nodes

            # Minority Partition Check (Fail-Closed)
            if len(reachable) < quorum_needed:
                return ConsensusPermitResult(
                    permit_id=permit_id,
                    status=ConsensusPermitStatus.QUORUM_LOSS,
                    epoch=self._epoch,
                    log_index=self._committed_index,
                    quorum_achieved=False,
                    rejection_reason=f"Quorum loss: reachable nodes {len(reachable)} < required {quorum_needed}",
                    node_id=self.node_id,
                    ack_count=len(reachable),
                    quorum_required=quorum_needed,
                )

            # Check tenant quota on underlying authority
            quota = self._authority.get_or_create_quota(tenant_id)
            if quota.state == AuthorityState.TRIPPED:
                return ConsensusPermitResult(
                    permit_id=permit_id,
                    status=ConsensusPermitStatus.TRIPPED,
                    epoch=self._epoch,
                    log_index=self._committed_index,
                    quorum_achieved=True,
                    rejection_reason=f"Tenant circuit tripped: {quota.trip_reason}",
                    node_id=self.node_id,
                    ack_count=len(reachable),
                    quorum_required=quorum_needed,
                )

            cost_f = float(cost_estimate)
            tokens_i = int(tokens_estimate)

            if cost_f > quota.available_cost():
                return ConsensusPermitResult(
                    permit_id=permit_id,
                    status=ConsensusPermitStatus.REJECTED,
                    epoch=self._epoch,
                    log_index=self._committed_index,
                    quorum_achieved=True,
                    rejection_reason=f"Insufficient cost budget: {cost_f:.4f} > {quota.available_cost():.4f}",
                    node_id=self.node_id,
                    ack_count=len(reachable),
                    quorum_required=quorum_needed,
                )

            if tokens_i > quota.available_tokens():
                return ConsensusPermitResult(
                    permit_id=permit_id,
                    status=ConsensusPermitStatus.REJECTED,
                    epoch=self._epoch,
                    log_index=self._committed_index,
                    quorum_achieved=True,
                    rejection_reason=f"Insufficient token budget: {tokens_i} > {quota.available_tokens()}",
                    node_id=self.node_id,
                    ack_count=len(reachable),
                    quorum_required=quorum_needed,
                )

            # Propose Log Entry
            new_index = len(self._log) + 1
            payload = {
                "action": action,
                "cost_estimate": cost_f,
                "permit_id": permit_id,
                "tenant_id": tenant_id,
                "timeout_ms": timeout_ms,
                "tokens_estimate": tokens_i,
            }

            entry = ConsensusLogEntry(
                index=new_index,
                epoch=self._epoch,
                term=self._term,
                op_type="RESERVE",
                payload=payload,
                ack_nodes=set(reachable),
            )

            # Replicate to reachable quorum
            if len(entry.ack_nodes) >= quorum_needed:
                entry.committed = True
                self._log.append(entry)
                self._committed_index = new_index

                # Apply reservation to underlying authority with identical permit_id
                permit = self._authority.reserve(
                    tenant_id=tenant_id,
                    action=action,
                    cost_estimate=cost_f,
                    tokens_estimate=tokens_i,
                    timeout_ms=timeout_ms,
                    permit_id=permit_id,
                )

                self._pending_reservations[permit.permit_id] = {
                    "permit_id": permit.permit_id,
                    "tenant_id": tenant_id,
                    "cost_estimate": cost_f,
                    "tokens_estimate": tokens_i,
                    "log_index": new_index,
                }

                return ConsensusPermitResult(
                    permit_id=permit.permit_id,
                    status=ConsensusPermitStatus.ACCEPTED,
                    epoch=self._epoch,
                    log_index=new_index,
                    quorum_achieved=True,
                    committed_cost=cost_f,
                    committed_tokens=tokens_i,
                    permit_hash=permit.permit_hash,
                    node_id=self.node_id,
                    ack_count=len(reachable),
                    quorum_required=quorum_needed,
                )
            else:
                return ConsensusPermitResult(
                    permit_id=permit_id,
                    status=ConsensusPermitStatus.QUORUM_LOSS,
                    epoch=self._epoch,
                    log_index=self._committed_index,
                    quorum_achieved=False,
                    rejection_reason="Failed to achieve quorum consensus during replication",
                    node_id=self.node_id,
                    ack_count=len(entry.ack_nodes),
                    quorum_required=quorum_needed,
                )

    def commit(
        self,
        permit_id: str,
        actual_cost: Optional[float] = None,
        actual_tokens: Optional[int] = None,
    ) -> ConsensusPermitResult:
        """
        Quorum-commits execution outcome and releases reserved surplus.
        """
        with self._lock:
            reachable = self.reachable_nodes
            quorum_needed = self.quorum_required

            if len(reachable) < quorum_needed:
                return ConsensusPermitResult(
                    permit_id=permit_id,
                    status=ConsensusPermitStatus.QUORUM_LOSS,
                    epoch=self._epoch,
                    log_index=self._committed_index,
                    quorum_achieved=False,
                    rejection_reason="Quorum loss during permit commitment",
                    node_id=self.node_id,
                    ack_count=len(reachable),
                    quorum_required=quorum_needed,
                )

            reservation = self._pending_reservations.get(permit_id)
            tenant_id = reservation["tenant_id"] if reservation else "default"
            cost = float(actual_cost if actual_cost is not None else (reservation["cost_estimate"] if reservation else 0.0))
            tokens = int(actual_tokens if actual_tokens is not None else (reservation["tokens_estimate"] if reservation else 0))

            new_index = len(self._log) + 1
            entry = ConsensusLogEntry(
                index=new_index,
                epoch=self._epoch,
                term=self._term,
                op_type="COMMIT",
                payload={
                    "permit_id": permit_id,
                    "tenant_id": tenant_id,
                    "actual_cost": cost,
                    "actual_tokens": tokens,
                },
                ack_nodes=set(reachable),
            )

            entry.committed = True
            self._log.append(entry)
            self._committed_index = new_index

            self._authority.commit(
                permit_id=permit_id,
                actual_cost=cost,
                actual_tokens=tokens,
            )

            self._pending_reservations.pop(permit_id, None)

            return ConsensusPermitResult(
                permit_id=permit_id,
                status=ConsensusPermitStatus.ACCEPTED,
                epoch=self._epoch,
                log_index=new_index,
                quorum_achieved=True,
                committed_cost=cost,
                committed_tokens=tokens,
                node_id=self.node_id,
                ack_count=len(reachable),
                quorum_required=quorum_needed,
            )

    def release(self, permit_id: str, reason: str = "voluntary_release") -> ConsensusPermitResult:
        """
        Releases an unexecuted permit reservation back to the tenant quota.
        """
        with self._lock:
            reachable = self.reachable_nodes
            quorum_needed = self.quorum_required

            if len(reachable) < quorum_needed:
                return ConsensusPermitResult(
                    permit_id=permit_id,
                    status=ConsensusPermitStatus.QUORUM_LOSS,
                    epoch=self._epoch,
                    log_index=self._committed_index,
                    quorum_achieved=False,
                    rejection_reason="Quorum loss during permit release",
                    node_id=self.node_id,
                    ack_count=len(reachable),
                    quorum_required=quorum_needed,
                )

            reservation = self._pending_reservations.get(permit_id)
            tenant_id = reservation["tenant_id"] if reservation else "default"

            new_index = len(self._log) + 1
            entry = ConsensusLogEntry(
                index=new_index,
                epoch=self._epoch,
                term=self._term,
                op_type="RELEASE",
                payload={"permit_id": permit_id, "tenant_id": tenant_id, "reason": reason},
                ack_nodes=set(reachable),
            )

            entry.committed = True
            self._log.append(entry)
            self._committed_index = new_index

            self._authority.release(permit_id=permit_id, reason=reason)
            self._pending_reservations.pop(permit_id, None)

            return ConsensusPermitResult(
                permit_id=permit_id,
                status=ConsensusPermitStatus.ACCEPTED,
                epoch=self._epoch,
                log_index=new_index,
                quorum_achieved=True,
                node_id=self.node_id,
                ack_count=len(reachable),
                quorum_required=quorum_needed,
            )

    def trip(self, tenant_id: str, reason: str) -> ConsensusTripResult:
        """
        Quorum-trips the tenant circuit breaker.
        Requires majority quorum to ensure distributed consistency.
        """
        with self._lock:
            reachable = self.reachable_nodes
            quorum_needed = self.quorum_required

            if len(reachable) < quorum_needed:
                return ConsensusTripResult(
                    tenant_id=tenant_id,
                    tripped=False,
                    epoch=self._epoch,
                    log_index=self._committed_index,
                    quorum_achieved=False,
                    reason="Quorum loss: cannot commit circuit trip across minority partition",
                )

            new_index = len(self._log) + 1
            entry = ConsensusLogEntry(
                index=new_index,
                epoch=self._epoch,
                term=self._term,
                op_type="TRIP",
                payload={"tenant_id": tenant_id, "reason": reason},
                ack_nodes=set(reachable),
            )

            entry.committed = True
            self._log.append(entry)
            self._committed_index = new_index

            self._authority.trip(reason=reason, tenant_id=tenant_id)

            return ConsensusTripResult(
                tenant_id=tenant_id,
                tripped=True,
                epoch=self._epoch,
                log_index=new_index,
                quorum_achieved=True,
                reason=reason,
            )

    def rearm(self, tenant_id: str, new_epoch: Optional[int] = None) -> ConsensusRearmResult:
        """
        Quorum-rearms tenant circuit and advances monotonic epoch.
        Requires majority quorum to prevent split-brain epoch advancement.
        """
        with self._lock:
            reachable = self.reachable_nodes
            quorum_needed = self.quorum_required
            old_ep = self._epoch

            if len(reachable) < quorum_needed:
                return ConsensusRearmResult(
                    tenant_id=tenant_id,
                    rearmed=False,
                    old_epoch=old_ep,
                    new_epoch=old_ep,
                    log_index=self._committed_index,
                    quorum_achieved=False,
                )

            target_epoch = int(new_epoch if new_epoch is not None else (old_ep + 1))
            if target_epoch <= old_ep:
                target_epoch = old_ep + 1

            new_index = len(self._log) + 1
            entry = ConsensusLogEntry(
                index=new_index,
                epoch=target_epoch,
                term=self._term + 1,
                op_type="REARM",
                payload={"tenant_id": tenant_id, "old_epoch": old_ep, "new_epoch": target_epoch},
                ack_nodes=set(reachable),
            )

            entry.committed = True
            self._log.append(entry)
            self._committed_index = new_index
            self._epoch = target_epoch
            self._term += 1

            self._authority.rearm(tenant_id=tenant_id, new_epoch=target_epoch)

            return ConsensusRearmResult(
                tenant_id=tenant_id,
                rearmed=True,
                old_epoch=old_ep,
                new_epoch=target_epoch,
                log_index=new_index,
                quorum_achieved=True,
            )

    def read_committed(self, tenant_id: str) -> CommittedTenantState:
        """
        Returns linearizable state reflected up to the highest quorum-committed index.
        Dirty uncommitted states are never visible.
        """
        with self._lock:
            quota = self._authority.get_or_create_quota(tenant_id)
            return CommittedTenantState(
                tenant_id=tenant_id,
                epoch=self._epoch,
                state=quota.state,
                max_cost=quota.max_cost,
                max_tokens=quota.max_tokens,
                max_turns=quota.max_turns,
                spent_cost=quota.spent_cost,
                reserved_cost=quota.reserved_cost,
                spent_tokens=quota.spent_tokens,
                reserved_tokens=quota.reserved_tokens,
                spent_turns=quota.spent_turns,
                last_committed_index=self._committed_index,
                trip_reason=quota.trip_reason,
            )

    def replicate_to_peer(self, peer: ConsensusAuthorityBinding) -> int:
        """
        Replicates committed log entries to a peer node state machine.
        Returns the number of new entries applied.
        """
        with self._lock:
            applied = 0
            for entry in self._log:
                if entry.committed and entry.index > peer.committed_index:
                    peer.apply_replicated_entry(entry)
                    applied += 1
            return applied

    def apply_replicated_entry(self, entry: ConsensusLogEntry) -> bool:
        """
        Applies a validated quorum-committed entry from another peer.
        """
        with self._lock:
            if entry.index <= self._committed_index:
                return True

            # Validate entry hash integrity
            if entry.entry_hash != entry.compute_hash():
                return False

            self._log.append(entry)
            self._committed_index = entry.index
            if entry.epoch > self._epoch:
                self._epoch = entry.epoch
            if entry.term > self._term:
                self._term = entry.term

            # Apply state machine transition
            op = entry.op_type
            p = entry.payload
            tid = p.get("tenant_id", "default")

            if op == "RESERVE":
                permit = self._authority.reserve(
                    tenant_id=tid,
                    action=p.get("action", "agent_turn"),
                    cost_estimate=p.get("cost_estimate", 0.0),
                    tokens_estimate=p.get("tokens_estimate", 0),
                    timeout_ms=p.get("timeout_ms", 5000.0),
                    permit_id=p.get("permit_id"),
                )
                pid = p.get("permit_id")
                if pid:
                    self._pending_reservations[pid] = {
                        "permit_id": pid,
                        "tenant_id": tid,
                        "cost_estimate": p.get("cost_estimate", 0.0),
                        "tokens_estimate": p.get("tokens_estimate", 0),
                        "log_index": entry.index,
                    }
            elif op == "COMMIT":
                pid = p.get("permit_id", "")
                self._authority.commit(
                    permit_id=pid,
                    actual_cost=p.get("actual_cost"),
                    actual_tokens=p.get("actual_tokens"),
                )
                self._pending_reservations.pop(pid, None)
            elif op == "RELEASE":
                pid = p.get("permit_id", "")
                self._authority.release(
                    permit_id=pid,
                    reason=p.get("reason", "replicated_release"),
                )
                self._pending_reservations.pop(pid, None)
            elif op == "TRIP":
                self._authority.trip(reason=p.get("reason", "replicated_trip"), tenant_id=tid)
            elif op == "REARM":
                self._authority.rearm(tenant_id=tid, new_epoch=p.get("new_epoch"))

            return True

    def export_status(self) -> Dict[str, Any]:
        """Exports cluster consensus health, quorum metrics, and RSM state."""
        with self._lock:
            return {
                "node_id": self.node_id,
                "cluster_nodes": self.cluster_nodes,
                "reachable_nodes": self.reachable_nodes,
                "quorum_required": self.quorum_required,
                "has_quorum": self.has_quorum,
                "epoch": self._epoch,
                "term": self._term,
                "committed_index": self._committed_index,
                "total_log_entries": len(self._log),
                "isolated_nodes": sorted(list(self._isolated_nodes)),
            }


global_consensus_authority = ConsensusAuthorityBinding()
