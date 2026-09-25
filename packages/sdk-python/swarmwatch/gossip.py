"""
SwarmWatch Decentralized Epidemic Gossip Telemetry Mesh Sync (Seam 45 / ADR 0056).

Provides in-process, peer-to-peer epidemic gossip protocols, Vector Clock causality
tracking, and anti-entropy synchronization across distributed agent nodes with
zero central brokers and zero LLM token consumption.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import random
import time
from typing import Any, Dict, List, Optional
import uuid


class MeshNodeStatus(str, Enum):
    """Categorical operational state of a mesh node peer."""
    ACTIVE = "active"
    SUSPECT = "suspect"
    DEAD = "dead"


class MeshConvergenceHealth(str, Enum):
    """Categorical evaluation of overall mesh state coherence."""
    CONVERGED = "converged"
    SYNCING = "syncing"
    PARTITIONED = "partitioned"


@dataclass
class MeshNode:
    """An autonomous peer participating in the decentralized telemetry mesh."""
    node_id: str
    endpoint: str
    status: MeshNodeStatus = MeshNodeStatus.ACTIVE
    last_heartbeat: float = field(default_factory=time.time)
    generation: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "endpoint": self.endpoint,
            "status": self.status.value,
            "last_heartbeat": self.last_heartbeat,
            "generation": self.generation,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MeshNode":
        status_val = data.get("status", "active")
        status = MeshNodeStatus(status_val) if isinstance(status_val, str) else status_val
        return cls(
            node_id=data["node_id"],
            endpoint=data.get("endpoint", ""),
            status=status,
            last_heartbeat=float(data.get("last_heartbeat", time.time())),
            generation=int(data.get("generation", 1)),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class VectorClock:
    """
    Monotonically increasing vector clock mapping {node_id: counter}.
    Enforces causal event ordering and partial orders across distributed nodes.
    """
    clock: Dict[str, int] = field(default_factory=dict)

    def get(self, node_id: str) -> int:
        return self.clock.get(node_id, 0)

    def set(self, node_id: str, counter: int) -> None:
        self.clock[node_id] = max(0, counter)

    def increment(self, node_id: str) -> int:
        current = self.clock.get(node_id, 0) + 1
        self.clock[node_id] = current
        return current

    def merge(self, other: "VectorClock") -> "VectorClock":
        """Returns a new VectorClock combining the component-wise maxima."""
        merged: Dict[str, int] = dict(self.clock)
        for k, v in other.clock.items():
            merged[k] = max(merged.get(k, 0), v)
        return VectorClock(clock=merged)

    def dominates(self, other: "VectorClock") -> bool:
        """
        Returns True if self causally dominates other:
        self[k] >= other[k] for all k, and self[k] > other[k] for at least one k.
        """
        all_keys = set(self.clock.keys()).union(other.clock.keys())
        greater_found = False
        for k in all_keys:
            v_self = self.clock.get(k, 0)
            v_other = other.clock.get(k, 0)
            if v_self < v_other:
                return False
            if v_self > v_other:
                greater_found = True
        return greater_found

    def is_concurrent_with(self, other: "VectorClock") -> bool:
        """Returns True if self and other are concurrent (neither dominates the other and not equal)."""
        if self.clock == other.clock:
            return False
        return not self.dominates(other) and not other.dominates(self)

    def copy(self) -> "VectorClock":
        return VectorClock(clock=dict(self.clock))

    def to_dict(self) -> Dict[str, int]:
        return dict(self.clock)

    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> "VectorClock":
        return cls(clock={str(k): int(v) for k, v in data.items()})


@dataclass
class GossipDigest:
    """Compact state summary exchanged during periodic Anti-Entropy rounds."""
    sender_id: str
    vector_clock: Dict[str, int]
    known_span_ids: List[str]
    known_trip_ids: List[str]
    known_watermark_ids: List[str]
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sender_id": self.sender_id,
            "vector_clock": dict(self.vector_clock),
            "known_span_ids": list(self.known_span_ids),
            "known_trip_ids": list(self.known_trip_ids),
            "known_watermark_ids": list(self.known_watermark_ids),
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GossipDigest":
        return cls(
            sender_id=data["sender_id"],
            vector_clock={str(k): int(v) for k, v in data.get("vector_clock", {}).items()},
            known_span_ids=list(data.get("known_span_ids", [])),
            known_trip_ids=list(data.get("known_trip_ids", [])),
            known_watermark_ids=list(data.get("known_watermark_ids", [])),
            timestamp=float(data.get("timestamp", time.time())),
        )


@dataclass
class GossipDelta:
    """Reconciliation payload containing missing telemetry records to achieve convergence."""
    sender_id: str
    target_id: str
    vector_clock: Dict[str, int]
    spans: List[Dict[str, Any]] = field(default_factory=list)
    circuit_trips: List[Dict[str, Any]] = field(default_factory=list)
    watermarks: List[Dict[str, Any]] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sender_id": self.sender_id,
            "target_id": self.target_id,
            "vector_clock": dict(self.vector_clock),
            "spans": list(self.spans),
            "circuit_trips": list(self.circuit_trips),
            "watermarks": list(self.watermarks),
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GossipDelta":
        return cls(
            sender_id=data["sender_id"],
            target_id=data.get("target_id", ""),
            vector_clock={str(k): int(v) for k, v in data.get("vector_clock", {}).items()},
            spans=list(data.get("spans", [])),
            circuit_trips=list(data.get("circuit_trips", [])),
            watermarks=list(data.get("watermarks", [])),
            timestamp=float(data.get("timestamp", time.time())),
        )


@dataclass
class MeshStatusReport:
    """Comprehensive diagnostic status of the decentralized telemetry mesh."""
    local_node_id: str
    local_endpoint: str
    active_peers_count: int
    suspect_peers_count: int
    dead_peers_count: int
    total_synced_spans: int
    total_synced_trips: int
    total_synced_watermarks: int
    gossip_rounds_completed: int
    convergence_health: MeshConvergenceHealth
    vector_clock: Dict[str, int]
    peers: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "local_node_id": self.local_node_id,
            "local_endpoint": self.local_endpoint,
            "active_peers_count": self.active_peers_count,
            "suspect_peers_count": self.suspect_peers_count,
            "dead_peers_count": self.dead_peers_count,
            "total_synced_spans": self.total_synced_spans,
            "total_synced_trips": self.total_synced_trips,
            "total_synced_watermarks": self.total_synced_watermarks,
            "gossip_rounds_completed": self.gossip_rounds_completed,
            "convergence_health": self.convergence_health.value,
            "vector_clock": dict(self.vector_clock),
            "peers": list(self.peers),
        }


class GossipMeshManager:
    """
    Deep module coordinating decentralized peer-to-peer epidemic gossip,
    Vector Clock causality tracking, and anti-entropy synchronization with
    zero external brokers and zero LLM token costs.
    """

    def __init__(
        self,
        node_id: Optional[str] = None,
        endpoint: str = "http://localhost:8765",
        fanout: int = 3,
        heartbeat_timeout: float = 30.0,
    ) -> None:
        self.node_id: str = node_id or f"node-{uuid.uuid4().hex[:8]}"
        self.endpoint: str = endpoint
        self.fanout: int = max(1, fanout)
        self.heartbeat_timeout: float = heartbeat_timeout

        self.vector_clock: VectorClock = VectorClock(clock={self.node_id: 0})
        self.peers: Dict[str, MeshNode] = {}
        self.spans: Dict[str, Dict[str, Any]] = {}
        self.circuit_trips: Dict[str, Dict[str, Any]] = {}
        self.watermarks: Dict[str, Dict[str, Any]] = {}
        self.gossip_rounds_completed: int = 0

    def register_peer(
        self,
        node_id: str,
        endpoint: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> MeshNode:
        """Registers or refreshes an active peer in the mesh roster."""
        if node_id == self.node_id:
            # Self is not stored as an external peer
            self.endpoint = endpoint
            if metadata:
                self.vector_clock.set(node_id, max(self.vector_clock.get(node_id), 0))
            return MeshNode(node_id=self.node_id, endpoint=self.endpoint)

        existing = self.peers.get(node_id)
        if existing:
            existing.endpoint = endpoint
            existing.status = MeshNodeStatus.ACTIVE
            existing.last_heartbeat = time.time()
            if metadata:
                existing.metadata.update(metadata)
            return existing

        new_node = MeshNode(
            node_id=node_id,
            endpoint=endpoint,
            status=MeshNodeStatus.ACTIVE,
            last_heartbeat=time.time(),
            metadata=dict(metadata or {}),
        )
        self.peers[node_id] = new_node
        if node_id not in self.vector_clock.clock:
            self.vector_clock.clock[node_id] = 0
        return new_node

    def deregister_peer(self, node_id: str) -> bool:
        """Removes a peer completely from the mesh roster."""
        if node_id in self.peers:
            del self.peers[node_id]
            return True
        return False

    def mark_peer_suspect(self, node_id: str) -> None:
        """Marks a peer as suspect upon missing heartbeats."""
        if node_id in self.peers:
            self.peers[node_id].status = MeshNodeStatus.SUSPECT

    def mark_peer_dead(self, node_id: str) -> None:
        """Marks a peer as dead upon persistent failure."""
        if node_id in self.peers:
            self.peers[node_id].status = MeshNodeStatus.DEAD

    def list_peers(self, include_dead: bool = False) -> List[MeshNode]:
        """Lists active and suspect peers (and optionally dead peers)."""
        now = time.time()
        res: List[MeshNode] = []
        for p in self.peers.values():
            # Auto-check heartbeat expiration
            if p.status == MeshNodeStatus.ACTIVE and (now - p.last_heartbeat) > self.heartbeat_timeout:
                p.status = MeshNodeStatus.SUSPECT
            if include_dead or p.status != MeshNodeStatus.DEAD:
                res.append(p)
        return res

    def record_local_span(self, span_dict: Dict[str, Any]) -> str:
        """
        Records a local telemetry span, advancing the node's vector clock
        and making it available for epidemic dissemination.
        """
        span_id = str(span_dict.get("span_id", f"span-{uuid.uuid4().hex[:8]}"))
        record = dict(span_dict)
        record["span_id"] = span_id
        record["_origin_node"] = self.node_id
        record["_recorded_at"] = time.time()

        self.vector_clock.increment(self.node_id)
        self.spans[span_id] = record
        return span_id

    def record_local_circuit_trip(self, trip_dict: Dict[str, Any]) -> str:
        """
        Records an urgent local circuit breaker trip intervention, advancing
        vector clock for immediate eager rumor dissemination.
        """
        trip_id = str(trip_dict.get("trip_id", f"trip-{uuid.uuid4().hex[:8]}"))
        record = dict(trip_dict)
        record["trip_id"] = trip_id
        record["_origin_node"] = self.node_id
        record["_recorded_at"] = time.time()

        self.vector_clock.increment(self.node_id)
        self.circuit_trips[trip_id] = record
        return trip_id

    def record_local_watermark(self, watermark_dict: Dict[str, Any]) -> str:
        """Records an artifact provenance watermark for mesh sync."""
        wm_id = str(watermark_dict.get("artifact_id", f"wm-{uuid.uuid4().hex[:8]}"))
        record = dict(watermark_dict)
        record["_origin_node"] = self.node_id
        self.watermarks[wm_id] = record
        return wm_id

    def generate_digest(self) -> GossipDigest:
        """Generates a compact state digest for pairwise Anti-Entropy rounds."""
        return GossipDigest(
            sender_id=self.node_id,
            vector_clock=self.vector_clock.to_dict(),
            known_span_ids=list(self.spans.keys()),
            known_trip_ids=list(self.circuit_trips.keys()),
            known_watermark_ids=list(self.watermarks.keys()),
            timestamp=time.time(),
        )

    def compute_delta_for_digest(
        self,
        peer_digest: GossipDigest,
        target_peer_id: Optional[str] = None,
    ) -> GossipDelta:
        """
        Compares peer digest against local state and computes missing delta
        to bring the peer up to date.
        """
        peer_known_spans = set(peer_digest.known_span_ids)
        peer_known_trips = set(peer_digest.known_trip_ids)
        peer_known_wms = set(peer_digest.known_watermark_ids)

        missing_spans = [
            span for sid, span in self.spans.items()
            if sid not in peer_known_spans
        ]
        missing_trips = [
            trip for tid, trip in self.circuit_trips.items()
            if tid not in peer_known_trips
        ]
        missing_wms = [
            wm for wid, wm in self.watermarks.items()
            if wid not in peer_known_wms
        ]

        target = target_peer_id or peer_digest.sender_id
        return GossipDelta(
            sender_id=self.node_id,
            target_id=target,
            vector_clock=self.vector_clock.to_dict(),
            spans=missing_spans,
            circuit_trips=missing_trips,
            watermarks=missing_wms,
            timestamp=time.time(),
        )

    def apply_delta(self, delta: GossipDelta) -> Dict[str, int]:
        """
        Applies an incoming GossipDelta from a peer, merging vector clocks
        and integrating missing telemetry records.
        """
        spans_added = 0
        trips_added = 0
        wms_added = 0

        for span in delta.spans:
            sid = str(span.get("span_id", ""))
            if sid and sid not in self.spans:
                self.spans[sid] = dict(span)
                spans_added += 1

        for trip in delta.circuit_trips:
            tid = str(trip.get("trip_id", ""))
            if tid and tid not in self.circuit_trips:
                self.circuit_trips[tid] = dict(trip)
                trips_added += 1

        for wm in delta.watermarks:
            wid = str(wm.get("artifact_id", ""))
            if wid and wid not in self.watermarks:
                self.watermarks[wid] = dict(wm)
                wms_added += 1

        # Merge vector clocks
        peer_clock = VectorClock.from_dict(delta.vector_clock)
        self.vector_clock = self.vector_clock.merge(peer_clock)

        # Refresh sender heartbeat if known peer
        if delta.sender_id in self.peers:
            self.peers[delta.sender_id].last_heartbeat = time.time()
            self.peers[delta.sender_id].status = MeshNodeStatus.ACTIVE

        return {
            "spans_added": spans_added,
            "trips_added": trips_added,
            "watermarks_added": wms_added,
        }

    def select_gossip_targets(self, k: Optional[int] = None) -> List[MeshNode]:
        """Selects up to k random active peers for epidemic dissemination."""
        fanout = k or self.fanout
        active_peers = [
            p for p in self.peers.values()
            if p.status == MeshNodeStatus.ACTIVE
        ]
        if not active_peers:
            return []
        sample_size = min(len(active_peers), fanout)
        return random.sample(active_peers, sample_size)

    def run_gossip_round(
        self,
        peer_managers: Optional[List["GossipMeshManager"]] = None,
    ) -> Dict[str, Any]:
        """
        Executes an epidemic anti-entropy round with active peers.
        If in-process peer_managers are supplied, executes direct bilateral exchange.
        """
        self.gossip_rounds_completed += 1
        synced_peers = 0
        total_spans_exchanged = 0
        total_trips_exchanged = 0
        total_wms_exchanged = 0

        if peer_managers:
            # In-process pairwise simulation
            active_peer_map = {m.node_id: m for m in peer_managers if m.node_id != self.node_id}
            targets = self.select_gossip_targets()
            for target in targets:
                target_mgr = active_peer_map.get(target.node_id)
                if target_mgr:
                    # 1. Target generates digest, self computes delta -> target applies delta
                    target_digest = target_mgr.generate_digest()
                    outbound_delta = self.compute_delta_for_digest(target_digest, target_peer_id=target.node_id)
                    res_out = target_mgr.apply_delta(outbound_delta)

                    # 2. Self generates digest, target computes delta -> self applies delta
                    local_digest = self.generate_digest()
                    inbound_delta = target_mgr.compute_delta_for_digest(local_digest, target_peer_id=self.node_id)
                    res_in = self.apply_delta(inbound_delta)

                    total_spans_exchanged += res_out["spans_added"] + res_in["spans_added"]
                    total_trips_exchanged += res_out["trips_added"] + res_in["trips_added"]
                    total_wms_exchanged += res_out["watermarks_added"] + res_in["watermarks_added"]
                    synced_peers += 1

        health = self.evaluate_convergence()
        return {
            "round": self.gossip_rounds_completed,
            "peers_synced": synced_peers,
            "spans_exchanged": total_spans_exchanged,
            "trips_exchanged": total_trips_exchanged,
            "watermarks_exchanged": total_wms_exchanged,
            "convergence_health": health.value,
        }

    def evaluate_convergence(self) -> MeshConvergenceHealth:
        """Determines convergence health based on active peer connectivity and clock states."""
        if not self.peers:
            return MeshConvergenceHealth.CONVERGED

        active = [p for p in self.peers.values() if p.status == MeshNodeStatus.ACTIVE]
        suspect = [p for p in self.peers.values() if p.status == MeshNodeStatus.SUSPECT]
        dead = [p for p in self.peers.values() if p.status == MeshNodeStatus.DEAD]

        if len(dead) > len(active):
            return MeshConvergenceHealth.PARTITIONED
        if suspect:
            return MeshConvergenceHealth.SYNCING
        return MeshConvergenceHealth.CONVERGED

    def get_status(self) -> MeshStatusReport:
        """Returns comprehensive diagnostic status for REST and CLI."""
        active = 0
        suspect = 0
        dead = 0
        peers_list = []
        for p in self.list_peers(include_dead=True):
            if p.status == MeshNodeStatus.ACTIVE:
                active += 1
            elif p.status == MeshNodeStatus.SUSPECT:
                suspect += 1
            else:
                dead += 1
            peers_list.append(p.to_dict())

        health = self.evaluate_convergence()
        return MeshStatusReport(
            local_node_id=self.node_id,
            local_endpoint=self.endpoint,
            active_peers_count=active,
            suspect_peers_count=suspect,
            dead_peers_count=dead,
            total_synced_spans=len(self.spans),
            total_synced_trips=len(self.circuit_trips),
            total_synced_watermarks=len(self.watermarks),
            gossip_rounds_completed=self.gossip_rounds_completed,
            convergence_health=health,
            vector_clock=self.vector_clock.to_dict(),
            peers=peers_list,
        )

    def reset(self) -> None:
        """Resets local state for testing."""
        self.vector_clock = VectorClock(clock={self.node_id: 0})
        self.peers.clear()
        self.spans.clear()
        self.circuit_trips.clear()
        self.watermarks.clear()
        self.gossip_rounds_completed = 0


# Global singleton instance
global_gossip_manager = GossipMeshManager()
