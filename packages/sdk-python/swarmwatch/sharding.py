"""Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer (Seam 46 / ADR 0057).

This deep module implements compute-weighted consistent hashing with dynamic virtual node
allocation, real-time node capacity scoring, causal session affinity, and automatic spillover
routing in <25 microseconds per decision with zero external LLM API calls.
"""

from __future__ import annotations

import bisect
import hashlib
import struct
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union


class NodeHealthTier(str, Enum):
    """Categorical operational health classification determined by compute capacity headroom."""
    OPTIMAL = "optimal"       # Capacity >= 0.70
    NOMINAL = "nominal"       # 0.40 <= Capacity < 0.70
    CONGESTED = "congested"   # 0.15 <= Capacity < 0.40
    OVERLOADED = "overloaded" # Capacity < 0.15


@dataclass
class NodeComputeMetrics:
    """Real-time compute and queue telemetry for a mesh execution node."""
    cpu_utilization: float = 0.0      # 0.0 to 1.0
    memory_utilization: float = 0.0   # 0.0 to 1.0
    queue_depth: int = 0              # Current awaiting tasks
    max_queue_depth: int = 1000       # Target queue saturation limit
    avg_latency_ms: float = 0.0       # Rolling execution latency in ms
    reported_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cpu_utilization": round(self.cpu_utilization, 4),
            "memory_utilization": round(self.memory_utilization, 4),
            "queue_depth": self.queue_depth,
            "max_queue_depth": self.max_queue_depth,
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "reported_at": self.reported_at,
        }


def compute_capacity_score(
    metrics: NodeComputeMetrics,
    w_cpu: float = 0.40,
    w_mem: float = 0.35,
    w_queue: float = 0.25,
) -> float:
    """Computes normalized compute capacity score C_i in [0.0, 1.0].

    C_i = w_cpu * (1 - u_cpu) + w_mem * (1 - u_mem) + w_queue * (1 - min(1.0, q / q_max))
    """
    u_cpu = max(0.0, min(1.0, metrics.cpu_utilization))
    u_mem = max(0.0, min(1.0, metrics.memory_utilization))
    q_ratio = max(0.0, min(1.0, metrics.queue_depth / max(1, metrics.max_queue_depth)))

    raw_score = (w_cpu * (1.0 - u_cpu)) + (w_mem * (1.0 - u_mem)) + (w_queue * (1.0 - q_ratio))
    return round(max(0.0, min(1.0, raw_score)), 4)


def get_health_tier(capacity_score: float) -> NodeHealthTier:
    """Maps a normalized capacity score to categorical operational health tier."""
    if capacity_score >= 0.70:
        return NodeHealthTier.OPTIMAL
    elif capacity_score >= 0.40:
        return NodeHealthTier.NOMINAL
    elif capacity_score >= 0.15:
        return NodeHealthTier.CONGESTED
    return NodeHealthTier.OVERLOADED


def hash_token_32(key: str) -> int:
    """Deterministic 32-bit unsigned integer hash matching TypeScript readUInt32BE(0)."""
    digest = hashlib.md5(key.encode("utf-8")).digest()
    return struct.unpack(">I", digest[:4])[0]


@dataclass
class VirtualNode:
    """A virtual node on the consistent hash ring."""
    token: int
    node_id: str
    vnode_idx: int


@dataclass
class ShardRoutingKey:
    """Structured routing identifier preserving multi-agent causal affinity."""
    tenant_id: str
    project_id: str
    swarm_id: str
    session_affinity_key: Optional[str] = None

    def to_key_string(self) -> str:
        if self.session_affinity_key:
            return f"{self.tenant_id}:{self.project_id}:{self.swarm_id}:{self.session_affinity_key}"
        return f"{self.tenant_id}:{self.project_id}:{self.swarm_id}"

    @classmethod
    def from_string(cls, raw: str) -> ShardRoutingKey:
        parts = raw.split(":")
        if len(parts) >= 4:
            return cls(tenant_id=parts[0], project_id=parts[1], swarm_id=parts[2], session_affinity_key=parts[3])
        elif len(parts) == 3:
            return cls(tenant_id=parts[0], project_id=parts[1], swarm_id=parts[2])
        elif len(parts) == 2:
            return cls(tenant_id="default", project_id=parts[0], swarm_id=parts[1])
        return cls(tenant_id="default", project_id="default", swarm_id=raw)


@dataclass
class ShardRouteResult:
    """Immutable resolution outcome of a sharding routing request."""
    routing_key: str
    primary_node_id: str
    endpoint: str
    spillover_node_id: Optional[str] = None
    spillover_endpoint: Optional[str] = None
    route_mode: str = "primary"  # "primary" | "spillover" | "fallback"
    capacity_score: float = 1.0
    health_tier: str = "optimal"
    lookup_latency_ns: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "routing_key": self.routing_key,
            "primary_node_id": self.primary_node_id,
            "endpoint": self.endpoint,
            "spillover_node_id": self.spillover_node_id,
            "spillover_endpoint": self.spillover_endpoint,
            "route_mode": self.route_mode,
            "capacity_score": self.capacity_score,
            "health_tier": self.health_tier,
            "lookup_latency_ns": self.lookup_latency_ns,
        }


class ConsistentHashRing:
    """Deterministic 32-bit consistent hash ring with compute-weighted virtual node counts."""

    def __init__(self, base_vnodes: int = 64) -> None:
        self.base_vnodes = max(1, base_vnodes)
        self._ring: Dict[int, VirtualNode] = {}
        self._sorted_tokens: List[int] = []
        self._node_weights: Dict[str, float] = {}  # node_id -> capacity_score
        self._lock = threading.RLock()

    def add_node(self, node_id: str, capacity_score: float) -> int:
        """Adds or updates a node with vnodes proportional to capacity score. Returns vnode count."""
        with self._lock:
            self.remove_node(node_id)
            score = max(0.0, min(1.0, capacity_score))
            self._node_weights[node_id] = score
            vnode_count = max(1, int(self.base_vnodes * score))

            for i in range(vnode_count):
                vnode_key = f"{node_id}#vnode{i}"
                token = hash_token_32(vnode_key)
                self._ring[token] = VirtualNode(token=token, node_id=node_id, vnode_idx=i)

            self._sorted_tokens = sorted(self._ring.keys())
            return vnode_count

    def remove_node(self, node_id: str) -> None:
        """Removes all virtual nodes belonging to node_id."""
        with self._lock:
            tokens_to_remove = [tok for tok, vn in self._ring.items() if vn.node_id == node_id]
            for tok in tokens_to_remove:
                del self._ring[tok]
            if node_id in self._node_weights:
                del self._node_weights[node_id]
            self._sorted_tokens = sorted(self._ring.keys())

    def lookup(self, key: str) -> Tuple[Optional[str], Optional[str]]:
        """Returns (primary_node_id, spillover_node_id) for given key via O(log N) bisect."""
        with self._lock:
            if not self._sorted_tokens:
                return None, None

            token = hash_token_32(key)
            idx = bisect.bisect_left(self._sorted_tokens, token)
            if idx == len(self._sorted_tokens):
                idx = 0

            primary_token = self._sorted_tokens[idx]
            primary_node = self._ring[primary_token].node_id

            # Locate spillover node (next distinct physical node in ring clockwise)
            spillover_node: Optional[str] = None
            total_tokens = len(self._sorted_tokens)
            for step in range(1, total_tokens):
                next_idx = (idx + step) % total_tokens
                cand_node = self._ring[self._sorted_tokens[next_idx]].node_id
                if cand_node != primary_node:
                    spillover_node = cand_node
                    break

            return primary_node, spillover_node

    def get_successor(self, node_id: str) -> Optional[str]:
        """Finds the next distinct physical node in clockwise direction on the ring."""
        with self._lock:
            if not self._sorted_tokens:
                return None
            for idx, tok in enumerate(self._sorted_tokens):
                if self._ring[tok].node_id == node_id:
                    total = len(self._sorted_tokens)
                    for step in range(1, total):
                        cand = self._ring[self._sorted_tokens[(idx + step) % total]].node_id
                        if cand != node_id:
                            return cand
            first_tok = self._sorted_tokens[0]
            cand = self._ring[first_tok].node_id
            return cand if cand != node_id else None

    @property
    def total_tokens(self) -> int:
        with self._lock:
            return len(self._sorted_tokens)

    @property
    def node_count(self) -> int:
        with self._lock:
            return len(self._node_weights)


@dataclass
class ShardingStatusReport:
    """Summary of cluster sharding status and distribution."""
    total_physical_nodes: int
    total_virtual_nodes: int
    nodes: Dict[str, Dict[str, Any]]
    rebalance_count: int
    last_rebalance_at: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_physical_nodes": self.total_physical_nodes,
            "total_virtual_nodes": self.total_virtual_nodes,
            "nodes": self.nodes,
            "rebalance_count": self.rebalance_count,
            "last_rebalance_at": self.last_rebalance_at,
        }


class ComputeAwareBalancer:
    """Autonomous deep module orchestrating dynamic workload sharding and compute-aware routing."""

    def __init__(self, base_vnodes: int = 64, spillover_threshold: float = 0.15) -> None:
        self.ring = ConsistentHashRing(base_vnodes=base_vnodes)
        self.spillover_threshold = spillover_threshold
        self._endpoints: Dict[str, str] = {}
        self._metrics: Dict[str, NodeComputeMetrics] = {}
        self._session_affinity: Dict[str, str] = {}  # key_str -> primary_node_id
        self._rebalance_count: int = 0
        self._last_rebalance_at: float = time.time()
        self._lock = threading.RLock()

    def register_node(
        self,
        node_id: str,
        endpoint: str,
        metrics: Optional[NodeComputeMetrics] = None,
    ) -> None:
        """Registers a physical execution node and provisions virtual ring positions."""
        with self._lock:
            self._endpoints[node_id] = endpoint
            m = metrics or NodeComputeMetrics()
            self._metrics[node_id] = m
            score = compute_capacity_score(m)
            self.ring.add_node(node_id, score)

    def deregister_node(self, node_id: str) -> None:
        """Deregisters a node and updates hash ring."""
        with self._lock:
            self.ring.remove_node(node_id)
            if node_id in self._endpoints:
                del self._endpoints[node_id]
            if node_id in self._metrics:
                del self._metrics[node_id]
            # Evict dead node from affinity table
            self._session_affinity = {k: v for k, v in self._session_affinity.items() if v != node_id}

    def clear_session_affinity(self) -> None:
        """Clears the session affinity cache."""
        with self._lock:
            self._session_affinity.clear()

    def update_node_metrics(self, node_id: str, metrics: NodeComputeMetrics) -> None:
        """Updates compute metrics and recalculates ring virtual node allocations if score drifted."""
        with self._lock:
            if node_id not in self._endpoints:
                self._endpoints[node_id] = f"http://{node_id}:8765"
            self._metrics[node_id] = metrics
            score = compute_capacity_score(metrics)
            self.ring.add_node(node_id, score)

    def route(self, routing_key: Union[ShardRoutingKey, str]) -> ShardRouteResult:
        """Routes a task key in <25 microseconds with session affinity and spillover protection."""
        start_ns = time.perf_counter_ns()
        key_str = routing_key.to_key_string() if isinstance(routing_key, ShardRoutingKey) else str(routing_key)

        with self._lock:
            # Check session affinity cache to guarantee warm DAG cache locality
            if key_str in self._session_affinity and self._session_affinity[key_str] in self._endpoints:
                primary_id = self._session_affinity[key_str]
                spillover_id = self.ring.get_successor(primary_id)
            else:
                primary_id, spillover_id = self.ring.lookup(key_str)
                if primary_id:
                    self._session_affinity[key_str] = primary_id

            if not primary_id:
                latency_ns = time.perf_counter_ns() - start_ns
                return ShardRouteResult(
                    routing_key=key_str,
                    primary_node_id="local-fallback",
                    endpoint="http://127.0.0.1:8765",
                    route_mode="fallback",
                    capacity_score=1.0,
                    health_tier=NodeHealthTier.OPTIMAL.value,
                    lookup_latency_ns=latency_ns,
                )

            primary_metrics = self._metrics.get(primary_id, NodeComputeMetrics())
            primary_score = compute_capacity_score(primary_metrics)
            primary_tier = get_health_tier(primary_score)
            primary_endpoint = self._endpoints.get(primary_id, f"http://{primary_id}:8765")

            spillover_endpoint = self._endpoints.get(spillover_id, f"http://{spillover_id}:8765") if spillover_id else None

            # Check if primary node is overloaded (capacity < spillover_threshold)
            if primary_score < self.spillover_threshold and spillover_id:
                latency_ns = time.perf_counter_ns() - start_ns
                spillover_metrics = self._metrics.get(spillover_id, NodeComputeMetrics())
                spillover_score = compute_capacity_score(spillover_metrics)
                return ShardRouteResult(
                    routing_key=key_str,
                    primary_node_id=spillover_id,
                    endpoint=spillover_endpoint or primary_endpoint,
                    spillover_node_id=primary_id,
                    spillover_endpoint=primary_endpoint,
                    route_mode="spillover",
                    capacity_score=spillover_score,
                    health_tier=get_health_tier(spillover_score).value,
                    lookup_latency_ns=latency_ns,
                )

            latency_ns = time.perf_counter_ns() - start_ns
            return ShardRouteResult(
                routing_key=key_str,
                primary_node_id=primary_id,
                endpoint=primary_endpoint,
                spillover_node_id=spillover_id,
                spillover_endpoint=spillover_endpoint,
                route_mode="primary",
                capacity_score=primary_score,
                health_tier=primary_tier.value,
                lookup_latency_ns=latency_ns,
            )

    def rebalance(self) -> ShardingStatusReport:
        """Forces recalculation and reconstruction of the consistent hash ring."""
        with self._lock:
            for node_id, metrics in self._metrics.items():
                score = compute_capacity_score(metrics)
                self.ring.add_node(node_id, score)
            self._rebalance_count += 1
            self._last_rebalance_at = time.time()
            return self.get_status()

    def get_status(self) -> ShardingStatusReport:
        """Returns consolidated report on sharding state and node capacity tiers."""
        with self._lock:
            nodes_info: Dict[str, Dict[str, Any]] = {}
            for node_id, endpoint in self._endpoints.items():
                m = self._metrics.get(node_id, NodeComputeMetrics())
                score = compute_capacity_score(m)
                tier = get_health_tier(score)
                # Count vnodes for this node
                vnode_count = sum(1 for vn in self.ring._ring.values() if vn.node_id == node_id)
                nodes_info[node_id] = {
                    "endpoint": endpoint,
                    "metrics": m.to_dict(),
                    "capacity_score": score,
                    "health_tier": tier.value,
                    "vnodes": vnode_count,
                }

            return ShardingStatusReport(
                total_physical_nodes=len(self._endpoints),
                total_virtual_nodes=self.ring.total_tokens,
                nodes=nodes_info,
                rebalance_count=self._rebalance_count,
                last_rebalance_at=self._last_rebalance_at,
            )

    def get_ring_tokens(self) -> List[Dict[str, Any]]:
        """Returns ordered representation of all tokens on the ring."""
        with self._lock:
            tokens_list = []
            for tok in self.ring._sorted_tokens:
                vn = self.ring._ring[tok]
                tokens_list.append({
                    "token": tok,
                    "node_id": vn.node_id,
                    "vnode_idx": vn.vnode_idx,
                })
            return tokens_list


# Global process singleton initialized with local default node
global_compute_balancer = ComputeAwareBalancer()
global_compute_balancer.register_node(
    node_id="node-alpha",
    endpoint="http://127.0.0.1:8765",
    metrics=NodeComputeMetrics(cpu_utilization=0.15, memory_utilization=0.25, queue_depth=4),
)
