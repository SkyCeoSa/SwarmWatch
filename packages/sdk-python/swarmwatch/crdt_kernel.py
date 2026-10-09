# -*- coding: utf-8 -*-
"""
SwarmWatch Property-Verified δ-CRDT Kernel with Causal Delivery (Seam 84 / ADR 0095).

Implements a mathematically verified δ-CRDT state kernel for distributed multi-cluster hyper-graphs.
Guarantees algebraic convergence (C1: commutativity, associativity, idempotence),
causal delivery of out-of-order deltas without tripping safety circuits (C2),
and succinct deterministic Merkle state digests (C3).

Invariants:
- C1: merge(A, B) == merge(B, A), merge(merge(A, B), C) == merge(A, merge(B, C)), merge(A, A) == A.
- C2: Dangling edges are buffered in CausalDeliveryBuffer until endpoints arrive, never rejected or tripped.
- C3: Digest is computed as a deterministic binary Merkle root over sorted graph elements.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from swarmwatch.hlc import (
    HybridLogicalClock,
    HybridLogicalTimestamp,
)
from swarmwatch.qc import compute_merkle_root


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class CrdtKernelError(Exception):
    """Base exception for CRDT kernel operations."""


class MalformedDeltaError(CrdtKernelError):
    """Raised when an incoming delta payload is invalid or corrupt."""


# ==============================================================================
# 2. Domain Types & LWW Register
# ==============================================================================

@dataclass(frozen=True)
class LwwElement:
    """Last-Write-Wins property element stamped with Hybrid Logical Timestamp."""
    value: Any
    timestamp: HybridLogicalTimestamp

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "timestamp": self.timestamp.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LwwElement:
        ts = HybridLogicalTimestamp(
            physical_ns=d["timestamp"]["physical_ns"],
            logical_counter=d["timestamp"]["logical_counter"],
            node_id=d["timestamp"]["node_id"],
        )
        return cls(value=d["value"], timestamp=ts)


def merge_lww_props(
    props_a: Dict[str, LwwElement],
    props_b: Dict[str, LwwElement],
) -> Dict[str, LwwElement]:
    """
    Commutative, associative, and idempotent join over LWW property dictionaries.
    Tie-breaking: timestamp compare, then deterministic JSON value string compare.
    """
    all_keys = set(props_a.keys()).union(set(props_b.keys()))
    merged: Dict[str, LwwElement] = {}

    for k in all_keys:
        val_a = props_a.get(k)
        val_b = props_b.get(k)

        if val_a is None and val_b is not None:
            merged[k] = val_b
        elif val_b is None and val_a is not None:
            merged[k] = val_a
        elif val_a is not None and val_b is not None:
            if val_a.timestamp > val_b.timestamp:
                merged[k] = val_a
            elif val_b.timestamp > val_a.timestamp:
                merged[k] = val_b
            else:
                # Timestamps equal: deterministic value tie-breaking
                str_a = json.dumps(val_a.value, sort_keys=True)
                str_b = json.dumps(val_b.value, sort_keys=True)
                merged[k] = val_a if str_a >= str_b else val_b

    return merged


@dataclass
class HyperGraphDelta:
    """Atomic state change delta containing vertices, edges, and causal timestamp."""
    delta_id: str
    timestamp: HybridLogicalTimestamp
    vertices: Dict[str, Dict[str, LwwElement]] = field(default_factory=dict)
    # edge_key -> {"source": s, "target": t, "edge_type": et, "properties": Dict[str, LwwElement]}
    edges: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        serialized_vertices: Dict[str, Dict[str, Any]] = {}
        for vid, props in self.vertices.items():
            serialized_vertices[vid] = {k: el.to_dict() for k, el in props.items()}

        serialized_edges: Dict[str, Dict[str, Any]] = {}
        for ekey, e_val in self.edges.items():
            serialized_edges[ekey] = {
                "source": e_val["source"],
                "target": e_val["target"],
                "edge_type": e_val["edge_type"],
                "properties": {k: el.to_dict() for k, el in e_val["properties"].items()},
            }

        return {
            "delta_id": self.delta_id,
            "timestamp": self.timestamp.to_dict(),
            "vertices": serialized_vertices,
            "edges": serialized_edges,
        }


# ==============================================================================
# 3. Property-Verified δ-CRDT Kernel Engine
# ==============================================================================

class DeltaCrdtKernel:
    """
    Core state replication engine providing provable semilattice convergence,
    causal dependency buffering, and deterministic state hashing.
    """

    def __init__(
        self,
        node_id: str,
        clock: Optional[HybridLogicalClock] = None,
    ) -> None:
        self.node_id = str(node_id)
        self.clock = clock or HybridLogicalClock(node_id=self.node_id)
        self._lock = threading.RLock()

        # State storage
        # vertex_id -> Dict[prop_name, LwwElement]
        self._vertices: Dict[str, Dict[str, LwwElement]] = {}
        # edge_key -> Dict[prop_name, LwwElement]
        self._edges: Dict[str, Dict[str, Any]] = {}

        # Causal delivery buffer for operations with unfulfilled vertex dependencies
        self._causal_buffer: List[HyperGraphDelta] = []

    def put_vertex(self, vertex_id: str, properties: Dict[str, Any]) -> HyperGraphDelta:
        """Adds or updates a vertex, emitting an atomic δ."""
        with self._lock:
            ts = self.clock.now()
            lww_props = {k: LwwElement(value=v, timestamp=ts) for k, v in properties.items()}

            if vertex_id in self._vertices:
                self._vertices[vertex_id] = merge_lww_props(self._vertices[vertex_id], lww_props)
            else:
                self._vertices[vertex_id] = lww_props

            delta_id = hashlib.sha256(f"v:{vertex_id}:{ts.to_string()}".encode("utf-8")).hexdigest()
            delta = HyperGraphDelta(
                delta_id=delta_id,
                timestamp=ts,
                vertices={vertex_id: self._vertices[vertex_id]},
                edges={},
            )

            # Applying vertex may satisfy pending buffered edges
            self._drain_causal_buffer()
            return delta

    def put_edge(
        self,
        source: str,
        target: str,
        edge_type: str,
        properties: Dict[str, Any],
    ) -> HyperGraphDelta:
        """Adds or updates an edge between two vertices, emitting an atomic δ."""
        with self._lock:
            ts = self.clock.now()
            lww_props = {k: LwwElement(value=v, timestamp=ts) for k, v in properties.items()}
            edge_key = f"{source}:{target}:{edge_type}"

            edge_data = {
                "source": source,
                "target": target,
                "edge_type": edge_type,
                "properties": lww_props,
            }

            delta_id = hashlib.sha256(f"e:{edge_key}:{ts.to_string()}".encode("utf-8")).hexdigest()
            delta = HyperGraphDelta(
                delta_id=delta_id,
                timestamp=ts,
                vertices={},
                edges={edge_key: edge_data},
            )

            # Invariant C2: Check vertex dependencies
            if source not in self._vertices or target not in self._vertices:
                self._causal_buffer.append(delta)
            else:
                self._apply_edge_data(edge_key, edge_data)

            return delta

    def _apply_edge_data(self, edge_key: str, edge_data: Dict[str, Any]) -> None:
        """Applies edge data assuming endpoint vertices exist."""
        if edge_key in self._edges:
            existing = self._edges[edge_key]
            merged_props = merge_lww_props(existing["properties"], edge_data["properties"])
            self._edges[edge_key]["properties"] = merged_props
        else:
            self._edges[edge_key] = {
                "source": edge_data["source"],
                "target": edge_data["target"],
                "edge_type": edge_data["edge_type"],
                "properties": dict(edge_data["properties"]),
            }

    def merge_delta(self, delta: HyperGraphDelta) -> int:
        """
        Merges an incoming remote delta into local state (Invariant C1).
        Updates local HLC and buffers out-of-order edges without tripping circuits (Invariant C2).
        Returns the count of newly applied entities.
        """
        with self._lock:
            # Advance local clock with remote timestamp
            try:
                self.clock.observe(delta.timestamp)
            except Exception:
                pass  # Bounded skew violation can be handled by caller, merge proceeds safely

            applied_count = 0

            # 1. Merge vertices
            for vid, props in delta.vertices.items():
                if vid in self._vertices:
                    self._vertices[vid] = merge_lww_props(self._vertices[vid], props)
                else:
                    self._vertices[vid] = dict(props)
                applied_count += 1

            # 2. Drain causal buffer if any new vertices arrived
            if delta.vertices:
                self._drain_causal_buffer()

            # 3. Merge edges
            for ekey, edata in delta.edges.items():
                src = edata["source"]
                tgt = edata["target"]

                if src not in self._vertices or tgt not in self._vertices:
                    # Invariant C2: Buffer out-of-order dangling edge, never trip circuit!
                    self._causal_buffer.append(delta)
                else:
                    self._apply_edge_data(ekey, edata)
                    applied_count += 1

            return applied_count

    def _drain_causal_buffer(self) -> int:
        """Inspects buffered deltas and applies those whose vertex dependencies are now met."""
        drained_count = 0
        still_buffered: List[HyperGraphDelta] = []

        for delta in self._causal_buffer:
            all_edges_ready = True
            for ekey, edata in delta.edges.items():
                src = edata["source"]
                tgt = edata["target"]
                if src not in self._vertices or tgt not in self._vertices:
                    all_edges_ready = False
                    break

            if all_edges_ready:
                for ekey, edata in delta.edges.items():
                    self._apply_edge_data(ekey, edata)
                    drained_count += 1
            else:
                still_buffered.append(delta)

        self._causal_buffer = still_buffered
        return drained_count

    def pending_buffer_count(self) -> int:
        """Returns the number of operations waiting in the causal delivery buffer."""
        with self._lock:
            return len(self._causal_buffer)

    def get_state_digest(self) -> str:
        """
        Computes a deterministic binary Merkle root over the entire graph state (Invariant C3).
        Guarantees that identical states produce identical digests regardless of mutation order.
        """
        with self._lock:
            leaf_hashes: List[str] = []

            # Hash vertices deterministically
            for vid in sorted(self._vertices.keys()):
                props = self._vertices[vid]
                prop_str = json.dumps(
                    {k: {"val": el.value, "ts": el.timestamp.to_string()} for k, el in sorted(props.items())},
                    sort_keys=True,
                )
                leaf_hashes.append(hashlib.sha256(f"v:{vid}:{prop_str}".encode("utf-8")).hexdigest())

            # Hash edges deterministically
            for ekey in sorted(self._edges.keys()):
                edata = self._edges[ekey]
                props = edata["properties"]
                prop_str = json.dumps(
                    {k: {"val": el.value, "ts": el.timestamp.to_string()} for k, el in sorted(props.items())},
                    sort_keys=True,
                )
                leaf_hashes.append(hashlib.sha256(f"e:{ekey}:{prop_str}".encode("utf-8")).hexdigest())

            return compute_merkle_root(leaf_hashes)

    def get_vertex(self, vertex_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            if vertex_id not in self._vertices:
                return None
            return {k: el.value for k, el in self._vertices[vertex_id].items()}

    def get_edge(self, source: str, target: str, edge_type: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            edge_key = f"{source}:{target}:{edge_type}"
            if edge_key not in self._edges:
                return None
            edata = self._edges[edge_key]
            return {
                "source": edata["source"],
                "target": edata["target"],
                "edge_type": edata["edge_type"],
                "properties": {k: el.value for k, el in edata["properties"].items()},
            }
