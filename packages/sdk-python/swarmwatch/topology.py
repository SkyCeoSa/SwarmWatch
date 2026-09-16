"""
Visual Causal Topology Engine for SwarmWatch (ADR 0017 / Seam 9).
Provides deterministic extraction of AgentNodes, directed CausalEdges,
communication metrics, and cycle detection from SwarmSpans.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Set, Tuple
import math
from swarmwatch.schema import SwarmSpan


@dataclass
class AgentNode:
    id: str
    name: str
    role: str = "Agent"
    in_degree: int = 0
    out_degree: int = 0
    turn_count: int = 0
    total_tokens: int = 0
    x: float = 0.0
    y: float = 0.0

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "role": self.role,
            "in_degree": self.in_degree,
            "out_degree": self.out_degree,
            "turn_count": self.turn_count,
            "total_tokens": self.total_tokens,
            "x": round(self.x, 1),
            "y": round(self.y, 1),
        }


@dataclass
class CausalEdge:
    id: str
    source: str
    target: str
    turn_count: int = 0
    total_tokens: int = 0
    mean_latency_ms: float = 0.0
    is_cyclic: bool = False
    _latencies: List[float] = field(default_factory=list, repr=False)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "source": self.source,
            "target": self.target,
            "turn_count": self.turn_count,
            "total_tokens": self.total_tokens,
            "mean_latency_ms": round(self.mean_latency_ms, 1),
            "is_cyclic": self.is_cyclic,
        }


@dataclass
class CausalTopology:
    nodes: List[AgentNode] = field(default_factory=list)
    edges: List[CausalEdge] = field(default_factory=list)
    cycles: List[List[str]] = field(default_factory=list)
    total_turns: int = 0

    def to_dict(self) -> dict:
        return {
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "cycles": self.cycles,
            "total_turns": self.total_turns,
        }


ROLE_MAP: Dict[str, str] = {
    "ProductArchitect": "Lead",
    "BackendEngineer": "Core",
    "CodeReviewer": "Review",
    "SecurityAuditor": "Guard",
    "PythonCodeRunner": "Tool",
    "SystemArchitect": "Lead",
    "LeadDeveloper": "Core",
}


def _detect_cycles(adj: Dict[str, Set[str]]) -> List[List[str]]:
    """
    Finds elementary directed cycles in an adjacency dictionary using DFS.
    Returns a list of cycle paths (e.g. [['A', 'B', 'A']]).
    """
    visited: Set[str] = set()
    rec_stack: List[str] = []
    cycles: List[List[str]] = []

    def dfs(node: str):
        visited.add(node)
        rec_stack.append(node)

        for neighbor in adj.get(node, set()):
            if neighbor not in visited:
                dfs(neighbor)
            elif neighbor in rec_stack:
                cycle_start_idx = rec_stack.index(neighbor)
                cycle = rec_stack[cycle_start_idx:] + [neighbor]
                # Avoid duplicate cycles with rotated permutations
                cycle_set = set(cycle)
                if not any(set(c) == cycle_set for c in cycles):
                    cycles.append(cycle)

        rec_stack.pop()

    for n in list(adj.keys()):
        if n not in visited:
            dfs(n)

    return cycles


def build_topology_from_spans(spans: Sequence[SwarmSpan]) -> CausalTopology:
    """
    Builds a CausalTopology instance from an ordered sequence of SwarmSpans.
    Computes spatial layouts, communication metrics, and cycle detections.
    """
    if not spans:
        return CausalTopology()

    nodes_map: Dict[str, AgentNode] = {}
    edges_map: Dict[Tuple[str, str], CausalEdge] = {}
    adj: Dict[str, Set[str]] = {}

    for s in spans:
        src = s.source_agent or "Agent"
        tgt = s.target_agent or "Swarm"

        if src not in nodes_map:
            nodes_map[src] = AgentNode(
                id=src,
                name=src,
                role=ROLE_MAP.get(src, "Agent"),
            )
        if tgt not in nodes_map:
            nodes_map[tgt] = AgentNode(
                id=tgt,
                name=tgt,
                role=ROLE_MAP.get(tgt, "Agent"),
            )

        nodes_map[src].out_degree += 1
        nodes_map[src].turn_count += 1
        nodes_map[src].total_tokens += s.token_count

        nodes_map[tgt].in_degree += 1

        pair = (src, tgt)
        if pair not in edges_map:
            edge_id = f"edge_{src}_{tgt}"
            edges_map[pair] = CausalEdge(id=edge_id, source=src, target=tgt)
            if src not in adj:
                adj[src] = set()
            adj[src].add(tgt)

        edge = edges_map[pair]
        edge.turn_count += 1
        edge.total_tokens += s.token_count
        edge._latencies.append(s.duration_ms)
        edge.mean_latency_ms = sum(edge._latencies) / len(edge._latencies)

    # Detect cycles
    cycles = _detect_cycles(adj)
    cyclic_pairs: Set[Tuple[str, str]] = set()
    for c in cycles:
        for i in range(len(c) - 1):
            cyclic_pairs.add((c[i], c[i + 1]))

    for pair, edge in edges_map.items():
        if pair in cyclic_pairs:
            edge.is_cyclic = True

    # Compute radial 2D spatial layout coordinates for nodes (width: 760, height: 420)
    node_list = list(nodes_map.values())
    n_count = len(node_list)
    center_x, center_y = 380.0, 210.0
    radius = min(220.0, max(120.0, 40.0 * n_count))

    for idx, node in enumerate(node_list):
        angle = (2.0 * math.pi * idx) / max(1, n_count) - (math.pi / 2.0)
        node.x = center_x + radius * math.cos(angle)
        node.y = center_y + radius * math.sin(angle)

    return CausalTopology(
        nodes=node_list,
        edges=list(edges_map.values()),
        cycles=cycles,
        total_turns=len(spans),
    )
