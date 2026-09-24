# -*- coding: utf-8 -*-
"""
SwarmWatch Real-Time Topological Graph Clustering & Dynamic Agent Coalitions Engine.
Analyzes directed causal interaction graphs, calculates degree and bottleneck transit
centrality, computes Newman-Girvan modularity, and dynamically forms autonomous agent
coalitions with designated Coalition Leaders (ADR 0047).
"""

from collections import defaultdict
from datetime import datetime, timezone
import math
import threading
from typing import Any, Dict, List, Optional, Set
import uuid
from pydantic import BaseModel, Field


class NodeCentrality(BaseModel):
    """Topological centrality metrics for an individual agent in the interaction graph."""
    agent_id: str
    in_degree: float = 0.0
    out_degree: float = 0.0
    total_degree: float = 0.0
    bottleneck_score: float = 0.0
    is_bottleneck: bool = False


class AgentCoalition(BaseModel):
    """An autonomous, tightly-coupled sub-collective of swarm agents."""
    coalition_id: str = Field(default_factory=lambda: f"coalition-{uuid.uuid4().hex[:8]}")
    member_agents: List[str]
    leader_agent: str
    interaction_density: float = 0.0
    is_deadlocked: bool = False
    total_tokens: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TopologicalClusterResult(BaseModel):
    """The complete result of a topological graph clustering epoch for a swarm."""
    swarm_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_agents: int
    total_interactions: int
    modularity_score: float
    clusters: List[List[str]]
    coalitions: List[AgentCoalition]
    node_centralities: Dict[str, NodeCentrality]
    bottleneck_agents: List[str]


class TopologicalCoalitionEngine:
    """
    In-process, zero-token-cost graph analytics and community clustering engine.
    Analyzes causal agent interaction topologies, detects transit bottlenecks,
    and dynamically provisions isolated agent coalitions in <50us per turn.
    """

    def __init__(self, bottleneck_threshold: float = 0.15, density_threshold: float = 0.40):
        self.bottleneck_threshold = bottleneck_threshold
        self.density_threshold = density_threshold
        self._lock = threading.RLock()
        # swarm_id -> source -> target -> weight
        self._adjacency: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        # swarm_id -> source -> target -> total tokens
        self._edge_tokens: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        # swarm_id -> set of active agents
        self._agents: Dict[str, Set[str]] = defaultdict(set)
        # swarm_id -> total interaction count
        self._interaction_counts: Dict[str, int] = defaultdict(int)
        # swarm_id -> latest result
        self._latest_results: Dict[str, TopologicalClusterResult] = {}

    def record_interaction(
        self,
        swarm_id: str,
        source_agent: str,
        target_agent: str,
        tokens: int = 0,
    ) -> None:
        """
        Records a directed causal interaction between source and target agents,
        updating edge weights and participation volumes.
        """
        if not source_agent or not target_agent or source_agent == target_agent:
            return

        with self._lock:
            weight_delta = 1.0 + math.log1p(max(0, tokens))
            self._adjacency[swarm_id][source_agent][target_agent] += weight_delta
            self._edge_tokens[swarm_id][source_agent][target_agent] += tokens
            self._agents[swarm_id].add(source_agent)
            self._agents[swarm_id].add(target_agent)
            self._interaction_counts[swarm_id] += 1

    def compute_centrality(self, swarm_id: str) -> Dict[str, NodeCentrality]:
        """
        Computes in-degree, out-degree, and bottleneck transit centrality scores B(v)
        for all active agents in a swarm session.
        """
        with self._lock:
            agents = list(self._agents.get(swarm_id, set()))
            adj = self._adjacency.get(swarm_id, {})
            if not agents:
                return {}

            in_weights: Dict[str, float] = defaultdict(float)
            out_weights: Dict[str, float] = defaultdict(float)
            total_graph_weight = 0.0

            for u, targets in adj.items():
                for v, w in targets.items():
                    out_weights[u] += w
                    in_weights[v] += w
                    total_graph_weight += w

            total_graph_weight = max(1.0, total_graph_weight)
            centralities: Dict[str, NodeCentrality] = {}

            for a in agents:
                cin = in_weights[a] / total_graph_weight
                cout = out_weights[a] / total_graph_weight
                deg = in_weights[a] + out_weights[a]
                # Bottleneck score: geometric mean of in/out normalized flow scaled by local degree
                b_score = math.sqrt(cin * cout) * (1.0 + math.log1p(deg))
                is_btn = b_score >= self.bottleneck_threshold

                centralities[a] = NodeCentrality(
                    agent_id=a,
                    in_degree=round(cin, 4),
                    out_degree=round(cout, 4),
                    total_degree=round(deg, 4),
                    bottleneck_score=round(b_score, 4),
                    is_bottleneck=is_btn,
                )

            return centralities

    def compute_clusters(self, swarm_id: str) -> TopologicalClusterResult:
        """
        Executes modularity-based community clustering and dynamic coalition formation
        across the directed causal interaction graph.
        """
        with self._lock:
            agents = sorted(list(self._agents.get(swarm_id, set())))
            adj = self._adjacency.get(swarm_id, {})
            edge_toks = self._edge_tokens.get(swarm_id, {})
            num_interactions = self._interaction_counts.get(swarm_id, 0)

            if not agents:
                empty_result = TopologicalClusterResult(
                    swarm_id=swarm_id,
                    total_agents=0,
                    total_interactions=0,
                    modularity_score=0.0,
                    clusters=[],
                    coalitions=[],
                    node_centralities={},
                    bottleneck_agents=[],
                )
                self._latest_results[swarm_id] = empty_result
                return empty_result

            centralities = self.compute_centrality(swarm_id)

            # Symmetrized affinity matrix for undirected community clustering
            # affinity(u, v) = (W(u, v) + W(v, u)) / 2
            affinity: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
            total_m = 0.0
            degree: Dict[str, float] = defaultdict(float)

            for u in agents:
                for v in agents:
                    if u == v:
                        continue
                    w_uv = adj.get(u, {}).get(v, 0.0)
                    w_vu = adj.get(v, {}).get(u, 0.0)
                    sym_w = (w_uv + w_vu) / 2.0
                    if sym_w > 0:
                        affinity[u][v] = sym_w
                        degree[u] += sym_w
                        total_m += sym_w

            total_m = total_m / 2.0  # undirected sum of weights

            # Fast Label Propagation Clustering with mutual affinity coalescence
            labels: Dict[str, str] = {a: a for a in agents}
            # Up to 5 iterations for fast convergence (<20us)
            for _ in range(5):
                changed = False
                for u in agents:
                    neighbors = affinity.get(u, {})
                    if not neighbors:
                        continue
                    label_weights: Dict[str, float] = defaultdict(float)
                    for v, w in neighbors.items():
                        label_weights[labels[v]] += w

                    best_label = max(label_weights.items(), key=lambda kv: kv[1])[0]
                    if best_label != labels[u]:
                        labels[u] = best_label
                        changed = True
                if not changed:
                    break

            # Group nodes by label
            cluster_groups: Dict[str, List[str]] = defaultdict(list)
            for a, lbl in labels.items():
                cluster_groups[lbl].append(a)

            clusters = [sorted(nodes) for nodes in cluster_groups.values() if nodes]

            # Compute Newman-Girvan Modularity Q
            modularity = 0.0
            if total_m > 0:
                for cluster in clusters:
                    for u in cluster:
                        for v in cluster:
                            a_uv = affinity.get(u, {}).get(v, 0.0)
                            expected = (degree[u] * degree[v]) / (2.0 * total_m)
                            modularity += (a_uv - expected)
                modularity = modularity / (2.0 * total_m)
                modularity = max(-0.5, min(1.0, modularity))

            # Form Dynamic Agent Coalitions for cohesive clusters
            coalitions: List[AgentCoalition] = []
            for cluster in clusters:
                if len(cluster) < 2:
                    continue

                # Calculate intra-cluster interaction density
                k_nodes = len(cluster)
                max_possible_edges = k_nodes * (k_nodes - 1)
                actual_internal_weight = 0.0
                internal_edge_count = 0
                max_internal_edge = 1.0
                coalition_tokens = 0

                node_internal_degree: Dict[str, float] = defaultdict(float)
                for u in cluster:
                    for v in cluster:
                        if u != v:
                            w = adj.get(u, {}).get(v, 0.0)
                            if w > 0:
                                actual_internal_weight += w
                                internal_edge_count += 1
                                max_internal_edge = max(max_internal_edge, w)
                                node_internal_degree[u] += w
                                node_internal_degree[v] += w
                            coalition_tokens += edge_toks.get(u, {}).get(v, 0)

                # Normalized density in [0.0, 1.0]
                density = (
                    actual_internal_weight / (max_possible_edges * max_internal_edge)
                    if max_possible_edges > 0
                    else 1.0
                )
                density = max(0.0, min(1.0, density))

                # Designate Coalition Leader: agent with maximum internal degree
                leader = max(
                    cluster,
                    key=lambda a: (node_internal_degree[a], centralities[a].total_degree),
                )

                # Deadlock Check: high internal bidirectional traffic with zero external outgoing edges
                external_edges = 0
                for u in cluster:
                    for v in adj.get(u, {}):
                        if v not in cluster:
                            external_edges += 1

                is_deadlocked = internal_edge_count >= 2 and external_edges == 0 and density >= 0.70

                if density >= self.density_threshold or len(cluster) >= 3:
                    coalitions.append(
                        AgentCoalition(
                            member_agents=cluster,
                            leader_agent=leader,
                            interaction_density=round(density, 4),
                            is_deadlocked=is_deadlocked,
                            total_tokens=coalition_tokens,
                        )
                    )

            bottleneck_agents = [
                aid for aid, c in centralities.items() if c.is_bottleneck
            ]

            result = TopologicalClusterResult(
                swarm_id=swarm_id,
                total_agents=len(agents),
                total_interactions=num_interactions,
                modularity_score=round(modularity, 4),
                clusters=clusters,
                coalitions=coalitions,
                node_centralities=centralities,
                bottleneck_agents=bottleneck_agents,
            )
            self._latest_results[swarm_id] = result
            return result

    def get_coalitions(self, swarm_id: str) -> List[AgentCoalition]:
        """Returns the active coalitions for a swarm."""
        with self._lock:
            res = self._latest_results.get(swarm_id)
            if res:
                return res.coalitions
            return self.compute_clusters(swarm_id).coalitions

    def get_latest_result(self, swarm_id: str) -> Optional[TopologicalClusterResult]:
        """Returns the most recent clustering result for a swarm."""
        with self._lock:
            return self._latest_results.get(swarm_id)

    def reset(self, swarm_id: str) -> None:
        """Resets the topology state for a swarm session."""
        with self._lock:
            self._adjacency.pop(swarm_id, None)
            self._edge_tokens.pop(swarm_id, None)
            self._agents.pop(swarm_id, None)
            self._interaction_counts.pop(swarm_id, None)
            self._latest_results.pop(swarm_id, None)


# Global singleton instance
global_coalition_engine = TopologicalCoalitionEngine()
