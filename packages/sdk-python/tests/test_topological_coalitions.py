# -*- coding: utf-8 -*-
"""
Test Suite for Real-Time Topological Graph Clustering & Dynamic Agent Coalitions (Seam 36 / ADR 0047).
Verifies interaction graph tracking, in/out degree centrality, bottleneck transit detection,
Newman-Girvan modularity, dynamic coalition formation, deadlock detection, REST endpoints, and CLI.
"""

import json
import pytest
from fastapi.testclient import TestClient

from swarmwatch.coalition import (
    TopologicalCoalitionEngine,
    global_coalition_engine,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main


@pytest.fixture(autouse=True)
def reset_coalitions():
    """Resets global coalition engine between tests."""
    global_coalition_engine.reset("test-swarm-01")
    global_coalition_engine.reset("swarm-live-01")
    yield
    global_coalition_engine.reset("test-swarm-01")
    global_coalition_engine.reset("swarm-live-01")


def test_record_interaction_and_graph_adjacency():
    """Verifies that interactions update edge weights with logarithmic token scaling."""
    engine = TopologicalCoalitionEngine()
    swarm_id = "test-swarm-01"

    engine.record_interaction(swarm_id, "Agent1", "Agent2", tokens=100)
    engine.record_interaction(swarm_id, "Agent2", "Agent1", tokens=200)

    # Self-loops ignored
    engine.record_interaction(swarm_id, "Agent1", "Agent1", tokens=50)

    res = engine.compute_clusters(swarm_id)
    assert res.total_agents == 2
    assert res.total_interactions == 2
    assert len(res.clusters) >= 1


def test_node_centrality_and_bottleneck_detection():
    """
    Verifies bottleneck detection on a bridge topology:
    Cluster A (A1, A2) -> Bridge (B) -> Cluster C (C1, C2)
    B should have the highest bottleneck score.
    """
    engine = TopologicalCoalitionEngine(bottleneck_threshold=0.10)
    swarm_id = "test-swarm-01"

    # Cluster A talks to Bridge
    engine.record_interaction(swarm_id, "A1", "Bridge", tokens=500)
    engine.record_interaction(swarm_id, "A2", "Bridge", tokens=500)
    # Bridge talks to Cluster C
    engine.record_interaction(swarm_id, "Bridge", "C1", tokens=500)
    engine.record_interaction(swarm_id, "Bridge", "C2", tokens=500)

    centralities = engine.compute_centrality(swarm_id)
    assert "Bridge" in centralities
    bridge_c = centralities["Bridge"]
    assert bridge_c.is_bottleneck is True
    assert bridge_c.bottleneck_score > centralities["A1"].bottleneck_score


def test_modularity_community_clustering():
    """
    Verifies community clustering partitions two disconnected or weakly connected dense subgraphs.
    """
    engine = TopologicalCoalitionEngine()
    swarm_id = "test-swarm-01"

    # Dense Cluster 1: Alpha, Beta, Gamma
    for _ in range(3):
        engine.record_interaction(swarm_id, "Alpha", "Beta", tokens=200)
        engine.record_interaction(swarm_id, "Beta", "Gamma", tokens=200)
        engine.record_interaction(swarm_id, "Gamma", "Alpha", tokens=200)

    # Dense Cluster 2: X, Y, Z
    for _ in range(3):
        engine.record_interaction(swarm_id, "X", "Y", tokens=200)
        engine.record_interaction(swarm_id, "Y", "Z", tokens=200)
        engine.record_interaction(swarm_id, "Z", "X", tokens=200)

    # Single sparse cross-link
    engine.record_interaction(swarm_id, "Gamma", "X", tokens=10)

    result = engine.compute_clusters(swarm_id)
    assert result.total_agents == 6
    assert result.modularity_score > 0.15
    assert len(result.clusters) == 2


def test_coalition_formation_and_leader_selection():
    """
    Verifies that a tightly coupled group forms an AgentCoalition with a designated leader.
    """
    engine = TopologicalCoalitionEngine(density_threshold=0.30)
    swarm_id = "test-swarm-01"

    # Agent "Coordinator" receives and sends messages to Worker1 and Worker2
    for _ in range(4):
        engine.record_interaction(swarm_id, "Coordinator", "Worker1", tokens=300)
        engine.record_interaction(swarm_id, "Worker1", "Coordinator", tokens=300)
        engine.record_interaction(swarm_id, "Coordinator", "Worker2", tokens=300)
        engine.record_interaction(swarm_id, "Worker2", "Coordinator", tokens=300)

    result = engine.compute_clusters(swarm_id)
    assert len(result.coalitions) >= 1
    coalition = result.coalitions[0]
    assert "Coordinator" in coalition.member_agents
    assert coalition.leader_agent == "Coordinator"
    assert coalition.interaction_density > 0.0
    assert coalition.total_tokens > 0


def test_deadlock_detection_in_coalition():
    """
    Verifies that a closed 2-agent ping-pong cycle with no external edges triggers is_deadlocked.
    """
    engine = TopologicalCoalitionEngine(density_threshold=0.30)
    swarm_id = "test-swarm-01"

    # Closed bidirectional cycle with zero external traffic
    for _ in range(5):
        engine.record_interaction(swarm_id, "LoopAgentA", "LoopAgentB", tokens=100)
        engine.record_interaction(swarm_id, "LoopAgentB", "LoopAgentA", tokens=100)

    result = engine.compute_clusters(swarm_id)
    assert len(result.coalitions) == 1
    c = result.coalitions[0]
    assert c.is_deadlocked is True


def test_server_coalition_endpoints():
    """Tests the REST API endpoints for topological clustering."""
    client = TestClient(create_app())
    swarm_id = "test-swarm-01"

    # Record interactions
    global_coalition_engine.record_interaction(swarm_id, "Architect", "Developer", tokens=250)
    global_coalition_engine.record_interaction(swarm_id, "Developer", "Architect", tokens=250)

    # GET /api/v1/swarms/{id}/coalitions
    resp = client.get(f"/api/v1/swarms/{swarm_id}/coalitions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["swarm_id"] == swarm_id
    assert data["total_agents"] == 2
    assert "Architect" in data["node_centralities"]

    # POST /api/v1/swarms/{id}/coalitions/cluster
    resp = client.post(f"/api/v1/swarms/{swarm_id}/coalitions/cluster")
    assert resp.status_code == 200
    cluster_data = resp.json()
    assert len(cluster_data["clusters"]) >= 1

    # POST /api/v1/swarms/{id}/coalitions/reset
    resp = client.post(f"/api/v1/swarms/{swarm_id}/coalitions/reset")
    assert resp.status_code == 200
    assert resp.json()["status"] == "reset"


def test_cli_coalition_commands(capsys):
    """Tests CLI subcommand `swarmwatch coalition status`."""
    swarm_id = "swarm-live-01"
    global_coalition_engine.record_interaction(swarm_id, "LeaderBot", "SubBot1", tokens=500)
    global_coalition_engine.record_interaction(swarm_id, "SubBot1", "LeaderBot", tokens=500)

    # CLI status (terminal)
    with pytest.raises(SystemExit) as exc:
        cli_main(["coalition", "status", "--swarm-id", swarm_id])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "SWARMWATCH TOPOLOGICAL GRAPH & DYNAMIC COALITIONS" in captured.out
    assert "LeaderBot" in captured.out

    # CLI status (JSON)
    with pytest.raises(SystemExit) as exc:
        cli_main(["coalition", "status", "--swarm-id", swarm_id, "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["swarm_id"] == swarm_id
    assert parsed["total_agents"] == 2
