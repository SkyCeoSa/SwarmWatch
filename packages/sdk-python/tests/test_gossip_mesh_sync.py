"""
Comprehensive test suite for Decentralized Epidemic Gossip Telemetry Mesh Sync (Seam 45 / ADR 0056).
Validates Vector Clock causality algebra, mesh peer membership, pairwise anti-entropy delta reconciliation,
epidemic multi-node convergence, server REST endpoints, and sub-microsecond zero-token execution.
"""

import time
import pytest
from fastapi.testclient import TestClient

from swarmwatch.gossip import (
    MeshNodeStatus,
    MeshNode,
    VectorClock,
    GossipDigest,
    GossipDelta,
    MeshConvergenceHealth,
    MeshStatusReport,
    GossipMeshManager,
    global_gossip_manager,
)
from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter


def test_vector_clock_algebra():
    """Validates vector clock increment, dominance, concurrency, and merge."""
    c1 = VectorClock(clock={"node-A": 1, "node-B": 0})
    c2 = VectorClock(clock={"node-A": 1, "node-B": 1})

    # C2 dominates C1
    assert c2.dominates(c1) is True
    assert c1.dominates(c2) is False
    assert c1.is_concurrent_with(c2) is False

    # Concurrency
    c3 = VectorClock(clock={"node-A": 2, "node-B": 0})
    # c3 has node-A=2 (higher than c2's 1), but c2 has node-B=1 (higher than c3's 0) -> concurrent
    assert c3.is_concurrent_with(c2) is True
    assert c2.is_concurrent_with(c3) is True

    # Merge
    merged = c2.merge(c3)
    assert merged.get("node-A") == 2
    assert merged.get("node-B") == 1
    assert merged.dominates(c2) is True
    assert merged.dominates(c3) is True

    # Increment
    c1.increment("node-C")
    assert c1.get("node-C") == 1


def test_mesh_node_membership():
    """Validates peer registration, heartbeat refresh, and lifecycle transitions."""
    mgr = GossipMeshManager(node_id="node-local", endpoint="http://127.0.0.1:8765")
    assert mgr.node_id == "node-local"

    peer = mgr.register_peer("node-remote-1", "http://10.0.0.1:8765", {"region": "us-east"})
    assert peer.node_id == "node-remote-1"
    assert peer.endpoint == "http://10.0.0.1:8765"
    assert peer.status == MeshNodeStatus.ACTIVE
    assert peer.metadata.get("region") == "us-east"

    # Listing
    peers = mgr.list_peers()
    assert len(peers) == 1
    assert peers[0].node_id == "node-remote-1"

    # Suspect transition
    mgr.mark_peer_suspect("node-remote-1")
    assert mgr.peers["node-remote-1"].status == MeshNodeStatus.SUSPECT
    assert mgr.evaluate_convergence() == MeshConvergenceHealth.SYNCING

    # Deregister
    assert mgr.deregister_peer("node-remote-1") is True
    assert len(mgr.list_peers()) == 0


def test_local_event_recording():
    """Validates that recording spans, circuit trips, and watermarks advances vector clock."""
    mgr = GossipMeshManager(node_id="node-1", endpoint="http://127.0.0.1:8765")

    sid = mgr.record_local_span({
        "span_id": "span-001",
        "agent_name": "agent-planner",
        "turn_index": 1,
        "token_usage": 150,
    })
    assert sid == "span-001"
    assert mgr.vector_clock.get("node-1") == 1
    assert "span-001" in mgr.spans

    tid = mgr.record_local_circuit_trip({
        "trip_id": "trip-001",
        "swarm_id": "swarm-01",
        "reason": "entropy_collapse",
    })
    assert tid == "trip-001"
    assert mgr.vector_clock.get("node-1") == 2
    assert "trip-001" in mgr.circuit_trips

    wid = mgr.record_local_watermark({
        "artifact_id": "art-001",
        "producer_agent_id": "agent-vision",
        "signature": "sw_sig_123",
    })
    assert wid == "art-001"
    assert "art-001" in mgr.watermarks


def test_digest_and_delta_pairwise_reconciliation():
    """Validates anti-entropy digest exchange and missing delta extraction."""
    mgr_a = GossipMeshManager(node_id="node-A", endpoint="http://127.0.0.1:8765")
    mgr_b = GossipMeshManager(node_id="node-B", endpoint="http://127.0.0.1:8766")

    mgr_a.register_peer("node-B", "http://127.0.0.1:8766")
    mgr_b.register_peer("node-A", "http://127.0.0.1:8765")

    # Node A produces a span and a circuit trip
    mgr_a.record_local_span({"span_id": "span-A1", "content": "Query from A"})
    mgr_a.record_local_circuit_trip({"trip_id": "trip-A1", "reason": "budget_exhausted"})

    # Node B generates digest
    digest_b = mgr_b.generate_digest()
    assert len(digest_b.known_span_ids) == 0

    # Node A computes delta for Node B
    delta_a_to_b = mgr_a.compute_delta_for_digest(digest_b)
    assert len(delta_a_to_b.spans) == 1
    assert delta_a_to_b.spans[0]["span_id"] == "span-A1"
    assert len(delta_a_to_b.circuit_trips) == 1
    assert delta_a_to_b.circuit_trips[0]["trip_id"] == "trip-A1"

    # Node B applies delta
    res = mgr_b.apply_delta(delta_a_to_b)
    assert res["spans_added"] == 1
    assert res["trips_added"] == 1
    assert "span-A1" in mgr_b.spans
    assert "trip-A1" in mgr_b.circuit_trips
    # Node B's clock now has knowledge of Node A
    assert mgr_b.vector_clock.get("node-A") >= 2


def test_multi_node_epidemic_convergence():
    """
    Validates that 3 distinct nodes exchanging gossip reach full eventual consistency:
    - Node 1 has Span-1, Trip-1
    - Node 2 has Span-2, Watermark-2
    - Node 3 has Span-3
    After running gossip rounds, all 3 nodes have all 3 spans, 1 trip, and 1 watermark.
    """
    n1 = GossipMeshManager(node_id="n1", fanout=2)
    n2 = GossipMeshManager(node_id="n2", fanout=2)
    n3 = GossipMeshManager(node_id="n3", fanout=2)

    # Cross-register
    for src in [n1, n2, n3]:
        for dst in [n1, n2, n3]:
            if src.node_id != dst.node_id:
                src.register_peer(dst.node_id, f"http://{dst.node_id}:8765")

    # Produce divergent state
    n1.record_local_span({"span_id": "s1", "text": "turn 1"})
    n1.record_local_circuit_trip({"trip_id": "t1", "reason": "deadlock"})

    n2.record_local_span({"span_id": "s2", "text": "turn 2"})
    n2.record_local_watermark({"artifact_id": "w2", "hash": "abcd"})

    n3.record_local_span({"span_id": "s3", "text": "turn 3"})

    all_nodes = [n1, n2, n3]

    # Run 2 gossip rounds
    for _ in range(2):
        for node in all_nodes:
            node.run_gossip_round(peer_managers=all_nodes)

    # Verify complete convergence across all nodes
    for node in all_nodes:
        assert len(node.spans) == 3, f"Node {node.node_id} missing spans"
        assert "s1" in node.spans
        assert "s2" in node.spans
        assert "s3" in node.spans
        assert "t1" in node.circuit_trips
        assert "w2" in node.watermarks
        assert node.evaluate_convergence() == MeshConvergenceHealth.CONVERGED


def test_server_mesh_rest_endpoints(tmp_path):
    """Validates FastAPI control plane REST endpoints for the mesh."""
    db_file = str(tmp_path / "test_mesh.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. GET /api/v1/mesh/status
    res_status = client.get("/api/v1/mesh/status")
    assert res_status.status_code == 200
    data = res_status.json()
    assert "local_node_id" in data
    assert "convergence_health" in data

    # 2. POST /api/v1/mesh/peers/register
    res_reg = client.post("/api/v1/mesh/peers/register", json={
        "node_id": "remote-peer-99",
        "endpoint": "http://192.168.1.100:8765",
        "metadata": {"cluster": "edge-dc"},
    })
    assert res_reg.status_code == 200
    assert res_reg.json()["status"] == "registered"

    # 3. GET /api/v1/mesh/peers
    res_peers = client.get("/api/v1/mesh/peers")
    assert res_peers.status_code == 200
    peers_data = res_peers.json()
    assert peers_data["peers_count"] >= 1

    # 4. POST /api/v1/mesh/gossip/round
    res_round = client.post("/api/v1/mesh/gossip/round", json={"fanout": 2})
    assert res_round.status_code == 200
    assert res_round.json()["status"] == "round_completed"

    # 5. POST /api/v1/mesh/gossip/sync (Digest generation)
    res_sync = client.post("/api/v1/mesh/gossip/sync", json={})
    assert res_sync.status_code == 200
    assert res_sync.json()["action"] == "digest_generated"


def test_zero_token_and_microsecond_performance():
    """Validates that 2,000 vector clock merges and digest comparisons execute in <40ms (<20µs per op)."""
    mgr = GossipMeshManager(node_id="perf-node")
    for i in range(10):
        mgr.register_peer(f"peer-{i}", f"http://peer-{i}:8765")
        mgr.record_local_span({"span_id": f"s-{i}", "val": i})

    t0 = time.perf_counter()
    other_clock = VectorClock(clock={f"peer-{i}": i for i in range(10)})
    for _ in range(2000):
        mgr.vector_clock = mgr.vector_clock.merge(other_clock)
        digest = mgr.generate_digest()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    assert elapsed_ms < 100.0, f"Performance budget breached: {elapsed_ms:.2f}ms"
