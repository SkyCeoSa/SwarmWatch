"""Test suite for Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer (Seam 46 / ADR 0057)."""

import json
from starlette.testclient import TestClient

from swarmwatch.sharding import (
    NodeHealthTier,
    NodeComputeMetrics,
    compute_capacity_score,
    get_health_tier,
    hash_token_32,
    VirtualNode,
    ConsistentHashRing,
    ShardRoutingKey,
    ShardRouteResult,
    ComputeAwareBalancer,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main


def test_compute_capacity_score_calculation():
    """Validates compute capacity score formulas, weighting, and health tier classification."""
    # Zero load -> score 1.0 (Optimal)
    m_idle = NodeComputeMetrics(cpu_utilization=0.0, memory_utilization=0.0, queue_depth=0)
    score_idle = compute_capacity_score(m_idle)
    assert score_idle == 1.0
    assert get_health_tier(score_idle) == NodeHealthTier.OPTIMAL

    # Saturated load -> score 0.0 (Overloaded)
    m_full = NodeComputeMetrics(cpu_utilization=1.0, memory_utilization=1.0, queue_depth=1000, max_queue_depth=1000)
    score_full = compute_capacity_score(m_full)
    assert score_full == 0.0
    assert get_health_tier(score_full) == NodeHealthTier.OVERLOADED

    # Intermediate load: CPU 0.4, Mem 0.5, Queue 200/1000 -> 0.40*(0.60) + 0.35*(0.50) + 0.25*(0.80) = 0.24 + 0.175 + 0.20 = 0.615 (Nominal)
    m_mid = NodeComputeMetrics(cpu_utilization=0.4, memory_utilization=0.5, queue_depth=200, max_queue_depth=1000)
    score_mid = compute_capacity_score(m_mid)
    assert 0.60 <= score_mid <= 0.63
    assert get_health_tier(score_mid) == NodeHealthTier.NOMINAL

    # Congested tier: score between 0.15 and 0.40
    assert get_health_tier(0.25) == NodeHealthTier.CONGESTED


def test_consistent_hash_ring_deterministic_tokens_and_vnodes():
    """Validates deterministic 32-bit token generation and dynamic virtual node allocation."""
    ring = ConsistentHashRing(base_vnodes=64)

    # Deterministic hash token
    tok1 = hash_token_32("node-1#vnode0")
    tok2 = hash_token_32("node-1#vnode0")
    assert tok1 == tok2
    assert 0 <= tok1 <= 0xFFFFFFFF

    # Node with capacity 1.0 receives base_vnodes (64)
    v1 = ring.add_node("node-1", capacity_score=1.0)
    assert v1 == 64
    assert ring.total_tokens == 64
    assert ring.node_count == 1

    # Node with capacity 0.5 receives 32 vnodes
    v2 = ring.add_node("node-2", capacity_score=0.5)
    assert v2 == 32
    assert ring.total_tokens == 96
    assert ring.node_count == 2

    # Remove node-1
    ring.remove_node("node-1")
    assert ring.total_tokens == 32
    assert ring.node_count == 1
    assert "node-1" not in ring._node_weights


def test_consistent_hash_ring_lookup_and_circular_wrap():
    """Validates binary search token lookup and circular wrap."""
    ring = ConsistentHashRing(base_vnodes=32)
    ring.add_node("node-alpha", capacity_score=1.0)
    ring.add_node("node-beta", capacity_score=1.0)

    p1, s1 = ring.lookup("swarm-session-12345")
    assert p1 in ("node-alpha", "node-beta")
    assert s1 in ("node-alpha", "node-beta")
    assert p1 != s1

    # Deterministic consistency: same key resolves to same primary node
    p2, s2 = ring.lookup("swarm-session-12345")
    assert p1 == p2
    assert s1 == s2


def test_compute_aware_balancer_routing_and_affinity():
    """Validates ComputeAwareBalancer session affinity, latency tracking, and key parsing."""
    balancer = ComputeAwareBalancer(base_vnodes=32)
    balancer.register_node("node-a", "http://10.0.0.1:8765", NodeComputeMetrics(cpu_utilization=0.2, memory_utilization=0.3))
    balancer.register_node("node-b", "http://10.0.0.2:8765", NodeComputeMetrics(cpu_utilization=0.25, memory_utilization=0.35))
    balancer.register_node("node-c", "http://10.0.0.3:8765", NodeComputeMetrics(cpu_utilization=0.1, memory_utilization=0.2))

    key = ShardRoutingKey(tenant_id="tenant-acme", project_id="proj-finance", swarm_id="swarm-007")
    res1 = balancer.route(key)

    assert isinstance(res1, ShardRouteResult)
    assert res1.route_mode == "primary"
    assert res1.primary_node_id in ("node-a", "node-b", "node-c")
    assert res1.endpoint.startswith("http://")
    assert res1.lookup_latency_ns > 0

    # Session affinity guarantee: same key hits the exact same node
    res2 = balancer.route(key)
    assert res2.primary_node_id == res1.primary_node_id
    assert res2.endpoint == res1.endpoint


def test_compute_aware_balancer_overload_spillover_routing():
    """Validates automatic spillover failover when primary node capacity drops below threshold."""
    balancer = ComputeAwareBalancer(base_vnodes=32, spillover_threshold=0.15)
    balancer.register_node("node-1", "http://10.0.0.1:8765", NodeComputeMetrics(cpu_utilization=0.1, memory_utilization=0.1))
    balancer.register_node("node-2", "http://10.0.0.2:8765", NodeComputeMetrics(cpu_utilization=0.1, memory_utilization=0.1))

    key = "swarm-heavy-task-99"
    initial_res = balancer.route(key)
    assert initial_res.route_mode == "primary"
    primary_id = initial_res.primary_node_id

    # Overload the primary node: CPU 95%, Mem 95%, Queue 980
    overloaded_metrics = NodeComputeMetrics(
        cpu_utilization=0.95,
        memory_utilization=0.95,
        queue_depth=980,
        max_queue_depth=1000,
    )
    score = compute_capacity_score(overloaded_metrics)
    assert score < 0.15
    balancer.update_node_metrics(primary_id, overloaded_metrics)

    # Route key again: primary is overloaded, balancer must spillover to successor node
    spillover_res = balancer.route(key)
    assert spillover_res.route_mode == "spillover"
    assert spillover_res.primary_node_id != primary_id
    assert spillover_res.spillover_node_id == primary_id

    # Recover the primary node:
    recovered_metrics = NodeComputeMetrics(cpu_utilization=0.1, memory_utilization=0.1, queue_depth=10)
    balancer.update_node_metrics(primary_id, recovered_metrics)
    restored_res = balancer.route(key)
    assert restored_res.route_mode == "primary"
    assert restored_res.primary_node_id == primary_id


def test_sharding_fastapi_endpoints():
    """Validates REST control plane endpoints for sharding status, ring, route, capacity, and rebalance."""
    app = create_app()
    client = TestClient(app)

    # 1. GET /api/v1/sharding/status
    res_status = client.get("/api/v1/sharding/status")
    assert res_status.status_code == 200
    status_data = res_status.json()
    assert "total_physical_nodes" in status_data
    assert "total_virtual_nodes" in status_data
    assert "nodes" in status_data

    # 2. GET /api/v1/sharding/ring
    res_ring = client.get("/api/v1/sharding/ring")
    assert res_ring.status_code == 200
    ring_data = res_ring.json()
    assert "tokens_count" in ring_data
    assert "tokens" in ring_data

    # 3. POST /api/v1/sharding/route
    res_route = client.post(
        "/api/v1/sharding/route",
        json={"tenant_id": "t1", "project_id": "p1", "swarm_id": "swarm-test-01"},
    )
    assert res_route.status_code == 200
    route_data = res_route.json()
    assert "primary_node_id" in route_data
    assert "endpoint" in route_data
    assert route_data["route_mode"] in ("primary", "spillover", "fallback")

    # 4. POST /api/v1/sharding/node-capacity
    res_cap = client.post(
        "/api/v1/sharding/node-capacity",
        json={
            "node_id": "node-worker-99",
            "endpoint": "http://10.0.0.99:8765",
            "cpu_utilization": 0.3,
            "memory_utilization": 0.4,
            "queue_depth": 50,
        },
    )
    assert res_cap.status_code == 200
    cap_data = res_cap.json()
    assert cap_data["status"] == "updated"
    assert cap_data["node_id"] == "node-worker-99"
    assert 0.0 <= cap_data["capacity_score"] <= 1.0

    # 5. POST /api/v1/sharding/rebalance
    res_reb = client.post("/api/v1/sharding/rebalance", json={"base_vnodes": 48})
    assert res_reb.status_code == 200
    reb_data = res_reb.json()
    assert reb_data["status"] == "rebalanced"
    assert "report" in reb_data


def test_sharding_cli_execution(capsys):
    """Validates CLI subcommand `swarmwatch shard status|ring|route|rebalance`."""
    # 1. status
    try:
        main(["shard", "status", "--format", "json"])
    except SystemExit as e:
        assert e.code == 0
    captured = capsys.readouterr()
    status_obj = json.loads(captured.out)
    assert "total_physical_nodes" in status_obj

    # 2. ring
    try:
        main(["shard", "ring", "--format", "json"])
    except SystemExit as e:
        assert e.code == 0
    captured = capsys.readouterr()
    ring_obj = json.loads(captured.out)
    assert "tokens_count" in ring_obj

    # 3. route
    try:
        main(["shard", "route", "--key", "tenant:proj:swarm-cli", "--format", "json"])
    except SystemExit as e:
        assert e.code == 0
    captured = capsys.readouterr()
    route_obj = json.loads(captured.out)
    assert "primary_node_id" in route_obj

    # 4. rebalance
    try:
        main(["shard", "rebalance", "--base-vnodes", "32", "--format", "json"])
    except SystemExit as e:
        assert e.code == 0
    captured = capsys.readouterr()
    reb_obj = json.loads(captured.out)
    assert reb_obj["status"] == "rebalanced"
