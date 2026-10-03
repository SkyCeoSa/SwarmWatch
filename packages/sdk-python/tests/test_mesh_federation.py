# -*- coding: utf-8 -*-
"""
Unit and integration tests for Dynamic Agent Federation & Swarm Mesh Topology Routing (Seam 72 / ADR 0083).
"""

import time
import pytest

from swarmwatch.mesh_federation import (
    MeshFederationError,
    MeshPeerAuthenticationError,
    MeshRoutingLoopError,
    MeshHopLimitExceededError,
    MeshDestinationUnreachableError,
    MeshCapabilityFencingError,
    MeshCircuitTrippedError,
    MeshNodeStatus,
    MeshNodeRole,
    MeshRoutingStatus,
    PeerNode,
    MeshLink,
    PeerHandshakeRequest,
    PeerHandshakeResponse,
    MeshPacket,
    MeshRoutingReceipt,
    MeshFederationRouter,
    global_mesh_router,
    generate_peer_keypair,
    sign_peer_data,
)
from swarmwatch.root_witness import WitnessKeyManager, sha256_digest


def test_peer_registration_and_signature_verification() -> None:
    router = MeshFederationRouter()
    km1, pub_hex = generate_peer_keypair()

    # 1. Registration without signature succeeds
    peer1 = router.register_peer(
        node_id="node-01",
        cluster_id="us-east-1",
        tenant_id="tenant-alpha",
        endpoint_url="https://node01.east.internal",
        public_key_hex=pub_hex,
        role=MeshNodeRole.GATEWAY,
    )
    assert peer1.node_id == "node-01"
    assert peer1.status == MeshNodeStatus.ONLINE

    # 2. Registration with valid signature succeeds
    km2, pub2_hex = generate_peer_keypair()
    data = "node-02:us-west-2:tenant-alpha:https://node02.west.internal".encode("utf-8")
    sig = sign_peer_data(km2, data)

    peer2 = router.register_peer(
        node_id="node-02",
        cluster_id="us-west-2",
        tenant_id="tenant-alpha",
        endpoint_url="https://node02.west.internal",
        public_key_hex=pub2_hex,
        signature=sig,
    )
    assert peer2.node_id == "node-02"

    # 3. Registration with invalid signature raises MeshPeerAuthenticationError
    with pytest.raises(MeshPeerAuthenticationError, match="Cryptographic signature verification failed"):
        router.register_peer(
            node_id="node-fake",
            cluster_id="us-east-1",
            tenant_id="tenant-alpha",
            endpoint_url="https://fake.internal",
            public_key_hex=pub_hex,
            signature="deadbeef" * 8,
        )


def test_link_establishment_and_topology_graph() -> None:
    router = MeshFederationRouter()
    router.register_peer("n1", "c1", "t1", "http://n1", "pub1")
    router.register_peer("n2", "c1", "t1", "http://n2", "pub2")
    router.register_peer("n3", "c1", "t1", "http://n3", "pub3")

    # Establish links
    assert router.establish_link("n1", "n2", weight_cost=1.5, bidirectional=True) is True
    assert router.establish_link("n2", "n3", weight_cost=2.0, bidirectional=False) is True

    links = router.list_links()
    assert len(links) == 3  # n1->n2, n2->n1, n2->n3

    # Link to unknown node fails
    with pytest.raises(MeshDestinationUnreachableError):
        router.establish_link("n1", "unknown-node")


def test_dijkstra_shortest_path_routing() -> None:
    router = MeshFederationRouter()
    # Build diamond graph: A -> B (1) -> D (1); A -> C (5) -> D (1)
    router.register_peer("A", "c1", "t1", "http://a", "pub_a")
    router.register_peer("B", "c1", "t1", "http://b", "pub_b")
    router.register_peer("C", "c1", "t1", "http://c", "pub_c")
    router.register_peer("D", "c1", "t1", "http://d", "pub_d")

    router.establish_link("A", "B", weight_cost=1.0)
    router.establish_link("B", "D", weight_cost=1.0)
    router.establish_link("A", "C", weight_cost=5.0)
    router.establish_link("C", "D", weight_cost=1.0)

    # Shortest path from A to D must go through B (total weight 2.0 vs 6.0)
    path = router.find_optimal_path("A", "D")
    assert path == ["A", "B", "D"]

    # Deactivate B -> D link, path dynamically reroutes through C
    router.set_link_status("B", "D", is_active=False)
    new_path = router.find_optimal_path("A", "D")
    assert new_path == ["A", "C", "D"]


def test_stale_peer_heartbeat_and_eviction() -> None:
    router = MeshFederationRouter(stale_peer_timeout_sec=5.0)
    router.register_peer("p1", "c1", "t1", "http://p1", "pub1")
    router.register_peer("p2", "c1", "t1", "http://p2", "pub2")
    router.establish_link("p1", "p2")

    t_now = time.time()
    # At t_now, no eviction
    evicted = router.evict_stale_peers(now=t_now)
    assert len(evicted) == 0

    # At t_now + 10s, both are stale
    evicted = router.evict_stale_peers(now=t_now + 10.0)
    assert set(evicted) == {"p1", "p2"}
    assert router.get_peer("p1").status == MeshNodeStatus.EVICTED

    # Routing to evicted peer raises MeshDestinationUnreachableError
    with pytest.raises(MeshDestinationUnreachableError):
        router.find_optimal_path("p1", "p2")

    # Heartbeat revives p1
    router.heartbeat("p1")
    assert router.get_peer("p1").status == MeshNodeStatus.ONLINE


def test_mutual_cryptographic_handshake() -> None:
    router = MeshFederationRouter()
    km_a, pub_a = generate_peer_keypair()
    km_b, pub_b = generate_peer_keypair()

    router.register_peer("node-a", "cluster-east", "tenant-1", "http://a", pub_a)
    router.register_peer("node-b", "cluster-west", "tenant-1", "http://b", pub_b)

    req, res = router.perform_handshake(
        source_node_id="node-a",
        target_node_id="node-b",
        source_signer=km_a,
        target_signer=km_b,
    )

    assert req.source_node_id == "node-a"
    assert req.target_node_id == "node-b"
    assert res.status == "ACCEPTED"
    assert res.session_id.startswith("mesh-sess-")
    assert router.status()["active_sessions_count"] == 1


def test_packet_routing_and_receipt_generation() -> None:
    router = MeshFederationRouter()
    router.register_peer("node-src", "c1", "tenant-alpha", "http://src", "pub_src")
    router.register_peer("node-relay", "c1", "tenant-alpha", "http://relay", "pub_relay")
    router.register_peer("node-dst", "c1", "tenant-alpha", "http://dst", "pub_dst")

    router.establish_link("node-src", "node-relay")
    router.establish_link("node-relay", "node-dst")

    packet = MeshPacket(
        packet_id="pkt-100",
        source_node_id="node-src",
        dest_node_id="node-dst",
        tenant_id="tenant-alpha",
        payload={"task": "federated_eval", "tokens": 42},
    )

    receipt = router.route_packet(packet)
    assert receipt.status == MeshRoutingStatus.ROUTED_RELAY
    assert receipt.path_traversed == ["node-src", "node-relay", "node-dst"]
    assert receipt.total_hops == 2
    assert receipt.receipt_hash.startswith("sw:mesh:v1:")
    assert receipt.duration_us > 0

    # Look up receipt
    found = router.get_receipt(receipt.receipt_id)
    assert found is not None
    assert found.packet_id == "pkt-100"


def test_hop_limit_ttl_exceeded() -> None:
    router = MeshFederationRouter()
    router.register_peer("src", "c1", "t1", "http://s", "p1")
    router.register_peer("dst", "c1", "t1", "http://d", "p2")
    router.establish_link("src", "dst")

    packet = MeshPacket(
        packet_id="pkt-ttl",
        source_node_id="src",
        dest_node_id="dst",
        tenant_id="t1",
        payload={},
        hop_count=16,
        max_hops=16,  # Already at limit
    )

    with pytest.raises(MeshHopLimitExceededError, match="exceeded max hop limit"):
        router.route_packet(packet)

    receipts = router.get_receipts("t1")
    assert len(receipts) == 1
    assert receipts[0].status == MeshRoutingStatus.DROPPED_TTL


def test_loop_detection_and_prevention() -> None:
    router = MeshFederationRouter()
    router.register_peer("node-x", "c1", "t1", "http://x", "px")
    router.register_peer("node-y", "c1", "t1", "http://y", "py")
    router.establish_link("node-x", "node-y")

    # Packet claims node-x is already in path (loop)
    packet = MeshPacket(
        packet_id="pkt-loop",
        source_node_id="node-x",
        dest_node_id="node-y",
        tenant_id="t1",
        payload={},
        path_traversed=["node-x", "node-z"],
    )

    with pytest.raises(MeshRoutingLoopError, match="encountered duplicate node"):
        router.route_packet(packet)


def test_cross_tenant_capability_fencing() -> None:
    router = MeshFederationRouter()
    router.register_peer("node-t1", "c1", "tenant-corp", "http://t1", "p1")
    router.register_peer("node-t2", "c1", "tenant-finance", "http://t2", "p2")
    router.establish_link("node-t1", "node-t2")

    # 1. Cross-tenant packet without capability token fails
    packet_unauth = MeshPacket(
        packet_id="pkt-cross-1",
        source_node_id="node-t1",
        dest_node_id="node-t2",
        tenant_id="tenant-corp",
        payload={},
    )
    with pytest.raises(MeshCapabilityFencingError, match="Cross-tenant transit denied"):
        router.route_packet(packet_unauth)

    # 2. Cross-tenant packet with valid token succeeds
    packet_auth = MeshPacket(
        packet_id="pkt-cross-2",
        source_node_id="node-t1",
        dest_node_id="node-t2",
        tenant_id="tenant-corp",
        payload={"msg": "authorized_cross_audit"},
        capability_token="valid-cross-tenant-token-alpha",
    )
    receipt = router.route_packet(packet_auth)
    assert receipt.status == MeshRoutingStatus.DELIVERED


def test_safety_circuit_tripped_blocks_mesh_fail_closed() -> None:
    router = MeshFederationRouter()
    router.register_peer("s1", "c1", "t1", "http://s1", "p1")
    router.register_peer("s2", "c1", "t1", "http://s2", "p2")
    router.establish_link("s1", "s2")

    router.set_circuit_tripped(True)

    packet = MeshPacket(
        packet_id="pkt-blocked",
        source_node_id="s1",
        dest_node_id="s2",
        tenant_id="t1",
        payload={},
    )

    with pytest.raises(MeshCircuitTrippedError, match="Safety circuit tripped"):
        router.route_packet(packet)

    receipts = router.get_receipts("t1")
    assert receipts[-1].status == MeshRoutingStatus.BLOCKED_CIRCUIT


def test_tamper_evident_receipt_canonical_hash() -> None:
    router = MeshFederationRouter()
    router.register_peer("n-a", "c1", "t1", "http://a", "pa")
    router.register_peer("n-b", "c1", "t1", "http://b", "pb")
    router.establish_link("n-a", "n-b")

    pkt = MeshPacket("pkt-hash", "n-a", "n-b", "t1", {"v": 1})
    receipt = router.route_packet(pkt)

    assert receipt.receipt_hash.startswith("sw:mesh:v1:")
    d = receipt.to_dict()
    reconstructed = MeshRoutingReceipt.from_dict(d)
    assert reconstructed.receipt_hash == receipt.receipt_hash
    assert reconstructed.duration_us == receipt.duration_us


def test_sub_250us_mesh_routing_slo() -> None:
    router = MeshFederationRouter()
    # Build 5-node chain
    for i in range(5):
        router.register_peer(f"bench-node-{i}", "c1", "t-bench", f"http://b{i}", f"pub{i}")
        if i > 0:
            router.establish_link(f"bench-node-{i-1}", f"bench-node-{i}")

    # Warmup
    for _ in range(10):
        pkt = MeshPacket("pkt-warm", "bench-node-0", "bench-node-4", "t-bench", {})
        router.route_packet(pkt)

    # Benchmark: 500 complete packet route calculations across 4 hops
    iterations = 500
    t0 = time.perf_counter()
    for j in range(iterations):
        pkt = MeshPacket(f"pkt-bench-{j}", "bench-node-0", "bench-node-4", "t-bench", {})
        router.route_packet(pkt)
    t1 = time.perf_counter()

    avg_us = ((t1 - t0) / iterations) * 1_000_000
    assert avg_us < 250.0, f"Mesh routing roundtrip took {avg_us:.2f}µs, exceeding 250µs SLO"


def test_global_mesh_router_singleton_status_and_reset() -> None:
    st = global_mesh_router.status()
    assert "online_peers_count" in st
    assert "active_links_count" in st

    global_mesh_router.register_peer("g-peer", "g-cluster", "g-tenant", "http://g", "pub_g")
    assert global_mesh_router.status()["total_peers_count"] >= 1

    global_mesh_router.reset()
    assert global_mesh_router.status()["total_peers_count"] == 0
    assert global_mesh_router.status()["circuit_tripped"] is False


def test_mesh_federation_rest_api() -> None:
    import os
    import tempfile
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app
    from swarmwatch.storage import SQLiteStorageAdapter

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        storage = SQLiteStorageAdapter(db_path)
        app = create_app(storage=storage)
        client = TestClient(app)

        global_mesh_router.reset()

        # 1. Register peers via API
        reg1_res = client.post("/api/v1/mesh/register-peer", json={
            "node_id": "api-node-a",
            "cluster_id": "c-east",
            "tenant_id": "tenant-api",
            "endpoint_url": "http://node-a:8000",
            "public_key_hex": "pub-hex-a",
            "role": "GATEWAY",
        })
        assert reg1_res.status_code == 200
        assert reg1_res.json()["node_id"] == "api-node-a"

        reg2_res = client.post("/api/v1/mesh/register-peer", json={
            "node_id": "api-node-b",
            "cluster_id": "c-east",
            "tenant_id": "tenant-api",
            "endpoint_url": "http://node-b:8000",
            "public_key_hex": "pub-hex-b",
            "role": "WORKER",
        })
        assert reg2_res.status_code == 200

        # 2. Establish link
        link_res = client.post("/api/v1/mesh/establish-link", json={
            "source_node_id": "api-node-a",
            "target_node_id": "api-node-b",
            "weight_cost": 2.5,
            "bidirectional": True,
        })
        assert link_res.status_code == 200
        assert link_res.json()["status"] == "LINK_ESTABLISHED"

        # 3. Heartbeat
        hb_res = client.post("/api/v1/mesh/heartbeat", json={"node_id": "api-node-a"})
        assert hb_res.status_code == 200
        assert hb_res.json()["status"] == "HEARTBEAT_ACK"

        # 4. Find path
        path_res = client.get("/api/v1/mesh/path?source_node_id=api-node-a&dest_node_id=api-node-b")
        assert path_res.status_code == 200
        assert path_res.json()["path"] == ["api-node-a", "api-node-b"]
        assert path_res.json()["hops"] == 1

        # 5. Route packet
        route_res = client.post("/api/v1/mesh/route", json={
            "source_node_id": "api-node-a",
            "dest_node_id": "api-node-b",
            "tenant_id": "tenant-api",
            "payload": {"hello": "federated_mesh"},
        })
        assert route_res.status_code == 200
        rcpt = route_res.json()
        assert rcpt["status"] == "DELIVERED"
        assert rcpt["receipt_hash"].startswith("sw:mesh:v1:")
        receipt_id = rcpt["receipt_id"]

        # 6. Query peers, links, status, receipts
        peers_res = client.get("/api/v1/federation/peers?tenant_id=tenant-api")
        assert peers_res.status_code == 200
        assert len(peers_res.json()) == 2

        links_res = client.get("/api/v1/federation/links")
        assert links_res.status_code == 200
        assert len(links_res.json()) == 2

        status_res = client.get("/api/v1/federation/status")
        assert status_res.status_code == 200
        assert status_res.json()["online_peers_count"] == 2

        receipts_res = client.get("/api/v1/federation/receipts?tenant_id=tenant-api")
        assert receipts_res.status_code == 200
        assert len(receipts_res.json()) >= 1

        get_rcpt_res = client.get(f"/api/v1/mesh/receipts/{receipt_id}")
        assert get_rcpt_res.status_code == 200
        assert get_rcpt_res.json()["receipt_id"] == receipt_id

        # 7. Clean reset
        global_mesh_router.reset()
    finally:
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except OSError:
                pass
