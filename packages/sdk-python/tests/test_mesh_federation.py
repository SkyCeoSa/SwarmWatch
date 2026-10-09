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
    sign_peer_registration,
    compute_peer_registration_payload,
)
from swarmwatch.root_witness import WitnessKeyManager, sha256_digest


def make_reg_kwargs(
    node_id: str,
    cluster_id: str = "c1",
    tenant_id: str = "t1",
    endpoint_url: str = "http://node",
    role: MeshNodeRole = MeshNodeRole.WORKER,
) -> dict:
    km, pub = generate_peer_keypair()
    sig = sign_peer_registration(km, node_id, cluster_id, tenant_id, endpoint_url, pub, role)
    return {
        "node_id": node_id,
        "cluster_id": cluster_id,
        "tenant_id": tenant_id,
        "endpoint_url": endpoint_url,
        "public_key_hex": pub,
        "role": role,
        "signature": sig,
    }


def test_peer_registration_and_signature_verification() -> None:
    router = MeshFederationRouter()
    km1, pub_hex = generate_peer_keypair()

    # 1. Registration without signature is rejected (PoC-H)
    with pytest.raises(MeshPeerAuthenticationError, match="requires valid cryptographic signature"):
        router.register_peer(
            node_id="node-01",
            cluster_id="us-east-1",
            tenant_id="tenant-alpha",
            endpoint_url="https://node01.east.internal",
            public_key_hex=pub_hex,
            role=MeshNodeRole.GATEWAY,
        )

    # 1b. Registration with malformed key is rejected (PoC-H squatting)
    with pytest.raises(MeshPeerAuthenticationError, match="Malformed public key"):
        router.register_peer(
            node_id="node-victim",
            cluster_id="us-east-1",
            tenant_id="tenant-alpha",
            endpoint_url="https://evil.internal",
            public_key_hex="zz-not-hex",
            signature="any-sig",
        )

    # 2. Registration with valid signature succeeds
    sig1 = sign_peer_registration(
        km1, "node-01", "us-east-1", "tenant-alpha", "https://node01.east.internal", pub_hex, MeshNodeRole.GATEWAY
    )
    peer1 = router.register_peer(
        node_id="node-01",
        cluster_id="us-east-1",
        tenant_id="tenant-alpha",
        endpoint_url="https://node01.east.internal",
        public_key_hex=pub_hex,
        role=MeshNodeRole.GATEWAY,
        signature=sig1,
    )
    assert peer1.node_id == "node-01"
    assert peer1.status == MeshNodeStatus.ONLINE

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

    # 4. Attempting to overwrite existing node without signature is rejected
    with pytest.raises(MeshPeerAuthenticationError, match="Cannot overwrite existing peer node"):
        router.register_peer(
            node_id="node-01",
            cluster_id="us-east-1",
            tenant_id="tenant-alpha",
            endpoint_url="https://hijack.evil.internal",
            public_key_hex=pub_hex,
        )

    # 5. Attempting to overwrite existing node with Mallory's key signature is rejected (PoC-G)
    km_mallory, pub_mallory = generate_peer_keypair()
    mallory_sig = sign_peer_registration(
        km_mallory, "node-01", "us-east-1", "tenant-alpha", "https://hijack.evil.internal", pub_mallory, MeshNodeRole.GATEWAY
    )
    with pytest.raises(MeshPeerAuthenticationError, match="Node hijacking attempt detected"):
        router.register_peer(
            node_id="node-01",
            cluster_id="us-east-1",
            tenant_id="tenant-alpha",
            endpoint_url="https://hijack.evil.internal",
            public_key_hex=pub_mallory,
            signature=mallory_sig,
        )

    # 5b. Signature replay attack with attacker key is rejected (PoC-G replay)
    with pytest.raises(MeshPeerAuthenticationError, match="Node hijacking attempt detected"):
        router.register_peer(
            node_id="node-01",
            cluster_id="us-east-1",
            tenant_id="tenant-alpha",
            endpoint_url="https://hijack.evil.internal",
            public_key_hex=pub_mallory,
            signature=sig1,  # Replay Alice's sig
        )

    # 6. Legitimate owner re-registration signed by km1 succeeds
    valid_update_sig = sign_peer_registration(
        km1, "node-01", "us-east-1", "tenant-alpha", "https://node01-updated.east.internal", pub_hex, MeshNodeRole.GATEWAY
    )
    updated_peer = router.register_peer(
        node_id="node-01",
        cluster_id="us-east-1",
        tenant_id="tenant-alpha",
        endpoint_url="https://node01-updated.east.internal",
        public_key_hex=pub_hex,
        signature=valid_update_sig,
        role=MeshNodeRole.GATEWAY,
    )
    assert updated_peer.endpoint_url == "https://node01-updated.east.internal"


def test_link_establishment_and_topology_graph() -> None:
    router = MeshFederationRouter()
    router.register_peer(**make_reg_kwargs("n1", "c1", "t1", "http://n1"))
    router.register_peer(**make_reg_kwargs("n2", "c1", "t1", "http://n2"))
    router.register_peer(**make_reg_kwargs("n3", "c1", "t1", "http://n3"))

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
    router.register_peer(**make_reg_kwargs("A", "c1", "t1", "http://a"))
    router.register_peer(**make_reg_kwargs("B", "c1", "t1", "http://b"))
    router.register_peer(**make_reg_kwargs("C", "c1", "t1", "http://c"))
    router.register_peer(**make_reg_kwargs("D", "c1", "t1", "http://d"))

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
    router.register_peer(**make_reg_kwargs("p1", "c1", "t1", "http://p1"))
    router.register_peer(**make_reg_kwargs("p2", "c1", "t1", "http://p2"))
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

    sig_a = sign_peer_registration(km_a, "node-a", "cluster-east", "tenant-1", "http://a", pub_a)
    sig_b = sign_peer_registration(km_b, "node-b", "cluster-west", "tenant-1", "http://b", pub_b)
    router.register_peer("node-a", "cluster-east", "tenant-1", "http://a", pub_a, signature=sig_a)
    router.register_peer("node-b", "cluster-west", "tenant-1", "http://b", pub_b, signature=sig_b)

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
    router.register_peer(**make_reg_kwargs("node-src", "c1", "tenant-alpha", "http://src"))
    router.register_peer(**make_reg_kwargs("node-relay", "c1", "tenant-alpha", "http://relay"))
    router.register_peer(**make_reg_kwargs("node-dst", "c1", "tenant-alpha", "http://dst"))

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
    router.register_peer(**make_reg_kwargs("src", "c1", "t1", "http://s"))
    router.register_peer(**make_reg_kwargs("dst", "c1", "t1", "http://d"))
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
    router.register_peer(**make_reg_kwargs("node-x", "c1", "t1", "http://x"))
    router.register_peer(**make_reg_kwargs("node-y", "c1", "t1", "http://y"))
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
    router.register_peer(**make_reg_kwargs("node-t1", "c1", "tenant-corp", "http://t1"))
    router.register_peer(**make_reg_kwargs("node-t2", "c1", "tenant-finance", "http://t2"))
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
    router.register_peer(**make_reg_kwargs("s1", "c1", "t1", "http://s1"))
    router.register_peer(**make_reg_kwargs("s2", "c1", "t1", "http://s2"))
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
    router.register_peer(**make_reg_kwargs("n-a", "c1", "t1", "http://a"))
    router.register_peer(**make_reg_kwargs("n-b", "c1", "t1", "http://b"))
    router.establish_link("n-a", "n-b")

    pkt = MeshPacket("pkt-hash", "n-a", "n-b", "t1", {"v": 1})
    receipt = router.route_packet(pkt)

    assert receipt.receipt_hash.startswith("sw:mesh:v1:")
    d = receipt.to_dict()
    reconstructed = MeshRoutingReceipt.from_dict(d)
    assert reconstructed.receipt_hash == receipt.receipt_hash
    assert reconstructed.duration_us == receipt.duration_us


def test_sub_250us_mesh_routing_slo() -> None:
    import gc
    router = MeshFederationRouter()
    # Build 5-node chain
    for i in range(5):
        router.register_peer(**make_reg_kwargs(f"bench-node-{i}", "c1", "t-bench", f"http://b{i}"))
        if i > 0:
            router.establish_link(f"bench-node-{i-1}", f"bench-node-{i}")

    # Warmup
    for _ in range(10):
        pkt = MeshPacket("pkt-warm", "bench-node-0", "bench-node-4", "t-bench", {})
        router.route_packet(pkt)

    # Benchmark: 100 complete packet route calculations across 4 hops with GC isolation
    iterations = 100
    gc.collect()
    gc.disable()
    latencies_us = []
    try:
        for j in range(iterations):
            pkt = MeshPacket(f"pkt-bench-{j}", "bench-node-0", "bench-node-4", "t-bench", {})
            t0 = time.perf_counter_ns()
            router.route_packet(pkt)
            latencies_us.append((time.perf_counter_ns() - t0) / 1000.0)
    finally:
        gc.enable()

    latencies_us.sort()
    trimmed = latencies_us[5:-5]
    median_us = trimmed[len(trimmed) // 2]
    assert median_us < 250.0, f"Mesh routing roundtrip took {median_us:.2f}µs, exceeding 250µs SLO"



def test_global_mesh_router_singleton_status_and_reset() -> None:
    st = global_mesh_router.status()
    assert "online_peers_count" in st
    assert "active_links_count" in st

    global_mesh_router.register_peer(**make_reg_kwargs("g-peer", "g-cluster", "g-tenant", "http://g"))
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

        # Bootstrap operator key for control plane mesh management
        key_res = client.post("/api/v1/keys", json={
            "project_id": "default-project",
            "name": "Mesh Operator Key",
            "is_test": False,
            "scopes": ["*"],
        })
        assert key_res.status_code == 200
        op_key = key_res.json()["api_key"]
        auth_headers = {"Authorization": f"Bearer {op_key}"}

        # 1. Register peers via API
        km_a, pub_a = generate_peer_keypair()
        sig_a = sign_peer_registration(km_a, "api-node-a", "c-east", "tenant-api", "http://node-a:8000", pub_a, MeshNodeRole.GATEWAY)
        reg1_res = client.post("/api/v1/mesh/register-peer", headers=auth_headers, json={
            "node_id": "api-node-a",
            "cluster_id": "c-east",
            "tenant_id": "tenant-api",
            "endpoint_url": "http://node-a:8000",
            "public_key_hex": pub_a,
            "role": "GATEWAY",
            "signature": sig_a,
        })
        assert reg1_res.status_code == 200
        assert reg1_res.json()["node_id"] == "api-node-a"

        km_b, pub_b = generate_peer_keypair()
        sig_b = sign_peer_registration(km_b, "api-node-b", "c-east", "tenant-api", "http://node-b:8000", pub_b, MeshNodeRole.WORKER)
        reg2_res = client.post("/api/v1/mesh/register-peer", headers=auth_headers, json={
            "node_id": "api-node-b",
            "cluster_id": "c-east",
            "tenant_id": "tenant-api",
            "endpoint_url": "http://node-b:8000",
            "public_key_hex": pub_b,
            "role": "WORKER",
            "signature": sig_b,
        })
        assert reg2_res.status_code == 200

        # 2. Establish link
        link_res = client.post("/api/v1/mesh/establish-link", headers=auth_headers, json={
            "source_node_id": "api-node-a",
            "target_node_id": "api-node-b",
            "weight_cost": 2.5,
            "bidirectional": True,
        })
        assert link_res.status_code == 200
        assert link_res.json()["status"] == "LINK_ESTABLISHED"

        # 3. Heartbeat
        hb_res = client.post("/api/v1/mesh/heartbeat", headers=auth_headers, json={"node_id": "api-node-a"})
        assert hb_res.status_code == 200
        assert hb_res.json()["status"] == "HEARTBEAT_ACK"

        # 4. Find path
        path_res = client.get("/api/v1/mesh/path?source_node_id=api-node-a&dest_node_id=api-node-b")
        assert path_res.status_code == 200
        assert path_res.json()["path"] == ["api-node-a", "api-node-b"]
        assert path_res.json()["hops"] == 1

        # 5. Route packet
        route_res = client.post("/api/v1/mesh/route", headers=auth_headers, json={
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
