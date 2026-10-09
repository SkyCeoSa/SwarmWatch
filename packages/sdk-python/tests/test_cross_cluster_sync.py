# -*- coding: utf-8 -*-
"""
Unit and integration tests for Autonomous Cross-Cluster State Synchronization
and Distributed Hyper-Graph Reconciliation (Seam 77 / ADR 0088).

Tests:
1. Hyper-Graph entity creation and structural integrity enforcement.
2. Vector clock causal ordering classification.
3. Invariant I1: Issuer-bound Proof-of-Execution certificate verification.
4. CRDT join semilattice reconciliation and multi-replica convergence.
5. Fail-closed safety circuit breaker coupling on causal gaps, dangling edges, and forged certificates.
6. Tamper-evident sync reconciliation receipts ('sw:sync:v1:<sha256>').
7. Control plane REST API endpoints and CLI commands.
"""

import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.byzantine_oracle import (
    NodeSigner,
    generate_node_keypair,
    compute_canonical_poe_hash,
    sign_bft_payload,
)
from swarmwatch.cross_cluster_sync import (
    CrossClusterSyncError,
    SyncCircuitTrippedError,
    UnverifiableCertificateError,
    CausalDiscontinuityError,
    HyperGraphIntegrityError,
    SyncStatus,
    CausalOrderingVerdict,
    HyperNodeType,
    HyperEdgeType,
    HyperNode,
    HyperEdge,
    DistributedHyperGraph,
    SyncPacket,
    SyncReconciliationReceipt,
    compare_vector_clocks,
    sign_cross_cluster_poe,
    verify_cross_cluster_poe,
    CrossClusterSyncEngine,
    global_cross_cluster_sync_engine,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main


@pytest.fixture(autouse=True)
def reset_sync_engine():
    global_cross_cluster_sync_engine.reset()
    yield
    global_cross_cluster_sync_engine.reset()


def test_hypergraph_entities_and_integrity():
    hg = DistributedHyperGraph(cluster_vector={"cluster-a": 1})
    
    n1 = HyperNode(
        node_id="n1",
        cluster_id="cluster-a",
        state_type=HyperNodeType.AGENT_CONTEXT,
        state_payload={"role": "planner", "turn": 4},
    )
    n2 = HyperNode(
        node_id="n2",
        cluster_id="cluster-a",
        state_type=HyperNodeType.RESOURCE_LEASE,
        state_payload={"resource_id": "res-99", "leased_to": "agent-1"},
    )
    hg.add_node(n1)
    hg.add_node(n2)

    # Valid hyper-edge connecting 2 nodes
    e1 = HyperEdge(
        edge_id="e1",
        node_ids=["n1", "n2"],
        edge_type=HyperEdgeType.COORDINATION_LEASE,
        properties={"timeout_ms": 5000},
        weight=0.95,
    )
    hg.add_edge(e1)
    assert hg.validate_integrity() is True
    assert len(hg.compute_root_digest()) == 64

    # HyperEdge with < 2 nodes must raise HyperGraphIntegrityError
    with pytest.raises(HyperGraphIntegrityError):
        HyperEdge(
            edge_id="e_invalid",
            node_ids=["n1"],
            edge_type=HyperEdgeType.COORDINATION_LEASE,
        )

    # Adding edge referencing non-existent node must raise HyperGraphIntegrityError
    e_dangling = HyperEdge(
        edge_id="e_dangling",
        node_ids=["n1", "ghost-node"],
        edge_type=HyperEdgeType.CAUSAL_DEPENDENCY,
    )
    with pytest.raises(HyperGraphIntegrityError):
        hg.add_edge(e_dangling)


def test_vector_clock_causal_ordering():
    # 1. Strictly ahead
    vc_local = {"c1": 2, "c2": 3}
    vc_ahead = {"c1": 2, "c2": 4}
    assert compare_vector_clocks(vc_local, vc_ahead) == CausalOrderingVerdict.CAUSALLY_AHEAD

    # 2. Strictly behind / stale
    vc_behind = {"c1": 1, "c2": 3}
    assert compare_vector_clocks(vc_local, vc_behind) == CausalOrderingVerdict.CAUSALLY_BEHIND

    # 3. Identical -> CAUSALLY_BEHIND (no new transitions)
    assert compare_vector_clocks(vc_local, dict(vc_local)) == CausalOrderingVerdict.CAUSALLY_BEHIND

    # 4. Concurrent
    vc_concurrent = {"c1": 3, "c2": 2}
    assert compare_vector_clocks(vc_local, vc_concurrent) == CausalOrderingVerdict.CONCURRENT

    # 5. Causal sequence gap
    vc_gap = {"c1": 2, "c2": 5}  # jump from 3 to 5
    assert compare_vector_clocks(vc_local, vc_gap) == CausalOrderingVerdict.CAUSAL_GAP_DETECTED


def test_invariant_i1_cross_cluster_poe_verification():
    # Setup issuing committee keys and root
    priv0, pub0 = generate_node_keypair()
    priv1, pub1 = generate_node_keypair()
    priv2, pub2 = generate_node_keypair()
    priv3, pub3 = generate_node_keypair()

    known_keys = {"v0": pub0, "v1": pub1, "v2": pub2, "v3": pub3}
    from swarmwatch.byzantine_oracle import ByzantineConsensusOracle
    oracle = ByzantineConsensusOracle()
    for vid, pk in known_keys.items():
        oracle.register_node(vid, "cluster-remote", pk)
    committee_root = oracle.compute_committee_root()

    # Valid PoE certificate issued for cluster-remote at epoch 2
    raw_cert = {
        "certificate_id": "",
        "proposal_id": "prop-cross-01",
        "action_type": "DEPLOY_PIPELINE",
        "view": 0,
        "sequence_number": 1,
        "digest": "aa" * 32,
        "pre_state_hash": "bb" * 32,
        "post_state_hash": "cc" * 32,
        "signers": ["v0", "v1", "v2"],
        "signatures": {},
        "epoch": 2,
        "committee_root": committee_root,
        "quorum_required": 3,
    }
    raw_cert["certificate_id"] = "poe-cert-cross-01"
    raw_cert["signatures"] = {
        "v0": sign_cross_cluster_poe(raw_cert, priv0),
        "v1": sign_cross_cluster_poe(raw_cert, priv1),
        "v2": sign_cross_cluster_poe(raw_cert, priv2),
    }
    raw_cert["receipt_hash"] = compute_canonical_poe_hash(raw_cert)

    # 1. Authentic certificate passes Invariant I1
    assert verify_cross_cluster_poe(
        raw_cert,
        known_committee_root=committee_root,
        expected_epoch=2,
        known_signers_keys=known_keys,
    ) is True

    # 2. Wrong epoch fails closed
    assert verify_cross_cluster_poe(
        raw_cert,
        known_committee_root=committee_root,
        expected_epoch=3,
        known_signers_keys=known_keys,
    ) is False

    # 3. Wrong committee root fails closed
    assert verify_cross_cluster_poe(
        raw_cert,
        known_committee_root="ff" * 32,
        expected_epoch=2,
        known_signers_keys=known_keys,
    ) is False

    # 4. Insufficient signatures fails closed
    under_quorum_cert = dict(raw_cert)
    under_quorum_cert["signers"] = ["v0", "v1"]
    under_quorum_cert["receipt_hash"] = compute_canonical_poe_hash(under_quorum_cert)
    assert verify_cross_cluster_poe(
        under_quorum_cert,
        known_committee_root=committee_root,
        expected_epoch=2,
        known_signers_keys=known_keys,
    ) is False

    # 5. Tampered receipt hash fails closed
    tampered_id_cert = dict(raw_cert)
    tampered_id_cert["receipt_hash"] = "sw:oracle:poe:v1:forged0000000000000000"
    assert verify_cross_cluster_poe(
        tampered_id_cert,
        known_committee_root=committee_root,
        expected_epoch=2,
        known_signers_keys=known_keys,
    ) is False


def test_cross_cluster_crdt_reconciliation_and_convergence():
    engine_a = CrossClusterSyncEngine(cluster_id="cluster-a", epoch=1, require_foreign_certificate=False)
    engine_b = CrossClusterSyncEngine(cluster_id="cluster-b", epoch=1, require_foreign_certificate=False)
    engine_a.register_foreign_committee("cluster-b", 1, "00" * 32)
    engine_b.register_foreign_committee("cluster-a", 1, "00" * 32)

    # Cluster A creates node and edge
    na1 = engine_a.upsert_local_node("shared-node-1", HyperNodeType.AGENT_CONTEXT, {"version": 1})
    na2 = engine_a.upsert_local_node("shared-node-2", HyperNodeType.RESOURCE_LEASE, {"status": "ACTIVE"})
    ea1 = engine_a.upsert_local_edge("shared-edge-1", ["shared-node-1", "shared-node-2"], HyperEdgeType.COORDINATION_LEASE, weight=0.8)

    # Cluster B creates node and edge concurrently
    nb1 = engine_b.upsert_local_node("shared-node-1", HyperNodeType.AGENT_CONTEXT, {"version": 2})
    nb3 = engine_b.upsert_local_node("shared-node-3", HyperNodeType.SHARED_MEMORY, {"memory": "bootstrapped"})
    eb1 = engine_b.upsert_local_edge("shared-edge-1", ["shared-node-1", "shared-node-3"], HyperEdgeType.COORDINATION_LEASE, weight=0.95)

    # Sync A -> B
    pkt_a_to_b = engine_a.create_sync_packet(target_cluster="cluster-b")
    receipt_b = engine_b.ingest_sync_packet(pkt_a_to_b)
    assert receipt_b.causal_verdict == CausalOrderingVerdict.CONCURRENT
    assert receipt_b.receipt_hash.startswith("sw:sync:v1:")
    assert engine_b.verify_reconciliation_receipt(receipt_b) is True

    # Sync B -> A
    pkt_b_to_a = engine_b.create_sync_packet(target_cluster="cluster-a")
    receipt_a = engine_a.ingest_sync_packet(pkt_b_to_a)
    assert receipt_a.receipt_hash.startswith("sw:sync:v1:")

    # Convergence check: both clusters have all 3 nodes and 1 merged edge
    assert len(engine_a.hypergraph.nodes) == 3
    assert len(engine_b.hypergraph.nodes) == 3
    
    # Merged edge connects all 3 nodes
    assert sorted(engine_a.hypergraph.edges["shared-edge-1"].node_ids) == ["shared-node-1", "shared-node-2", "shared-node-3"]
    assert sorted(engine_b.hypergraph.edges["shared-edge-1"].node_ids) == ["shared-node-1", "shared-node-2", "shared-node-3"]
    assert engine_a.hypergraph.edges["shared-edge-1"].weight == 0.95
    assert engine_b.hypergraph.edges["shared-edge-1"].weight == 0.95


def test_fail_closed_circuit_coupling_on_invariant_violations():
    engine = CrossClusterSyncEngine(cluster_id="cluster-c", epoch=1)
    
    # 1. Foreign packet with unregistered committee root raises UnverifiableCertificateError and trips circuit
    bad_cert_pkt = SyncPacket(
        packet_id="pkt-bad",
        source_cluster="cluster-unregistered",
        target_cluster="cluster-c",
        epoch=5,
        committee_root="ee" * 32,
        quorum_required=3,
        vector_clock={"cluster-unregistered": 1},
        nodes_delta=[],
        edges_delta=[],
        certificate={"epoch": 5, "committee_root": "ee" * 32, "quorum_required": 3, "signers": ["u1", "u2", "u3"]},
    )
    with pytest.raises(UnverifiableCertificateError):
        engine.ingest_sync_packet(bad_cert_pkt)

    assert engine.circuit_tripped is True
    assert engine.get_status()["status"] == "CIRCUIT_TRIPPED"

    # Subsequent operation rejected due to active trip
    with pytest.raises(SyncCircuitTrippedError):
        engine.upsert_local_node("n-fail", HyperNodeType.AGENT_CONTEXT, {})

    # Rearm circuit
    engine.rearm_circuit()
    assert engine.circuit_tripped is False

    # 2. Causal sequence gap trips circuit
    engine.register_foreign_committee("cluster-remote", 1, "00" * 32)
    engine.require_foreign_certificate = False
    engine.upsert_local_node("n0", HyperNodeType.AGENT_CONTEXT, {})  # vector_clock: {cluster-c: 1}
    gap_pkt = SyncPacket(
        packet_id="pkt-gap",
        source_cluster="cluster-remote",
        target_cluster="cluster-c",
        epoch=1,
        committee_root="",
        quorum_required=0,
        vector_clock={"cluster-c": 1, "cluster-remote": 10},  # gap from 0 to 10
        nodes_delta=[],
        edges_delta=[],
    )
    with pytest.raises(CausalDiscontinuityError):
        engine.ingest_sync_packet(gap_pkt)

    assert engine.circuit_tripped is True


def test_adversarial_p0_2_audit_vulnerabilities_neutralized():
    # 1. Repeated signer cannot satisfy quorum
    priv0, pub0 = generate_node_keypair()
    known_keys = {"v0": pub0, "v1": "01" * 32, "v2": "02" * 32, "v3": "03" * 32}
    raw_cert = {
        "certificate_id": "poe-fake-repeat",
        "proposal_id": "p1",
        "action_type": "EXECUTE",
        "view": 0,
        "sequence_number": 1,
        "digest": "aa" * 32,
        "pre_state_hash": "bb" * 32,
        "post_state_hash": "cc" * 32,
        "signers": ["v0", "v0", "v0"],  # repeated signer
        "signatures": {"v0": sign_bft_payload(priv0, "aa" * 32)},
        "epoch": 1,
        "committee_root": "00" * 32,
        "quorum_required": 3,
    }
    raw_cert["receipt_hash"] = compute_canonical_poe_hash(raw_cert)
    assert verify_cross_cluster_poe(raw_cert, "00" * 32, 1, known_keys) is False

    # 2. Unpinned keys fails closed
    assert verify_cross_cluster_poe(raw_cert, "00" * 32, 1, known_signers_keys=None) is False
    assert verify_cross_cluster_poe(raw_cert, "00" * 32, 1, known_signers_keys={}) is False

    # 3. Unregistered foreign cluster rejected even without certificate
    engine = CrossClusterSyncEngine(cluster_id="cluster-c", epoch=1)
    unreg_pkt = SyncPacket(
        packet_id="pkt-unreg",
        source_cluster="cluster-never-registered",
        target_cluster="cluster-c",
        epoch=1,
        committee_root="00" * 32,
        quorum_required=0,
        vector_clock={"cluster-c": 0},
        nodes_delta=[],
        edges_delta=[],
        certificate=None,
    )
    with pytest.raises(UnverifiableCertificateError):
        engine.ingest_sync_packet(unreg_pkt)
    assert engine.circuit_tripped is True



def test_rest_api_sync_endpoints():
    app = create_app()
    client = TestClient(app)

    # Obtain operator key for administrative routes
    k_res = client.post("/api/v1/keys", json={
        "project_id": "default-project",
        "name": "Sync Operator Key",
        "is_test": False,
        "scopes": ["*"]
    })
    assert k_res.status_code == 200
    auth_headers = {"Authorization": f"Bearer {k_res.json()['api_key']}"}

    # 1. Register foreign committee
    res = client.post("/api/v1/sync/register-committee", json={
        "cluster_id": "cluster-remote-api",
        "epoch": 1,
        "committee_root": "11" * 32,
    }, headers=auth_headers)
    assert res.status_code == 200
    assert res.json()["status"] == "REGISTERED"

    # 2. Upsert local node
    res_node = client.post("/api/v1/sync/node", json={
        "node_id": "api-node-1",
        "state_type": "AGENT_CONTEXT",
        "state_payload": {"role": "worker"},
    })
    assert res_node.status_code == 200
    assert res_node.json()["node_id"] == "api-node-1"

    # Upsert second node
    client.post("/api/v1/sync/node", json={
        "node_id": "api-node-2",
        "state_type": "RESOURCE_LEASE",
        "state_payload": {"resource": "db"},
    })

    # 3. Upsert local edge
    res_edge = client.post("/api/v1/sync/edge", json={
        "edge_id": "api-edge-1",
        "node_ids": ["api-node-1", "api-node-2"],
        "edge_type": "COORDINATION_LEASE",
        "weight": 0.9,
    })
    assert res_edge.status_code == 200
    assert res_edge.json()["edge_id"] == "api-edge-1"

    # 4. Generate outbound sync packet
    res_pkt = client.post("/api/v1/sync/packet?target_cluster=cluster-remote-api")
    assert res_pkt.status_code == 200
    pkt_data = res_pkt.json()
    assert pkt_data["target_cluster"] == "cluster-remote-api"

    # 5. Get anti-entropy digest
    res_digest = client.get("/api/v1/sync/digest")
    assert res_digest.status_code == 200
    assert res_digest.json()["node_count"] == 2
    assert res_digest.json()["edge_count"] == 1

    # 6. Reconcile foreign packet
    global_cross_cluster_sync_engine.require_foreign_certificate = False
    foreign_pkt = dict(pkt_data)
    foreign_pkt["source_cluster"] = "cluster-remote-api"
    foreign_pkt["target_cluster"] = global_cross_cluster_sync_engine.cluster_id
    res_rec = client.post("/api/v1/sync/reconcile", json={"packet": foreign_pkt})
    assert res_rec.status_code == 200
    rec_data = res_rec.json()
    assert rec_data["receipt_hash"].startswith("sw:sync:v1:")

    # 7. Verify receipt
    res_ver = client.post("/api/v1/sync/verify-receipt", json={"receipt": rec_data})
    assert res_ver.status_code == 200
    assert res_ver.json()["valid"] is True

    # 8. Check status and hypergraph endpoints
    res_stat = client.get("/api/v1/sync/status")
    assert res_stat.status_code == 200
    assert res_stat.json()["status"] == "NOMINAL"

    res_hg = client.get("/api/v1/sync/hypergraph")
    assert res_hg.status_code == 200
    assert len(res_hg.json()["nodes"]) == 2


def test_cli_cross_cluster_sync_commands(capsys):
    with pytest.raises(SystemExit) as exc:
        cli_main(["cross-cluster-sync", "status", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    st = json.loads(captured.out)
    assert "cluster_id" in st
    assert st["status"] == "NOMINAL"

    with pytest.raises(SystemExit) as exc:
        cli_main(["cross-cluster-sync", "node", "--node-id", "cli-node-1"])
    assert exc.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as exc:
        cli_main(["cross-cluster-sync", "packet", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    pkt = json.loads(captured.out)
    assert pkt["packet_id"].startswith("sync-pkt-")
