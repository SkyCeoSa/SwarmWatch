# -*- coding: utf-8 -*-
"""
Unit and integration tests for Autonomous Distributed Hyper-Graph Partition Healing
and Causal Vector Resumption (Seam 78 / ADR 0089).

Tests:
1. Partition detection via symmetric difference matrix and concurrency analysis.
2. Deterministic healing plan formulation.
3. Resumption delta creation and backlog extraction.
4. Invariant I2: Vector clock monotonicity enforcement (zero clock regression).
5. Invariant I1: Foreign Proof-of-Execution certificate verification during healing.
6. CRDT join semilattice hyper-graph splicing (node LWW, edge union, weight supremum).
7. Dangling edge structural integrity violation fail-closed circuit tripping.
8. Tamper-evident partition healing receipt ('sw:heal:v1:<sha256>').
9. Control plane REST API endpoints in server.py.
10. CLI commands in cli.py.
"""

import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.byzantine_oracle import (
    generate_node_keypair,
    compute_canonical_poe_hash,
    sign_bft_payload,
)
from swarmwatch.cross_cluster_sync import (
    HyperNodeType,
    HyperEdgeType,
    HyperNode,
    HyperEdge,
    HyperGraphIntegrityError,
    CrossClusterSyncEngine,
    sign_cross_cluster_poe,
)
from swarmwatch.partition_healer import (
    PartitionState,
    PartitionHealingError,
    UnhealedPartitionConflictError,
    InvalidResumptionProofError,
    ResumptionVectorRegressionError,
    HealingCircuitTrippedError,
    ResumptionDelta,
    PartitionHealingPlan,
    PartitionHealingReceipt,
    PartitionHealingEngine,
    global_partition_healer,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main


@pytest.fixture(autouse=True)
def reset_healer_engine():
    global_partition_healer.reset()
    yield
    global_partition_healer.reset()


def test_partition_detection_symmetric_difference():
    engine = PartitionHealingEngine(local_cluster="cluster-us-east", epoch=2)
    engine.hypergraph.cluster_vector["cluster-us-east"] = 10
    engine.hypergraph.cluster_vector["cluster-eu-west"] = 4

    # Case 1: Remote clock is purely ahead on eu-west, behind on us-east -> Concurrent divergence
    remote_vc = {
        "cluster-us-east": 6,
        "cluster-eu-west": 12,
    }
    is_partition, diffs = engine.detect_partition("cluster-eu-west", remote_vc)
    assert is_partition is True
    assert diffs["cluster-us-east"] == -4
    assert diffs["cluster-eu-west"] == 8
    assert engine.state == PartitionState.PARTITION_DETECTED

    # Case 2: Nominal advance without concurrent diversion or excessive gap
    engine.reset()
    engine.hypergraph.cluster_vector["cluster-us-east"] = 5
    is_part_nominal, diffs_nominal = engine.detect_partition(
        "cluster-eu-west", {"cluster-us-east": 5, "cluster-eu-west": 2}, gap_threshold=5
    )
    assert is_part_nominal is False


def test_healing_plan_formulation():
    engine = PartitionHealingEngine(local_cluster="cluster-us-east", epoch=3)
    engine.hypergraph.cluster_vector["cluster-us-east"] = 20
    engine.hypergraph.cluster_vector["cluster-eu-west"] = 5

    remote_vc = {
        "cluster-us-east": 15,
        "cluster-eu-west": 25,
    }
    plan = engine.formulate_healing_plan("cluster-eu-west", remote_vc)
    assert plan.is_partition_detected is True
    assert plan.local_cluster == "cluster-us-east"
    assert plan.remote_cluster == "cluster-eu-west"
    assert "cluster-us-east" in plan.divergent_clusters
    assert "cluster-eu-west" in plan.divergent_clusters
    assert plan.missing_local_steps == 5
    assert plan.missing_remote_steps == 20
    # Joined supremum clock
    assert plan.merged_vector_clock["cluster-us-east"] == 20
    assert plan.merged_vector_clock["cluster-eu-west"] == 25
    assert engine.state == PartitionState.PLANNING_RESUMPTION


def test_resumption_delta_creation():
    engine = PartitionHealingEngine(local_cluster="cluster-us-east", epoch=1)
    n1 = HyperNode(
        node_id="node-alpha",
        cluster_id="cluster-us-east",
        state_type=HyperNodeType.AGENT_CONTEXT,
        state_payload={"plan": "step-1"},
    )
    n2 = HyperNode(
        node_id="node-beta",
        cluster_id="cluster-us-east",
        state_type=HyperNodeType.RESOURCE_LEASE,
        state_payload={"resource": "gpu-01"},
    )
    e1 = HyperEdge(
        edge_id="edge-1",
        node_ids=["node-alpha", "node-beta"],
        edge_type=HyperEdgeType.COORDINATION_LEASE,
        weight=1.0,
    )
    engine.hypergraph.add_node(n1)
    engine.hypergraph.add_node(n2)
    engine.hypergraph.add_edge(e1)
    engine.hypergraph.cluster_vector["cluster-us-east"] = 7

    target_vc = {"cluster-us-east": 0}
    delta = engine.create_resumption_delta("cluster-eu-west", target_vc)
    assert delta.source_cluster == "cluster-us-east"
    assert delta.target_cluster == "cluster-eu-west"
    assert len(delta.nodes_delta) == 2
    assert len(delta.edges_delta) == 1
    assert len(delta.delta_hash) == 64
    assert delta.delta_id in engine.resumption_deltas


def test_crdt_hypergraph_splicing_and_receipt():
    engine = PartitionHealingEngine(local_cluster="cluster-us-east", epoch=1)
    n1 = HyperNode(
        node_id="node-1",
        cluster_id="cluster-us-east",
        state_type=HyperNodeType.AGENT_CONTEXT,
        state_payload={"turn": 1},
        epoch=1,
        updated_at_ns=1000,
    )
    n2 = HyperNode(
        node_id="node-2",
        cluster_id="cluster-us-east",
        state_type=HyperNodeType.RESOURCE_LEASE,
        state_payload={"lease": "active"},
        epoch=1,
        updated_at_ns=1000,
    )
    engine.hypergraph.add_node(n1)
    engine.hypergraph.add_node(n2)
    engine.hypergraph.cluster_vector["cluster-us-east"] = 5

    # Remote side has updated node-1, added node-3, and created edge-123
    r_n1 = HyperNode(
        node_id="node-1",
        cluster_id="cluster-eu-west",
        state_type=HyperNodeType.AGENT_CONTEXT,
        state_payload={"turn": 2, "updated_by": "eu"},
        epoch=1,
        updated_at_ns=2000,
    )
    r_n3 = HyperNode(
        node_id="node-3",
        cluster_id="cluster-eu-west",
        state_type=HyperNodeType.POLICY_STATE,
        state_payload={"quota": 100},
        epoch=1,
        updated_at_ns=1500,
    )
    r_e1 = HyperEdge(
        edge_id="edge-123",
        node_ids=["node-1", "node-2", "node-3"],
        edge_type=HyperEdgeType.COORDINATION_LEASE,
        properties={"status": "reconciled"},
        weight=0.85,
    )

    delta = ResumptionDelta(
        delta_id="delta-remote-001",
        source_cluster="cluster-eu-west",
        target_cluster="cluster-us-east",
        start_epoch=1,
        end_epoch=1,
        nodes_delta=[r_n1, r_n3],
        edges_delta=[r_e1],
        vector_clock={"cluster-eu-west": 8, "cluster-us-east": 2},
    )

    receipt = engine.execute_healing(delta)
    assert receipt.receipt_hash.startswith("sw:heal:v1:")
    assert receipt.nodes_reconciled == 2  # updated node-1 + new node-3
    assert receipt.edges_spliced == 1
    assert engine.verify_healing_receipt(receipt) is True

    # Check merged hypergraph state
    assert len(engine.hypergraph.nodes) == 3
    assert engine.hypergraph.nodes["node-1"].state_payload["turn"] == 2
    assert "edge-123" in engine.hypergraph.edges
    assert engine.hypergraph.edges["edge-123"].weight == 0.85

    # Check Invariant I2: clock never regresses and local counter advances
    assert engine.hypergraph.cluster_vector["cluster-eu-west"] == 8
    assert engine.hypergraph.cluster_vector["cluster-us-east"] == 6  # 5 + 1
    assert engine.state == PartitionState.HEALED


def test_dangling_edge_trips_circuit_fail_closed():
    engine = PartitionHealingEngine(local_cluster="cluster-us-east", epoch=1)
    n1 = HyperNode(
        node_id="node-exist-1",
        cluster_id="cluster-us-east",
        state_type=HyperNodeType.AGENT_CONTEXT,
        state_payload={"ok": True},
    )
    engine.hypergraph.add_node(n1)

    # Edge referencing a non-existent node
    bad_edge = HyperEdge(
        edge_id="edge-broken",
        node_ids=["node-exist-1", "node-missing-99"],
        edge_type=HyperEdgeType.COORDINATION_LEASE,
    )
    delta = ResumptionDelta(
        delta_id="delta-bad",
        source_cluster="cluster-eu-west",
        target_cluster="cluster-us-east",
        start_epoch=1,
        end_epoch=1,
        nodes_delta=[],
        edges_delta=[bad_edge],
        vector_clock={"cluster-eu-west": 5},
    )

    with pytest.raises(HyperGraphIntegrityError):
        engine.execute_healing(delta)

    assert engine.circuit_tripped is True
    assert engine.state == PartitionState.CIRCUIT_TRIPPED
    assert "Dangling edge" in engine.trip_reason

    # Once tripped, further operations must fail with HealingCircuitTrippedError
    with pytest.raises(HealingCircuitTrippedError):
        engine.formulate_healing_plan("cluster-eu-west", {"cluster-eu-west": 6})

    # Rearming circuit restores nominal state
    engine.rearm_circuit()
    assert engine.circuit_tripped is False
    assert engine.state == PartitionState.NOMINAL


def test_invariant_i2_negative_counter_regression_rejected():
    engine = PartitionHealingEngine(local_cluster="cluster-us-east", epoch=1)
    engine.hypergraph.cluster_vector["cluster-us-east"] = 10

    # Malicious delta asserting negative clock counter
    bad_delta = ResumptionDelta(
        delta_id="delta-malicious",
        source_cluster="cluster-eu-west",
        target_cluster="cluster-us-east",
        start_epoch=1,
        end_epoch=1,
        nodes_delta=[],
        edges_delta=[],
        vector_clock={"cluster-us-east": -1, "cluster-eu-west": 5},
    )

    with pytest.raises(ResumptionVectorRegressionError):
        engine.execute_healing(bad_delta)

    assert engine.circuit_tripped is True
    assert engine.state == PartitionState.CIRCUIT_TRIPPED


def test_invariant_i1_poe_verification_and_tamper():
    engine = PartitionHealingEngine(local_cluster="cluster-us-east", epoch=1)
    priv1, pub1 = generate_node_keypair()
    priv2, pub2 = generate_node_keypair()
    priv3, pub3 = generate_node_keypair()

    from swarmwatch.byzantine_oracle import ByzantineConsensusOracle
    oracle = ByzantineConsensusOracle()
    validators_map = {"v1": pub1, "v2": pub2, "v3": pub3}
    for vid, pk in validators_map.items():
        oracle.register_node(vid, "cluster-eu-west", pk)
    committee_root = oracle.compute_committee_root()

    engine.sync_engine.register_foreign_committee(
        cluster_id="cluster-eu-west",
        epoch=1,
        committee_root=committee_root,
        validator_keys=validators_map,
    )

    # Valid PoE certificate for epoch 1
    raw_cert = {
        "certificate_id": "poe-cert-cross-01",
        "proposal_id": "prop-cross-01",
        "action_type": "HEALING_RESUMPTION",
        "view": 0,
        "sequence_number": 1,
        "digest": "aa" * 32,
        "pre_state_hash": "bb" * 32,
        "post_state_hash": "cc" * 32,
        "signers": ["v1", "v2", "v3"],
        "signatures": {},
        "epoch": 1,
        "committee_root": committee_root,
        "quorum_required": 3,
    }
    raw_cert["signatures"] = {
        "v1": sign_cross_cluster_poe(raw_cert, priv1),
        "v2": sign_cross_cluster_poe(raw_cert, priv2),
        "v3": sign_cross_cluster_poe(raw_cert, priv3),
    }
    raw_cert["receipt_hash"] = compute_canonical_poe_hash(raw_cert)

    # Execute healing with valid PoE
    n1 = HyperNode(
        node_id="node-poe-1",
        cluster_id="cluster-eu-west",
        state_type=HyperNodeType.AGENT_CONTEXT,
        state_payload={"verified": True},
    )
    delta_valid = ResumptionDelta(
        delta_id="delta-poe-valid",
        source_cluster="cluster-eu-west",
        target_cluster="cluster-us-east",
        start_epoch=1,
        end_epoch=1,
        nodes_delta=[n1],
        edges_delta=[],
        vector_clock={"cluster-eu-west": 1},
        proof_of_execution=raw_cert,
    )
    receipt = engine.execute_healing(delta_valid)
    assert receipt.nodes_reconciled == 1

    # Tampered PoE signature must be rejected and trip circuit
    engine.reset()
    engine.sync_engine.register_foreign_committee(
        cluster_id="cluster-eu-west",
        epoch=1,
        committee_root=committee_root,
        validator_keys=validators_map,
    )
    tampered_poe = dict(raw_cert)
    tampered_poe["signatures"] = {"v1": "00" * 64, "v2": "00" * 64, "v3": "00" * 64}
    delta_tampered = ResumptionDelta(
        delta_id="delta-poe-tampered",
        source_cluster="cluster-eu-west",
        target_cluster="cluster-us-east",
        start_epoch=1,
        end_epoch=1,
        nodes_delta=[n1],
        edges_delta=[],
        vector_clock={"cluster-eu-west": 2},
        proof_of_execution=tampered_poe,
    )
    with pytest.raises(InvalidResumptionProofError):
        engine.execute_healing(delta_tampered)
    assert engine.circuit_tripped is True


def test_healer_rest_api():
    app = create_app()
    client = TestClient(app)

    # Obtain operator key for administrative routes
    k_res = client.post("/api/v1/keys", json={
        "project_id": "default-project",
        "name": "Healer Operator Key",
        "is_test": False,
        "scopes": ["*"]
    })
    assert k_res.status_code == 200
    auth_headers = {"Authorization": f"Bearer {k_res.json()['api_key']}"}

    # 1. Status
    res = client.get("/api/v1/healer/status")
    assert res.status_code == 200
    assert res.json()["state"] == "NOMINAL"

    # 2. Detect
    res = client.post("/api/v1/healer/detect", json={
        "remote_cluster": "cluster-apac",
        "remote_vector_clock": {"cluster-apac": 10, "cluster-local": 0},
    })
    assert res.status_code == 200
    assert res.json()["is_partition_detected"] is True

    # 3. Plan
    res = client.post("/api/v1/healer/plan", json={
        "remote_cluster": "cluster-apac",
        "remote_vector_clock": {"cluster-apac": 10, "cluster-local": 0},
    })
    assert res.status_code == 200
    assert "plan_id" in res.json()

    # 4. Delta
    res = client.post("/api/v1/healer/delta", json={
        "target_cluster": "cluster-apac",
        "target_vector_clock": {},
    })
    assert res.status_code == 200
    assert "delta_id" in res.json()

    # 5. Execute
    node_a = HyperNode(node_id="api-n1", cluster_id="cluster-apac", state_type=HyperNodeType.AGENT_CONTEXT, state_payload={"turn": 1}).to_dict()
    node_b = HyperNode(node_id="api-n2", cluster_id="cluster-apac", state_type=HyperNodeType.RESOURCE_LEASE, state_payload={"lease": "active"}).to_dict()
    edge_ab = HyperEdge(edge_id="api-e1", node_ids=["api-n1", "api-n2"], edge_type=HyperEdgeType.COORDINATION_LEASE).to_dict()
    exec_payload = {
        "delta": {
            "delta_id": "delta-api-01",
            "source_cluster": "cluster-apac",
            "target_cluster": "cluster-local",
            "start_epoch": 1,
            "end_epoch": 1,
            "nodes_delta": [node_a, node_b],
            "edges_delta": [edge_ab],
            "vector_clock": {"cluster-apac": 5},
        }
    }
    res = client.post("/api/v1/healer/execute", json=exec_payload)
    assert res.status_code == 200
    receipt = res.json()
    assert receipt["receipt_hash"].startswith("sw:heal:v1:")
    assert receipt["nodes_reconciled"] == 2

    # 6. Verify Receipt
    res = client.post("/api/v1/healer/verify-receipt", json={"receipt": receipt})
    assert res.status_code == 200
    assert res.json()["valid"] is True

    # 7. Receipts and Plans list
    res = client.get("/api/v1/healer/receipts")
    assert res.status_code == 200
    assert len(res.json()) == 1

    res = client.get("/api/v1/healer/plans")
    assert res.status_code == 200
    assert len(res.json()) == 1

    # 8. Rearm
    res = client.post("/api/v1/healer/circuit/rearm", headers=auth_headers)
    assert res.status_code == 200
    assert res.json()["circuit_tripped"] is False


def test_healer_cli_actions(capsys):
    # Status
    with pytest.raises(SystemExit) as exc:
        cli_main(["partition-healer", "status", "-f", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    st = json.loads(captured.out)
    assert st["state"] == "NOMINAL"

    # Detect
    with pytest.raises(SystemExit) as exc:
        cli_main(["healer", "detect", "--remote-cluster", "cluster-apac", "--remote-vc", '{"cluster-apac": 8}'])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "Partition Detected: True" in captured.out

    # Plan
    with pytest.raises(SystemExit) as exc:
        cli_main(["healer", "plan", "--remote-cluster", "cluster-apac", "--remote-vc", '{"cluster-apac": 8}', "-f", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    plan = json.loads(captured.out)
    assert plan["is_partition_detected"] is True

    # Delta
    with pytest.raises(SystemExit) as exc:
        cli_main(["healer", "delta", "--remote-cluster", "cluster-apac", "-f", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    delta = json.loads(captured.out)
    assert len(delta["delta_hash"]) == 64

    # Rearm
    with pytest.raises(SystemExit) as exc:
        cli_main(["healer", "rearm", "-f", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    rearm = json.loads(captured.out)
    assert rearm["circuit_tripped"] is False
