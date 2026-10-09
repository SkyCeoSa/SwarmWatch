# -*- coding: utf-8 -*-
"""
Unit and integration tests for Dynamic Topology Re-Partitioning & Adaptive Sub-Graph Migration
(Seam 79 / ADR 0090).

Tests:
1. Load profile registration and hyper-graph cut skew evaluation.
2. Deterministic migration plan formulation (internal vs boundary cut edges).
3. SubGraphBundle preparation with boundary contracts.
4. Atomic sub-graph ingestion on target cluster and receipt generation.
5. Invariant M1: Causal vector clock monotonicity handover (zero regression).
6. Invariant M2: Boundary closure and dangling edge prevention.
7. Invariant M3: Cryptographic Proof-of-Execution certificate attestation & verification.
8. Fail-closed circuit breaker latching and administrative rearming.
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
    DistributedHyperGraph,
    sign_cross_cluster_poe,
)
from swarmwatch.topology_migrator import (
    TopologyMigrationError,
    MigrationCircuitTrippedError,
    MigrationCausalRegressionError,
    OrphanSubGraphEdgeError,
    InvalidMigrationProofError,
    SubGraphNotFoundError,
    InvalidMigrationPlanError,
    MigrationState,
    ClusterLoadProfile,
    SubGraphBoundaryContract,
    SubGraphBundle,
    MigrationPlan,
    MigrationReceipt,
    DynamicTopologyMigrator,
    global_topology_migrator,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main


@pytest.fixture(autouse=True)
def reset_migration_engine():
    global_topology_migrator.rearm_circuit()
    global_topology_migrator._load_profiles.clear()
    global_topology_migrator._plans.clear()
    global_topology_migrator._bundles.clear()
    global_topology_migrator._receipts.clear()
    global_topology_migrator._vector_clock = {"cluster-default": 1}
    global_topology_migrator._hypergraph = DistributedHyperGraph()
    yield
    global_topology_migrator.rearm_circuit()


def test_load_profile_registration_and_skew_evaluation():
    engine = DynamicTopologyMigrator(cluster_id="cluster-us-east")

    p1 = ClusterLoadProfile("cluster-us-east", node_count=100, edge_count=150, cpu_utilization=0.9, memory_utilization=0.85)
    p2 = ClusterLoadProfile("cluster-eu-west", node_count=20, edge_count=25, cpu_utilization=0.15, memory_utilization=0.20)

    engine.register_cluster_profile(p1)
    engine.register_cluster_profile(p2)

    skew = engine.evaluate_partition_skew()
    assert skew["cluster_count"] == 2
    assert skew["most_loaded_cluster"] == "cluster-us-east"
    assert skew["least_loaded_cluster"] == "cluster-eu-west"
    assert skew["load_delta"] > 0.40
    assert skew["migration_recommended"] is True


def test_migration_plan_formulation_internal_and_boundary_edges():
    engine = DynamicTopologyMigrator(cluster_id="cluster-us-east")

    src_graph = DistributedHyperGraph()
    # Nodes
    src_graph.nodes["node-A"] = HyperNode("node-A", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {})
    src_graph.nodes["node-B"] = HyperNode("node-B", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {})
    src_graph.nodes["node-C"] = HyperNode("node-C", "cluster-us-east", HyperNodeType.SHARED_MEMORY, {})
    src_graph.nodes["node-D"] = HyperNode("node-D", "cluster-us-east", HyperNodeType.RESOURCE_LEASE, {})

    # Edge 1: internal to {node-A, node-B}
    src_graph.edges["edge-int-1"] = HyperEdge("edge-int-1", ["node-A", "node-B"], HyperEdgeType.COORDINATION_LEASE, {}, weight=0.8)

    # Edge 2: cut edge spanning {node-B, node-C}
    src_graph.edges["edge-cut-1"] = HyperEdge("edge-cut-1", ["node-B", "node-C"], HyperEdgeType.CAUSAL_DEPENDENCY, {}, weight=0.5)

    # Edge 3: completely unrelated {node-C, node-D}
    src_graph.edges["edge-other"] = HyperEdge("edge-other", ["node-C", "node-D"], HyperEdgeType.COORDINATION_LEASE, {}, weight=0.3)

    plan = engine.formulate_migration_plan(
        source_cluster_id="cluster-us-east",
        target_cluster_id="cluster-eu-west",
        candidate_node_ids=["node-A", "node-B"],
        source_hypergraph=src_graph,
    )

    assert plan.plan_id.startswith("mig-plan-")
    assert plan.plan_hash.startswith("sw:mig:plan:v1:")
    assert plan.node_ids == ["node-A", "node-B"]
    assert plan.edge_ids == ["edge-int-1"]
    assert plan.boundary_edge_ids == ["edge-cut-1"]
    assert plan.estimated_cut_reduction == 0.4  # 0.8 * 0.5
    assert engine.state == MigrationState.PLAN_SYNTHESIZED


def test_subgraph_bundle_preparation_and_contracts():
    engine = DynamicTopologyMigrator(cluster_id="cluster-us-east")

    src_graph = DistributedHyperGraph()
    src_graph.nodes["n1"] = HyperNode("n1", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {})
    src_graph.nodes["n2"] = HyperNode("n2", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {})
    src_graph.nodes["n3"] = HyperNode("n3", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {})

    src_graph.edges["e-int"] = HyperEdge("e-int", ["n1", "n2"], HyperEdgeType.COORDINATION_LEASE, {}, weight=0.9)
    src_graph.edges["e-cut"] = HyperEdge("e-cut", ["n2", "n3"], HyperEdgeType.COORDINATION_LEASE, {}, weight=0.6)

    plan = engine.formulate_migration_plan("cluster-us-east", "cluster-eu-west", ["n1", "n2"], src_graph)

    source_clock = {"cluster-us-east": 15, "cluster-eu-west": 4}
    bundle = engine.prepare_subgraph_transfer(plan.plan_id, src_graph, source_clock)

    assert bundle.subgraph_id == plan.subgraph_id
    assert bundle.bundle_digest.startswith("sw:mig:bundle:v1:")
    assert "n1" in bundle.nodes and "n2" in bundle.nodes
    assert "e-int" in bundle.internal_edges
    assert len(bundle.boundary_contracts) == 1

    bc = bundle.boundary_contracts[0]
    assert bc.edge_id == "e-cut"
    assert bc.cut_node_ids == ["n2"]
    assert bc.retained_node_ids == ["n3"]


def test_atomic_migration_execution_and_receipt():
    engine_src = DynamicTopologyMigrator(cluster_id="cluster-us-east")
    engine_dst = DynamicTopologyMigrator(cluster_id="cluster-eu-west")

    src_graph = DistributedHyperGraph()
    src_graph.nodes["worker-1"] = HyperNode("worker-1", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {})
    src_graph.nodes["worker-2"] = HyperNode("worker-2", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {})
    src_graph.edges["edge-w"] = HyperEdge("edge-w", ["worker-1", "worker-2"], HyperEdgeType.COORDINATION_LEASE, {}, weight=0.95)

    plan = engine_src.formulate_migration_plan("cluster-us-east", "cluster-eu-west", ["worker-1", "worker-2"], src_graph)
    bundle = engine_src.prepare_subgraph_transfer(plan.plan_id, src_graph, {"cluster-us-east": 20})

    target_graph = DistributedHyperGraph()
    target_clock = {"cluster-eu-west": 5}

    receipt = engine_dst.execute_migration(bundle, target_graph, target_clock)

    assert receipt.receipt_hash.startswith("sw:mig:rcpt:v1:")
    assert receipt.migrated_nodes_count == 2
    assert receipt.migrated_edges_count == 1
    assert "worker-1" in target_graph.nodes
    assert "worker-2" in target_graph.nodes
    assert "edge-w" in target_graph.edges

    # Target post-migration clock must have source advanced + target incremented
    assert receipt.post_migration_vector_clock["cluster-us-east"] >= 20
    assert receipt.post_migration_vector_clock["cluster-eu-west"] == 6
    assert engine_dst.verify_migration_receipt(receipt) is True


def test_invariant_m1_causal_regression_defense():
    engine = DynamicTopologyMigrator(cluster_id="cluster-eu-west")

    bundle = SubGraphBundle(
        subgraph_id="subgraph-test",
        source_cluster_id="cluster-us-east",
        target_cluster_id="cluster-eu-west",
        node_ids=["n1"],
        nodes={"n1": HyperNode("n1", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}).to_dict()},
        internal_edges={},
        boundary_contracts=[],
        vector_clock={"cluster-us-east": -5},  # Negative clock counter violation
        epoch=1,
    )

    target_graph = DistributedHyperGraph()
    target_clock = {"cluster-eu-west": 10}

    with pytest.raises(MigrationCausalRegressionError):
        engine.execute_migration(bundle, target_graph, target_clock)

    assert engine.circuit_tripped is True
    assert engine.state == MigrationState.CIRCUIT_TRIPPED


def test_invariant_m2_boundary_closure_and_orphan_edges():
    engine = DynamicTopologyMigrator(cluster_id="cluster-eu-west")

    # Internal edge in bundle references node "orphan-node" which is NOT in bundle.node_ids
    bundle = SubGraphBundle(
        subgraph_id="subgraph-leak",
        source_cluster_id="cluster-us-east",
        target_cluster_id="cluster-eu-west",
        node_ids=["n1"],
        nodes={"n1": HyperNode("n1", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}).to_dict()},
        internal_edges={
            "leaky-edge": HyperEdge("leaky-edge", ["n1", "orphan-node"], HyperEdgeType.COORDINATION_LEASE, {}).to_dict()
        },
        boundary_contracts=[],
        vector_clock={"cluster-us-east": 1},
        epoch=1,
    )

    target_graph = DistributedHyperGraph()
    with pytest.raises(OrphanSubGraphEdgeError):
        engine.execute_migration(bundle, target_graph, {"cluster-eu-west": 1})

    assert engine.circuit_tripped is True
    assert engine.state == MigrationState.CIRCUIT_TRIPPED


def test_invariant_m3_poe_verification_and_circuit_tripping():
    engine = DynamicTopologyMigrator(cluster_id="cluster-eu-west", epoch=1)

    priv1, pub1 = generate_node_keypair()
    priv2, pub2 = generate_node_keypair()
    validator_keys = {"v1": pub1, "v2": pub2}
    comm_root = "root-comm-12345"

    engine.register_foreign_committee(
        cluster_id="cluster-us-east",
        epoch=1,
        committee_root=comm_root,
        validator_keys=validator_keys,
        quorum_required=2,
    )

    cert_data = {
        "certificate_id": "poe-1",
        "proposal_id": "prop-1",
        "action_type": "SUBGRAPH_MIGRATION",
        "view": 0,
        "sequence_number": 1,
        "digest": "digest-1",
        "pre_state_hash": "pre-1",
        "post_state_hash": "post-1",
        "signers": ["v1", "v2"],
        "epoch": 1,
        "committee_root": comm_root,
        "quorum_required": 2,
    }
    cert_hash = compute_canonical_poe_hash(cert_data)
    valid_cert = dict(cert_data)
    valid_cert["receipt_hash"] = cert_hash
    valid_cert["signatures"] = {
        "v1": sign_cross_cluster_poe(cert_data, priv1),
        "v2": sign_cross_cluster_poe(cert_data, priv2),
    }

    # Valid bundle transfer
    bundle = SubGraphBundle(
        subgraph_id="subgraph-poe",
        source_cluster_id="cluster-us-east",
        target_cluster_id="cluster-eu-west",
        node_ids=["n1"],
        nodes={"n1": HyperNode("n1", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}).to_dict()},
        internal_edges={},
        boundary_contracts=[],
        vector_clock={"cluster-us-east": 1},
        epoch=1,
    )

    target_graph = DistributedHyperGraph()
    rcpt = engine.execute_migration(bundle, target_graph, {"cluster-eu-west": 1}, poe_certificate=valid_cert)
    assert rcpt.receipt_hash.startswith("sw:mig:rcpt:v1:")

    # Tampered certificate must raise InvalidMigrationProofError and trip circuit
    tampered_cert = dict(valid_cert)
    tampered_cert["signatures"] = {"v1": "deadbeef" * 8, "v2": "deadbeef" * 8}

    with pytest.raises(InvalidMigrationProofError):
        engine.execute_migration(bundle, target_graph, {"cluster-eu-west": 2}, poe_certificate=tampered_cert)

    assert engine.circuit_tripped is True


def test_migration_rest_api():
    app = create_app()
    client = TestClient(app)

    # 1. Register profile
    res1 = client.post("/api/v1/migration/profile", json={
        "cluster_id": "c1",
        "node_count": 80,
        "edge_count": 120,
        "cpu_utilization": 0.85,
        "memory_utilization": 0.80,
    })
    assert res1.status_code == 200
    assert res1.json()["status"] == "REGISTERED"

    # 2. Evaluate skew
    res2 = client.post("/api/v1/migration/evaluate", json={})
    assert res2.status_code == 200
    assert "mean_load" in res2.json()

    # 3. Formulate plan
    src_g = DistributedHyperGraph()
    src_g.nodes["agent-a"] = HyperNode("agent-a", "c1", HyperNodeType.AGENT_CONTEXT, {})
    res3 = client.post("/api/v1/migration/plan", json={
        "source_cluster_id": "c1",
        "target_cluster_id": "c2",
        "candidate_node_ids": ["agent-a"],
        "source_graph": src_g.to_dict(),
    })
    assert res3.status_code == 200
    plan_data = res3.json()
    assert plan_data["plan_id"].startswith("mig-plan-")

    # 4. Prepare transfer
    res4 = client.post("/api/v1/migration/prepare", json={
        "plan_id": plan_data["plan_id"],
        "source_graph": src_g.to_dict(),
    })
    assert res4.status_code == 200
    bundle_data = res4.json()
    assert bundle_data["subgraph_id"] == plan_data["subgraph_id"]

    # 5. Execute migration
    res5 = client.post("/api/v1/migration/execute", json={
        "bundle": bundle_data,
        "target_clock": {"c2": 1},
    })
    assert res5.status_code == 200
    rcpt_data = res5.json()
    assert rcpt_data["receipt_hash"].startswith("sw:mig:rcpt:v1:")

    # 6. Verify receipt
    res6 = client.post("/api/v1/migration/verify-receipt", json={"receipt": rcpt_data})
    assert res6.status_code == 200
    assert res6.json()["valid"] is True

    # 7. Status endpoint
    res7 = client.get("/api/v1/migration/status")
    assert res7.status_code == 200
    assert res7.json()["receipts_count"] >= 1


def test_migration_cli_actions(capsys):
    # Action 1: status
    with pytest.raises(SystemExit) as e1:
        cli_main(["migration", "status", "--format", "json"])
    assert e1.value.code == 0
    out1 = capsys.readouterr().out
    st = json.loads(out1)
    assert st["state"] in ("IDLE", "COMPLETED_STABLE")

    # Action 2: evaluate
    with pytest.raises(SystemExit) as e2:
        cli_main(["migration", "evaluate", "--format", "json"])
    assert e2.value.code == 0
    out2 = capsys.readouterr().out
    skew = json.loads(out2)
    assert "mean_load" in skew

    # Action 3: plan
    with pytest.raises(SystemExit) as e3:
        cli_main(["migration", "plan", "--source-cluster", "c-src", "--target-cluster", "c-dst", "--nodes", "worker-x", "--format", "json"])
    assert e3.value.code == 0
    out3 = capsys.readouterr().out
    plan = json.loads(out3)
    assert plan["plan_hash"].startswith("sw:mig:plan:v1:")

    # Action 4: rearm
    with pytest.raises(SystemExit) as e4:
        cli_main(["migration", "rearm", "--format", "json"])
    assert e4.value.code == 0
    out4 = capsys.readouterr().out
    assert json.loads(out4)["status"] == "ok"
