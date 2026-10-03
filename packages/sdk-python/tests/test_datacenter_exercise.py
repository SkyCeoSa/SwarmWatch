"""
Unit and integration tests for Datacenter Failure Exercise & Region Loss Fault Injection (Seam 70 / ADR 0081).
"""

import time
import pytest
from swarmwatch.datacenter_exercise import (
    DatacenterExerciseError,
    MinorityPartitionBlockedError,
    SplitBrainViolationError,
    SafetyMonotonicityViolationError,
    FailureScenarioType,
    ExercisePhase,
    NodeRole,
    ClusterNode,
    NetworkMatrix,
    PermitAttemptResult,
    FailureExerciseEvent,
    FailureExerciseReport,
    DatacenterFailureOrchestrator,
    global_datacenter_orchestrator,
)


def test_cluster_node_and_network_matrix() -> None:
    node = ClusterNode(node_id="n1", region_id="us-east-1", role=NodeRole.LEADER)
    d = node.to_dict()
    assert d["node_id"] == "n1"
    assert d["role"] == "LEADER"

    reconstructed = ClusterNode.from_dict(d)
    assert reconstructed.region_id == "us-east-1"
    assert reconstructed.role == NodeRole.LEADER

    matrix = NetworkMatrix(["us-east-1", "us-west-2", "eu-central-1"])
    assert matrix.is_reachable("us-east-1", "us-west-2") is True
    assert matrix.is_reachable("us-east-1", "eu-central-1") is True

    matrix.sever_region("eu-central-1")
    assert matrix.is_reachable("us-east-1", "us-west-2") is True
    assert matrix.is_reachable("us-east-1", "eu-central-1") is False
    assert matrix.is_reachable("eu-central-1", "us-east-1") is False

    matrix.restore_all()
    assert matrix.is_reachable("eu-central-1", "us-east-1") is True


def test_majority_quorum_calculation() -> None:
    nodes = [
        ClusterNode("n1", "us-east-1"),
        ClusterNode("n2", "us-east-1"),
        ClusterNode("n3", "us-west-2"),
        ClusterNode("n4", "us-west-2"),
        ClusterNode("n5", "eu-central-1"),
    ]
    orchestrator = DatacenterFailureOrchestrator(nodes=nodes)

    # In nominal state, all 5 nodes have majority quorum
    for n in ["n1", "n2", "n3", "n4", "n5"]:
        assert orchestrator.has_majority_quorum(n) is True

    # Sever eu-central-1 (1 node severed out of 5)
    orchestrator.inject_partition(severed_regions=["eu-central-1"])

    # us-east-1 (2) + us-west-2 (2) = 4 reachable nodes >= 3 (majority)
    assert orchestrator.has_majority_quorum("n1") is True
    assert orchestrator.has_majority_quorum("n3") is True

    # eu-central-1 (1 node) has only 1 reachable node < 3 -> minority partition
    assert orchestrator.has_majority_quorum("n5") is False


def test_nominal_permit_grant() -> None:
    orchestrator = DatacenterFailureOrchestrator()
    res = orchestrator.request_permit("node-us-east-1", tenant_id="tenant-alpha", tokens=100)
    assert res.granted is True
    assert res.node_id == "node-us-east-1"
    assert res.permit_id is not None
    assert "Majority quorum verified" in res.reason


def test_minority_partition_blocked_fail_closed() -> None:
    orchestrator = DatacenterFailureOrchestrator()
    orchestrator.inject_partition(severed_regions=["eu-central-1"])

    # Attempting permit on isolated node MUST throw MinorityPartitionBlockedError
    with pytest.raises(MinorityPartitionBlockedError):
        orchestrator.request_permit("node-eu-cent-1", tenant_id="tenant-alpha", tokens=50)


def test_majority_continues_during_minority_severance() -> None:
    orchestrator = DatacenterFailureOrchestrator()
    orchestrator.inject_partition(severed_regions=["eu-central-1"])

    # Nodes in us-east-1 and us-west-2 continue issuing permits smoothly
    res1 = orchestrator.request_permit("node-us-east-1", tokens=25)
    assert res1.granted is True

    res2 = orchestrator.request_permit("node-us-west-1", tokens=30)
    assert res2.granted is True


def test_safety_trip_monotonicity_across_partitions() -> None:
    orchestrator = DatacenterFailureOrchestrator()
    orchestrator.inject_partition(severed_regions=["eu-central-1"])

    # Trigger trip from a majority node
    trip_res = orchestrator.broadcast_trip("node-us-east-1", reason="Excessive entropy drop")
    assert trip_res["cluster_globally_tripped"] is True

    # Reconnect network
    orchestrator.heal_partition()

    # Invariant: Cluster remains tripped monotonically!
    st = orchestrator.status()
    assert st["cluster_globally_tripped"] is True

    # Post-heal permit attempt must fail closed
    res = orchestrator.request_permit("node-us-east-1", tokens=10)
    assert res.granted is False
    assert "Cluster safety trip latched" in res.reason


def test_trip_from_minority_node_latches_upon_heal() -> None:
    orchestrator = DatacenterFailureOrchestrator()
    orchestrator.inject_partition(severed_regions=["eu-central-1"])

    # Trigger trip from minority node
    orchestrator.broadcast_trip("node-eu-cent-1", reason="Minority node detected local corruption")

    # Heal partition
    orchestrator.heal_partition()

    # Invariant: Trip from minority node propagates to global cluster upon heal
    st = orchestrator.status()
    assert st["cluster_globally_tripped"] is True


def test_asymmetric_network_partition() -> None:
    orchestrator = DatacenterFailureOrchestrator()
    # us-east can send to us-west, but us-west cannot send back to us-east
    orchestrator.inject_partition(
        severed_regions=[],
        asymmetric_routes=[("us-west-2", "us-east-1")],
    )

    # us-east-1 can reach us-east-1 (2) + us-west-2 (2) + eu-central-1 (1) = 5 nodes >= 3
    assert orchestrator.has_majority_quorum("node-us-east-1") is True

    # us-west-2 can reach us-west-2 (2) + eu-central-1 (1) = 3 nodes >= 3
    assert orchestrator.has_majority_quorum("node-us-west-1") is True


def test_end_to_end_run_exercise_nominal() -> None:
    orchestrator = DatacenterFailureOrchestrator()
    report = orchestrator.run_exercise(
        scenario=FailureScenarioType.REGION_SEVERED,
        target_severed_regions=["eu-central-1"],
    )

    assert report.scenario == FailureScenarioType.REGION_SEVERED
    assert report.majority_partition_operational is True
    assert report.minority_partition_isolated is True
    assert report.split_brain_prevented is True
    assert report.safety_monotonicity_verified is True
    assert report.invariants_passed is True
    assert len(report.exercise_hash) == 64
    assert len(report.events) >= 5

    d = report.to_dict()
    assert d["invariants_passed"] is True
    assert "sw:fail:v1" not in d["exercise_hash"]  # raw hex hash


def test_sub_250us_orchestrator_slo() -> None:
    orchestrator = DatacenterFailureOrchestrator()

    # Warmup
    for _ in range(10):
        orchestrator.has_majority_quorum("node-us-east-1")

    # Benchmark: 500 iterations of quorum and reachability checks
    iterations = 500
    t0 = time.perf_counter()
    for _ in range(iterations):
        orchestrator.has_majority_quorum("node-us-east-1")
        orchestrator.get_reachable_nodes("node-us-east-1")
    t1 = time.perf_counter()

    avg_us = ((t1 - t0) / iterations) * 1_000_000
    assert avg_us < 250.0, f"Datacenter quorum check took {avg_us:.2f}µs, exceeding 250µs SLO"


def test_global_singleton_status_and_reset() -> None:
    st = global_datacenter_orchestrator.status()
    assert "total_nodes" in st
    assert st["total_nodes"] == 5
    assert st["total_regions"] == 3

    global_datacenter_orchestrator.inject_partition(severed_regions=["eu-central-1"])
    assert global_datacenter_orchestrator.status()["active_phase"] == "REGION_ISOLATION"

    global_datacenter_orchestrator.reset()
    assert global_datacenter_orchestrator.status()["active_phase"] == "NOMINAL"
    assert global_datacenter_orchestrator.status()["cluster_globally_tripped"] is False


def test_datacenter_exercise_rest_api() -> None:
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app
    from swarmwatch.storage import SQLiteStorageAdapter

    app = create_app(storage=SQLiteStorageAdapter(db_path=":memory:"))
    client = TestClient(app)

    # Reset state
    global_datacenter_orchestrator.reset()

    # 1. Status endpoint
    res_st = client.get("/api/v1/exercise/status")
    assert res_st.status_code == 200
    st_data = res_st.json()
    assert st_data["total_nodes"] == 5
    assert st_data["active_phase"] == "NOMINAL"

    # 2. Permit in nominal state
    res_pmt = client.post(
        "/api/v1/exercise/permit",
        json={"node_id": "node-us-east-1", "tenant_id": "tenant-api-test", "tokens": 50},
    )
    assert res_pmt.status_code == 200
    pmt_data = res_pmt.json()
    assert pmt_data["granted"] is True
    assert pmt_data["permit_id"] is not None

    # 3. Inject partition severing eu-central-1
    res_inj = client.post(
        "/api/v1/exercise/inject-partition",
        json={"severed_regions": ["eu-central-1"]},
    )
    assert res_inj.status_code == 200
    assert res_inj.json()["status"] == "partition_injected"

    # 4. Permit on isolated node must fail-closed (503 Service Unavailable)
    res_iso = client.post(
        "/api/v1/exercise/permit",
        json={"node_id": "node-eu-cent-1", "tenant_id": "tenant-api-test", "tokens": 50},
    )
    assert res_iso.status_code == 503

    # 5. Broadcast trip from majority node
    res_trip = client.post(
        "/api/v1/exercise/trip",
        json={"origin_node_id": "node-us-east-1", "reason": "REST API Emergency Trip"},
    )
    assert res_trip.status_code == 200
    assert res_trip.json()["cluster_globally_tripped"] is True

    # 6. Heal partition
    res_heal = client.post("/api/v1/exercise/heal-partition")
    assert res_heal.status_code == 200
    assert res_heal.json()["status"] == "partition_healed"

    # 7. Post-heal permit fails because cluster safety trip latched monotonically
    res_post_heal = client.post(
        "/api/v1/exercise/permit",
        json={"node_id": "node-us-east-1", "tenant_id": "tenant-api-test", "tokens": 10},
    )
    assert res_post_heal.status_code == 200
    assert res_post_heal.json()["granted"] is False

    # 8. Reset and run full automated exercise
    global_datacenter_orchestrator.reset()
    res_run = client.post(
        "/api/v1/exercise/run",
        json={"scenario": "REGION_SEVERED", "target_severed_regions": ["eu-central-1"]},
    )
    assert res_run.status_code == 200
    run_data = res_run.json()
    assert run_data["invariants_passed"] is True
    assert run_data["majority_partition_operational"] is True
    assert run_data["minority_partition_isolated"] is True
    assert run_data["split_brain_prevented"] is True
    assert run_data["safety_monotonicity_verified"] is True

    # 9. Get latest report
    res_rep = client.get("/api/v1/exercise/report")
    assert res_rep.status_code == 200
    rep_data = res_rep.json()
    assert rep_data["exercise_id"] == run_data["exercise_id"]
    assert len(rep_data["exercise_hash"]) == 64

