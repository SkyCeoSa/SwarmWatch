# -*- coding: utf-8 -*-
"""
Tests for Partition Reconciliation & Conflict Lattice Engine (Seam 57 / ADR 0068).
Verifies:
1. Vector clock lattice partial ordering (DOMINATES, DOMINATED, IDENTICAL, CONCURRENT).
2. Delta diffing and structured ReconciliationPlan generation.
3. Telemetry evidence merging, deduplication, and concurrent fork provenance retention.
4. Trip Monotonicity Invariant: TRIPPED state is an absorbing barrier across partitions.
5. Monotonic epoch advancement and stale pre-partition permit revocation on conflict.
6. REST endpoints and CLI subcommands.
"""

from __future__ import annotations

import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.canonical import CanonicalCodec, CanonicalDomainTag
from swarmwatch.cli import main as cli_main
from swarmwatch.mesh_reconciler import (
    AuthorityReconciliationResult,
    LatticeVerdict,
    MergeResult,
    MeshReconciler,
    ReconciliationPlan,
    compare_vector_clocks,
    global_mesh_reconciler,
    merge_vector_clocks,
)
from swarmwatch.server import create_app


@pytest.fixture(autouse=True)
def reset_reconciler():
    global_mesh_reconciler.reset()
    yield
    global_mesh_reconciler.reset()


def test_vector_clock_comparisons():
    # Identical
    assert compare_vector_clocks({"nodeA": 2, "nodeB": 3}, {"nodeA": 2, "nodeB": 3}) == LatticeVerdict.IDENTICAL

    # Dominates (Local > Remote)
    assert compare_vector_clocks({"nodeA": 3, "nodeB": 3}, {"nodeA": 2, "nodeB": 3}) == LatticeVerdict.DOMINATES
    assert compare_vector_clocks({"nodeA": 2, "nodeB": 3, "nodeC": 1}, {"nodeA": 2, "nodeB": 3}) == LatticeVerdict.DOMINATES

    # Dominated (Local < Remote)
    assert compare_vector_clocks({"nodeA": 2, "nodeB": 2}, {"nodeA": 2, "nodeB": 3}) == LatticeVerdict.DOMINATED
    assert compare_vector_clocks({"nodeA": 2}, {"nodeA": 2, "nodeB": 1}) == LatticeVerdict.DOMINATED

    # Concurrent (Split-Brain)
    assert compare_vector_clocks({"nodeA": 3, "nodeB": 1}, {"nodeA": 2, "nodeB": 2}) == LatticeVerdict.CONCURRENT
    assert compare_vector_clocks({"nodeA": 1}, {"nodeB": 1}) == LatticeVerdict.CONCURRENT


def test_merge_vector_clocks():
    vc_a = {"nodeA": 5, "nodeB": 2, "nodeC": 0}
    vc_b = {"nodeA": 3, "nodeB": 4, "nodeD": 1}
    merged = merge_vector_clocks(vc_a, vc_b)
    assert merged == {"nodeA": 5, "nodeB": 4, "nodeC": 0, "nodeD": 1}


def test_diff_reconciliation_plan_dominant_and_concurrent():
    reconciler = MeshReconciler()

    # Local dominates remote
    local_state = {"vector_clock": {"nodeA": 3, "nodeB": 2}, "keys": {"k1": 1, "k2": 2}, "max_sequence": 100}
    remote_state = {"vector_clock": {"nodeA": 2, "nodeB": 2}, "keys": {"k1": 1}, "max_sequence": 90}
    plan = reconciler.diff(local_state, remote_state, tenant_id="tenant-alpha")

    assert plan.verdict == LatticeVerdict.DOMINATES
    assert "k2" in plan.divergent_keys
    assert plan.missing_remote_events == 10
    assert plan.missing_local_events == 0
    assert "PULL_REMOTE_FROM_LOCAL" in plan.action_items
    assert plan.plan_hash is not None

    # Concurrent divergence
    local_conc = {"vector_clock": {"nodeA": 3, "nodeB": 1}, "keys": {"k_loc": 1}, "max_sequence": 50}
    remote_conc = {"vector_clock": {"nodeA": 1, "nodeB": 3}, "keys": {"k_rem": 1}, "max_sequence": 55}
    plan_conc = reconciler.diff(local_conc, remote_conc, tenant_id="tenant-alpha")

    assert plan_conc.verdict == LatticeVerdict.CONCURRENT
    assert plan_conc.conflict_detected is True
    assert set(plan_conc.divergent_keys) == {"k_loc", "k_rem"}
    assert "RESOLVE_CONCURRENT_LATTICE_PRESERVE_PROVENANCE" in plan_conc.action_items


def test_merge_evidence_deduplication_and_provenance_retention():
    reconciler = MeshReconciler()

    # Shared event
    shared_evt = {
        "span_id": "span-shared-1",
        "turn_idx": 1,
        "agent_id": "agent-lead",
        "payload": {"action": "plan"},
        "vector_clock": {"nodeA": 1},
        "timestamp": 100.0,
    }

    # Distinct local event
    local_evt = {
        "span_id": "span-loc-1",
        "turn_idx": 2,
        "agent_id": "agent-worker-1",
        "payload": {"action": "compute"},
        "vector_clock": {"nodeA": 2},
        "timestamp": 105.0,
        "is_concurrent_fork": False,
        "origin_peer": "peer_A",
    }

    # Concurrent remote event with fork flag
    remote_evt = {
        "span_id": "span-rem-fork-1",
        "turn_idx": 2,
        "agent_id": "agent-worker-2",
        "payload": {"action": "compute_alt"},
        "vector_clock": {"nodeB": 2},
        "timestamp": 106.0,
        "is_concurrent_fork": True,
        "origin_peer": "peer_B",
    }

    result = reconciler.merge_evidence(
        local_events=[shared_evt, local_evt],
        remote_events=[shared_evt, remote_evt],
        tenant_id="tenant-prod",
    )

    assert result.total_spans == 3
    assert result.deduplicated_count == 1
    assert result.concurrent_conflicts_retained == 1
    assert result.merged_vector_clock == {"nodeA": 2, "nodeB": 2}
    assert result.convergence_hash is not None
    assert result.execution_latency_us >= 0.0

    # Verify fork event retained provenance
    fork_retained = next(s for s in result.merged_spans if s["span_id"] == "span-rem-fork-1")
    assert fork_retained["is_concurrent_fork"] is True
    assert fork_retained["provenance_peer"] == "peer_B"


def test_reconcile_authority_trip_monotonicity_invariant():
    reconciler = MeshReconciler()

    # Scenario A: Local TRIPPED, Remote NOMINAL -> Converged MUST be TRIPPED
    res_a = reconciler.reconcile_authority(
        local_authority={"state": "TRIPPED", "epoch": 3, "active_permits": 2},
        remote_authority={"state": "NOMINAL", "epoch": 4, "active_permits": 5},
        tenant_id="tenant-prod",
    )
    assert res_a.converged_state == "TRIPPED"
    assert res_a.trip_monotonicity_enforced is True
    assert res_a.reconciled_epoch == 5  # max(3, 4) + 1 due to divergence
    assert res_a.active_permits_revoked == 7  # 2 + 5 revoked

    # Scenario B: Local NOMINAL, Remote TRIPPED -> Converged MUST be TRIPPED
    res_b = reconciler.reconcile_authority(
        local_authority={"state": "NOMINAL", "epoch": 10, "active_permits": 1},
        remote_authority={"state": "TRIPPED", "epoch": 8, "active_permits": 0},
        tenant_id="tenant-prod",
    )
    assert res_b.converged_state == "TRIPPED"
    assert res_b.trip_monotonicity_enforced is True
    assert res_b.reconciled_epoch == 11  # max(10, 8) + 1

    # Scenario C: Both NOMINAL -> Converged NOMINAL, no epoch step up if in agreement
    res_c = reconciler.reconcile_authority(
        local_authority={"state": "NOMINAL", "epoch": 12, "active_permits": 3},
        remote_authority={"state": "NOMINAL", "epoch": 12, "active_permits": 3},
        tenant_id="tenant-prod",
    )
    assert res_c.converged_state == "NOMINAL"
    assert res_c.trip_monotonicity_enforced is False
    assert res_c.reconciled_epoch == 12
    assert res_c.active_permits_revoked == 0


def test_reconciler_status_and_reset():
    reconciler = MeshReconciler()
    reconciler.reconcile_authority(
        {"state": "TRIPPED", "epoch": 1},
        {"state": "NOMINAL", "epoch": 1},
        tenant_id="t1",
    )
    status = reconciler.status("t1")
    assert status["total_reconciliations"] == 1
    assert status["trips_preserved_count"] == 1
    assert status["last_reconciliation"] is not None

    reconciler.reset()
    status_empty = reconciler.status("t1")
    assert status_empty["total_reconciliations"] == 0
    assert status_empty["trips_preserved_count"] == 0


def test_server_reconciler_endpoints():
    app = create_app()
    client = TestClient(app)

    # 1. Diff endpoint
    diff_payload = {
        "tenant_id": "tenant-test",
        "local_state": {"vector_clock": {"nodeA": 2}, "keys": {"k1": 1}, "max_sequence": 10},
        "remote_state": {"vector_clock": {"nodeA": 1}, "keys": {}, "max_sequence": 5},
    }
    resp_diff = client.post("/api/v1/reconciler/diff", json=diff_payload)
    assert resp_diff.status_code == 200
    plan_data = resp_diff.json()
    assert plan_data["verdict"] == "DOMINATES"
    assert "k1" in plan_data["divergent_keys"]

    # 2. Merge endpoint
    merge_payload = {
        "tenant_id": "tenant-test",
        "local_events": [{"span_id": "s1", "turn_idx": 1, "agent_id": "ag1", "payload": {}}],
        "remote_events": [{"span_id": "s2", "turn_idx": 2, "agent_id": "ag2", "payload": {}, "is_concurrent_fork": True}],
    }
    resp_merge = client.post("/api/v1/reconciler/merge", json=merge_payload)
    assert resp_merge.status_code == 200
    merge_data = resp_merge.json()
    assert merge_data["total_spans"] == 2
    assert merge_data["concurrent_conflicts_retained"] == 1

    # 3. Reconcile authority endpoint
    auth_payload = {
        "tenant_id": "tenant-test",
        "local_authority": {"state": "TRIPPED", "epoch": 2, "active_permits": 1},
        "remote_authority": {"state": "NOMINAL", "epoch": 3, "active_permits": 2},
    }
    resp_auth = client.post("/api/v1/reconciler/reconcile-authority", json=auth_payload)
    assert resp_auth.status_code == 200
    auth_data = resp_auth.json()
    assert auth_data["converged_state"] == "TRIPPED"
    assert auth_data["trip_monotonicity_enforced"] is True
    assert auth_data["reconciled_epoch"] == 4

    # 4. Status endpoint
    resp_status = client.get("/api/v1/reconciler/status?tenant_id=tenant-test")
    assert resp_status.status_code == 200
    status_data = resp_status.json()
    assert status_data["total_reconciliations"] >= 1
    assert status_data["trips_preserved_count"] >= 1


def test_cli_reconciler_subcommands(capsys, tmp_path):
    # Status via CLI
    with pytest.raises(SystemExit) as exc:
        cli_main(["reconciler", "status", "--tenant", "default", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    st_data = json.loads(captured.out)
    assert st_data["status"] == "CONVERGED"

    # Status terminal output
    with pytest.raises(SystemExit) as exc:
        cli_main(["reconciler", "status", "--tenant", "default"])
    assert exc.value.code == 0
    captured_term = capsys.readouterr()
    assert "SWARMWATCH MESH RECONCILER STATUS" in captured_term.out

    # Diff via CLI using files
    loc_file = tmp_path / "local.json"
    rem_file = tmp_path / "remote.json"
    loc_file.write_text(json.dumps({"vector_clock": {"node1": 5}, "keys": {"a": 1}}), encoding="utf-8")
    rem_file.write_text(json.dumps({"vector_clock": {"node1": 3}, "keys": {}}), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main(["reconciler", "diff", "--local-state-file", str(loc_file), "--remote-state-file", str(rem_file), "--format", "json"])
    assert exc.value.code == 0
    captured_diff = capsys.readouterr()
    diff_data = json.loads(captured_diff.out)
    assert diff_data["verdict"] == "DOMINATES"

    # Reconcile authority via CLI
    with pytest.raises(SystemExit) as exc:
        cli_main([
            "reconciler", "reconcile",
            "--local-state", "TRIPPED",
            "--local-epoch", "2",
            "--remote-state", "NOMINAL",
            "--remote-epoch", "2",
            "--format", "json",
        ])
    assert exc.value.code == 0
    captured_rec = capsys.readouterr()
    rec_data = json.loads(captured_rec.out)
    assert rec_data["converged_state"] == "TRIPPED"
    assert rec_data["trip_monotonicity_enforced"] is True
    assert rec_data["reconciled_epoch"] == 3

