# -*- coding: utf-8 -*-
"""
Tests for Governed Remediation & Fenced Action State Machine (Seam 56 / ADR 0067).
Verifies:
1. Multi-tiered approval gating (Tier 0 automatic, Tier 1 supervised, Tier 2 manual).
2. Blast radius ceiling enforcement and cross-tenant isolation.
3. Monotonic fencing epoch rejection of stale leaders (STALE_EPOCH).
4. Precondition verification failure fail-closed behavior.
5. Postcondition verification failure triggering automated compensation.
6. Idempotent compensation rollback.
7. REST endpoints and CLI subcommands.
"""

from __future__ import annotations

import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.canonical import CanonicalCodec, CanonicalDomainTag
from swarmwatch.cli import main as cli_main
from swarmwatch.remediation_governor import (
    ApprovalTier,
    BlastRadius,
    CompensationResult,
    ProposalStatus,
    RemediationDecision,
    RemediationGovernor,
    RemediationProposal,
    RemediationResult,
    global_remediation_governor,
)
from swarmwatch.server import create_app


@pytest.fixture(autouse=True)
def reset_governor():
    global_remediation_governor.reset()
    yield
    global_remediation_governor.reset()


def test_propose_lifecycle_tier0_automatic():
    gov = RemediationGovernor(current_epoch=1)
    prop = gov.propose(
        tenant_id="tenant-prod",
        action_type="probation_clamp",
        target_entity="Worker-01",
        parameters={"rate_limit_rpm": 30},
        approval_tier=ApprovalTier.TIER_0_AUTOMATIC,
        blast_radius=BlastRadius(affected_agents=["Worker-01"], max_cost_delta=0.5),
        preconditions={"status": "degraded"},
        postconditions={"status": "probationary"},
    )

    assert prop.proposal_id.startswith("rem-")
    assert prop.status == ProposalStatus.AUTHORIZED
    assert prop.approval_tier == ApprovalTier.TIER_0_AUTOMATIC
    assert prop.approver_role == "AUTOMATIC_POLICY"
    assert prop.proposal_hash is not None

    # Apply proposal with valid preconditions and simulated context
    context = {"status": "degraded"}
    res = gov.apply(
        proposal_id=prop.proposal_id,
        current_epoch=1,
        context=context,
        executor=lambda p, ctx: ctx.update({"status": "probationary"}),
    )

    assert res.success is True
    assert res.status == ProposalStatus.APPLIED
    assert res.applied_epoch == 1
    assert res.execution_latency_us > 0.0

    st = gov.status("tenant-prod")
    assert st["active_remediations_count"] == 1
    assert st["total_proposals"] == 1


def test_tier_limits_and_cross_tenant_rejection():
    gov = RemediationGovernor(current_epoch=1)

    # 1. Cross-tenant attempt in Tier 0 must be REJECTED
    prop_cross = gov.propose(
        tenant_id="tenant-prod",
        action_type="isolate_agent",
        target_entity="Agent-X",
        approval_tier=ApprovalTier.TIER_0_AUTOMATIC,
        blast_radius=BlastRadius(cross_tenant=True),
    )
    assert prop_cross.status == ProposalStatus.REJECTED
    assert "Cross-tenant" in (prop_cross.rejection_reason or "")

    # 2. Exceeding max_agents in Tier 0 (ceiling = 2)
    prop_agents = gov.propose(
        tenant_id="tenant-prod",
        action_type="probation_clamp",
        target_entity="Worker-01",
        approval_tier=ApprovalTier.TIER_0_AUTOMATIC,
        blast_radius=BlastRadius(affected_agents=["A1", "A2", "A3"]),
    )
    assert prop_agents.status == ProposalStatus.REJECTED
    assert "exceed tier ceiling" in (prop_agents.rejection_reason or "")

    # 3. Exceeding max_cost_delta in Tier 0 (ceiling = 2.0)
    prop_cost = gov.propose(
        tenant_id="tenant-prod",
        action_type="probation_clamp",
        target_entity="Worker-01",
        approval_tier=ApprovalTier.TIER_0_AUTOMATIC,
        blast_radius=BlastRadius(max_cost_delta=10.0),
    )
    assert prop_cost.status == ProposalStatus.REJECTED
    assert "Cost delta" in (prop_cost.rejection_reason or "")


def test_tier1_supervised_authorization_flow():
    gov = RemediationGovernor(current_epoch=2)
    prop = gov.propose(
        tenant_id="tenant-prod",
        action_type="down_route_model",
        target_entity="Router-01",
        parameters={"fallback_model": "claude-haiku"},
        approval_tier=ApprovalTier.TIER_1_SUPERVISED,
        blast_radius=BlastRadius(affected_agents=["Agent-1", "Agent-2"], max_cost_delta=15.0),
    )

    assert prop.status == ProposalStatus.PROPOSED

    # Authorize without permit or supervisor role fails
    dec_fail = gov.authorize(proposal_id=prop.proposal_id)
    assert dec_fail.approved is False
    assert prop.status == ProposalStatus.REJECTED

    # Propose again and authorize with permit_hash
    prop2 = gov.propose(
        tenant_id="tenant-prod",
        action_type="down_route_model",
        target_entity="Router-01",
        approval_tier=ApprovalTier.TIER_1_SUPERVISED,
    )
    dec_ok = gov.authorize(
        proposal_id=prop2.proposal_id,
        permit_hash="sha256-pmt-abcd-1234",
        current_epoch=2,
    )
    assert dec_ok.approved is True
    assert dec_ok.status == ProposalStatus.AUTHORIZED
    assert prop2.status == ProposalStatus.AUTHORIZED


def test_tier2_manual_authorization_and_rbac():
    gov = RemediationGovernor(current_epoch=3)
    prop = gov.propose(
        tenant_id="tenant-prod",
        action_type="blacklist_tool",
        target_entity="bash_exec",
        approval_tier=ApprovalTier.TIER_2_MANUAL,
        blast_radius=BlastRadius(affected_tools=["bash_exec"], cross_tenant=True, max_cost_delta=100.0),
    )

    assert prop.status == ProposalStatus.PROPOSED

    # Non-admin role fails
    dec_denied = gov.authorize(proposal_id=prop.proposal_id, approver_role="VIEWER")
    assert dec_denied.approved is False
    assert prop.status == ProposalStatus.REJECTED

    # Re-propose and authorize with ADMIN
    prop2 = gov.propose(
        tenant_id="tenant-prod",
        action_type="blacklist_tool",
        target_entity="bash_exec",
        approval_tier=ApprovalTier.TIER_2_MANUAL,
        blast_radius=BlastRadius(affected_tools=["bash_exec"], cross_tenant=True),
    )
    dec_admin = gov.authorize(proposal_id=prop2.proposal_id, approver_role="ADMIN")
    assert dec_admin.approved is True
    assert prop2.status == ProposalStatus.AUTHORIZED
    assert prop2.approver_role == "ADMIN"


def test_monotonic_epoch_fencing_stale_leader_rejected():
    gov = RemediationGovernor(current_epoch=1)
    prop = gov.propose(
        tenant_id="tenant-prod",
        action_type="probation_clamp",
        target_entity="Worker-01",
        approval_tier=ApprovalTier.TIER_0_AUTOMATIC,
    )
    assert prop.status == ProposalStatus.AUTHORIZED

    # Advance governor epoch to 5
    gov.advance_epoch(5)
    assert gov.current_epoch == 5

    # A lagging leader attempts to apply using stale epoch 3
    res_stale = gov.apply(proposal_id=prop.proposal_id, current_epoch=3)
    assert res_stale.success is False
    assert "STALE_EPOCH" in (res_stale.error or "")
    assert prop.status == ProposalStatus.AUTHORIZED  # Remains authorized, not committed

    # Application under authoritative epoch 5 succeeds
    res_ok = gov.apply(proposal_id=prop.proposal_id, current_epoch=5)
    assert res_ok.success is True
    assert res_ok.status == ProposalStatus.APPLIED


def test_precondition_failure_rejection():
    gov = RemediationGovernor(current_epoch=1)
    prop = gov.propose(
        tenant_id="tenant-prod",
        action_type="rewire_topology",
        target_entity="Graph-01",
        approval_tier=ApprovalTier.TIER_0_AUTOMATIC,
        preconditions={"cycle_detected": True, "active_branch": "main"},
    )

    # Applying with mismatched precondition
    res = gov.apply(
        proposal_id=prop.proposal_id,
        current_epoch=1,
        context={"cycle_detected": False, "active_branch": "main"},
    )
    assert res.success is False
    assert res.status == ProposalStatus.REJECTED
    assert "Precondition failed" in (res.error or "")


def test_postcondition_failure_automatic_compensation():
    gov = RemediationGovernor(current_epoch=1)
    compensated_called = []

    def mock_compensate(p):
        compensated_called.append(p.proposal_id)
        return {"reverted": True}

    prop = gov.propose(
        tenant_id="tenant-prod",
        action_type="probation_clamp",
        target_entity="Worker-01",
        approval_tier=ApprovalTier.TIER_0_AUTOMATIC,
        postconditions={"verified_safe": True},
        compensation_action={"action": "unclamp_probation"},
    )

    # Apply mutation where executor fails to establish postcondition
    res = gov.apply(
        proposal_id=prop.proposal_id,
        current_epoch=1,
        context={"verified_safe": False},
        executor=lambda p, ctx: {"noop": True},
    )

    assert res.success is False
    assert res.status == ProposalStatus.FAILED
    assert "Postcondition failed" in (res.error or "")
    # Check proposal is compensated automatically
    assert prop.status == ProposalStatus.COMPENSATED


def test_idempotent_compensation_rollback():
    gov = RemediationGovernor(current_epoch=1)
    prop = gov.propose(
        tenant_id="tenant-prod",
        action_type="probation_clamp",
        target_entity="Worker-01",
        approval_tier=ApprovalTier.TIER_0_AUTOMATIC,
        compensation_action={"action": "restore_nominal_rates"},
    )
    apply_res = gov.apply(proposal_id=prop.proposal_id, current_epoch=1)
    assert apply_res.success is True
    assert prop.status == ProposalStatus.APPLIED

    # Compensate once
    c1 = gov.compensate(proposal_id=prop.proposal_id, reason="Operator test rollback")
    assert c1.success is True
    assert c1.status == ProposalStatus.COMPENSATED
    assert prop.status == ProposalStatus.COMPENSATED

    # Compensate second time: idempotent no-op
    c2 = gov.compensate(proposal_id=prop.proposal_id, reason="Duplicate rollback call")
    assert c2.success is True
    assert c2.status == ProposalStatus.COMPENSATED
    assert c2.details.get("already_compensated") is True


def test_server_remediation_endpoints():
    app = create_app()
    client = TestClient(app)

    # 1. Propose endpoint
    resp = client.post(
        "/api/v1/remediation/propose",
        json={
            "tenant_id": "tenant-api-01",
            "action_type": "probation_clamp",
            "target_entity": "Agent-REST",
            "approval_tier": "TIER_0_AUTOMATIC",
            "parameters": {"clamp_level": 2},
        },
    )
    assert resp.status_code == 200
    prop_data = resp.json()
    assert prop_data["status"] == "AUTHORIZED"
    proposal_id = prop_data["proposal_id"]

    # 2. Authorize endpoint (Tier 0 idempotency)
    auth_resp = client.post(
        "/api/v1/remediation/authorize",
        json={"proposal_id": proposal_id},
    )
    assert auth_resp.status_code == 200
    assert auth_resp.json()["approved"] is True

    # 3. Apply endpoint
    apply_resp = client.post(
        "/api/v1/remediation/apply",
        json={
            "proposal_id": proposal_id,
            "current_epoch": 1,
            "context": {},
        },
    )
    assert apply_resp.status_code == 200
    assert apply_resp.json()["success"] is True
    assert apply_resp.json()["status"] == "APPLIED"

    # 4. Compensate endpoint
    comp_resp = client.post(
        "/api/v1/remediation/compensate",
        json={
            "proposal_id": proposal_id,
            "reason": "Test REST rollback",
        },
    )
    assert comp_resp.status_code == 200
    assert comp_resp.json()["status"] == "COMPENSATED"

    # 5. Status endpoint
    status_resp = client.get("/api/v1/remediation/status?tenant_id=tenant-api-01")
    assert status_resp.status_code == 200
    s_data = status_resp.json()
    assert s_data["total_proposals"] == 1


def test_cli_remediation_subcommand(capsys):
    # Propose via CLI
    with pytest.raises(SystemExit) as exc:
        cli_main(["remediation", "propose", "--action-type", "probation_clamp", "--target-entity", "CLI-Agent", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["status"] == "AUTHORIZED"
    pid = data["proposal_id"]

    # Apply via CLI
    with pytest.raises(SystemExit) as exc:
        cli_main(["remediation", "apply", "--proposal-id", pid, "--epoch", "1", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    apply_data = json.loads(captured.out)
    assert apply_data["success"] is True

    # Compensate via CLI
    with pytest.raises(SystemExit) as exc:
        cli_main(["remediation", "compensate", "--proposal-id", pid, "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    comp_data = json.loads(captured.out)
    assert comp_data["success"] is True

    # Status via CLI
    with pytest.raises(SystemExit) as exc:
        cli_main(["remediation", "status", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    st_data = json.loads(captured.out)
    assert st_data["total_proposals"] >= 1
