/**
 * Unit and integration tests for Governed Remediation & Fenced Action State Machine (Seam 56 / ADR 0067).
 * Follows Node 22 native test runner conventions and strict type-stripping rules (no 'as ...' type casts).
 */

import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
  ApprovalTier,
  ProposalStatus,
  RemediationGovernor,
  globalRemediationGovernor,
} from "../src/remediation_governor.ts";

describe("RemediationGovernor (Seam 56 / ADR 0067)", () => {
  let governor;

  beforeEach(() => {
    governor = new RemediationGovernor(1);
    globalRemediationGovernor.reset();
  });

  test("Tier 0 auto-authorizes and applies successfully under current epoch", () => {
    const prop = governor.propose({
      tenant_id: "tenant-alpha",
      action_type: "probation_clamp",
      target_entity: "Worker-01",
      parameters: { max_rpm: 20 },
      approval_tier: ApprovalTier.TIER_0_AUTOMATIC,
      blast_radius: { affected_agents: ["Worker-01"], max_cost_delta: 0.5 },
      preconditions: { state: "degraded" },
      postconditions: { state: "probation" },
    });

    assert.ok(prop.proposal_id.startsWith("rem-"));
    assert.equal(prop.status, ProposalStatus.AUTHORIZED);
    assert.equal(prop.approval_tier, ApprovalTier.TIER_0_AUTOMATIC);
    assert.equal(prop.approver_role, "AUTOMATIC_POLICY");
    assert.ok(prop.proposal_hash);

    const ctx = { state: "degraded" };
    const res = governor.apply({
      proposal_id: prop.proposal_id,
      current_epoch: 1,
      context: ctx,
      executor: (p, c) => {
        c.state = "probation";
        return { clamped: true };
      },
    });

    assert.equal(res.success, true);
    assert.equal(res.status, ProposalStatus.APPLIED);
    assert.equal(res.applied_epoch, 1);
    assert.ok(res.execution_latency_us > 0);

    const status = governor.status("tenant-alpha");
    assert.equal(status.active_remediations_count, 1);
    assert.equal(status.total_proposals, 1);
  });

  test("Rejects proposals exceeding blast radius ceilings or cross-tenant boundaries without Tier 2", () => {
    // 1. Cross-tenant attempt in Tier 0 must be REJECTED
    const crossProp = governor.propose({
      tenant_id: "tenant-alpha",
      action_type: "isolate_agent",
      target_entity: "Worker-01",
      approval_tier: ApprovalTier.TIER_0_AUTOMATIC,
      blast_radius: { cross_tenant: true },
    });
    assert.equal(crossProp.status, ProposalStatus.REJECTED);
    assert.ok(crossProp.rejection_reason.includes("Cross-tenant"));

    // 2. Affected agents ceiling in Tier 0 (max 2)
    const agentsProp = governor.propose({
      tenant_id: "tenant-alpha",
      action_type: "probation_clamp",
      target_entity: "Worker-01",
      approval_tier: ApprovalTier.TIER_0_AUTOMATIC,
      blast_radius: { affected_agents: ["A1", "A2", "A3"] },
    });
    assert.equal(agentsProp.status, ProposalStatus.REJECTED);
    assert.ok(agentsProp.rejection_reason.includes("exceed tier ceiling"));

    // 3. Cost delta ceiling in Tier 0 (max 2.0)
    const costProp = governor.propose({
      tenant_id: "tenant-alpha",
      action_type: "probation_clamp",
      target_entity: "Worker-01",
      approval_tier: ApprovalTier.TIER_0_AUTOMATIC,
      blast_radius: { max_cost_delta: 15.0 },
    });
    assert.equal(costProp.status, ProposalStatus.REJECTED);
    assert.ok(costProp.rejection_reason.includes("Cost delta"));
  });

  test("Tier 1 requires supervisor role or permit_hash", () => {
    governor.advanceEpoch(2);

    const prop = governor.propose({
      tenant_id: "tenant-beta",
      action_type: "down_route_model",
      target_entity: "Router-01",
      approval_tier: ApprovalTier.TIER_1_SUPERVISED,
      blast_radius: { affected_agents: ["A1"], max_cost_delta: 10.0 },
    });

    assert.equal(prop.status, ProposalStatus.PROPOSED);

    // Authorize without permit fails
    const decFail = governor.authorize({ proposal_id: prop.proposal_id });
    assert.equal(decFail.approved, false);
    assert.equal(decFail.status, ProposalStatus.REJECTED);

    // Re-propose and authorize with permit_hash
    const prop2 = governor.propose({
      tenant_id: "tenant-beta",
      action_type: "down_route_model",
      target_entity: "Router-01",
      approval_tier: ApprovalTier.TIER_1_SUPERVISED,
    });
    const decOk = governor.authorize({
      proposal_id: prop2.proposal_id,
      permit_hash: "sha256-pmt-9999",
      current_epoch: 2,
    });
    assert.equal(decOk.approved, true);
    assert.equal(decOk.status, ProposalStatus.AUTHORIZED);
  });

  test("Tier 2 manual requires administrative approver_role", () => {
    governor.advanceEpoch(3);

    const prop = governor.propose({
      tenant_id: "tenant-gamma",
      action_type: "blacklist_tool",
      target_entity: "bash_exec",
      approval_tier: ApprovalTier.TIER_2_MANUAL,
      blast_radius: { cross_tenant: true },
    });

    assert.equal(prop.status, ProposalStatus.PROPOSED);

    // Viewer role fails
    const decViewer = governor.authorize({ proposal_id: prop.proposal_id, approver_role: "VIEWER" });
    assert.equal(decViewer.approved, false);
    assert.equal(prop.status, ProposalStatus.REJECTED);

    // Admin role succeeds
    const prop2 = governor.propose({
      tenant_id: "tenant-gamma",
      action_type: "blacklist_tool",
      target_entity: "bash_exec",
      approval_tier: ApprovalTier.TIER_2_MANUAL,
      blast_radius: { cross_tenant: true },
    });
    const decAdmin = governor.authorize({ proposal_id: prop2.proposal_id, approver_role: "ADMIN" });
    assert.equal(decAdmin.approved, true);
    assert.equal(prop2.status, ProposalStatus.AUTHORIZED);
  });

  test("Monotonic fencing epoch rejects stale caller fail-closed (STALE_EPOCH)", () => {
    const prop = governor.propose({
      tenant_id: "tenant-delta",
      action_type: "probation_clamp",
      target_entity: "Worker-01",
      approval_tier: ApprovalTier.TIER_0_AUTOMATIC,
    });
    assert.equal(prop.status, ProposalStatus.AUTHORIZED);

    governor.advanceEpoch(4);

    // Caller presents stale epoch 2
    const resStale = governor.apply({
      proposal_id: prop.proposal_id,
      current_epoch: 2,
    });
    assert.equal(resStale.success, false);
    assert.ok(resStale.error.includes("STALE_EPOCH"));

    // Caller presents authoritative epoch 4
    const resOk = governor.apply({
      proposal_id: prop.proposal_id,
      current_epoch: 4,
    });
    assert.equal(resOk.success, true);
    assert.equal(resOk.status, ProposalStatus.APPLIED);
  });

  test("Precondition failure causes immediate proposal rejection fail-closed", () => {
    const prop = governor.propose({
      tenant_id: "tenant-epsilon",
      action_type: "rewire_topology",
      target_entity: "Graph-01",
      approval_tier: ApprovalTier.TIER_0_AUTOMATIC,
      preconditions: { active_branch: "canary" },
    });

    const res = governor.apply({
      proposal_id: prop.proposal_id,
      current_epoch: 1,
      context: { active_branch: "production" },
    });

    assert.equal(res.success, false);
    assert.equal(res.status, ProposalStatus.REJECTED);
    assert.ok(res.error.includes("Precondition failed"));
  });

  test("Postcondition failure triggers automatic compensation rollback", () => {
    const prop = governor.propose({
      tenant_id: "tenant-zeta",
      action_type: "probation_clamp",
      target_entity: "Worker-01",
      approval_tier: ApprovalTier.TIER_0_AUTOMATIC,
      postconditions: { rate_limited: true },
      compensation_action: { rollback: "unclamp" },
    });

    const res = governor.apply({
      proposal_id: prop.proposal_id,
      current_epoch: 1,
      context: { rate_limited: false },
    });

    assert.equal(res.success, false);
    assert.equal(res.status, ProposalStatus.FAILED);
    assert.ok(res.error.includes("Postcondition failed"));
    assert.equal(prop.status, ProposalStatus.COMPENSATED);
  });

  test("Idempotent compensation rollback returns success without repeated side-effects", () => {
    const prop = governor.propose({
      tenant_id: "tenant-eta",
      action_type: "probation_clamp",
      target_entity: "Worker-01",
      approval_tier: ApprovalTier.TIER_0_AUTOMATIC,
      compensation_action: { action: "reset_limits" },
    });

    governor.apply({ proposal_id: prop.proposal_id, current_epoch: 1 });
    assert.equal(prop.status, ProposalStatus.APPLIED);

    const c1 = governor.compensate({ proposal_id: prop.proposal_id, reason: "First rollback" });
    assert.equal(c1.success, true);
    assert.equal(c1.status, ProposalStatus.COMPENSATED);
    assert.equal(prop.status, ProposalStatus.COMPENSATED);

    const c2 = governor.compensate({ proposal_id: prop.proposal_id, reason: "Second rollback" });
    assert.equal(c2.success, true);
    assert.equal(c2.details.already_compensated, true);
  });

  test("Status reporting aggregates proposals and active remediations", () => {
    governor.propose({
      tenant_id: "tenant-status",
      action_type: "probation_clamp",
      target_entity: "W1",
      approval_tier: ApprovalTier.TIER_0_AUTOMATIC,
    });
    governor.propose({
      tenant_id: "tenant-status",
      action_type: "down_route_model",
      target_entity: "W2",
      approval_tier: ApprovalTier.TIER_1_SUPERVISED,
    });

    const st = governor.status("tenant-status");
    assert.equal(st.total_proposals, 2);
    assert.equal(st.by_tier[ApprovalTier.TIER_0_AUTOMATIC], 1);
    assert.equal(st.by_tier[ApprovalTier.TIER_1_SUPERVISED], 1);
  });
});
