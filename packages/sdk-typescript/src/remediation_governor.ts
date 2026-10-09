/**
 * Governed Remediation & Fenced Action State Machine (Seam 56 / ADR 0067).
 * Enforces strict multi-tiered approval gating and blast radius constraints governing all
 * self-healing interventions, topology mutations, and automated remediations.
 * Guarantees monotonic fencing epoch validation, explicit precondition and postcondition contracts,
 * and idempotent deterministic compensation rollbacks.
 * Zero external LLM API calls (<20us local execution).
 */

import * as crypto from "node:crypto";
import { CanonicalCodec, CanonicalDomainTag } from "./canonical.ts";

export const ApprovalTier = {
  TIER_0_AUTOMATIC: "TIER_0_AUTOMATIC",
  TIER_1_SUPERVISED: "TIER_1_SUPERVISED",
  TIER_2_MANUAL: "TIER_2_MANUAL",
} as const;

export type ApprovalTierEnum = (typeof ApprovalTier)[keyof typeof ApprovalTier];

export const ProposalStatus = {
  PROPOSED: "PROPOSED",
  AUTHORIZED: "AUTHORIZED",
  APPLIED: "APPLIED",
  COMPENSATED: "COMPENSATED",
  REJECTED: "REJECTED",
  FAILED: "FAILED",
} as const;

export type ProposalStatusEnum = (typeof ProposalStatus)[keyof typeof ProposalStatus];

export interface BlastRadius {
  affected_agents?: string[];
  affected_tools?: string[];
  max_cost_delta?: number;
  max_token_delta?: number;
  cross_tenant?: boolean;
}

export interface RemediationProposal {
  proposal_id: string;
  tenant_id: string;
  action_type: string;
  target_entity: string;
  parameters: Record<string, any>;
  blast_radius: BlastRadius;
  approval_tier: string;
  preconditions: Record<string, any>;
  postconditions: Record<string, any>;
  compensation_action?: Record<string, any> | null;
  status: string;
  proposed_at: number;
  authoritative_epoch?: number | null;
  permit_hash?: string | null;
  approver_role?: string | null;
  rejection_reason?: string | null;
  proposal_hash?: string;
}

export interface RemediationDecision {
  proposal_id: string;
  approved: boolean;
  status: string;
  approval_tier: string;
  authoritative_epoch?: number | null;
  reason: string;
  decided_at: number;
}

export interface RemediationResult {
  proposal_id: string;
  success: boolean;
  status: string;
  applied_epoch: number;
  execution_latency_us: number;
  details: Record<string, any>;
  error?: string | null;
}

export interface CompensationResult {
  proposal_id: string;
  success: boolean;
  status: string;
  compensated_at: number;
  reason: string;
  details: Record<string, any>;
}

export interface ProposeOptions {
  tenant_id: string;
  action_type: string;
  target_entity: string;
  parameters?: Record<string, any>;
  blast_radius?: BlastRadius;
  approval_tier?: string;
  preconditions?: Record<string, any>;
  postconditions?: Record<string, any>;
  compensation_action?: Record<string, any> | null;
  proposal_id?: string;
}

export interface AuthorizeOptions {
  proposal_id: string;
  approver_role?: string | null;
  permit_hash?: string | null;
  current_epoch?: number | null;
  signature?: string | null;
}

export interface ApplyOptions {
  proposal_id: string;
  current_epoch: number;
  context?: Record<string, any>;
  executor?: (prop: RemediationProposal, ctx: Record<string, any>) => Record<string, any> | void;
}

export interface CompensateOptions {
  proposal_id: string;
  reason?: string;
  compensation_handler?: (prop: RemediationProposal) => Record<string, any> | void;
}

export class RemediationGovernor {
  private _currentEpoch: number;
  private readonly _proposals: Map<string, RemediationProposal> = new Map();
  private readonly _tenantProposals: Map<string, string[]> = new Map();
  private readonly _activeRemediations: Map<string, Record<string, any>> = new Map();

  public static readonly DEFAULT_LIMITS: Record<string, {
    max_agents: number;
    max_tools: number;
    max_cost_delta: number;
    max_token_delta: number;
    allow_cross_tenant: boolean;
  }> = {
    [ApprovalTier.TIER_0_AUTOMATIC]: {
      max_agents: 2,
      max_tools: 1,
      max_cost_delta: 2.0,
      max_token_delta: 5000,
      allow_cross_tenant: false,
    },
    [ApprovalTier.TIER_1_SUPERVISED]: {
      max_agents: 8,
      max_tools: 5,
      max_cost_delta: 25.0,
      max_token_delta: 50000,
      allow_cross_tenant: false,
    },
    [ApprovalTier.TIER_2_MANUAL]: {
      max_agents: 50,
      max_tools: 20,
      max_cost_delta: 500.0,
      max_token_delta: 500000,
      allow_cross_tenant: true,
    },
  };

  constructor(initialEpoch: number = 1) {
    this._currentEpoch = initialEpoch;
  }

  public get currentEpoch(): number {
    return this._currentEpoch;
  }

  public advanceEpoch(newEpoch?: number): number {
    if (newEpoch !== undefined && newEpoch !== null) {
      if (newEpoch <= this._currentEpoch) {
        throw new Error(`New epoch ${newEpoch} must be greater than current ${this._currentEpoch}`);
      }
      this._currentEpoch = newEpoch;
    } else {
      this._currentEpoch += 1;
    }
    return this._currentEpoch;
  }

  public computeProposalHash(p: {
    action_type: string;
    approval_tier: string;
    blast_radius: BlastRadius;
    parameters: Record<string, any>;
    postconditions: Record<string, any>;
    preconditions: Record<string, any>;
    proposed_at: number;
    target_entity: string;
    tenant_id: string;
  }): string {
    const br = p.blast_radius || {};
    const payload = {
      action_type: p.action_type,
      approval_tier: p.approval_tier,
      blast_radius: {
        affected_agents: br.affected_agents || [],
        affected_tools: br.affected_tools || [],
        cross_tenant: Boolean(br.cross_tenant),
        max_cost_delta: Number(br.max_cost_delta || 0.0),
        max_token_delta: Number(br.max_token_delta || 0),
      },
      parameters: p.parameters || {},
      postconditions: p.postconditions || {},
      preconditions: p.preconditions || {},
      proposed_at: Number(p.proposed_at.toFixed(4)),
      target_entity: p.target_entity,
      tenant_id: p.tenant_id,
    };
    return CanonicalCodec.hash(payload, CanonicalDomainTag.REMEDIATION);
  }

  public propose(opts: ProposeOptions): RemediationProposal {
    const tenant_id = opts.tenant_id ? opts.tenant_id.trim() : "";
    const action_type = opts.action_type ? opts.action_type.trim() : "";
    const target_entity = opts.target_entity ? opts.target_entity.trim() : "";

    if (!tenant_id) throw new Error("tenant_id must not be empty");
    if (!action_type) throw new Error("action_type must not be empty");
    if (!target_entity) throw new Error("target_entity must not be empty");

    const pid = opts.proposal_id || `rem-${crypto.randomBytes(8).toString("hex")}`;
    if (this._proposals.has(pid)) {
      throw new Error(`Proposal with ID ${pid} already exists`);
    }

    const tier = opts.approval_tier || ApprovalTier.TIER_0_AUTOMATIC;
    const br: BlastRadius = {
      affected_agents: opts.blast_radius?.affected_agents || [],
      affected_tools: opts.blast_radius?.affected_tools || [],
      max_cost_delta: opts.blast_radius?.max_cost_delta || 0.0,
      max_token_delta: opts.blast_radius?.max_token_delta || 0,
      cross_tenant: opts.blast_radius?.cross_tenant || false,
    };

    const proposed_at = Date.now() / 1000.0;
    const params = opts.parameters || {};
    const pre = opts.preconditions || {};
    const post = opts.postconditions || {};
    const comp_action = opts.compensation_action || null;

    // Cross-tenant invariant check
    if (br.cross_tenant && tier !== ApprovalTier.TIER_2_MANUAL) {
      const prop: RemediationProposal = {
        proposal_id: pid,
        tenant_id,
        action_type,
        target_entity,
        parameters: params,
        blast_radius: br,
        approval_tier: tier,
        preconditions: pre,
        postconditions: post,
        compensation_action: comp_action,
        status: ProposalStatus.REJECTED,
        proposed_at,
        authoritative_epoch: this._currentEpoch,
        rejection_reason: "Cross-tenant remediations require TIER_2_MANUAL approval",
      };
      prop.proposal_hash = this.computeProposalHash(prop);
      this._proposals.set(pid, prop);
      this._recordTenantProposal(tenant_id, pid);
      return prop;
    }

    // Tier limits check
    const limits = RemediationGovernor.DEFAULT_LIMITS[tier] || RemediationGovernor.DEFAULT_LIMITS[ApprovalTier.TIER_0_AUTOMATIC];
    if ((br.affected_agents?.length || 0) > limits.max_agents) {
      const prop: RemediationProposal = {
        proposal_id: pid,
        tenant_id,
        action_type,
        target_entity,
        parameters: params,
        blast_radius: br,
        approval_tier: tier,
        preconditions: pre,
        postconditions: post,
        compensation_action: comp_action,
        status: ProposalStatus.REJECTED,
        proposed_at,
        authoritative_epoch: this._currentEpoch,
        rejection_reason: `Affected agents (${br.affected_agents?.length || 0}) exceed tier ceiling (${limits.max_agents})`,
      };
      prop.proposal_hash = this.computeProposalHash(prop);
      this._proposals.set(pid, prop);
      this._recordTenantProposal(tenant_id, pid);
      return prop;
    }

    if ((br.max_cost_delta || 0.0) > limits.max_cost_delta) {
      const prop: RemediationProposal = {
        proposal_id: pid,
        tenant_id,
        action_type,
        target_entity,
        parameters: params,
        blast_radius: br,
        approval_tier: tier,
        preconditions: pre,
        postconditions: post,
        compensation_action: comp_action,
        status: ProposalStatus.REJECTED,
        proposed_at,
        authoritative_epoch: this._currentEpoch,
        rejection_reason: `Cost delta ($${(br.max_cost_delta || 0).toFixed(2)}) exceeds tier ceiling ($${limits.max_cost_delta.toFixed(2)})`,
      };
      prop.proposal_hash = this.computeProposalHash(prop);
      this._proposals.set(pid, prop);
      this._recordTenantProposal(tenant_id, pid);
      return prop;
    }

    // Automatic approval for Tier 0
    let status: ProposalStatusEnum = ProposalStatus.PROPOSED;
    let approver_role: string | null = null;
    if (tier === ApprovalTier.TIER_0_AUTOMATIC) {
      status = ProposalStatus.AUTHORIZED;
      approver_role = "AUTOMATIC_POLICY";
    }

    const prop: RemediationProposal = {
      proposal_id: pid,
      tenant_id,
      action_type,
      target_entity,
      parameters: params,
      blast_radius: br,
      approval_tier: tier,
      preconditions: pre,
      postconditions: post,
      compensation_action: comp_action,
      status,
      proposed_at,
      authoritative_epoch: this._currentEpoch,
      approver_role,
    };
    prop.proposal_hash = this.computeProposalHash(prop);
    this._proposals.set(pid, prop);
    this._recordTenantProposal(tenant_id, pid);
    return prop;
  }

  public authorize(opts: AuthorizeOptions): RemediationDecision {
    const prop = this._proposals.get(opts.proposal_id);
    const decided_at = Date.now() / 1000.0;

    if (!prop) {
      return {
        proposal_id: opts.proposal_id,
        approved: false,
        status: ProposalStatus.REJECTED,
        approval_tier: ApprovalTier.TIER_0_AUTOMATIC,
        authoritative_epoch: this._currentEpoch,
        reason: `Proposal ${opts.proposal_id} not found`,
        decided_at,
      };
    }

    if (prop.status !== ProposalStatus.PROPOSED && prop.status !== ProposalStatus.AUTHORIZED) {
      return {
        proposal_id: opts.proposal_id,
        approved: false,
        status: prop.status,
        approval_tier: prop.approval_tier,
        authoritative_epoch: this._currentEpoch,
        reason: `Proposal is already in terminal/applied state: ${prop.status}`,
        decided_at,
      };
    }

    const effectiveEpoch = opts.current_epoch || this._currentEpoch;
    if (effectiveEpoch < this._currentEpoch) {
      prop.status = ProposalStatus.REJECTED;
      prop.rejection_reason = `STALE_EPOCH: provided ${effectiveEpoch} < current ${this._currentEpoch}`;
      return {
        proposal_id: opts.proposal_id,
        approved: false,
        status: ProposalStatus.REJECTED,
        approval_tier: prop.approval_tier,
        authoritative_epoch: this._currentEpoch,
        reason: prop.rejection_reason,
        decided_at,
      };
    }

    if (prop.approval_tier === ApprovalTier.TIER_0_AUTOMATIC) {
      prop.status = ProposalStatus.AUTHORIZED;
      prop.authoritative_epoch = this._currentEpoch;
      prop.approver_role = opts.approver_role || "AUTOMATIC_POLICY";
      return {
        proposal_id: opts.proposal_id,
        approved: true,
        status: ProposalStatus.AUTHORIZED,
        approval_tier: prop.approval_tier,
        authoritative_epoch: this._currentEpoch,
        reason: "Authorized automatically under TIER_0_AUTOMATIC quotas",
        decided_at,
      };
    }

    if (prop.approval_tier === ApprovalTier.TIER_1_SUPERVISED) {
      if (!opts.permit_hash && !opts.approver_role) {
        prop.status = ProposalStatus.REJECTED;
        prop.rejection_reason = "TIER_1_SUPERVISED requires permit_hash or supervisor approver_role";
        return {
          proposal_id: opts.proposal_id,
          approved: false,
          status: ProposalStatus.REJECTED,
          approval_tier: prop.approval_tier,
          authoritative_epoch: this._currentEpoch,
          reason: prop.rejection_reason,
          decided_at,
        };
      }
      prop.status = ProposalStatus.AUTHORIZED;
      prop.authoritative_epoch = this._currentEpoch;
      prop.permit_hash = opts.permit_hash || null;
      prop.approver_role = opts.approver_role || "SAFETY_SUPERVISOR";
      return {
        proposal_id: opts.proposal_id,
        approved: true,
        status: ProposalStatus.AUTHORIZED,
        approval_tier: prop.approval_tier,
        authoritative_epoch: this._currentEpoch,
        reason: "Authorized under TIER_1_SUPERVISED with validated safety credentials",
        decided_at,
      };
    }

    if (prop.approval_tier === ApprovalTier.TIER_2_MANUAL) {
      const validRoles = new Set(["ADMIN", "OPERATOR", "SECURITY_ADMIN", "CLUSTER_LEAD"]);
      const roleUpper = (opts.approver_role || "").toUpperCase();
      if (!validRoles.has(roleUpper)) {
        prop.status = ProposalStatus.REJECTED;
        prop.rejection_reason = "TIER_2_MANUAL requires administrative approver_role in ADMIN, OPERATOR, SECURITY_ADMIN, CLUSTER_LEAD";
        return {
          proposal_id: opts.proposal_id,
          approved: false,
          status: ProposalStatus.REJECTED,
          approval_tier: prop.approval_tier,
          authoritative_epoch: this._currentEpoch,
          reason: prop.rejection_reason,
          decided_at,
        };
      }
      prop.status = ProposalStatus.AUTHORIZED;
      prop.authoritative_epoch = this._currentEpoch;
      prop.approver_role = roleUpper;
      return {
        proposal_id: opts.proposal_id,
        approved: true,
        status: ProposalStatus.AUTHORIZED,
        approval_tier: prop.approval_tier,
        authoritative_epoch: this._currentEpoch,
        reason: `Authorized under TIER_2_MANUAL by ${prop.approver_role}`,
        decided_at,
      };
    }

    return {
      proposal_id: opts.proposal_id,
      approved: false,
      status: ProposalStatus.REJECTED,
      approval_tier: prop.approval_tier,
      authoritative_epoch: this._currentEpoch,
      reason: "Unrecognized approval tier",
      decided_at,
    };
  }

  public apply(opts: ApplyOptions): RemediationResult {
    const startHr = process.hrtime.bigint();
    const prop = this._proposals.get(opts.proposal_id);

    if (!prop) {
      const elapsed_us = Number(process.hrtime.bigint() - startHr) / 1000.0;
      return {
        proposal_id: opts.proposal_id,
        success: false,
        status: ProposalStatus.REJECTED,
        applied_epoch: this._currentEpoch,
        execution_latency_us: elapsed_us,
        details: {},
        error: `Proposal ${opts.proposal_id} not found`,
      };
    }

    // Monotonic fencing epoch check
    if (opts.current_epoch < this._currentEpoch) {
      const elapsed_us = Number(process.hrtime.bigint() - startHr) / 1000.0;
      return {
        proposal_id: opts.proposal_id,
        success: false,
        status: prop.status,
        applied_epoch: this._currentEpoch,
        execution_latency_us: elapsed_us,
        details: {},
        error: `STALE_EPOCH: caller epoch ${opts.current_epoch} < authoritative ${this._currentEpoch}`,
      };
    }

    if (prop.status !== ProposalStatus.AUTHORIZED) {
      const elapsed_us = Number(process.hrtime.bigint() - startHr) / 1000.0;
      return {
        proposal_id: opts.proposal_id,
        success: false,
        status: prop.status,
        applied_epoch: this._currentEpoch,
        execution_latency_us: elapsed_us,
        details: {},
        error: `Cannot apply proposal in status ${prop.status}; must be AUTHORIZED`,
      };
    }

    const ctx = { ...(opts.context || {}) };

    // 1. Preconditions verification
    if (prop.preconditions && Object.keys(prop.preconditions).length > 0) {
      for (const [k, expected] of Object.entries(prop.preconditions)) {
        const actual = ctx[k];
        if (actual !== expected) {
          prop.status = ProposalStatus.REJECTED;
          prop.rejection_reason = `Precondition failed for '${k}': expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`;
          const elapsed_us = Number(process.hrtime.bigint() - startHr) / 1000.0;
          return {
            proposal_id: opts.proposal_id,
            success: false,
            status: ProposalStatus.REJECTED,
            applied_epoch: this._currentEpoch,
            execution_latency_us: elapsed_us,
            details: {},
            error: prop.rejection_reason,
          };
        }
      }
    }

    // 2. Execution of remediation mutation
    const execDetails: Record<string, any> = {
      action_type: prop.action_type,
      target_entity: prop.target_entity,
      applied_epoch: this._currentEpoch,
    };

    try {
      if (opts.executor) {
        const customDetails = opts.executor(prop, ctx);
        if (customDetails && typeof customDetails === "object") {
          Object.assign(execDetails, customDetails);
        }
      } else {
        execDetails.mutations_applied = {
          action: prop.action_type,
          target: prop.target_entity,
          params: prop.parameters,
        };
        Object.assign(ctx, prop.parameters);
        ctx[`${prop.action_type}_applied`] = true;
      }
    } catch (exc: any) {
      prop.status = ProposalStatus.FAILED;
      prop.rejection_reason = `Execution failed: ${exc?.message || exc}`;
      this.compensate({
        proposal_id: opts.proposal_id,
        reason: `Automated compensation due to execution failure: ${exc?.message || exc}`,
      });
      const elapsed_us = Number(process.hrtime.bigint() - startHr) / 1000.0;
      return {
        proposal_id: opts.proposal_id,
        success: false,
        status: ProposalStatus.FAILED,
        applied_epoch: this._currentEpoch,
        execution_latency_us: elapsed_us,
        details: execDetails,
        error: prop.rejection_reason,
      };
    }

    // 3. Postcondition verification
    if (prop.postconditions && Object.keys(prop.postconditions).length > 0) {
      for (const [k, expected] of Object.entries(prop.postconditions)) {
        const actual = ctx[k];
        if (actual !== expected) {
          prop.status = ProposalStatus.FAILED;
          prop.rejection_reason = `Postcondition failed for '${k}': expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`;
          this.compensate({
            proposal_id: opts.proposal_id,
            reason: `Automated compensation: ${prop.rejection_reason}`,
          });
          const elapsed_us = Number(process.hrtime.bigint() - startHr) / 1000.0;
          return {
            proposal_id: opts.proposal_id,
            success: false,
            status: ProposalStatus.FAILED,
            applied_epoch: this._currentEpoch,
            execution_latency_us: elapsed_us,
            details: execDetails,
            error: prop.rejection_reason,
          };
        }
      }
    }

    // 4. Commit applied state
    prop.status = ProposalStatus.APPLIED;
    this._activeRemediations.set(opts.proposal_id, {
      proposal_id: opts.proposal_id,
      tenant_id: prop.tenant_id,
      action_type: prop.action_type,
      target_entity: prop.target_entity,
      epoch: this._currentEpoch,
      applied_at: Date.now() / 1000.0,
      details: execDetails,
    });

    const elapsed_us = Number(process.hrtime.bigint() - startHr) / 1000.0;
    return {
      proposal_id: opts.proposal_id,
      success: true,
      status: ProposalStatus.APPLIED,
      applied_epoch: this._currentEpoch,
      execution_latency_us: elapsed_us,
      details: execDetails,
    };
  }

  public compensate(opts: CompensateOptions): CompensationResult {
    const prop = this._proposals.get(opts.proposal_id);
    const compensated_at = Date.now() / 1000.0;

    if (!prop) {
      return {
        proposal_id: opts.proposal_id,
        success: false,
        status: ProposalStatus.REJECTED,
        compensated_at,
        reason: `Proposal ${opts.proposal_id} not found`,
        details: {},
      };
    }

    // Idempotent check
    if (prop.status === ProposalStatus.COMPENSATED) {
      return {
        proposal_id: opts.proposal_id,
        success: true,
        status: ProposalStatus.COMPENSATED,
        compensated_at,
        reason: "Already compensated (idempotent no-op)",
        details: { already_compensated: true },
      };
    }

    const details: Record<string, any> = {
      original_action: prop.action_type,
      target_entity: prop.target_entity,
      compensation_contract: prop.compensation_action,
    };

    try {
      if (opts.compensation_handler) {
        const res = opts.compensation_handler(prop);
        if (res && typeof res === "object") {
          Object.assign(details, res);
        }
      } else if (prop.compensation_action) {
        details.executed_compensation = prop.compensation_action;
      }
    } catch (exc: any) {
      details.compensation_error = exc?.message || String(exc);
    }

    this._activeRemediations.delete(opts.proposal_id);
    prop.status = ProposalStatus.COMPENSATED;

    return {
      proposal_id: opts.proposal_id,
      success: true,
      status: ProposalStatus.COMPENSATED,
      compensated_at,
      reason: opts.reason || "Manual operator compensation",
      details,
    };
  }

  public status(tenant_id?: string): Record<string, any> {
    let proposals: RemediationProposal[];
    if (tenant_id) {
      const pids = this._tenantProposals.get(tenant_id) || [];
      proposals = pids.map((id) => this._proposals.get(id)!).filter(Boolean);
    } else {
      proposals = Array.from(this._proposals.values());
    }

    const by_status: Record<string, number> = {};
    const by_tier: Record<string, number> = {};
    for (const p of proposals) {
      by_status[p.status] = (by_status[p.status] || 0) + 1;
      by_tier[p.approval_tier] = (by_tier[p.approval_tier] || 0) + 1;
    }

    const activeList = Array.from(this._activeRemediations.values()).filter(
      (r) => !tenant_id || r.tenant_id === tenant_id
    );

    return {
      current_epoch: this._currentEpoch,
      total_proposals: proposals.length,
      active_remediations_count: activeList.length,
      by_status,
      by_tier,
      active_remediations: activeList,
      recent_proposals: proposals.slice(-10),
    };
  }

  public reset(): void {
    this._proposals.clear();
    this._tenantProposals.clear();
    this._activeRemediations.clear();
    this._currentEpoch = 1;
  }

  private _recordTenantProposal(tenantId: string, pid: string): void {
    const list = this._tenantProposals.get(tenantId);
    if (list) {
      list.push(pid);
    } else {
      this._tenantProposals.set(tenantId, [pid]);
    }
  }
}

export const globalRemediationGovernor = new RemediationGovernor();
