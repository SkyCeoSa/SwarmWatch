/**
 * SwarmWatch Autonomous Action Gateway & Governed Downstream Agent Execution (Seam 71 / ADR 0082).
 *
 * Implements capability-gated downstream agent execution, multi-party human quorum authorization,
 * reversibility and risk classification, fail-closed circuit synchronization, and
 * cryptographically verifiable action execution receipts.
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";
import { CanonicalCodec } from "./canonical.ts";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class ActionGatewayError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ActionGatewayError";
  }
}

export class UnauthorizedActionError extends ActionGatewayError {
  constructor(message: string) {
    super(message);
    this.name = "UnauthorizedActionError";
  }
}

export class ActionQuorumPendingError extends ActionGatewayError {
  constructor(message: string) {
    super(message);
    this.name = "ActionQuorumPendingError";
  }
}

export class ActionCircuitBlockedError extends ActionGatewayError {
  constructor(message: string) {
    super(message);
    this.name = "ActionCircuitBlockedError";
  }
}

export class ActionExecutionFailedError extends ActionGatewayError {
  constructor(message: string) {
    super(message);
    this.name = "ActionExecutionFailedError";
  }
}

// ==============================================================================
// 2. Enums (Const objects for Node 22 native type stripping)
// ==============================================================================

export const ActionRiskLevel = {
  LOW: "LOW",
  MEDIUM: "MEDIUM",
  HIGH: "HIGH",
  CRITICAL: "CRITICAL",
} as const;
export type ActionRiskLevel = typeof ActionRiskLevel[keyof typeof ActionRiskLevel];

export const ActionApprovalStatus = {
  PENDING_APPROVAL: "PENDING_APPROVAL",
  APPROVED: "APPROVED",
  REJECTED: "REJECTED",
  EXECUTED: "EXECUTED",
  FAILED: "FAILED",
  CANCELLED: "CANCELLED",
} as const;
export type ActionApprovalStatus = typeof ActionApprovalStatus[keyof typeof ActionApprovalStatus];

// ==============================================================================
// 3. Models & Data Structures
// ==============================================================================

export function sha256Digest(data: string | Buffer): string {
  return crypto.createHash("sha256").update(data).digest("hex");
}

export interface ActionRegistrationData {
  action_name: string;
  default_risk: string;
  default_reversibility: string;
  required_quorum_count: number;
  description: string;
}

export type ActionHandler = (params: Record<string, unknown>) => unknown;

export class ActionRegistration {
  public readonly action_name: string;
  public readonly handler: ActionHandler;
  public readonly default_risk: ActionRiskLevel;
  public readonly default_reversibility: string;
  public readonly required_quorum_count: number;
  public readonly description: string;

  constructor(
    action_name: string,
    handler: ActionHandler,
    default_risk: ActionRiskLevel = ActionRiskLevel.LOW,
    default_reversibility: string = "REVERSIBLE",
    required_quorum_count: number = 1,
    description: string = ""
  ) {
    this.action_name = action_name;
    this.handler = handler;
    this.default_risk = default_risk;
    this.default_reversibility = default_reversibility;
    this.required_quorum_count = required_quorum_count;
    this.description = description;
  }

  public toDict(): ActionRegistrationData {
    return {
      action_name: this.action_name,
      default_risk: this.default_risk,
      default_reversibility: this.default_reversibility,
      required_quorum_count: this.required_quorum_count,
      description: this.description,
    };
  }
}

export interface ActionApprovalData {
  approval_id: string;
  request_id: string;
  approver_principal: string;
  decision: string;
  comment: string;
  timestamp_ns: number;
  signature: string;
}

export class ActionApproval {
  public readonly approval_id: string;
  public readonly request_id: string;
  public readonly approver_principal: string;
  public readonly decision: string;
  public readonly comment: string;
  public readonly timestamp_ns: number;
  public readonly signature: string;

  constructor(
    approval_id: string,
    request_id: string,
    approver_principal: string,
    decision: string,
    comment: string = "",
    timestamp_ns?: number,
    signature: string = ""
  ) {
    this.approval_id = approval_id;
    this.request_id = request_id;
    this.approver_principal = approver_principal;
    this.decision = decision.toUpperCase();
    this.comment = comment;
    this.timestamp_ns = timestamp_ns ?? Date.now() * 1_000_000;
    this.signature = signature;
  }

  public toDict(): ActionApprovalData {
    return {
      approval_id: this.approval_id,
      request_id: this.request_id,
      approver_principal: this.approver_principal,
      decision: this.decision,
      comment: this.comment,
      timestamp_ns: this.timestamp_ns,
      signature: this.signature,
    };
  }

  public static fromDict(data: ActionApprovalData): ActionApproval {
    return new ActionApproval(
      data.approval_id,
      data.request_id,
      data.approver_principal,
      data.decision,
      data.comment || "",
      data.timestamp_ns,
      data.signature || ""
    );
  }
}

export interface ActionRequestData {
  request_id: string;
  tenant_id: string;
  agent_id: string;
  action_name: string;
  target_resource: string;
  parameters: Record<string, unknown>;
  capability_token: string | null;
  estimated_cost_usd: number;
  reversibility_class: string;
  risk_level: string;
  status: string;
  required_quorum_count: number;
  created_at_ns: number;
  approvals: ActionApprovalData[];
  rejection_reason: string | null;
}

export class ActionRequest {
  public readonly request_id: string;
  public readonly tenant_id: string;
  public readonly agent_id: string;
  public readonly action_name: string;
  public readonly target_resource: string;
  public readonly parameters: Record<string, unknown>;
  public readonly capability_token: string | null;
  public readonly estimated_cost_usd: number;
  public readonly reversibility_class: string;
  public readonly risk_level: ActionRiskLevel;
  public status: ActionApprovalStatus;
  public readonly required_quorum_count: number;
  public readonly created_at_ns: number;
  public readonly approvals: ActionApproval[];
  public rejection_reason: string | null;

  constructor(data: {
    request_id: string;
    tenant_id: string;
    agent_id: string;
    action_name: string;
    target_resource: string;
    parameters?: Record<string, unknown>;
    capability_token?: string | null;
    estimated_cost_usd?: number;
    reversibility_class?: string;
    risk_level?: ActionRiskLevel;
    status?: ActionApprovalStatus;
    required_quorum_count?: number;
    created_at_ns?: number;
    approvals?: ActionApproval[];
    rejection_reason?: string | null;
  }) {
    this.request_id = data.request_id;
    this.tenant_id = data.tenant_id;
    this.agent_id = data.agent_id;
    this.action_name = data.action_name;
    this.target_resource = data.target_resource;
    this.parameters = data.parameters ?? {};
    this.capability_token = data.capability_token ?? null;
    this.estimated_cost_usd = data.estimated_cost_usd ?? 0.0;
    this.reversibility_class = data.reversibility_class ?? "REVERSIBLE";
    this.risk_level = data.risk_level ?? ActionRiskLevel.LOW;
    this.status = data.status ?? ActionApprovalStatus.PENDING_APPROVAL;
    this.required_quorum_count = data.required_quorum_count ?? 0;
    this.created_at_ns = data.created_at_ns ?? Date.now() * 1_000_000;
    this.approvals = data.approvals ?? [];
    this.rejection_reason = data.rejection_reason ?? null;
  }

  public toDict(): ActionRequestData {
    return {
      request_id: this.request_id,
      tenant_id: this.tenant_id,
      agent_id: this.agent_id,
      action_name: this.action_name,
      target_resource: this.target_resource,
      parameters: this.parameters,
      capability_token: this.capability_token,
      estimated_cost_usd: this.estimated_cost_usd,
      reversibility_class: this.reversibility_class,
      risk_level: this.risk_level,
      status: this.status,
      required_quorum_count: this.required_quorum_count,
      created_at_ns: this.created_at_ns,
      approvals: this.approvals.map((a) => a.toDict()),
      rejection_reason: this.rejection_reason,
    };
  }

  public static fromDict(data: ActionRequestData): ActionRequest {
    const approvals = (data.approvals || []).map((a) => ActionApproval.fromDict(a));
    return new ActionRequest({
      request_id: data.request_id,
      tenant_id: data.tenant_id,
      agent_id: data.agent_id,
      action_name: data.action_name,
      target_resource: data.target_resource,
      parameters: data.parameters,
      capability_token: data.capability_token,
      estimated_cost_usd: data.estimated_cost_usd,
      reversibility_class: data.reversibility_class,
      risk_level: data.risk_level as ActionRiskLevel,
      status: data.status as ActionApprovalStatus,
      required_quorum_count: data.required_quorum_count,
      created_at_ns: data.created_at_ns,
      approvals,
      rejection_reason: data.rejection_reason,
    });
  }
}

export interface ActionExecutionReceiptData {
  receipt_id: string;
  request_id: string;
  tenant_id: string;
  agent_id: string;
  action_name: string;
  status: string;
  result_summary: Record<string, unknown>;
  executed_at_ns: number;
  duration_us: number;
  receipt_hash: string;
}

export class ActionExecutionReceipt {
  public readonly receipt_id: string;
  public readonly request_id: string;
  public readonly tenant_id: string;
  public readonly agent_id: string;
  public readonly action_name: string;
  public readonly status: string;
  public readonly result_summary: Record<string, unknown>;
  public readonly executed_at_ns: number;
  public readonly duration_us: number;
  public readonly receipt_hash: string;

  constructor(data: {
    receipt_id: string;
    request_id: string;
    tenant_id: string;
    agent_id: string;
    action_name: string;
    status: string;
    result_summary: Record<string, unknown>;
    executed_at_ns: number;
    duration_us: number;
    receipt_hash: string;
  }) {
    this.receipt_id = data.receipt_id;
    this.request_id = data.request_id;
    this.tenant_id = data.tenant_id;
    this.agent_id = data.agent_id;
    this.action_name = data.action_name;
    this.status = data.status;
    this.result_summary = data.result_summary;
    this.executed_at_ns = data.executed_at_ns;
    this.duration_us = data.duration_us;
    this.receipt_hash = data.receipt_hash;
  }

  public toDict(): ActionExecutionReceiptData {
    return {
      receipt_id: this.receipt_id,
      request_id: this.request_id,
      tenant_id: this.tenant_id,
      agent_id: this.agent_id,
      action_name: this.action_name,
      status: this.status,
      result_summary: this.result_summary,
      executed_at_ns: this.executed_at_ns,
      duration_us: this.duration_us,
      receipt_hash: this.receipt_hash,
    };
  }

  public static fromDict(data: ActionExecutionReceiptData): ActionExecutionReceipt {
    return new ActionExecutionReceipt({
      receipt_id: data.receipt_id,
      request_id: data.request_id,
      tenant_id: data.tenant_id,
      agent_id: data.agent_id,
      action_name: data.action_name,
      status: data.status,
      result_summary: data.result_summary || {},
      executed_at_ns: data.executed_at_ns,
      duration_us: data.duration_us,
      receipt_hash: data.receipt_hash,
    });
  }
}

// ==============================================================================
// 4. Core Engine: ActionGateway
// ==============================================================================

export type CapabilityValidator = (
  token: string,
  action: string,
  resource: string,
  tenant_id: string
) => boolean;

export class ActionGateway {
  private readonly _registry: Map<string, ActionRegistration> = new Map();
  private readonly _requests: Map<string, ActionRequest> = new Map();
  private readonly _receipts: Map<string, ActionExecutionReceipt> = new Map();
  private _circuitTripped = false;
  private _requireCapabilityTokens = false;
  private _capabilityValidator: CapabilityValidator | null = null;

  constructor() {
    this._autoRegisterDefaults();
  }

  private _autoRegisterDefaults(): void {
    this.registerExecutor(
      "db:query",
      (params) => ({ rows_returned: 42, query: params.sql || "" }),
      ActionRiskLevel.LOW,
      "REVERSIBLE",
      0,
      "Read-only SQL query execution"
    );
    this.registerExecutor(
      "api:webhook_call",
      (params) => ({ status: 200, delivered: true, target: params.url || "" }),
      ActionRiskLevel.MEDIUM,
      "COMPENSATABLE",
      1,
      "Outbound webhook HTTP invocation"
    );
    this.registerExecutor(
      "db:drop_table",
      (params) => ({ dropped: true, table: params.table || "" }),
      ActionRiskLevel.CRITICAL,
      "IRREVERSIBLE",
      2,
      "Destructive table schema drop"
    );
    this.registerExecutor(
      "finance:refund",
      (params) => ({ refund_id: `ref-${crypto.randomBytes(4).toString("hex")}`, amount: params.amount || 0 }),
      ActionRiskLevel.HIGH,
      "IRREVERSIBLE",
      1,
      "Customer payment refund transaction"
    );
  }

  public setCapabilityValidator(validator: CapabilityValidator | null, requireTokens: boolean = true): void {
    this._capabilityValidator = validator;
    this._requireCapabilityTokens = requireTokens;
  }

  public setCircuitTripped(tripped: boolean): void {
    this._circuitTripped = tripped;
  }

  public registerExecutor(
    actionName: string,
    handler: ActionHandler,
    defaultRisk: ActionRiskLevel = ActionRiskLevel.LOW,
    defaultReversibility: string = "REVERSIBLE",
    requiredQuorumCount: number = 1,
    description: string = ""
  ): void {
    this._registry.set(
      actionName,
      new ActionRegistration(
        actionName,
        handler,
        defaultRisk,
        defaultReversibility,
        requiredQuorumCount,
        description
      )
    );
  }

  public evaluateRisk(
    actionName: string,
    targetResource: string,
    reversibility: string,
    estimatedCostUsd: number
  ): [ActionRiskLevel, number] {
    const reg = this._registry.get(actionName);
    const baseQuorum = reg ? reg.required_quorum_count : 1;

    // 1. Critical actions
    if (
      reversibility === "IRREVERSIBLE" &&
      (estimatedCostUsd > 50.0 ||
        ["drop", "delete", "destroy", "purge"].some((k) => actionName.toLowerCase().includes(k)))
    ) {
      return [ActionRiskLevel.CRITICAL, Math.max(2, baseQuorum)];
    }

    // 2. High-risk actions
    if (reversibility === "IRREVERSIBLE" || estimatedCostUsd > 50.0) {
      return [ActionRiskLevel.HIGH, Math.max(1, baseQuorum)];
    }

    // 3. Medium-risk actions
    if (reversibility === "COMPENSATABLE" || estimatedCostUsd > 1.0) {
      return [ActionRiskLevel.MEDIUM, Math.max(1, baseQuorum)];
    }

    // 4. Low-risk actions
    return [ActionRiskLevel.LOW, 0];
  }

  public requestAction(data: {
    tenant_id: string;
    agent_id: string;
    action_name: string;
    target_resource: string;
    parameters?: Record<string, unknown>;
    capability_token?: string | null;
    estimated_cost_usd?: number;
    reversibility_class?: string;
    explicit_request_id?: string;
  }): ActionRequest {
    if (this._circuitTripped) {
      throw new ActionCircuitBlockedError("Action Gateway halted: safety circuit breaker is tripped");
    }

    const tenantId = data.tenant_id;
    const actionName = data.action_name;
    const targetResource = data.target_resource;
    const capabilityToken = data.capability_token ?? null;
    const cost = data.estimated_cost_usd ?? 0.0;

    if (this._requireCapabilityTokens) {
      if (!capabilityToken) {
        throw new UnauthorizedActionError(`Action '${actionName}' requires capability token`);
      }
      if (this._capabilityValidator) {
        const isValid = this._capabilityValidator(capabilityToken, actionName, targetResource, tenantId);
        if (!isValid) {
          throw new UnauthorizedActionError(
            `Capability token rejected for action '${actionName}' on '${targetResource}'`
          );
        }
      }
    }

    const reg = this._registry.get(actionName);
    const rev = data.reversibility_class || (reg ? reg.default_reversibility : "REVERSIBLE");
    const [riskLevel, requiredQuorum] = this.evaluateRisk(actionName, targetResource, rev, cost);

    const initialStatus =
      requiredQuorum === 0 ? ActionApprovalStatus.APPROVED : ActionApprovalStatus.PENDING_APPROVAL;

    const reqId = data.explicit_request_id || `act-req-${crypto.randomBytes(6).toString("hex")}`;
    const request = new ActionRequest({
      request_id: reqId,
      tenant_id: tenantId,
      agent_id: data.agent_id,
      action_name: actionName,
      target_resource: targetResource,
      parameters: data.parameters || {},
      capability_token: capabilityToken,
      estimated_cost_usd: cost,
      reversibility_class: rev,
      risk_level: riskLevel,
      status: initialStatus,
      required_quorum_count: requiredQuorum,
      created_at_ns: Date.now() * 1_000_000,
    });

    this._requests.set(reqId, request);
    return request;
  }

  public approveAction(
    requestId: string,
    approverPrincipal: string,
    decision: string = "APPROVED",
    comment: string = "",
    signature: string = ""
  ): ActionRequest {
    const request = this._requests.get(requestId);
    if (!request) {
      throw new ActionGatewayError(`Action request '${requestId}' not found`);
    }

    if (
      request.status !== ActionApprovalStatus.PENDING_APPROVAL &&
      request.status !== ActionApprovalStatus.APPROVED
    ) {
      throw new ActionGatewayError(
        `Cannot approve action request '${requestId}' in state '${request.status}'`
      );
    }

    const approval = new ActionApproval(
      `appr-${crypto.randomBytes(4).toString("hex")}`,
      requestId,
      approverPrincipal,
      decision,
      comment,
      Date.now() * 1_000_000,
      signature
    );
    request.approvals.push(approval);

    if (decision.toUpperCase() === "REJECTED") {
      request.status = ActionApprovalStatus.REJECTED;
      request.rejection_reason = comment || `Rejected by ${approverPrincipal}`;
      return request;
    }

    const positiveApprovals = request.approvals.filter((a) => a.decision === "APPROVED").length;
    if (positiveApprovals >= request.required_quorum_count) {
      request.status = ActionApprovalStatus.APPROVED;
    }

    return request;
  }

  public executeAction(requestId: string): ActionExecutionReceipt {
    if (this._circuitTripped) {
      throw new ActionCircuitBlockedError("Action Gateway execution blocked: safety circuit breaker is tripped");
    }

    const request = this._requests.get(requestId);
    if (!request) {
      throw new ActionGatewayError(`Action request '${requestId}' not found`);
    }

    if (request.status === ActionApprovalStatus.PENDING_APPROVAL) {
      const positive = request.approvals.filter((a) => a.decision === "APPROVED").length;
      throw new ActionQuorumPendingError(
        `Action request '${requestId}' requires ${request.required_quorum_count} approvals; has ${positive}`
      );
    }

    if (request.status === ActionApprovalStatus.REJECTED) {
      throw new ActionGatewayError(`Action request '${requestId}' was rejected: ${request.rejection_reason}`);
    }

    if (request.status === ActionApprovalStatus.EXECUTED) {
      throw new ActionGatewayError(`Action request '${requestId}' has already been executed`);
    }

    const reg = this._registry.get(request.action_name);
    if (!reg) {
      throw new ActionGatewayError(`No executor registered for action '${request.action_name}'`);
    }

    const t0 = performance.now();
    let execStatus = "SUCCESS";
    let resultSummary: Record<string, unknown> = {};

    try {
      const out = reg.handler(request.parameters);
      if (typeof out === "object" && out !== null) {
        resultSummary = out as Record<string, unknown>;
      } else {
        resultSummary = { result: out };
      }
    } catch (e: any) {
      execStatus = "FAILED";
      resultSummary = { error: String(e.message || e), error_type: e.name || "Error" };
    }

    const t1 = performance.now();
    const durationUs = (t1 - t0) * 1000;
    const executedAtNs = Date.now() * 1_000_000;
    const receiptId = `rcpt-act-${crypto.randomBytes(6).toString("hex")}`;

    const canonicalDict = {
      receipt_id: receiptId,
      request_id: requestId,
      tenant_id: request.tenant_id,
      agent_id: request.agent_id,
      action_name: request.action_name,
      status: execStatus,
      executed_at_ns: executedAtNs,
      duration_us: Math.round(durationUs * 100) / 100,
      result_summary: resultSummary,
    };
    const encoded = CanonicalCodec.canonicalize(canonicalDict);
    const receiptHash = sha256Digest(`sw:act:v1:${encoded}`);

    const receipt = new ActionExecutionReceipt({
      receipt_id: receiptId,
      request_id: requestId,
      tenant_id: request.tenant_id,
      agent_id: request.agent_id,
      action_name: request.action_name,
      status: execStatus,
      result_summary: resultSummary,
      executed_at_ns: executedAtNs,
      duration_us: durationUs,
      receipt_hash: receiptHash,
    });

    request.status = execStatus === "SUCCESS" ? ActionApprovalStatus.EXECUTED : ActionApprovalStatus.FAILED;
    this._receipts.set(receiptId, receipt);

    if (execStatus === "FAILED") {
      throw new ActionExecutionFailedError(
        `Action '${request.action_name}' execution failed: ${resultSummary.error}`
      );
    }

    return receipt;
  }

  public getPendingApprovals(tenantId?: string): ActionRequest[] {
    let pending = Array.from(this._requests.values()).filter(
      (r) => r.status === ActionApprovalStatus.PENDING_APPROVAL
    );
    if (tenantId) {
      pending = pending.filter((r) => r.tenant_id === tenantId);
    }
    return pending.sort((a, b) => a.created_at_ns - b.created_at_ns);
  }

  public getReceipt(receiptId: string): ActionExecutionReceipt | undefined {
    return this._receipts.get(receiptId);
  }

  public getRequest(requestId: string): ActionRequest | undefined {
    return this._requests.get(requestId);
  }

  public getReceipts(tenantId?: string): ActionExecutionReceipt[] {
    let receipts = Array.from(this._receipts.values());
    if (tenantId) {
      receipts = receipts.filter((r) => r.tenant_id === tenantId);
    }
    return receipts.sort((a, b) => a.executed_at_ns - b.executed_at_ns);
  }

  public status(): Record<string, unknown> {
    let executedCount = 0;
    let failedCount = 0;
    for (const r of this._receipts.values()) {
      if (r.status === "SUCCESS") executedCount++;
      if (r.status === "FAILED") failedCount++;
    }
    let pendingCount = 0;
    for (const req of this._requests.values()) {
      if (req.status === ActionApprovalStatus.PENDING_APPROVAL) pendingCount++;
    }

    return {
      circuit_tripped: this._circuitTripped,
      registered_executors_count: this._registry.size,
      total_requests: this._requests.size,
      pending_approvals_count: pendingCount,
      executed_receipts_count: executedCount,
      failed_receipts_count: failedCount,
      require_capability_tokens: this._requireCapabilityTokens,
    };
  }

  public reset(): void {
    this._requests.clear();
    this._receipts.clear();
    this._circuitTripped = false;
    this._requireCapabilityTokens = false;
    this._capabilityValidator = null;
    this._registry.clear();
    this._autoRegisterDefaults();
  }
}

// ==============================================================================
// 5. Global Singleton
// ==============================================================================

export const globalActionGateway = new ActionGateway();
