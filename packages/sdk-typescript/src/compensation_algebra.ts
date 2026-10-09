/**
 * Effect-Aware Compensation Algebra & Monotone Approval Lattices (Seam 66 / ADR 0077).
 * Implements formal action classification, reversibility lattice join composition,
 * deterministic reverse-ordered compensation planning, and fail-closed human escalation.
 */

import * as crypto from "node:crypto";

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

export class CompensationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CompensationError";
  }
}

export class IrreversibleActionError extends CompensationError {
  constructor(message: string) {
    super(message);
    this.name = "IrreversibleActionError";
  }
}

export class UnregisteredActionError extends CompensationError {
  constructor(message: string) {
    super(message);
    this.name = "UnregisteredActionError";
  }
}

export class EscalationPendingError extends CompensationError {
  constructor(message: string) {
    super(message);
    this.name = "EscalationPendingError";
  }
}

// ---------------------------------------------------------------------------
// Reversibility Lattice & Ordering
// ---------------------------------------------------------------------------

export const ReversibilityClass = {
  REVERSIBLE: 0,
  COMPENSATABLE: 1,
  IRREVERSIBLE: 2,
} as const;

export type ReversibilityClass = typeof ReversibilityClass[keyof typeof ReversibilityClass];

export function parseReversibilityClass(val: ReversibilityClass | string | number): ReversibilityClass {
  if (typeof val === "number") {
    if (val === 0) return ReversibilityClass.REVERSIBLE;
    if (val === 1) return ReversibilityClass.COMPENSATABLE;
    if (val === 2) return ReversibilityClass.IRREVERSIBLE;
  }
  const clean = String(val).toUpperCase().trim();
  if (clean === "REVERSIBLE" || clean === "0") return ReversibilityClass.REVERSIBLE;
  if (clean === "COMPENSATABLE" || clean === "1") return ReversibilityClass.COMPENSATABLE;
  if (clean === "IRREVERSIBLE" || clean === "2") return ReversibilityClass.IRREVERSIBLE;
  throw new Error(`Unknown reversibility class: ${val}`);
}

export function reversibilityClassName(cls: ReversibilityClass): string {
  switch (cls) {
    case ReversibilityClass.REVERSIBLE:
      return "REVERSIBLE";
    case ReversibilityClass.COMPENSATABLE:
      return "COMPENSATABLE";
    case ReversibilityClass.IRREVERSIBLE:
      return "IRREVERSIBLE";
    default:
      return "IRREVERSIBLE";
  }
}

export function reversibilityJoin(classes: (ReversibilityClass | string | number)[]): ReversibilityClass {
  if (!classes || classes.length === 0) {
    return ReversibilityClass.REVERSIBLE;
  }
  let maxVal: ReversibilityClass = ReversibilityClass.REVERSIBLE;
  for (const c of classes) {
    const parsed = parseReversibilityClass(c);
    if (parsed > maxVal) {
      maxVal = parsed;
    }
  }
  return maxVal;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export function sha256Hex(content: string | Buffer): string {
  return crypto.createHash("sha256").update(content).digest("hex");
}

function currentEpochNs(): bigint {
  return BigInt(Date.now()) * 1_000_000n;
}

// ---------------------------------------------------------------------------
// Data Models
// ---------------------------------------------------------------------------

export interface ActionExecutionData {
  actionId: string;
  actionType: string;
  tenantId: string;
  principal: string;
  payload?: Record<string, unknown>;
  timestampNs?: bigint | number;
  idempotencyKey?: string | null;
}

export class ActionExecution {
  public readonly actionId: string;
  public readonly actionType: string;
  public readonly tenantId: string;
  public readonly principal: string;
  public readonly payload: Record<string, unknown>;
  public readonly timestampNs: bigint;
  public readonly idempotencyKey: string | null;

  constructor(data: ActionExecutionData) {
    this.actionId = data.actionId;
    this.actionType = data.actionType;
    this.tenantId = data.tenantId;
    this.principal = data.principal;
    this.payload = { ...(data.payload || {}) };
    this.timestampNs =
      data.timestampNs !== undefined
        ? typeof data.timestampNs === "bigint"
          ? data.timestampNs
          : BigInt(data.timestampNs)
        : currentEpochNs();
    this.idempotencyKey = data.idempotencyKey ?? null;
  }

  public toJSON(): Record<string, unknown> {
    return {
      action_id: this.actionId,
      action_type: this.actionType,
      tenant_id: this.tenantId,
      principal: this.principal,
      payload: { ...this.payload },
      timestamp_ns: Number(this.timestampNs),
      idempotency_key: this.idempotencyKey,
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }

  public static fromDict(data: Record<string, unknown>): ActionExecution {
    return new ActionExecution({
      actionId: String(data.action_id || data.actionId || ""),
      actionType: String(data.action_type || data.actionType || ""),
      tenantId: String(data.tenant_id || data.tenantId || ""),
      principal: String(data.principal || data.principal || ""),
      payload: (data.payload as Record<string, unknown>) || {},
      timestampNs: BigInt(String(data.timestamp_ns ?? data.timestampNs ?? 0)),
      idempotencyKey: data.idempotency_key ? String(data.idempotency_key) : (data.idempotencyKey ? String(data.idempotencyKey) : null),
    });
  }
}

export interface ActionContractOptions {
  actionType: string;
  reversibilityClass: ReversibilityClass;
  description?: string;
  inverseHandler?: (action: ActionExecution) => boolean | Promise<boolean>;
  compensationHandler?: (action: ActionExecution) => boolean | Promise<boolean>;
}

export class ActionContract {
  public readonly actionType: string;
  public readonly reversibilityClass: ReversibilityClass;
  public readonly description: string;
  public readonly inverseHandler?: (action: ActionExecution) => boolean | Promise<boolean>;
  public readonly compensationHandler?: (action: ActionExecution) => boolean | Promise<boolean>;

  constructor(options: ActionContractOptions) {
    this.actionType = options.actionType;
    this.reversibilityClass = options.reversibilityClass;
    this.description = options.description || "";
    this.inverseHandler = options.inverseHandler;
    this.compensationHandler = options.compensationHandler;
  }

  public toJSON(): Record<string, unknown> {
    return {
      action_type: this.actionType,
      reversibility_class: reversibilityClassName(this.reversibilityClass),
      description: this.description,
      has_inverse_handler: Boolean(this.inverseHandler),
      has_compensation_handler: Boolean(this.compensationHandler),
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }
}

export interface CompositeClassificationData {
  reversibilityClass: ReversibilityClass;
  totalActions: number;
  reversibleCount: number;
  compensatableCount: number;
  irreversibleCount: number;
  irreversibleActionIds: string[];
}

export class CompositeClassification {
  public readonly reversibilityClass: ReversibilityClass;
  public readonly totalActions: number;
  public readonly reversibleCount: number;
  public readonly compensatableCount: number;
  public readonly irreversibleCount: number;
  public readonly irreversibleActionIds: string[];

  constructor(data: CompositeClassificationData) {
    this.reversibilityClass = data.reversibilityClass;
    this.totalActions = data.totalActions;
    this.reversibleCount = data.reversibleCount;
    this.compensatableCount = data.compensatableCount;
    this.irreversibleCount = data.irreversibleCount;
    this.irreversibleActionIds = [...data.irreversibleActionIds];
  }

  public get isFullyReversible(): boolean {
    return this.reversibilityClass === ReversibilityClass.REVERSIBLE;
  }

  public get isCompensatable(): boolean {
    return this.reversibilityClass <= ReversibilityClass.COMPENSATABLE;
  }

  public get hasIrreversibleSteps(): boolean {
    return this.reversibilityClass === ReversibilityClass.IRREVERSIBLE;
  }

  public toJSON(): Record<string, unknown> {
    return {
      reversibility_class: reversibilityClassName(this.reversibilityClass),
      total_actions: this.totalActions,
      reversible_count: this.reversibleCount,
      compensatable_count: this.compensatableCount,
      irreversible_count: this.irreversibleCount,
      irreversible_action_ids: [...this.irreversibleActionIds],
      is_fully_reversible: this.isFullyReversible,
      is_compensatable: this.isCompensatable,
      has_irreversible_steps: this.hasIrreversibleSteps,
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }
}

export interface CompensationStepData {
  stepIndex: number;
  targetActionId: string;
  actionType: string;
  reversibilityClass: ReversibilityClass;
  handlerType: "inverse" | "compensation" | "human_escalation";
  idempotencyKey: string;
}

export class CompensationStep {
  public readonly stepIndex: number;
  public readonly targetActionId: string;
  public readonly actionType: string;
  public readonly reversibilityClass: ReversibilityClass;
  public readonly handlerType: "inverse" | "compensation" | "human_escalation";
  public readonly idempotencyKey: string;

  constructor(data: CompensationStepData) {
    this.stepIndex = data.stepIndex;
    this.targetActionId = data.targetActionId;
    this.actionType = data.actionType;
    this.reversibilityClass = data.reversibilityClass;
    this.handlerType = data.handlerType;
    this.idempotencyKey = data.idempotencyKey;
  }

  public toJSON(): Record<string, unknown> {
    return {
      step_index: this.stepIndex,
      target_action_id: this.targetActionId,
      action_type: this.actionType,
      reversibility_class: reversibilityClassName(this.reversibilityClass),
      handler_type: this.handlerType,
      idempotency_key: this.idempotencyKey,
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }

  public static fromDict(data: Record<string, unknown>): CompensationStep {
    return new CompensationStep({
      stepIndex: Number(data.step_index ?? data.stepIndex ?? 0),
      targetActionId: String(data.target_action_id || data.targetActionId || ""),
      actionType: String(data.action_type || data.actionType || ""),
      reversibilityClass: parseReversibilityClass(String(data.reversibility_class || data.reversibilityClass || "IRREVERSIBLE")),
      handlerType: (data.handler_type || data.handlerType || "human_escalation") as any,
      idempotencyKey: String(data.idempotency_key || data.idempotencyKey || ""),
    });
  }
}

export interface CompensationPlanData {
  planId: string;
  compositeClass: ReversibilityClass;
  steps: CompensationStep[];
  createdAtNs?: bigint | number;
}

export class CompensationPlan {
  public readonly planId: string;
  public readonly compositeClass: ReversibilityClass;
  public readonly steps: CompensationStep[];
  public readonly createdAtNs: bigint;

  constructor(data: CompensationPlanData) {
    this.planId = data.planId;
    this.compositeClass = data.compositeClass;
    this.steps = [...data.steps];
    this.createdAtNs =
      data.createdAtNs !== undefined
        ? typeof data.createdAtNs === "bigint"
          ? data.createdAtNs
          : BigInt(data.createdAtNs)
        : currentEpochNs();
  }

  public toJSON(): Record<string, unknown> {
    return {
      plan_id: this.planId,
      composite_class: reversibilityClassName(this.compositeClass),
      steps: this.steps.map((s) => s.toJSON()),
      created_at_ns: Number(this.createdAtNs),
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }

  public static fromDict(data: Record<string, unknown>): CompensationPlan {
    const rawSteps = (data.steps as Record<string, unknown>[]) || [];
    return new CompensationPlan({
      planId: String(data.plan_id || data.planId || ""),
      compositeClass: parseReversibilityClass(String(data.composite_class || data.compositeClass || "IRREVERSIBLE")),
      steps: rawSteps.map(CompensationStep.fromDict),
      createdAtNs: BigInt(String(data.created_at_ns ?? data.createdAtNs ?? 0)),
    });
  }
}

export interface CompensationResultData {
  planId: string;
  success: boolean;
  stepsExecuted: number;
  stepsFailed: number;
  escalationsResolved: number;
  errors?: string[];
  executedAtNs?: bigint | number;
}

export class CompensationResult {
  public readonly planId: string;
  public readonly success: boolean;
  public readonly stepsExecuted: number;
  public readonly stepsFailed: number;
  public readonly escalationsResolved: number;
  public readonly errors: string[];
  public readonly executedAtNs: bigint;

  constructor(data: CompensationResultData) {
    this.planId = data.planId;
    this.success = data.success;
    this.stepsExecuted = data.stepsExecuted;
    this.stepsFailed = data.stepsFailed;
    this.escalationsResolved = data.escalationsResolved;
    this.errors = [...(data.errors || [])];
    this.executedAtNs =
      data.executedAtNs !== undefined
        ? typeof data.executedAtNs === "bigint"
          ? data.executedAtNs
          : BigInt(data.executedAtNs)
        : currentEpochNs();
  }

  public toJSON(): Record<string, unknown> {
    return {
      plan_id: this.planId,
      success: this.success,
      steps_executed: this.stepsExecuted,
      steps_failed: this.stepsFailed,
      escalations_resolved: this.escalationsResolved,
      errors: [...this.errors],
      executed_at_ns: Number(this.executedAtNs),
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }
}

export interface HumanEscalationReceiptData {
  escalationId: string;
  actionId: string;
  actionType: string;
  tenantId: string;
  principal: string;
  reason: string;
  status: "PENDING" | "RESOLVED" | "REJECTED";
  escalationHash: string;
  createdAtNs?: bigint | number;
}

export class HumanEscalationReceipt {
  public readonly escalationId: string;
  public readonly actionId: string;
  public readonly actionType: string;
  public readonly tenantId: string;
  public readonly principal: string;
  public readonly reason: string;
  public status: "PENDING" | "RESOLVED" | "REJECTED";
  public readonly escalationHash: string;
  public readonly createdAtNs: bigint;

  constructor(data: HumanEscalationReceiptData) {
    this.escalationId = data.escalationId;
    this.actionId = data.actionId;
    this.actionType = data.actionType;
    this.tenantId = data.tenantId;
    this.principal = data.principal;
    this.reason = data.reason;
    this.status = data.status;
    this.escalationHash = data.escalationHash;
    this.createdAtNs =
      data.createdAtNs !== undefined
        ? typeof data.createdAtNs === "bigint"
          ? data.createdAtNs
          : BigInt(data.createdAtNs)
        : currentEpochNs();
  }

  public static computeHash(params: {
    escalationId: string;
    actionId: string;
    actionType: string;
    tenantId: string;
    principal: string;
    reason: string;
  }): string {
    const seed = `sw:esc:v1:${params.escalationId}:${params.actionId}:${params.actionType}:${params.tenantId}:${params.principal}:${params.reason}`;
    return sha256Hex(seed);
  }

  public toJSON(): Record<string, unknown> {
    return {
      escalation_id: this.escalationId,
      action_id: this.actionId,
      action_type: this.actionType,
      tenant_id: this.tenantId,
      principal: this.principal,
      reason: this.reason,
      status: this.status,
      escalation_hash: this.escalationHash,
      created_at_ns: Number(this.createdAtNs),
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }

  public static fromDict(data: Record<string, unknown>): HumanEscalationReceipt {
    return new HumanEscalationReceipt({
      escalationId: String(data.escalation_id || data.escalationId || ""),
      actionId: String(data.action_id || data.actionId || ""),
      actionType: String(data.action_type || data.actionType || ""),
      tenantId: String(data.tenant_id || data.tenantId || ""),
      principal: String(data.principal || data.principal || ""),
      reason: String(data.reason || ""),
      status: (data.status as any) || "PENDING",
      escalationHash: String(data.escalation_hash || data.escalationHash || ""),
      createdAtNs: BigInt(String(data.created_at_ns ?? data.createdAtNs ?? 0)),
    });
  }
}

export interface EscalationResolutionData {
  resolutionId: string;
  escalationId: string;
  authorizerPrincipal: string;
  decision: "APPROVED_BYPASS" | "MANUAL_REMEDIATED" | "ABORTED";
  remediationNotes: string;
  resolvedAtNs?: bigint | number;
}

export class EscalationResolution {
  public readonly resolutionId: string;
  public readonly escalationId: string;
  public readonly authorizerPrincipal: string;
  public readonly decision: "APPROVED_BYPASS" | "MANUAL_REMEDIATED" | "ABORTED";
  public readonly remediationNotes: string;
  public readonly resolvedAtNs: bigint;

  constructor(data: EscalationResolutionData) {
    this.resolutionId = data.resolutionId;
    this.escalationId = data.escalationId;
    this.authorizerPrincipal = data.authorizerPrincipal;
    this.decision = data.decision;
    this.remediationNotes = data.remediationNotes;
    this.resolvedAtNs =
      data.resolvedAtNs !== undefined
        ? typeof data.resolvedAtNs === "bigint"
          ? data.resolvedAtNs
          : BigInt(data.resolvedAtNs)
        : currentEpochNs();
  }

  public toJSON(): Record<string, unknown> {
    return {
      resolution_id: this.resolutionId,
      escalation_id: this.escalationId,
      authorizer_principal: this.authorizerPrincipal,
      decision: this.decision,
      remediation_notes: this.remediationNotes,
      resolved_at_ns: Number(this.resolvedAtNs),
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }
}

export interface CompensationStatusData {
  totalRegisteredContracts: number;
  totalPlansGenerated: number;
  totalCompensationsExecuted: number;
  totalEscalationsPending: number;
  totalEscalationsResolved: number;
}

export class CompensationStatus {
  public readonly totalRegisteredContracts: number;
  public readonly totalPlansGenerated: number;
  public readonly totalCompensationsExecuted: number;
  public readonly totalEscalationsPending: number;
  public readonly totalEscalationsResolved: number;

  constructor(data: CompensationStatusData) {
    this.totalRegisteredContracts = data.totalRegisteredContracts;
    this.totalPlansGenerated = data.totalPlansGenerated;
    this.totalCompensationsExecuted = data.totalCompensationsExecuted;
    this.totalEscalationsPending = data.totalEscalationsPending;
    this.totalEscalationsResolved = data.totalEscalationsResolved;
  }

  public toJSON(): Record<string, unknown> {
    return {
      total_registered_contracts: this.totalRegisteredContracts,
      total_plans_generated: this.totalPlansGenerated,
      total_compensations_executed: this.totalCompensationsExecuted,
      total_escalations_pending: this.totalEscalationsPending,
      total_escalations_resolved: this.totalEscalationsResolved,
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }
}

// ---------------------------------------------------------------------------
// Core Deep Module: CompensationEngine
// ---------------------------------------------------------------------------

export class CompensationEngine {
  private readonly _contracts: Map<string, ActionContract> = new Map();
  private readonly _actionsById: Map<string, ActionExecution> = new Map();
  private readonly _escalations: Map<string, HumanEscalationReceipt> = new Map();
  private readonly _resolutions: Map<string, EscalationResolution> = new Map();

  private _totalPlansGenerated = 0;
  private _totalCompensationsExecuted = 0;

  public registerContract(contract: ActionContract): void {
    this._contracts.set(contract.actionType, contract);
  }

  public getContract(actionType: string): ActionContract | undefined {
    return this._contracts.get(actionType);
  }

  public classify(actions: ActionExecution[]): CompositeClassification {
    let revCount = 0;
    let compCount = 0;
    let irrCount = 0;
    const irreversibleIds: string[] = [];
    const classes: ReversibilityClass[] = [];

    for (const a of actions) {
      this._actionsById.set(a.actionId, a);
      const contract = this._contracts.get(a.actionType);
      const clsVal = contract ? contract.reversibilityClass : ReversibilityClass.IRREVERSIBLE;

      classes.push(clsVal);
      if (clsVal === ReversibilityClass.REVERSIBLE) {
        revCount++;
      } else if (clsVal === ReversibilityClass.COMPENSATABLE) {
        compCount++;
      } else {
        irrCount++;
        irreversibleIds.push(a.actionId);
      }
    }

    const composite = reversibilityJoin(classes);

    return new CompositeClassification({
      reversibilityClass: composite,
      totalActions: actions.length,
      reversibleCount: revCount,
      compensatableCount: compCount,
      irreversibleCount: irrCount,
      irreversibleActionIds: irreversibleIds,
    });
  }

  public buildCompensationPlan(actions: ActionExecution[]): CompensationPlan {
    const classification = this.classify(actions);
    const planId = crypto.randomUUID();
    const steps: CompensationStep[] = [];

    const reversedActions = [...actions].reverse();

    for (let idx = 0; idx < reversedActions.length; idx++) {
      const a = reversedActions[idx];
      const contract = this._contracts.get(a.actionType);
      const rClass = contract ? contract.reversibilityClass : ReversibilityClass.IRREVERSIBLE;

      let hType: "inverse" | "compensation" | "human_escalation";
      if (rClass === ReversibilityClass.REVERSIBLE) {
        hType = "inverse";
      } else if (rClass === ReversibilityClass.COMPENSATABLE) {
        hType = "compensation";
      } else {
        hType = "human_escalation";
      }

      const stepKey = sha256Hex(`sw:comp:${planId}:${a.actionId}:${idx}`);
      steps.push(new CompensationStep({
        stepIndex: idx,
        targetActionId: a.actionId,
        actionType: a.actionType,
        reversibilityClass: rClass,
        handlerType: hType,
        idempotencyKey: stepKey,
      }));
    }

    this._totalPlansGenerated++;

    return new CompensationPlan({
      planId,
      compositeClass: classification.reversibilityClass,
      steps,
    });
  }

  public async executeCompensation(
    plan: CompensationPlan,
    allowAuthorizedEscalations = false
  ): Promise<CompensationResult> {
    if (plan.compositeClass === ReversibilityClass.IRREVERSIBLE && !allowAuthorizedEscalations) {
      throw new IrreversibleActionError(
        `Automated rollback prohibited for irreversible action in plan '${plan.planId}'`
      );
    }

    let executed = 0;
    let failed = 0;
    let escalationsResolved = 0;
    const errors: string[] = [];

    for (const step of plan.steps) {
      let action = this._actionsById.get(step.targetActionId);
      if (!action) {
        action = new ActionExecution({
          actionId: step.targetActionId,
          actionType: step.actionType,
          tenantId: "unknown",
          principal: "unknown",
        });
      }

      if (step.reversibilityClass === ReversibilityClass.IRREVERSIBLE) {
        let isAuthorized = false;
        for (const esc of this._escalations.values()) {
          if (esc.actionId === step.targetActionId && esc.status === "RESOLVED") {
            if (this._resolutions.has(esc.escalationId)) {
              isAuthorized = true;
              break;
            }
          }
        }

        if (!isAuthorized) {
          throw new IrreversibleActionError(
            `Action '${step.targetActionId}' is irreversible and lacks approved human escalation`
          );
        }
        escalationsResolved++;
        executed++;
        continue;
      }

      const contract = this._contracts.get(step.actionType);
      let success = true;

      if (step.reversibilityClass === ReversibilityClass.REVERSIBLE) {
        if (contract && contract.inverseHandler) {
          try {
            success = Boolean(await contract.inverseHandler(action));
          } catch (e: any) {
            success = false;
            errors.push(`Step ${step.stepIndex} inverse handler error: ${e.message}`);
          }
        }
      } else if (step.reversibilityClass === ReversibilityClass.COMPENSATABLE) {
        if (contract && contract.compensationHandler) {
          try {
            success = Boolean(await contract.compensationHandler(action));
          } catch (e: any) {
            success = false;
            errors.push(`Step ${step.stepIndex} compensation handler error: ${e.message}`);
          }
        }
      }

      if (success) {
        executed++;
      } else {
        failed++;
      }
    }

    this._totalCompensationsExecuted++;

    return new CompensationResult({
      planId: plan.planId,
      success: failed === 0,
      stepsExecuted: executed,
      stepsFailed: failed,
      escalationsResolved,
      errors,
    });
  }

  public escalate(action: ActionExecution, reason: string): HumanEscalationReceipt {
    this._actionsById.set(action.actionId, action);
    const escalationId = crypto.randomUUID();
    const digest = HumanEscalationReceipt.computeHash({
      escalationId,
      actionId: action.actionId,
      actionType: action.actionType,
      tenantId: action.tenantId,
      principal: action.principal,
      reason,
    });

    const receipt = new HumanEscalationReceipt({
      escalationId,
      actionId: action.actionId,
      actionType: action.actionType,
      tenantId: action.tenantId,
      principal: action.principal,
      reason,
      status: "PENDING",
      escalationHash: digest,
    });

    this._escalations.set(escalationId, receipt);
    return receipt;
  }

  public authorizeEscalation(
    escalationId: string,
    authorizerPrincipal: string,
    decision: "APPROVED_BYPASS" | "MANUAL_REMEDIATED" | "ABORTED" = "MANUAL_REMEDIATED",
    remediationNotes = ""
  ): EscalationResolution {
    const esc = this._escalations.get(escalationId);
    if (!esc) {
      throw new CompensationError(`Escalation '${escalationId}' not found`);
    }

    const cleanDecision = String(decision).toUpperCase().trim() as any;
    if (!["APPROVED_BYPASS", "MANUAL_REMEDIATED", "ABORTED"].includes(cleanDecision)) {
      throw new Error(`Invalid escalation decision: ${decision}`);
    }

    esc.status = "RESOLVED";
    const resolution = new EscalationResolution({
      resolutionId: crypto.randomUUID(),
      escalationId,
      authorizerPrincipal,
      decision: cleanDecision,
      remediationNotes,
    });
    this._resolutions.set(escalationId, resolution);
    return resolution;
  }

  public status(): CompensationStatus {
    let pending = 0;
    for (const e of this._escalations.values()) {
      if (e.status === "PENDING") pending++;
    }
    return new CompensationStatus({
      totalRegisteredContracts: this._contracts.size,
      totalPlansGenerated: this._totalPlansGenerated,
      totalCompensationsExecuted: this._totalCompensationsExecuted,
      totalEscalationsPending: pending,
      totalEscalationsResolved: this._resolutions.size,
    });
  }

  public reset(): void {
    this._contracts.clear();
    this._actionsById.clear();
    this._escalations.clear();
    this._resolutions.clear();
    this._totalPlansGenerated = 0;
    this._totalCompensationsExecuted = 0;
  }
}

// ---------------------------------------------------------------------------
// Global Singleton Instance
// ---------------------------------------------------------------------------

export const globalCompensationEngine = new CompensationEngine();
