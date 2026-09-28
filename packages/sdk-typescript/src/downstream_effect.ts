/**
 * Downstream Effect Protocol & 2PC Outcome-Unknown Reconciliation (Seam 62 / ADR 0073).
 * Enforces explicit two-phase commit semantics across external downstream network boundaries,
 * eliminating false exactly-once claims by failing closed into OUTCOME_UNKNOWN during timeouts
 * and requiring explicit downstream-enforced idempotency or active reconciliation probes.
 */

import * as crypto from "node:crypto";
import { CanonicalCodec, CanonicalDomainTag } from "./canonical.ts";

export const EffectPhase = {
  PREPARED: "PREPARED",
  DISPATCHED: "DISPATCHED",
  COMMITTED: "COMMITTED",
  OUTCOME_UNKNOWN: "OUTCOME_UNKNOWN",
  ABORTED: "ABORTED",
} as const;

export type EffectPhaseType = typeof EffectPhase[keyof typeof EffectPhase];

export const DownstreamIdempotencyContract = {
  DOWNSTREAM_FENCED: "DOWNSTREAM_FENCED",
  UNCOORDINATED: "UNCOORDINATED",
} as const;

export type DownstreamIdempotencyContractType = typeof DownstreamIdempotencyContract[keyof typeof DownstreamIdempotencyContract];

export class UncoordinatedRetryBlockedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UncoordinatedRetryBlockedError";
  }
}

export class StaleEpochError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StaleEpochError";
  }
}

export class EffectNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EffectNotFoundError";
  }
}

export class DownstreamReceipt {
  public readonly downstreamConfirmationId: string;
  public readonly resultDigest: string;
  public readonly latencyMs: number;
  public readonly receivedAt: number;
  public readonly receiptHash: string;

  constructor(
    downstreamConfirmationId: string,
    resultDigest: string,
    latencyMs: number = 0.0,
    receivedAt?: number,
    receiptHash?: string
  ) {
    this.downstreamConfirmationId = downstreamConfirmationId;
    this.resultDigest = resultDigest;
    this.latencyMs = latencyMs;
    this.receivedAt = receivedAt !== undefined ? receivedAt : Date.now() / 1000;
    this.receiptHash = receiptHash || this.computeReceiptHash();
  }

  public computeReceiptHash(): string {
    const payload = {
      downstream_confirmation_id: this.downstreamConfirmationId,
      latency_ms: Math.round(this.latencyMs * 1000) / 1000,
      received_at: Math.round(this.receivedAt * 1000000) / 1000000,
      result_digest: this.resultDigest,
    };
    return CanonicalCodec.hash(payload, CanonicalDomainTag.EFFECT);
  }

  public toJSON(): Record<string, unknown> {
    return {
      downstream_confirmation_id: this.downstreamConfirmationId,
      result_digest: this.resultDigest,
      latency_ms: this.latencyMs,
      received_at: this.receivedAt,
      receipt_hash: this.receiptHash,
    };
  }
}

export interface PrepareOptions {
  tenantId: string;
  effectKey: string;
  targetUri: string;
  payload: Record<string, unknown>;
  epoch?: number;
  contract?: DownstreamIdempotencyContractType;
  idempotencyKey?: string;
}

export class EffectPreparation {
  public readonly effectId: string;
  public readonly tenantId: string;
  public readonly effectKey: string;
  public readonly targetUri: string;
  public readonly payload: Record<string, unknown>;
  public readonly payloadDigest: string;
  public readonly idempotencyKey: string;
  public readonly epoch: number;
  public readonly contract: DownstreamIdempotencyContractType;
  public status: EffectPhaseType;
  public preparedAt: number;
  public dispatchedAt?: number;
  public committedAt?: number;
  public reconciled: boolean;
  public reconciledAt?: number;
  public receipt?: DownstreamReceipt;
  public errorMessage?: string;
  public retryCount: number;
  public probeAttempts: number;

  constructor(
    effectId: string,
    tenantId: string,
    effectKey: string,
    targetUri: string,
    payload: Record<string, unknown>,
    payloadDigest: string,
    idempotencyKey: string,
    epoch: number,
    contract: DownstreamIdempotencyContractType = DownstreamIdempotencyContract.DOWNSTREAM_FENCED,
    status: EffectPhaseType = EffectPhase.PREPARED
  ) {
    this.effectId = effectId;
    this.tenantId = tenantId;
    this.effectKey = effectKey;
    this.targetUri = targetUri;
    this.payload = payload;
    this.payloadDigest = payloadDigest;
    this.idempotencyKey = idempotencyKey;
    this.epoch = epoch;
    this.contract = contract;
    this.status = status;
    this.preparedAt = Date.now() / 1000;
    this.reconciled = false;
    this.retryCount = 0;
    this.probeAttempts = 0;
  }

  public toJSON(): Record<string, unknown> {
    return {
      effect_id: this.effectId,
      tenant_id: this.tenantId,
      effect_key: this.effectKey,
      target_uri: this.targetUri,
      payload: this.payload,
      payload_digest: this.payloadDigest,
      idempotency_key: this.idempotencyKey,
      epoch: this.epoch,
      contract: this.contract,
      status: this.status,
      prepared_at: this.preparedAt,
      dispatched_at: this.dispatchedAt,
      committed_at: this.committedAt,
      reconciled: this.reconciled,
      reconciled_at: this.reconciledAt,
      receipt: this.receipt ? this.receipt.toJSON() : null,
      error_message: this.errorMessage,
      retry_count: this.retryCount,
      probe_attempts: this.probeAttempts,
      is_outcome_unknown: this.status === EffectPhase.OUTCOME_UNKNOWN,
    };
  }
}

export class DispatchResult {
  public readonly effectId: string;
  public readonly status: EffectPhaseType;
  public readonly isOutcomeUnknown: boolean;
  public readonly dispatchOutput?: unknown;
  public readonly errorMessage?: string;

  constructor(
    effectId: string,
    status: EffectPhaseType,
    isOutcomeUnknown: boolean,
    dispatchOutput?: unknown,
    errorMessage?: string
  ) {
    this.effectId = effectId;
    this.status = status;
    this.isOutcomeUnknown = isOutcomeUnknown;
    this.dispatchOutput = dispatchOutput;
    this.errorMessage = errorMessage;
  }

  public toJSON(): Record<string, unknown> {
    return {
      effect_id: this.effectId,
      status: this.status,
      is_outcome_unknown: this.isOutcomeUnknown,
      dispatch_output: this.dispatchOutput,
      error_message: this.errorMessage,
    };
  }
}

export class AcknowledgeResult {
  public readonly effectId: string;
  public readonly status: EffectPhaseType;
  public readonly isOutcomeUnknown: boolean;
  public readonly receipt?: DownstreamReceipt;

  constructor(
    effectId: string,
    status: EffectPhaseType,
    isOutcomeUnknown: boolean,
    receipt?: DownstreamReceipt
  ) {
    this.effectId = effectId;
    this.status = status;
    this.isOutcomeUnknown = isOutcomeUnknown;
    this.receipt = receipt;
  }

  public toJSON(): Record<string, unknown> {
    return {
      effect_id: this.effectId,
      status: this.status,
      is_outcome_unknown: this.isOutcomeUnknown,
      receipt: this.receipt ? this.receipt.toJSON() : null,
    };
  }
}

export class ReconciliationResult {
  public readonly effectId: string;
  public readonly status: EffectPhaseType;
  public readonly reconciled: boolean;
  public readonly isOutcomeUnknown: boolean;
  public readonly downstreamConfirmationId?: string;
  public readonly probeAttempts: number;
  public readonly errorMessage?: string;

  constructor(
    effectId: string,
    status: EffectPhaseType,
    reconciled: boolean,
    isOutcomeUnknown: boolean,
    downstreamConfirmationId?: string,
    probeAttempts: number = 1,
    errorMessage?: string
  ) {
    this.effectId = effectId;
    this.status = status;
    this.reconciled = reconciled;
    this.isOutcomeUnknown = isOutcomeUnknown;
    this.downstreamConfirmationId = downstreamConfirmationId;
    this.probeAttempts = probeAttempts;
    this.errorMessage = errorMessage;
  }

  public toJSON(): Record<string, unknown> {
    return {
      effect_id: this.effectId,
      status: this.status,
      reconciled: this.reconciled,
      is_outcome_unknown: this.isOutcomeUnknown,
      downstream_confirmation_id: this.downstreamConfirmationId,
      probe_attempts: this.probeAttempts,
      error_message: this.errorMessage,
    };
  }
}

export class DownstreamEffectBoundary {
  private readonly _effects: Map<string, EffectPreparation> = new Map();
  private readonly _tenantFences: Map<string, number> = new Map();

  public setTenantEpochFence(tenantId: string, currentEpoch: number): void {
    const existing = this._tenantFences.get(tenantId) || 0;
    if (currentEpoch > existing) {
      this._tenantFences.set(tenantId, currentEpoch);
    }
  }

  public prepare(options: PrepareOptions): EffectPreparation {
    const epoch = options.epoch ?? 1;
    const contract = options.contract ?? DownstreamIdempotencyContract.DOWNSTREAM_FENCED;
    const fence = this._tenantFences.get(options.tenantId) || 0;
    if (epoch < fence) {
      throw new StaleEpochError(`Provided epoch ${epoch} is stale compared to tenant fence ${fence}`);
    }

    const effectId = `eff_${crypto.randomBytes(12).toString("hex")}`;
    let idempotencyKey = options.idempotencyKey;
    if (!idempotencyKey) {
      const raw = `${options.tenantId}:${options.effectKey}:${epoch}:${crypto.randomBytes(8).toString("hex")}`;
      idempotencyKey = `idem_${crypto.createHash("sha256").update(raw).digest("hex").slice(0, 24)}`;
    }

    const payloadDigest = CanonicalCodec.hash(options.payload, CanonicalDomainTag.EFFECT);

    const prep = new EffectPreparation(
      effectId,
      options.tenantId,
      options.effectKey,
      options.targetUri,
      options.payload,
      payloadDigest,
      idempotencyKey,
      epoch,
      contract,
      EffectPhase.PREPARED
    );

    this._effects.set(effectId, prep);
    return prep;
  }

  public dispatch(
    effectId: string,
    dispatchFn: (effect: EffectPreparation) => unknown
  ): DispatchResult {
    const effect = this._effects.get(effectId);
    if (!effect) {
      throw new EffectNotFoundError(`Effect ${effectId} not found`);
    }

    if (effect.status === EffectPhase.OUTCOME_UNKNOWN) {
      if (effect.contract === DownstreamIdempotencyContract.UNCOORDINATED) {
        throw new UncoordinatedRetryBlockedError(
          `Effect ${effectId} is in OUTCOME_UNKNOWN and downstream service is UNCOORDINATED.`
        );
      }
      effect.retryCount += 1;
    }

    const fence = this._tenantFences.get(effect.tenantId) || 0;
    if (effect.epoch < fence) {
      throw new StaleEpochError(`Effect epoch ${effect.epoch} is stale compared to tenant fence ${fence}`);
    }

    try {
      const output = dispatchFn(effect);
      effect.status = EffectPhase.DISPATCHED;
      effect.dispatchedAt = Date.now() / 1000;
      effect.errorMessage = undefined;
      return new DispatchResult(effectId, EffectPhase.DISPATCHED, false, output);
    } catch (err: unknown) {
      const errorMsg = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
      effect.status = EffectPhase.OUTCOME_UNKNOWN;
      effect.errorMessage = errorMsg;
      return new DispatchResult(effectId, EffectPhase.OUTCOME_UNKNOWN, true, undefined, errorMsg);
    }
  }

  public acknowledge(
    effectId: string,
    receipt: DownstreamReceipt,
    epoch?: number
  ): AcknowledgeResult {
    const effect = this._effects.get(effectId);
    if (!effect) {
      throw new EffectNotFoundError(`Effect ${effectId} not found`);
    }

    const checkEpoch = epoch !== undefined ? epoch : effect.epoch;
    const fence = this._tenantFences.get(effect.tenantId) || 0;
    if (checkEpoch < fence) {
      throw new StaleEpochError(`Acknowledge epoch ${checkEpoch} is stale compared to tenant fence ${fence}`);
    }

    effect.status = EffectPhase.COMMITTED;
    effect.receipt = receipt;
    effect.committedAt = Date.now() / 1000;
    effect.errorMessage = undefined;

    return new AcknowledgeResult(effectId, EffectPhase.COMMITTED, false, receipt);
  }

  public reconcile(
    effectId: string,
    queryFn: (effectId: string, idempotencyKey: string) => Record<string, unknown>
  ): ReconciliationResult {
    const effect = this._effects.get(effectId);
    if (!effect) {
      throw new EffectNotFoundError(`Effect ${effectId} not found`);
    }

    try {
      const probeData = queryFn(effectId, effect.idempotencyKey);
      effect.probeAttempts += 1;
      const found = Boolean(probeData.found);
      if (found) {
        effect.status = EffectPhase.COMMITTED;
        effect.reconciled = true;
        effect.reconciledAt = Date.now() / 1000;
        const downstreamId = String(probeData.downstreamId || probeData.downstream_id || `recon_${effectId}`);
        const digest = String(probeData.digest || effect.payloadDigest);
        effect.receipt = new DownstreamReceipt(downstreamId, digest);
        effect.committedAt = effect.reconciledAt;
        return new ReconciliationResult(
          effectId,
          EffectPhase.COMMITTED,
          true,
          false,
          downstreamId,
          effect.probeAttempts
        );
      } else {
        effect.status = EffectPhase.ABORTED;
        effect.reconciled = true;
        effect.reconciledAt = Date.now() / 1000;
        return new ReconciliationResult(
          effectId,
          EffectPhase.ABORTED,
          true,
          false,
          undefined,
          effect.probeAttempts
        );
      }
    } catch (err: unknown) {
      effect.probeAttempts += 1;
      const errStr = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
      return new ReconciliationResult(
        effectId,
        EffectPhase.OUTCOME_UNKNOWN,
        false,
        true,
        undefined,
        effect.probeAttempts,
        errStr
      );
    }
  }

  public getEffect(effectId: string): EffectPreparation | undefined {
    return this._effects.get(effectId);
  }

  public getStatus(): Record<string, unknown> {
    const values = Array.from(this._effects.values());
    const total = values.length;
    const outcomeUnknown = values.filter((e) => e.status === EffectPhase.OUTCOME_UNKNOWN).length;
    const committed = values.filter((e) => e.status === EffectPhase.COMMITTED).length;
    const dispatched = values.filter((e) => e.status === EffectPhase.DISPATCHED).length;
    const aborted = values.filter((e) => e.status === EffectPhase.ABORTED).length;
    const prepared = values.filter((e) => e.status === EffectPhase.PREPARED).length;

    const fencesObj: Record<string, number> = {};
    for (const [k, v] of this._tenantFences.entries()) {
      fencesObj[k] = v;
    }

    return {
      total_effects: total,
      prepared_count: prepared,
      dispatched_count: dispatched,
      committed_count: committed,
      outcome_unknown_count: outcomeUnknown,
      aborted_count: aborted,
      epoch_fences: fencesObj,
    };
  }
}

export const globalDownstreamEffectBoundary = new DownstreamEffectBoundary();
