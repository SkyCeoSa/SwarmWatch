/**
 * Fenced Effect Ledger & Monotonic Idempotency Barrier (Seam 52 / ADR 0063).
 * Enforces compare-and-swap (CAS) transitions with monotonic fencing epochs,
 * guaranteeing that external tool interventions and distributed side-effects
 * execute and commit at most once across retries, failovers, and partitions.
 */

import * as crypto from "node:crypto";
import { CanonicalCodec, CanonicalDomainTag } from "./canonical.ts";

export const ClaimStatus = {
  CLAIMED: "CLAIMED",
  AUTHORIZED: "AUTHORIZED",
  COMMITTED: "COMMITTED",
  ABORTED: "ABORTED",
  EXPIRED: "EXPIRED",
} as const;

export type ClaimStatusEnum = typeof ClaimStatus[keyof typeof ClaimStatus];

export interface EffectReceiptOptions {
  receiptId: string;
  effectKey: string;
  idempotencyKey: string;
  epoch: number;
  tenantId?: string;
  committedAt?: number;
  resultDigest: string;
  receiptHash?: string;
}

export class EffectReceipt {
  public readonly receiptId: string;
  public readonly effectKey: string;
  public readonly idempotencyKey: string;
  public readonly epoch: number;
  public readonly tenantId: string;
  public readonly committedAt: number;
  public readonly resultDigest: string;
  public readonly receiptHash: string;

  constructor(opts: EffectReceiptOptions) {
    this.receiptId = opts.receiptId;
    this.effectKey = opts.effectKey;
    this.idempotencyKey = opts.idempotencyKey;
    this.epoch = opts.epoch;
    this.tenantId = opts.tenantId || "default";
    this.committedAt = opts.committedAt !== undefined ? opts.committedAt : Date.now() / 1000.0;
    this.resultDigest = opts.resultDigest;
    this.receiptHash = opts.receiptHash || this.computeReceiptHash();
  }

  public computeReceiptHash(): string {
    const payload = {
      committed_at: Number(this.committedAt.toFixed(6)),
      effect_key: this.effectKey,
      epoch: this.epoch,
      idempotency_key: this.idempotencyKey,
      receipt_id: this.receiptId,
      result_digest: this.resultDigest,
      tenant_id: this.tenantId,
    };
    return CanonicalCodec.hash(payload, CanonicalDomainTag.EFFECT);
  }

  public toDict(): Record<string, unknown> {
    return {
      receipt_id: this.receiptId,
      effect_key: this.effectKey,
      idempotency_key: this.idempotencyKey,
      epoch: this.epoch,
      tenant_id: this.tenantId,
      committed_at: this.committedAt,
      result_digest: this.resultDigest,
      receipt_hash: this.receiptHash,
    };
  }

  public toJSON(): Record<string, unknown> {
    return this.toDict();
  }

  public static fromDict(data: Record<string, unknown>): EffectReceipt {
    return new EffectReceipt({
      receiptId: String(data.receipt_id),
      effectKey: String(data.effect_key),
      idempotencyKey: String(data.idempotency_key),
      epoch: Number(data.epoch),
      tenantId: data.tenant_id ? String(data.tenant_id) : "default",
      committedAt: data.committed_at !== undefined ? Number(data.committed_at) : undefined,
      resultDigest: String(data.result_digest),
      receiptHash: data.receipt_hash ? String(data.receipt_hash) : undefined,
    });
  }
}

export interface EffectRecordOptions {
  effectKey: string;
  claimId: string;
  idempotencyKey: string;
  epoch: number;
  tenantId?: string;
  action?: string;
  status?: ClaimStatusEnum;
  permitHash?: string | null;
  claimedAt?: number;
  expiresAt?: number;
  executionResult?: unknown;
  receipt?: EffectReceipt | null;
}

export class EffectRecord {
  public readonly effectKey: string;
  public readonly claimId: string;
  public readonly idempotencyKey: string;
  public readonly epoch: number;
  public readonly tenantId: string;
  public readonly action: string;
  public status: ClaimStatusEnum;
  public permitHash: string | null;
  public readonly claimedAt: number;
  public expiresAt: number;
  public executionResult: unknown;
  public receipt: EffectReceipt | null;

  constructor(opts: EffectRecordOptions) {
    this.effectKey = opts.effectKey;
    this.claimId = opts.claimId;
    this.idempotencyKey = opts.idempotencyKey;
    this.epoch = opts.epoch;
    this.tenantId = opts.tenantId || "default";
    this.action = opts.action || "external_tool";
    this.status = opts.status || ClaimStatus.CLAIMED;
    this.permitHash = opts.permitHash || null;
    const now = Date.now() / 1000.0;
    this.claimedAt = opts.claimedAt !== undefined ? opts.claimedAt : now;
    this.expiresAt = opts.expiresAt !== undefined ? opts.expiresAt : this.claimedAt + 5.0;
    this.executionResult = opts.executionResult !== undefined ? opts.executionResult : null;
    this.receipt = opts.receipt || null;
  }

  public isExpired(currentTime?: number): boolean {
    const now = currentTime !== undefined ? currentTime : Date.now() / 1000.0;
    return now > this.expiresAt;
  }

  public toDict(): Record<string, unknown> {
    return {
      effect_key: this.effectKey,
      claim_id: this.claimId,
      idempotency_key: this.idempotencyKey,
      epoch: this.epoch,
      tenant_id: this.tenantId,
      action: this.action,
      status: this.status,
      permit_hash: this.permitHash,
      claimed_at: this.claimedAt,
      expires_at: this.expiresAt,
      execution_result: this.executionResult,
      receipt: this.receipt ? this.receipt.toDict() : null,
    };
  }

  public toJSON(): Record<string, unknown> {
    return this.toDict();
  }

  public static fromDict(data: Record<string, unknown>): EffectRecord {
    const receipt = data.receipt ? EffectReceipt.fromDict(data.receipt as Record<string, unknown>) : null;
    return new EffectRecord({
      effectKey: String(data.effect_key),
      claimId: String(data.claim_id),
      idempotencyKey: String(data.idempotency_key),
      epoch: Number(data.epoch),
      tenantId: data.tenant_id ? String(data.tenant_id) : "default",
      action: data.action ? String(data.action) : "external_tool",
      status: (data.status as ClaimStatusEnum) || ClaimStatus.CLAIMED,
      permitHash: data.permit_hash ? String(data.permit_hash) : null,
      claimedAt: data.claimed_at !== undefined ? Number(data.claimed_at) : undefined,
      expiresAt: data.expires_at !== undefined ? Number(data.expires_at) : undefined,
      executionResult: data.execution_result,
      receipt,
    });
  }
}

export class ClaimResult {
  public readonly status: string;
  public readonly claimId: string | null;
  public readonly epoch: number;
  public readonly record: EffectRecord | null;
  public readonly reason: string | null;
  public readonly receipt: EffectReceipt | null;

  constructor(opts: {
    status: string;
    claimId?: string | null;
    epoch?: number;
    record?: EffectRecord | null;
    reason?: string | null;
    receipt?: EffectReceipt | null;
  }) {
    this.status = opts.status;
    this.claimId = opts.claimId || null;
    this.epoch = opts.epoch !== undefined ? opts.epoch : 1;
    this.record = opts.record || null;
    this.reason = opts.reason || null;
    this.receipt = opts.receipt || null;
  }

  public toDict(): Record<string, unknown> {
    return {
      status: this.status,
      claim_id: this.claimId,
      epoch: this.epoch,
      record: this.record ? this.record.toDict() : null,
      reason: this.reason,
      receipt: this.receipt ? this.receipt.toDict() : null,
    };
  }

  public toJSON(): Record<string, unknown> {
    return this.toDict();
  }
}

export class EffectLedger {
  private _currentEpoch: number;
  private _records = new Map<string, EffectRecord>();
  private _claims = new Map<string, EffectRecord>();
  private _committedHistory: EffectRecord[] = [];

  constructor(initialEpoch: number = 1) {
    this._currentEpoch = initialEpoch;
  }

  public get currentEpoch(): number {
    return this._currentEpoch;
  }

  public setCurrentEpoch(epoch: number): void {
    if (epoch >= this._currentEpoch) {
      this._currentEpoch = epoch;
    }
  }

  public tripEpoch(_reason: string = "safety_trip"): number {
    this._currentEpoch += 1;
    for (const record of this._claims.values()) {
      if (record.status === ClaimStatus.CLAIMED || record.status === ClaimStatus.AUTHORIZED) {
        if (record.epoch < this._currentEpoch) {
          record.status = ClaimStatus.ABORTED;
        }
      }
    }
    return this._currentEpoch;
  }

  public claim(
    effectKey: string,
    tenantId: string = "default",
    epoch: number = 1,
    idempotencyKey?: string | null,
    action: string = "external_tool",
    timeoutMs: number = 5000.0,
  ): ClaimResult {
    // 1. Monotonic fencing epoch check
    if (epoch < this._currentEpoch) {
      return new ClaimResult({
        status: "REJECTED",
        epoch: this._currentEpoch,
        reason: `Stale epoch ${epoch} < ${this._currentEpoch} (STALE_EPOCH)`,
      });
    }

    const effectiveIdempotencyKey = idempotencyKey || `idemp-${crypto.randomBytes(8).toString("hex")}`;

    // 2. Check if effectKey exists
    const existing = this._records.get(effectKey);
    if (existing) {
      // Case A: COMMITTED
      if (existing.status === ClaimStatus.COMMITTED) {
        if (existing.idempotencyKey === effectiveIdempotencyKey) {
          return new ClaimResult({
            status: "IDEMPOTENT_COMMITTED",
            claimId: existing.claimId,
            epoch: existing.epoch,
            record: existing,
            receipt: existing.receipt,
            reason: "Effect already committed under identical idempotency key",
          });
        } else {
          return new ClaimResult({
            status: "REJECTED",
            epoch: this._currentEpoch,
            reason: `Effect ${effectKey} already committed under different idempotency key`,
          });
        }
      }

      // Case B: In-flight
      if (existing.status === ClaimStatus.CLAIMED || existing.status === ClaimStatus.AUTHORIZED) {
        const now = Date.now() / 1000.0;
        if (existing.isExpired(now)) {
          existing.status = ClaimStatus.EXPIRED;
        } else {
          if (existing.idempotencyKey === effectiveIdempotencyKey) {
            return new ClaimResult({
              status: "ACCEPTED",
              claimId: existing.claimId,
              epoch: existing.epoch,
              record: existing,
              reason: "Re-attached to active in-flight claim",
            });
          } else {
            return new ClaimResult({
              status: "REJECTED",
              epoch: this._currentEpoch,
              reason: `Effect ${effectKey} actively locked by claim ${existing.claimId}`,
            });
          }
        }
      }
    }

    // 3. Create fresh claim
    const claimId = `clm-${crypto.randomBytes(6).toString("hex")}`;
    const now = Date.now() / 1000.0;
    const expiresAt = now + Math.max(100.0, timeoutMs) / 1000.0;

    const record = new EffectRecord({
      effectKey,
      claimId,
      idempotencyKey: effectiveIdempotencyKey,
      epoch,
      tenantId,
      action,
      status: ClaimStatus.CLAIMED,
      claimedAt: now,
      expiresAt,
    });

    this._records.set(effectKey, record);
    this._claims.set(claimId, record);

    return new ClaimResult({
      status: "ACCEPTED",
      claimId,
      epoch,
      record,
      reason: "Effect claim accepted",
    });
  }

  public authorize(claimId: string, permitHash: string): { success: boolean; message: string } {
    const record = this._claims.get(claimId);
    if (!record) {
      return { success: false, message: `Claim ${claimId} not found` };
    }

    if (record.epoch < this._currentEpoch) {
      record.status = ClaimStatus.ABORTED;
      return {
        success: false,
        message: `Claim invalidated by epoch transition (${record.epoch} < ${this._currentEpoch})`,
      };
    }

    const now = Date.now() / 1000.0;
    if (record.isExpired(now)) {
      record.status = ClaimStatus.EXPIRED;
      return { success: false, message: `Claim ${claimId} expired before authorization` };
    }

    if (record.status !== ClaimStatus.CLAIMED) {
      return { success: false, message: `Cannot authorize claim in status ${record.status}` };
    }

    if (!permitHash || permitHash.length !== 64) {
      return { success: false, message: "Invalid permit hash format (must be 64-char hex SHA-256)" };
    }

    record.permitHash = permitHash;
    record.status = ClaimStatus.AUTHORIZED;
    return { success: true, message: "Claim authorized successfully" };
  }

  public complete(
    claimId: string,
    executionResult?: unknown,
  ): { success: boolean; receipt: EffectReceipt | null; message: string } {
    const record = this._claims.get(claimId);
    if (!record) {
      return { success: false, receipt: null, message: `Claim ${claimId} not found` };
    }

    if (record.status === ClaimStatus.COMMITTED && record.receipt) {
      return { success: true, receipt: record.receipt, message: "Effect already committed (idempotent)" };
    }

    if (record.epoch < this._currentEpoch) {
      record.status = ClaimStatus.ABORTED;
      return {
        success: false,
        receipt: null,
        message: `Claim invalidated by epoch transition (${record.epoch} < ${this._currentEpoch})`,
      };
    }

    const now = Date.now() / 1000.0;
    if (record.isExpired(now)) {
      record.status = ClaimStatus.EXPIRED;
      return { success: false, receipt: null, message: `Claim ${claimId} expired before completion` };
    }

    if (record.status !== ClaimStatus.CLAIMED && record.status !== ClaimStatus.AUTHORIZED) {
      return { success: false, receipt: null, message: `Cannot complete claim in status ${record.status}` };
    }

    let resultDigest: string;
    try {
      resultDigest = CanonicalCodec.hash(executionResult !== undefined && executionResult !== null ? executionResult : {});
    } catch {
      resultDigest = CanonicalCodec.hash({ raw: String(executionResult) });
    }

    const receiptId = `rcpt-eff-${crypto.randomBytes(6).toString("hex")}`;
    const receipt = new EffectReceipt({
      receiptId,
      effectKey: record.effectKey,
      idempotencyKey: record.idempotencyKey,
      epoch: record.epoch,
      tenantId: record.tenantId,
      committedAt: now,
      resultDigest,
    });

    record.status = ClaimStatus.COMMITTED;
    record.executionResult = executionResult;
    record.receipt = receipt;
    this._committedHistory.push(record);

    return { success: true, receipt, message: "Effect completed and committed successfully" };
  }

  public abort(claimId: string, reason: string = "canceled"): { success: boolean; message: string } {
    const record = this._claims.get(claimId);
    if (!record) {
      return { success: false, message: `Claim ${claimId} not found` };
    }
    if (record.status === ClaimStatus.COMMITTED) {
      return { success: false, message: "Cannot abort already committed effect" };
    }
    record.status = ClaimStatus.ABORTED;
    return { success: true, message: `Claim ${claimId} aborted: ${reason}` };
  }

  public reconcile(sinceEpoch: number = 1, tenantId?: string): Record<string, unknown>[] {
    const results: Record<string, unknown>[] = [];
    for (const record of this._committedHistory) {
      if (record.epoch >= sinceEpoch) {
        if (!tenantId || record.tenantId === tenantId) {
          results.push(record.toDict());
        }
      }
    }
    return results;
  }

  public status(tenantId: string = "default"): Record<string, unknown> {
    let activeClaims = 0;
    for (const r of this._claims.values()) {
      if (r.status === ClaimStatus.CLAIMED || r.status === ClaimStatus.AUTHORIZED) {
        if (tenantId === "default" || r.tenantId === tenantId) {
          activeClaims++;
        }
      }
    }

    let committedCount = 0;
    for (const r of this._committedHistory) {
      if (tenantId === "default" || r.tenantId === tenantId) {
        committedCount++;
      }
    }

    return {
      current_epoch: this._currentEpoch,
      total_records: this._records.size,
      active_claims_count: activeClaims,
      committed_effects_count: committedCount,
      tenant_id: tenantId,
    };
  }

  public reset(): void {
    this._currentEpoch = 1;
    this._records.clear();
    this._claims.clear();
    this._committedHistory = [];
  }
}

export const globalEffectLedger = new EffectLedger();
