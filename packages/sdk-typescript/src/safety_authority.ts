/**
 * Unified Safety Authority & Linearizable Execution Permit Engine (Seam 51 / ADR 0062).
 * Enforces single linearizable decision plane across distributed agent workers,
 * issuing unexpired, tenant-scoped execution permits (SafetyPermit) with monotonic fencing epochs.
 */

import * as crypto from "node:crypto";
import { CanonicalCodec, CanonicalDomainTag } from "./canonical.ts";

export const PermitStatus = {
  ACCEPTED: "ACCEPTED",
  REJECTED: "REJECTED",
  INDETERMINATE: "INDETERMINATE",
} as const;

export type PermitStatusEnum = typeof PermitStatus[keyof typeof PermitStatus];

export const AuthorityState = {
  NOMINAL: "NOMINAL",
  TRIPPED: "TRIPPED",
  DRAINING: "DRAINING",
} as const;

export type AuthorityStateEnum = typeof AuthorityState[keyof typeof AuthorityState];

export interface SafetyPermitOptions {
  permitId: string;
  tenantId?: string;
  epoch?: number;
  action?: string;
  costEstimate?: number;
  tokensEstimate?: number;
  status?: PermitStatusEnum;
  rejectionReason?: string | null;
  issuedAt?: number;
  expiresAt?: number;
  permitHash?: string;
}

export class SafetyPermit {
  public readonly permitId: string;
  public readonly tenantId: string;
  public readonly epoch: number;
  public readonly action: string;
  public readonly costEstimate: number;
  public readonly tokensEstimate: number;
  public readonly status: PermitStatusEnum;
  public readonly rejectionReason: string | null;
  public readonly issuedAt: number;
  public readonly expiresAt: number;
  public permitHash: string;

  constructor(opts: SafetyPermitOptions) {
    this.permitId = opts.permitId;
    this.tenantId = opts.tenantId || "default";
    this.epoch = opts.epoch !== undefined ? opts.epoch : 1;
    this.action = opts.action || "agent_turn";
    this.costEstimate = opts.costEstimate !== undefined ? opts.costEstimate : 0.0;
    this.tokensEstimate = opts.tokensEstimate !== undefined ? opts.tokensEstimate : 0;
    this.status = opts.status || PermitStatus.ACCEPTED;
    this.rejectionReason = opts.rejectionReason || null;
    const now = Date.now() / 1000.0;
    this.issuedAt = opts.issuedAt !== undefined ? opts.issuedAt : now;
    this.expiresAt = opts.expiresAt !== undefined ? opts.expiresAt : this.issuedAt + 5.0;
    this.permitHash = opts.permitHash || this.computePermitHash();
  }

  public computePermitHash(): string {
    const payload = {
      action: this.action,
      cost_estimate: this.costEstimate,
      epoch: this.epoch,
      expires_at: Number(this.expiresAt.toFixed(6)),
      issued_at: Number(this.issuedAt.toFixed(6)),
      permit_id: this.permitId,
      status: this.status,
      tenant_id: this.tenantId,
      tokens_estimate: this.tokensEstimate,
    };
    return CanonicalCodec.hash(payload, CanonicalDomainTag.PERMIT);
  }

  public isValid(currentEpoch: number, currentTime?: number): boolean {
    if (this.status !== PermitStatus.ACCEPTED) {
      return false;
    }
    if (this.epoch !== currentEpoch) {
      return false;
    }
    const now = currentTime !== undefined ? currentTime : Date.now() / 1000.0;
    return now <= this.expiresAt;
  }

  public toDict(): Record<string, unknown> {
    return {
      permit_id: this.permitId,
      tenant_id: this.tenantId,
      epoch: this.epoch,
      action: this.action,
      cost_estimate: this.costEstimate,
      tokens_estimate: this.tokensEstimate,
      status: this.status,
      rejection_reason: this.rejectionReason,
      issued_at: this.issuedAt,
      expires_at: this.expiresAt,
      permit_hash: this.permitHash,
    };
  }

  public toJSON(): Record<string, unknown> {
    return this.toDict();
  }

  public static fromDict(data: Record<string, unknown>): SafetyPermit {
    return new SafetyPermit({
      permitId: String(data.permit_id),
      tenantId: data.tenant_id ? String(data.tenant_id) : "default",
      epoch: data.epoch !== undefined ? Number(data.epoch) : 1,
      action: data.action ? String(data.action) : "agent_turn",
      costEstimate: data.cost_estimate !== undefined ? Number(data.cost_estimate) : 0.0,
      tokensEstimate: data.tokens_estimate !== undefined ? Number(data.tokens_estimate) : 0,
      status: data.status as PermitStatusEnum,
      rejectionReason: data.rejection_reason ? String(data.rejection_reason) : null,
      issuedAt: data.issued_at !== undefined ? Number(data.issued_at) : undefined,
      expiresAt: data.expires_at !== undefined ? Number(data.expires_at) : undefined,
      permitHash: data.permit_hash ? String(data.permit_hash) : undefined,
    });
  }
}

export class TenantQuota {
  public readonly tenantId: string;
  public maxCost: number;
  public spentCost: number = 0.0;
  public reservedCost: number = 0.0;
  public maxTokens: number;
  public spentTokens: number = 0;
  public reservedTokens: number = 0;
  public maxTurns: number;
  public spentTurns: number = 0;
  public state: AuthorityStateEnum = AuthorityState.NOMINAL;
  public tripReason: string | null = null;

  constructor(
    tenantId: string,
    maxCost: number = 100.0,
    maxTokens: number = 1_000_000,
    maxTurns: number = 1000,
  ) {
    this.tenantId = tenantId;
    this.maxCost = maxCost;
    this.maxTokens = maxTokens;
    this.maxTurns = maxTurns;
  }

  public availableCost(): number {
    return Math.max(0.0, this.maxCost - (this.spentCost + this.reservedCost));
  }

  public availableTokens(): number {
    return Math.max(0, this.maxTokens - (this.spentTokens + this.reservedTokens));
  }

  public toDict(): Record<string, unknown> {
    return {
      tenant_id: this.tenantId,
      max_cost: this.maxCost,
      spent_cost: this.spentCost,
      reserved_cost: this.reservedCost,
      available_cost: this.availableCost(),
      max_tokens: this.maxTokens,
      spent_tokens: this.spentTokens,
      reserved_tokens: this.reservedTokens,
      available_tokens: this.availableTokens(),
      max_turns: this.maxTurns,
      spent_turns: this.spentTurns,
      state: this.state,
      trip_reason: this.tripReason,
    };
  }
}

export class SafetyAuthority {
  private _epoch: number;
  private _globalState: AuthorityStateEnum = AuthorityState.NOMINAL;
  private _globalTripReason: string | null = null;
  private _tenantQuotas = new Map<string, TenantQuota>();
  private _activePermits = new Map<string, SafetyPermit>();
  private _failClosedPartitionMode: boolean = false;

  constructor(initialEpoch: number = 1) {
    this._epoch = initialEpoch;
  }

  public get epoch(): number {
    return this._epoch;
  }

  public get globalState(): AuthorityStateEnum {
    return this._globalState;
  }

  public setPartitionSimulation(enabled: boolean): void {
    this._failClosedPartitionMode = enabled;
  }

  public getOrCreateQuota(tenantId: string): TenantQuota {
    let quota = this._tenantQuotas.get(tenantId);
    if (!quota) {
      quota = new TenantQuota(tenantId);
      this._tenantQuotas.set(tenantId, quota);
    }
    return quota;
  }

  public configureTenant(
    tenantId: string,
    maxCost: number = 100.0,
    maxTokens: number = 1_000_000,
    maxTurns: number = 1000,
  ): TenantQuota {
    const quota = this.getOrCreateQuota(tenantId);
    quota.maxCost = maxCost;
    quota.maxTokens = maxTokens;
    quota.maxTurns = maxTurns;
    return quota;
  }

  public reserve(
    tenantId: string,
    action: string = "agent_turn",
    costEstimate: number = 0.0,
    tokensEstimate: number = 0,
    timeoutMs: number = 5000.0,
    permitId?: string,
  ): SafetyPermit {
    // 1. Fail-closed partition check
    if (this._failClosedPartitionMode) {
      return new SafetyPermit({
        permitId: `pmt-indet-${crypto.randomBytes(4).toString("hex")}`,
        tenantId,
        epoch: this._epoch,
        action,
        costEstimate,
        tokensEstimate,
        status: PermitStatus.INDETERMINATE,
        rejectionReason: "Authority is unreachable or experiencing network partition (fail-closed)",
      });
    }

    // 2. Global authority trip check
    if (this._globalState === AuthorityState.TRIPPED) {
      return new SafetyPermit({
        permitId: `pmt-rej-${crypto.randomBytes(4).toString("hex")}`,
        tenantId,
        epoch: this._epoch,
        action,
        costEstimate,
        tokensEstimate,
        status: PermitStatus.REJECTED,
        rejectionReason: `Global safety circuit tripped at epoch ${this._epoch}: ${this._globalTripReason}`,
      });
    }

    // 3. Tenant trip check
    const quota = this.getOrCreateQuota(tenantId);
    if (quota.state === AuthorityState.TRIPPED) {
      return new SafetyPermit({
        permitId: `pmt-rej-${crypto.randomBytes(4).toString("hex")}`,
        tenantId,
        epoch: this._epoch,
        action,
        costEstimate,
        tokensEstimate,
        status: PermitStatus.REJECTED,
        rejectionReason: `Tenant ${tenantId} circuit tripped: ${quota.tripReason}`,
      });
    }

    // 4. Headroom check
    if (costEstimate > quota.availableCost()) {
      return new SafetyPermit({
        permitId: `pmt-rej-${crypto.randomBytes(4).toString("hex")}`,
        tenantId,
        epoch: this._epoch,
        action,
        costEstimate,
        tokensEstimate,
        status: PermitStatus.REJECTED,
        rejectionReason: `Insufficient cost budget: estimated $${costEstimate.toFixed(4)}, available $${quota.availableCost().toFixed(4)}`,
      });
    }

    if (tokensEstimate > quota.availableTokens()) {
      return new SafetyPermit({
        permitId: `pmt-rej-${crypto.randomBytes(4).toString("hex")}`,
        tenantId,
        epoch: this._epoch,
        action,
        costEstimate,
        tokensEstimate,
        status: PermitStatus.REJECTED,
        rejectionReason: `Insufficient token budget: estimated ${tokensEstimate}, available ${quota.availableTokens()}`,
      });
    }

    // 5. Reserve and issue permit
    quota.reservedCost += costEstimate;
    quota.reservedTokens += tokensEstimate;

    const finalPermitId = permitId || `pmt-${crypto.randomBytes(6).toString("hex")}`;
    const now = Date.now() / 1000.0;
    const expiresAt = now + Math.max(100.0, timeoutMs) / 1000.0;

    const permit = new SafetyPermit({
      permitId: finalPermitId,
      tenantId,
      epoch: this._epoch,
      action,
      costEstimate,
      tokensEstimate,
      status: PermitStatus.ACCEPTED,
      issuedAt: now,
      expiresAt,
    });

    this._activePermits.set(finalPermitId, permit);
    return permit;
  }

  public commit(
    permitId: string,
    actualCost: number = 0.0,
    actualTokens: number = 0,
  ): { success: boolean; message: string } {
    const permit = this._activePermits.get(permitId);
    if (!permit) {
      return { success: false, message: `Permit ${permitId} not found in active permits or already closed` };
    }

    // Fencing epoch check
    if (permit.epoch !== this._epoch) {
      this._activePermits.delete(permitId);
      return { success: false, message: `Permit invalidated by epoch transition (${permit.epoch} != ${this._epoch})` };
    }

    // Expiry check
    const now = Date.now() / 1000.0;
    if (now > permit.expiresAt) {
      this._activePermits.delete(permitId);
      const quota = this.getOrCreateQuota(permit.tenantId);
      quota.reservedCost = Math.max(0.0, quota.reservedCost - permit.costEstimate);
      quota.reservedTokens = Math.max(0, quota.reservedTokens - permit.tokensEstimate);
      return { success: false, message: `Permit ${permitId} expired before commit (${now.toFixed(2)} > ${permit.expiresAt.toFixed(2)})` };
    }

    // Settle budget
    const quota = this.getOrCreateQuota(permit.tenantId);
    quota.reservedCost = Math.max(0.0, quota.reservedCost - permit.costEstimate);
    quota.reservedTokens = Math.max(0, quota.reservedTokens - permit.tokensEstimate);
    quota.spentCost += actualCost;
    quota.spentTokens += actualTokens;
    quota.spentTurns += 1;

    this._activePermits.delete(permitId);
    return { success: true, message: "Permit committed successfully" };
  }

  public release(permitId: string, reason: string = "aborted"): { success: boolean; message: string } {
    const permit = this._activePermits.get(permitId);
    if (!permit) {
      return { success: false, message: `Permit ${permitId} not found in active permits` };
    }
    this._activePermits.delete(permitId);

    const quota = this.getOrCreateQuota(permit.tenantId);
    quota.reservedCost = Math.max(0.0, quota.reservedCost - permit.costEstimate);
    quota.reservedTokens = Math.max(0, quota.reservedTokens - permit.tokensEstimate);
    return { success: true, message: `Permit ${permitId} released (${reason})` };
  }

  public trip(reason: string, tenantId?: string): number {
    this._epoch += 1;
    if (tenantId) {
      const quota = this.getOrCreateQuota(tenantId);
      quota.state = AuthorityState.TRIPPED;
      quota.tripReason = reason;
    } else {
      this._globalState = AuthorityState.TRIPPED;
      this._globalTripReason = reason;
    }
    this._activePermits.clear();
    return this._epoch;
  }

  public rearm(tenantId?: string, newEpoch?: number): number {
    if (newEpoch !== undefined && newEpoch > this._epoch) {
      this._epoch = newEpoch;
    } else {
      this._epoch += 1;
    }
    if (tenantId) {
      const quota = this.getOrCreateQuota(tenantId);
      quota.state = AuthorityState.NOMINAL;
      quota.tripReason = null;
    } else {
      this._globalState = AuthorityState.NOMINAL;
      this._globalTripReason = null;
    }
    return this._epoch;
  }

  public status(tenantId: string = "default"): Record<string, unknown> {
    const quota = this.getOrCreateQuota(tenantId);
    return {
      epoch: this._epoch,
      global_state: this._globalState,
      global_trip_reason: this._globalTripReason,
      active_permits_count: this._activePermits.size,
      tenant: quota.toDict(),
    };
  }

  public reset(): void {
    this._epoch = 1;
    this._globalState = AuthorityState.NOMINAL;
    this._globalTripReason = null;
    this._tenantQuotas.clear();
    this._activePermits.clear();
    this._failClosedPartitionMode = false;
  }
}

export const globalSafetyAuthority = new SafetyAuthority();
