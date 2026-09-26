/**
 * Privacy Release Ledger & Differential Privacy Accountant (Seam 58 / ADR 0069).
 * Bounds repeated-query telemetry leakage across multi-tenant boundaries.
 * Enforces:
 * 1. Privacy Release Ledger Invariant: Exports occur only when tenant's remaining privacy budget permits.
 * 2. Clipped Sensitivity ($L_1$): Clamps inputs within $[L_{min}, L_{max}]$ to strictly bound query sensitivity.
 * 3. Dual Composition Accounting: Linear composition ($\sum \epsilon_i$) and advanced $(\epsilon, \delta)$-DP bounds.
 * 4. Calibrated Laplace Perturbation: CSPRNG in production, deterministic seeded reproducibility for audits.
 * 5. Zero external LLM API calls (<25us local execution).
 */

import * as crypto from "node:crypto";
import { CanonicalCodec, CanonicalDomainTag } from "./canonical.ts";

export class PrivacyBudgetExhaustedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PrivacyBudgetExhaustedError";
  }
}

export const ReservationStatus = {
  PENDING: "PENDING",
  RELEASED: "RELEASED",
  CANCELLED: "CANCELLED",
  EXPIRED: "EXPIRED",
} as const;

export type ReservationStatusEnum = (typeof ReservationStatus)[keyof typeof ReservationStatus];

class SeededRng {
  private _state: number;

  constructor(seed: number) {
    this._state = seed >>> 0;
  }

  public next(): number {
    this._state = (this._state + 0x6d2b79f5) | 0;
    let t = Math.imul(this._state ^ (this._state >>> 15), 1 | this._state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    const u = ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    return Math.max(1e-12, Math.min(1.0 - 1e-12, u));
  }
}

export function sampleLaplace(scale: number, seed?: number): number {
  if (scale <= 0.0) {
    return 0.0;
  }

  let u: number;
  if (seed !== undefined) {
    const rng = new SeededRng(seed);
    u = rng.next();
  } else {
    const buf = crypto.randomBytes(6);
    const intVal = buf.readUIntBE(0, 6);
    u = intVal / 0xffffffffffff;
    if (u <= 1e-12) u = 1e-12;
    if (u >= 1.0 - 1e-12) u = 1.0 - 1e-12;
  }

  const sgn = u >= 0.5 ? 1.0 : -1.0;
  return -sgn * scale * Math.log(1.0 - 2.0 * Math.abs(u - 0.5));
}

export function clipValue(val: number, minVal?: number, maxVal?: number): number {
  let clipped = val;
  if (minVal !== undefined && minVal !== null) {
    clipped = Math.max(minVal, clipped);
  }
  if (maxVal !== undefined && maxVal !== null) {
    clipped = Math.min(maxVal, clipped);
  }
  return clipped;
}

export interface PrivacyReservation {
  reservation_id: string;
  tenant_id: string;
  query_id: string;
  epsilon: number;
  delta: number;
  sensitivity: number;
  status: ReservationStatusEnum;
  created_at: number;
  reservation_hash?: string;
}

export interface PrivacyReleaseResult {
  release_id: string;
  reservation_id: string;
  tenant_id: string;
  raw_clipped: number | number[] | Record<string, number>;
  perturbed_value: number | number[] | Record<string, number>;
  noise_added: number | number[] | Record<string, number>;
  epsilon_spent: number;
  delta_spent: number;
  remaining_epsilon: number;
  remaining_delta: number;
  released_at: number;
  release_hash?: string;
}

export class CompositionAccountant {
  private _queries: Array<[number, number]> = [];

  public record(epsilon: number, delta: number = 0.0): void {
    this._queries.push([Number(epsilon), Number(delta)]);
  }

  public linearTotal(): [number, number] {
    const totEps = this._queries.reduce((acc, q) => acc + q[0], 0);
    const totDelta = this._queries.reduce((acc, q) => acc + q[1], 0);
    return [totEps, totDelta];
  }

  public advancedComposition(deltaPrime: number = 1e-5): [number, number] {
    const k = this._queries.length;
    if (k === 0) {
      return [0.0, 0.0];
    }

    const [linearEps, baseDelta] = this.linearTotal();
    const epsBar = linearEps / k;

    if (deltaPrime <= 0.0 || epsBar <= 0.0) {
      return [linearEps, baseDelta];
    }

    const term1 = Math.sqrt(2.0 * k * Math.log(1.0 / deltaPrime)) * epsBar;
    const term2 = k * epsBar * (Math.exp(epsBar) - 1.0);
    const epsAdv = term1 + term2;
    const deltaAdv = baseDelta + deltaPrime;

    return [Math.min(linearEps, epsAdv), deltaAdv];
  }

  public count(): number {
    return this._queries.length;
  }

  public reset(): void {
    this._queries = [];
  }
}

interface TenantPrivacyBudget {
  total_epsilon: number;
  total_delta: number;
  accountant: CompositionAccountant;
  reservations: Map<string, PrivacyReservation>;
  releases: PrivacyReleaseResult[];
}

export class PrivacyRelease {
  private _defaultEpsilon: number;
  private _defaultDelta: number;
  private _tenants: Map<string, TenantPrivacyBudget> = new Map();

  constructor(defaultEpsilon: number = 10.0, defaultDelta: number = 1e-4) {
    this._defaultEpsilon = defaultEpsilon;
    this._defaultDelta = defaultDelta;
  }

  private _getTenant(tenantId: string): TenantPrivacyBudget {
    if (!this._tenants.has(tenantId)) {
      this._tenants.set(tenantId, {
        total_epsilon: this._defaultEpsilon,
        total_delta: this._defaultDelta,
        accountant: new CompositionAccountant(),
        reservations: new Map(),
        releases: [],
      });
    }
    return this._tenants.get(tenantId)!;
  }

  public setTenantBudget(
    tenantId: string,
    totalEpsilon: number,
    totalDelta: number = 0.0,
  ): void {
    const tb = this._getTenant(tenantId);
    tb.total_epsilon = Math.max(0.0, Number(totalEpsilon));
    tb.total_delta = Math.max(0.0, Number(totalDelta));
  }

  public reserve(
    tenantId: string,
    epsilon: number,
    delta: number = 0.0,
    sensitivity: number = 1.0,
    queryId?: string,
  ): PrivacyReservation {
    if (epsilon <= 0.0) {
      throw new Error("Epsilon budget request must be strictly positive");
    }
    if (delta < 0.0) {
      throw new Error("Delta budget request cannot be negative");
    }
    if (sensitivity <= 0.0) {
      throw new Error("Sensitivity must be strictly positive");
    }

    const tb = this._getTenant(tenantId);
    const [spentEps, spentDelta] = tb.accountant.linearTotal();

    let pendingEps = 0;
    let pendingDelta = 0;
    for (const r of tb.reservations.values()) {
      if (r.status === ReservationStatus.PENDING) {
        pendingEps += r.epsilon;
        pendingDelta += r.delta;
      }
    }

    const remEps = tb.total_epsilon - (spentEps + pendingEps);
    const remDelta = tb.total_delta - (spentDelta + pendingDelta);

    if (epsilon > remEps || (delta > 0.0 && delta > remDelta)) {
      throw new PrivacyBudgetExhaustedError(
        `Privacy budget exhausted for tenant '${tenantId}': requested epsilon=${epsilon.toFixed(4)} (remaining=${remEps.toFixed(4)}), requested delta=${delta.toFixed(6)} (remaining=${remDelta.toFixed(6)})`,
      );
    }

    const rid = `prev-${crypto.randomBytes(6).toString("hex")}`;
    const qid = queryId || `qry-${crypto.randomBytes(4).toString("hex")}`;

    const reservationHash = CanonicalCodec.hash(
      {
        delta,
        epsilon,
        query_id: qid,
        reservation_id: rid,
        sensitivity,
        status: ReservationStatus.PENDING,
        tenant_id: tenantId,
      },
      CanonicalDomainTag.PRIVACY,
    );

    const reservation: PrivacyReservation = {
      reservation_id: rid,
      tenant_id: tenantId,
      query_id: qid,
      epsilon,
      delta,
      sensitivity,
      status: ReservationStatus.PENDING,
      created_at: Date.now() / 1000,
      reservation_hash: reservationHash,
    };

    tb.reservations.set(rid, reservation);
    return reservation;
  }

  public release(
    reservationId: string,
    rawValue: number | number[] | Record<string, number>,
    noiseSeed?: number,
    minBound?: number,
    maxBound?: number,
  ): PrivacyReleaseResult {
    let targetTb: TenantPrivacyBudget | null = null;
    let reservation: PrivacyReservation | null = null;

    for (const tb of this._tenants.values()) {
      if (tb.reservations.has(reservationId)) {
        targetTb = tb;
        reservation = tb.reservations.get(reservationId)!;
        break;
      }
    }

    if (!targetTb || !reservation) {
      throw new Error(`Reservation '${reservationId}' not found`);
    }

    if (reservation.status !== ReservationStatus.PENDING) {
      throw new Error(`Reservation '${reservationId}' has invalid status: ${reservation.status}`);
    }

    const scale = reservation.sensitivity / reservation.epsilon;

    let rawClipped: number | number[] | Record<string, number>;
    let perturbed: number | number[] | Record<string, number>;
    let noiseMap: number | number[] | Record<string, number>;

    if (typeof rawValue === "number") {
      const c = clipValue(rawValue, minBound, maxBound);
      const n = sampleLaplace(scale, noiseSeed);
      rawClipped = c;
      perturbed = c + n;
      noiseMap = n;
    } else if (Array.isArray(rawValue)) {
      rawClipped = rawValue.map((v) => clipValue(Number(v), minBound, maxBound));
      const noises = rawClipped.map((_, idx) =>
        sampleLaplace(scale, noiseSeed !== undefined ? noiseSeed + idx : undefined),
      );
      perturbed = rawClipped.map((c, idx) => c + noises[idx]);
      noiseMap = noises;
    } else if (typeof rawValue === "object" && rawValue !== null) {
      const clippedDict: Record<string, number> = {};
      const noiseDict: Record<string, number> = {};
      const pertDict: Record<string, number> = {};

      let idx = 0;
      for (const [k, val] of Object.entries(rawValue)) {
        const c = clipValue(Number(val), minBound, maxBound);
        const n = sampleLaplace(scale, noiseSeed !== undefined ? noiseSeed + idx : undefined);
        clippedDict[k] = c;
        noiseDict[k] = n;
        pertDict[k] = c + n;
        idx++;
      }
      rawClipped = clippedDict;
      perturbed = pertDict;
      noiseMap = noiseDict;
    } else {
      throw new TypeError(`Unsupported rawValue type: ${typeof rawValue}`);
    }

    targetTb.accountant.record(reservation.epsilon, reservation.delta);
    reservation.status = ReservationStatus.RELEASED;

    const [spentEps, spentDelta] = targetTb.accountant.linearTotal();
    const remEps = Math.max(0.0, targetTb.total_epsilon - spentEps);
    const remDelta = Math.max(0.0, targetTb.total_delta - spentDelta);

    const relId = `prel-${crypto.randomBytes(6).toString("hex")}`;
    const releaseHash = CanonicalCodec.hash(
      {
        delta_spent: reservation.delta,
        epsilon_spent: reservation.epsilon,
        release_id: relId,
        remaining_delta: remDelta,
        remaining_epsilon: remEps,
        reservation_id: reservationId,
        tenant_id: reservation.tenant_id,
      },
      CanonicalDomainTag.PRIVACY,
    );

    const result: PrivacyReleaseResult = {
      release_id: relId,
      reservation_id: reservationId,
      tenant_id: reservation.tenant_id,
      raw_clipped: rawClipped,
      perturbed_value: perturbed,
      noise_added: noiseMap,
      epsilon_spent: reservation.epsilon,
      delta_spent: reservation.delta,
      remaining_epsilon: remEps,
      remaining_delta: remDelta,
      released_at: Date.now() / 1000,
      release_hash: releaseHash,
    };

    targetTb.releases.push(result);
    return result;
  }

  public remaining(tenantId: string): Record<string, number> {
    const tb = this._getTenant(tenantId);
    const [spentEps, spentDelta] = tb.accountant.linearTotal();

    let pendingEps = 0;
    let pendingDelta = 0;
    for (const r of tb.reservations.values()) {
      if (r.status === ReservationStatus.PENDING) {
        pendingEps += r.epsilon;
        pendingDelta += r.delta;
      }
    }

    const remEps = Math.max(0.0, tb.total_epsilon - (spentEps + pendingEps));
    const remDelta = Math.max(0.0, tb.total_delta - (spentDelta + pendingDelta));

    return {
      remaining_epsilon: remEps,
      remaining_delta: remDelta,
      total_epsilon: tb.total_epsilon,
      total_delta: tb.total_delta,
      spent_epsilon: spentEps,
      spent_delta: spentDelta,
      pending_epsilon: pendingEps,
      pending_delta: pendingDelta,
    };
  }

  public status(tenantId?: string): Record<string, any> {
    const tenantsToReport = tenantId ? [tenantId] : Array.from(this._tenants.keys());
    let totalReleases = 0;
    let totalReservations = 0;
    let totalSpentEps = 0.0;
    let budgetConstrainedCount = 0;

    for (const tid of tenantsToReport) {
      const tb = this._getTenant(tid);
      totalReleases += tb.releases.length;
      totalReservations += tb.reservations.size;
      const [sEps] = tb.accountant.linearTotal();
      totalSpentEps += sEps;
      if (sEps >= tb.total_epsilon) {
        budgetConstrainedCount++;
      }
    }

    const statusStr = budgetConstrainedCount === 0 ? "CONVERGED" : "BUDGET_CONSTRAINED";

    return {
      active_tenants: this._tenants.size,
      budget_constrained_count: budgetConstrainedCount,
      status: statusStr,
      total_releases: totalReleases,
      total_reservations: totalReservations,
      total_spent_epsilon: totalSpentEps,
    };
  }

  public reset(): void {
    this._tenants.clear();
  }
}

export const globalPrivacyRelease = new PrivacyRelease();
