/**
 * Durable Dataset-Level Privacy Accountant & Quorum (ε,δ) Ledger (Seam 67 / ADR 0078).
 * Bounds cumulative differential privacy leakage across dataset and entity granularities.
 * Fully compatible with Node 22 `--experimental-strip-types`.
 */

import * as crypto from "node:crypto";

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

export class DatasetPrivacyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DatasetPrivacyError";
  }
}

export class DatasetPrivacyBudgetExhaustedError extends DatasetPrivacyError {
  constructor(message: string) {
    super(message);
    this.name = "DatasetPrivacyBudgetExhaustedError";
  }
}

export class EntityPrivacyBudgetExhaustedError extends DatasetPrivacyError {
  constructor(message: string) {
    super(message);
    this.name = "EntityPrivacyBudgetExhaustedError";
  }
}

export class DatasetNotFoundError extends DatasetPrivacyError {
  constructor(message: string) {
    super(message);
    this.name = "DatasetNotFoundError";
  }
}

export class ReservationNotFoundError extends DatasetPrivacyError {
  constructor(message: string) {
    super(message);
    this.name = "ReservationNotFoundError";
  }
}

// ---------------------------------------------------------------------------
// Enums & Helpers
// ---------------------------------------------------------------------------

export const DatasetReservationStatus = {
  PENDING: "PENDING",
  COMMITTED: "COMMITTED",
  CANCELLED: "CANCELLED",
  EXPIRED: "EXPIRED",
} as const;

export type DatasetReservationStatus =
  typeof DatasetReservationStatus[keyof typeof DatasetReservationStatus];

export function sha256Hex(content: string | Buffer): string {
  return crypto.createHash("sha256").update(content).digest("hex");
}

export function sampleLaplace(scale: number, seed?: number | null): number {
  if (scale <= 0.0) {
    return 0.0;
  }

  let u: number;
  if (seed !== undefined && seed !== null) {
    // Linear congruential generator for reproducible test seeding
    let s = (seed ^ 0x5deece66d) & 0xffffffffffff;
    s = (s * 0x5deece66d + 0xb) & 0xffffffffffff;
    u = (s >> 16) / 0x100000000;
    if (u <= 1e-12) u = 1e-12;
    if (u >= 1.0 - 1e-12) u = 1.0 - 1e-12;
  } else {
    // CSPRNG
    const buf = crypto.randomBytes(8);
    const intVal = buf.readBigUInt64BE();
    u = Number(intVal) / Number(0xffffffffffffffffn);
    if (u <= 1e-12) u = 1e-12;
    if (u >= 1.0 - 1e-12) u = 1.0 - 1e-12;
  }

  const sgn = u >= 0.5 ? 1.0 : -1.0;
  return -sgn * scale * Math.log(1.0 - 2.0 * Math.abs(u - 0.5));
}

export function clipValue(
  val: number,
  minVal?: number | null,
  maxVal?: number | null
): number {
  let clipped = val;
  if (minVal !== undefined && minVal !== null) {
    clipped = Math.max(minVal, clipped);
  }
  if (maxVal !== undefined && maxVal !== null) {
    clipped = Math.min(maxVal, clipped);
  }
  return clipped;
}

// ---------------------------------------------------------------------------
// Data Models
// ---------------------------------------------------------------------------

export interface DatasetPrivacyConfigData {
  datasetId: string;
  tenantId: string;
  maxEpsilon: number;
  maxDelta?: number;
  entityEpsilonLimit?: number;
  entityDeltaLimit?: number;
  description?: string;
  createdAtNs?: bigint | number;
}

export class DatasetPrivacyConfig {
  public readonly datasetId: string;
  public readonly tenantId: string;
  public readonly maxEpsilon: number;
  public readonly maxDelta: number;
  public readonly entityEpsilonLimit: number;
  public readonly entityDeltaLimit: number;
  public readonly description: string;
  public readonly createdAtNs: bigint;

  constructor(data: DatasetPrivacyConfigData) {
    this.datasetId = data.datasetId;
    this.tenantId = data.tenantId;
    this.maxEpsilon = Math.max(0.0, data.maxEpsilon);
    this.maxDelta = Math.max(0.0, data.maxDelta ?? 0.0);
    this.entityEpsilonLimit = Math.max(0.0, data.entityEpsilonLimit ?? 1.0);
    this.entityDeltaLimit = Math.max(0.0, data.entityDeltaLimit ?? 0.0);
    this.description = data.description || "";
    this.createdAtNs =
      data.createdAtNs !== undefined
        ? typeof data.createdAtNs === "bigint"
          ? data.createdAtNs
          : BigInt(data.createdAtNs)
        : BigInt(Date.now()) * 1_000_000n;
  }

  public toJSON(): Record<string, unknown> {
    return {
      dataset_id: this.datasetId,
      tenant_id: this.tenantId,
      max_epsilon: this.maxEpsilon,
      max_delta: this.maxDelta,
      entity_epsilon_limit: this.entityEpsilonLimit,
      entity_delta_limit: this.entityDeltaLimit,
      description: this.description,
      created_at_ns: Number(this.createdAtNs),
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }

  public static fromDict(data: Record<string, unknown>): DatasetPrivacyConfig {
    return new DatasetPrivacyConfig({
      datasetId: String(data.dataset_id || data.datasetId || ""),
      tenantId: String(data.tenant_id || data.tenantId || ""),
      maxEpsilon: Number(data.max_epsilon ?? data.maxEpsilon ?? 0.0),
      maxDelta: Number(data.max_delta ?? data.maxDelta ?? 0.0),
      entityEpsilonLimit: Number(data.entity_epsilon_limit ?? data.entityEpsilonLimit ?? 1.0),
      entityDeltaLimit: Number(data.entity_delta_limit ?? data.entityDeltaLimit ?? 0.0),
      description: String(data.description || ""),
      createdAtNs: BigInt(String(data.created_at_ns ?? data.createdAtNs ?? 0)),
    });
  }
}

export interface DatasetPrivacyReservationData {
  reservationId: string;
  tenantId: string;
  datasetId: string;
  entityId: string;
  epsilon: number;
  delta?: number;
  sensitivity?: number;
  queryId?: string;
  status?: DatasetReservationStatus;
  createdAtNs?: bigint | number;
  receiptHash?: string;
}

export class DatasetPrivacyReservation {
  public readonly reservationId: string;
  public readonly tenantId: string;
  public readonly datasetId: string;
  public readonly entityId: string;
  public readonly epsilon: number;
  public readonly delta: number;
  public readonly sensitivity: number;
  public readonly queryId: string;
  public status: DatasetReservationStatus;
  public readonly createdAtNs: bigint;
  public receiptHash: string;

  constructor(data: DatasetPrivacyReservationData) {
    this.reservationId = data.reservationId;
    this.tenantId = data.tenantId;
    this.datasetId = data.datasetId;
    this.entityId = data.entityId;
    this.epsilon = data.epsilon;
    this.delta = data.delta ?? 0.0;
    this.sensitivity = data.sensitivity ?? 1.0;
    this.queryId = data.queryId || "";
    this.status = data.status || DatasetReservationStatus.PENDING;
    this.createdAtNs =
      data.createdAtNs !== undefined
        ? typeof data.createdAtNs === "bigint"
          ? data.createdAtNs
          : BigInt(data.createdAtNs)
        : BigInt(Date.now()) * 1_000_000n;
    this.receiptHash = data.receiptHash || this.computeHash();
  }

  public computeHash(): string {
    const seed = `sw:dpl:v1:${this.reservationId}:${this.datasetId}:${this.tenantId}:${this.entityId}:${this.epsilon.toFixed(6)}:${this.delta.toFixed(8)}:${this.status}`;
    return sha256Hex(seed);
  }

  public toJSON(): Record<string, unknown> {
    return {
      reservation_id: this.reservationId,
      tenant_id: this.tenantId,
      dataset_id: this.datasetId,
      entity_id: this.entityId,
      epsilon: this.epsilon,
      delta: this.delta,
      sensitivity: this.sensitivity,
      query_id: this.queryId,
      status: this.status,
      created_at_ns: Number(this.createdAtNs),
      receipt_hash: this.receiptHash,
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }

  public static fromDict(data: Record<string, unknown>): DatasetPrivacyReservation {
    return new DatasetPrivacyReservation({
      reservationId: String(data.reservation_id || data.reservationId || ""),
      tenantId: String(data.tenant_id || data.tenantId || ""),
      datasetId: String(data.dataset_id || data.datasetId || ""),
      entityId: String(data.entity_id || data.entityId || ""),
      epsilon: Number(data.epsilon),
      delta: Number(data.delta ?? 0.0),
      sensitivity: Number(data.sensitivity ?? 1.0),
      queryId: String(data.query_id || data.queryId || ""),
      status: (data.status as DatasetReservationStatus) || DatasetReservationStatus.PENDING,
      createdAtNs: BigInt(String(data.created_at_ns ?? data.createdAtNs ?? 0)),
      receiptHash: String(data.receipt_hash || data.receiptHash || ""),
    });
  }
}

export interface DatasetPrivacyReleaseResultData {
  reservationId: string;
  datasetId: string;
  tenantId: string;
  entityId: string;
  rawValue: number | number[] | Record<string, number>;
  perturbedValue: number | number[] | Record<string, number>;
  noiseAdded: number | number[] | Record<string, number>;
  committedEpsilon: number;
  committedDelta: number;
  committedAtNs?: bigint | number;
  releaseHash?: string;
}

export class DatasetPrivacyReleaseResult {
  public readonly reservationId: string;
  public readonly datasetId: string;
  public readonly tenantId: string;
  public readonly entityId: string;
  public readonly rawValue: number | number[] | Record<string, number>;
  public readonly perturbedValue: number | number[] | Record<string, number>;
  public readonly noiseAdded: number | number[] | Record<string, number>;
  public readonly committedEpsilon: number;
  public readonly committedDelta: number;
  public readonly committedAtNs: bigint;
  public readonly releaseHash: string;

  constructor(data: DatasetPrivacyReleaseResultData) {
    this.reservationId = data.reservationId;
    this.datasetId = data.datasetId;
    this.tenantId = data.tenantId;
    this.entityId = data.entityId;
    this.rawValue = data.rawValue;
    this.perturbedValue = data.perturbedValue;
    this.noiseAdded = data.noiseAdded;
    this.committedEpsilon = data.committedEpsilon;
    this.committedDelta = data.committedDelta;
    this.committedAtNs =
      data.committedAtNs !== undefined
        ? typeof data.committedAtNs === "bigint"
          ? data.committedAtNs
          : BigInt(data.committedAtNs)
        : BigInt(Date.now()) * 1_000_000n;
    this.releaseHash =
      data.releaseHash ||
      sha256Hex(
        `sw:rel:v1:${this.reservationId}:${this.datasetId}:${this.tenantId}:${this.entityId}:${this.committedEpsilon.toFixed(6)}:${this.committedAtNs}`
      );
  }

  public toJSON(): Record<string, unknown> {
    return {
      reservation_id: this.reservationId,
      dataset_id: this.datasetId,
      tenant_id: this.tenantId,
      entity_id: this.entityId,
      raw_value: this.rawValue,
      perturbed_value: this.perturbedValue,
      noise_added: this.noiseAdded,
      committed_epsilon: this.committedEpsilon,
      committed_delta: this.committedDelta,
      committed_at_ns: Number(this.committedAtNs),
      release_hash: this.releaseHash,
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }
}

export class EntityExpenditure {
  public committedEpsilon: number = 0.0;
  public committedDelta: number = 0.0;
  public pendingEpsilon: number = 0.0;
  public pendingDelta: number = 0.0;

  public get totalEpsilon(): number {
    return this.committedEpsilon + this.pendingEpsilon;
  }

  public get totalDelta(): number {
    return this.committedDelta + this.pendingDelta;
  }
}

export interface DatasetLedgerSnapshotData {
  datasetId: string;
  tenantId: string;
  datasetSpentEpsilon: number;
  datasetSpentDelta: number;
  entityExpenditures: Record<string, [number, number]>;
  snapshotEpochNs?: bigint | number;
}

export class DatasetLedgerSnapshot {
  public readonly datasetId: string;
  public readonly tenantId: string;
  public readonly datasetSpentEpsilon: number;
  public readonly datasetSpentDelta: number;
  public readonly entityExpenditures: Record<string, [number, number]>;
  public readonly snapshotEpochNs: bigint;

  constructor(data: DatasetLedgerSnapshotData) {
    this.datasetId = data.datasetId;
    this.tenantId = data.tenantId;
    this.datasetSpentEpsilon = data.datasetSpentEpsilon;
    this.datasetSpentDelta = data.datasetSpentDelta;
    this.entityExpenditures = { ...data.entityExpenditures };
    this.snapshotEpochNs =
      data.snapshotEpochNs !== undefined
        ? typeof data.snapshotEpochNs === "bigint"
          ? data.snapshotEpochNs
          : BigInt(data.snapshotEpochNs)
        : BigInt(Date.now()) * 1_000_000n;
  }

  public toJSON(): Record<string, unknown> {
    return {
      dataset_id: this.datasetId,
      tenant_id: this.tenantId,
      dataset_spent_epsilon: this.datasetSpentEpsilon,
      dataset_spent_delta: this.datasetSpentDelta,
      entity_expenditures: { ...this.entityExpenditures },
      snapshot_epoch_ns: Number(this.snapshotEpochNs),
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }

  public static fromDict(data: Record<string, unknown>): DatasetLedgerSnapshot {
    const rawMap = (data.entity_expenditures || data.entityExpenditures || {}) as Record<string, [number, number]>;
    return new DatasetLedgerSnapshot({
      datasetId: String(data.dataset_id || data.datasetId || ""),
      tenantId: String(data.tenant_id || data.tenantId || ""),
      datasetSpentEpsilon: Number(data.dataset_spent_epsilon ?? data.datasetSpentEpsilon ?? 0.0),
      datasetSpentDelta: Number(data.dataset_spent_delta ?? data.datasetSpentDelta ?? 0.0),
      entityExpenditures: rawMap,
      snapshotEpochNs: BigInt(String(data.snapshot_epoch_ns ?? data.snapshotEpochNs ?? 0)),
    });
  }
}

// ---------------------------------------------------------------------------
// Deep Module: DatasetPrivacyLedger
// ---------------------------------------------------------------------------

export class DatasetPrivacyLedger {
  private readonly datasets = new Map<string, DatasetPrivacyConfig>();
  private readonly entityExpenditures = new Map<string, EntityExpenditure>();
  private readonly reservations = new Map<string, DatasetPrivacyReservation>();
  private totalReservationsIssued = 0;
  private totalReleasesCommitted = 0;

  private makeDatasetKey(tenantId: string, datasetId: string): string {
    return `${tenantId}::${datasetId}`;
  }

  private makeEntityKey(tenantId: string, datasetId: string, entityId: string): string {
    return `${tenantId}::${datasetId}::${entityId}`;
  }

  public registerDataset(params: {
    datasetId: string;
    tenantId: string;
    maxEpsilon: number;
    maxDelta?: number;
    entityEpsilonLimit?: number;
    entityDeltaLimit?: number;
    description?: string;
  }): DatasetPrivacyConfig {
    const key = this.makeDatasetKey(params.tenantId, params.datasetId);
    const config = new DatasetPrivacyConfig({
      datasetId: params.datasetId,
      tenantId: params.tenantId,
      maxEpsilon: params.maxEpsilon,
      maxDelta: params.maxDelta,
      entityEpsilonLimit: params.entityEpsilonLimit,
      entityDeltaLimit: params.entityDeltaLimit,
      description: params.description,
    });
    this.datasets.set(key, config);
    return config;
  }

  public getDataset(tenantId: string, datasetId: string): DatasetPrivacyConfig | undefined {
    return this.datasets.get(this.makeDatasetKey(tenantId, datasetId));
  }

  private getEntityExpenditure(tenantId: string, datasetId: string, entityId: string): EntityExpenditure {
    const key = this.makeEntityKey(tenantId, datasetId, entityId);
    let exp = this.entityExpenditures.get(key);
    if (!exp) {
      exp = new EntityExpenditure();
      this.entityExpenditures.set(key, exp);
    }
    return exp;
  }

  private getDatasetTotalExpenditure(tenantId: string, datasetId: string): { totalEps: number; totalDelta: number } {
    let totalEps = 0.0;
    let totalDelta = 0.0;
    const prefix = `${tenantId}::${datasetId}::`;
    for (const [key, exp] of this.entityExpenditures.entries()) {
      if (key.startsWith(prefix)) {
        totalEps += exp.totalEpsilon;
        totalDelta += exp.totalDelta;
      }
    }
    return { totalEps, totalDelta };
  }

  public reserve(params: {
    tenantId: string;
    datasetId: string;
    entityId: string;
    epsilon: number;
    delta?: number;
    sensitivity?: number;
    queryId?: string;
  }): DatasetPrivacyReservation {
    const delta = params.delta ?? 0.0;
    const sensitivity = params.sensitivity ?? 1.0;

    if (params.epsilon <= 0.0) {
      throw new Error("Requested epsilon must be strictly positive");
    }
    if (delta < 0.0) {
      throw new Error("Requested delta cannot be negative");
    }
    if (sensitivity <= 0.0) {
      throw new Error("Sensitivity must be strictly positive");
    }

    const key = this.makeDatasetKey(params.tenantId, params.datasetId);
    let config = this.datasets.get(key);
    if (!config) {
      config = this.registerDataset({
        datasetId: params.datasetId,
        tenantId: params.tenantId,
        maxEpsilon: 100.0,
        maxDelta: 1e-3,
        entityEpsilonLimit: 2.0,
        entityDeltaLimit: 1e-5,
      });
    }

    // 1. Dataset-level check
    const dsExpenditure = this.getDatasetTotalExpenditure(params.tenantId, params.datasetId);
    const remDsEps = config.maxEpsilon - dsExpenditure.totalEps;
    const remDsDelta = config.maxDelta - dsExpenditure.totalDelta;

    if (params.epsilon > remDsEps || (delta > 0.0 && delta > remDsDelta)) {
      throw new DatasetPrivacyBudgetExhaustedError(
        `Dataset privacy budget exhausted for dataset '${params.datasetId}' (tenant '${params.tenantId}'): ` +
        `requested epsilon=${params.epsilon.toFixed(4)} (remaining=${remDsEps.toFixed(4)}), ` +
        `requested delta=${delta.toFixed(6)} (remaining=${remDsDelta.toFixed(6)})`
      );
    }

    // 2. Entity-level check
    const exp = this.getEntityExpenditure(params.tenantId, params.datasetId, params.entityId);
    const remEntityEps = config.entityEpsilonLimit - exp.totalEpsilon;
    const remEntityDelta = config.entityDeltaLimit - exp.totalDelta;

    if (params.epsilon > remEntityEps || (delta > 0.0 && delta > remEntityDelta)) {
      throw new EntityPrivacyBudgetExhaustedError(
        `Entity privacy budget exhausted for entity '${params.entityId}' in dataset '${params.datasetId}': ` +
        `requested epsilon=${params.epsilon.toFixed(4)} (remaining=${remEntityEps.toFixed(4)}), ` +
        `requested delta=${delta.toFixed(6)} (remaining=${remEntityDelta.toFixed(6)})`
      );
    }

    // Commit pending reservation
    exp.pendingEpsilon += params.epsilon;
    exp.pendingDelta += delta;

    const resId = `res-dp-${crypto.randomBytes(6).toString("hex")}`;
    const qid = params.queryId || `qry-${crypto.randomBytes(4).toString("hex")}`;

    const reservation = new DatasetPrivacyReservation({
      reservationId: resId,
      tenantId: params.tenantId,
      datasetId: params.datasetId,
      entityId: params.entityId,
      epsilon: params.epsilon,
      delta,
      sensitivity,
      queryId: qid,
      status: DatasetReservationStatus.PENDING,
    });

    this.reservations.set(resId, reservation);
    this.totalReservationsIssued += 1;
    return reservation;
  }

  public commitRelease(params: {
    reservationId: string;
    rawValue: number | number[] | Record<string, number>;
    noiseSeed?: number | null;
    minBound?: number | null;
    maxBound?: number | null;
  }): DatasetPrivacyReleaseResult {
    const res = this.reservations.get(params.reservationId);
    if (!res) {
      throw new ReservationNotFoundError(`Reservation '${params.reservationId}' not found`);
    }

    if (res.status !== DatasetReservationStatus.PENDING) {
      throw new DatasetPrivacyError(
        `Reservation '${params.reservationId}' has invalid status: ${res.status}`
      );
    }

    const scale = res.sensitivity / res.epsilon;
    const raw = params.rawValue;
    let perturbed: number | number[] | Record<string, number>;
    let noise: number | number[] | Record<string, number>;

    if (typeof raw === "number") {
      const clipped = clipValue(raw, params.minBound, params.maxBound);
      const n = sampleLaplace(scale, params.noiseSeed);
      perturbed = clipValue(clipped + n, params.minBound, params.maxBound);
      noise = n;
    } else if (Array.isArray(raw)) {
      const pArr: number[] = [];
      const nArr: number[] = [];
      for (let i = 0; i < raw.length; i++) {
        const seedI = params.noiseSeed !== undefined && params.noiseSeed !== null ? params.noiseSeed + i : undefined;
        const c = clipValue(raw[i], params.minBound, params.maxBound);
        const n = sampleLaplace(scale, seedI);
        pArr.push(clipValue(c + n, params.minBound, params.maxBound));
        nArr.push(n);
      }
      perturbed = pArr;
      noise = nArr;
    } else if (typeof raw === "object" && raw !== null) {
      const pObj: Record<string, number> = {};
      const nObj: Record<string, number> = {};
      let idx = 0;
      for (const [k, v] of Object.entries(raw)) {
        const seedI = params.noiseSeed !== undefined && params.noiseSeed !== null ? params.noiseSeed + idx : undefined;
        const c = clipValue(v, params.minBound, params.maxBound);
        const n = sampleLaplace(scale, seedI);
        pObj[k] = clipValue(c + n, params.minBound, params.maxBound);
        nObj[k] = n;
        idx++;
      }
      perturbed = pObj;
      noise = nObj;
    } else {
      throw new Error(`Unsupported data type for rawValue: ${typeof raw}`);
    }

    res.status = DatasetReservationStatus.COMMITTED;
    res.receiptHash = res.computeHash();

    const exp = this.getEntityExpenditure(res.tenantId, res.datasetId, res.entityId);
    exp.pendingEpsilon = Math.max(0.0, exp.pendingEpsilon - res.epsilon);
    exp.pendingDelta = Math.max(0.0, exp.pendingDelta - res.delta);
    exp.committedEpsilon += res.epsilon;
    exp.committedDelta += res.delta;

    this.totalReleasesCommitted += 1;

    return new DatasetPrivacyReleaseResult({
      reservationId: res.reservationId,
      datasetId: res.datasetId,
      tenantId: res.tenantId,
      entityId: res.entityId,
      rawValue: raw,
      perturbedValue: perturbed,
      noiseAdded: noise,
      committedEpsilon: res.epsilon,
      committedDelta: res.delta,
    });
  }

  public cancelReservation(reservationId: string): boolean {
    const res = this.reservations.get(reservationId);
    if (!res || res.status !== DatasetReservationStatus.PENDING) {
      return false;
    }

    res.status = DatasetReservationStatus.CANCELLED;
    res.receiptHash = res.computeHash();

    const exp = this.getEntityExpenditure(res.tenantId, res.datasetId, res.entityId);
    exp.pendingEpsilon = Math.max(0.0, exp.pendingEpsilon - res.epsilon);
    exp.pendingDelta = Math.max(0.0, exp.pendingDelta - res.delta);
    return true;
  }

  public remaining(params: {
    tenantId: string;
    datasetId: string;
    entityId: string;
  }): {
    dataset_remaining_epsilon: number;
    dataset_remaining_delta: number;
    entity_remaining_epsilon: number;
    entity_remaining_delta: number;
  } {
    const key = this.makeDatasetKey(params.tenantId, params.datasetId);
    const config = this.datasets.get(key);
    if (!config) {
      return {
        dataset_remaining_epsilon: 0.0,
        dataset_remaining_delta: 0.0,
        entity_remaining_epsilon: 0.0,
        entity_remaining_delta: 0.0,
      };
    }

    const dsExpenditure = this.getDatasetTotalExpenditure(params.tenantId, params.datasetId);
    const exp = this.getEntityExpenditure(params.tenantId, params.datasetId, params.entityId);

    return {
      dataset_remaining_epsilon: Math.max(0.0, config.maxEpsilon - dsExpenditure.totalEps),
      dataset_remaining_delta: Math.max(0.0, config.maxDelta - dsExpenditure.totalDelta),
      entity_remaining_epsilon: Math.max(0.0, config.entityEpsilonLimit - exp.totalEpsilon),
      entity_remaining_delta: Math.max(0.0, config.entityDeltaLimit - exp.totalDelta),
    };
  }

  public exportSnapshot(tenantId: string, datasetId: string): DatasetLedgerSnapshot {
    const dsExpenditure = this.getDatasetTotalExpenditure(tenantId, datasetId);
    const entMap: Record<string, [number, number]> = {};
    const prefix = `${tenantId}::${datasetId}::`;
    for (const [k, exp] of this.entityExpenditures.entries()) {
      if (k.startsWith(prefix)) {
        const entId = k.slice(prefix.length);
        entMap[entId] = [exp.committedEpsilon, exp.committedDelta];
      }
    }

    return new DatasetLedgerSnapshot({
      datasetId,
      tenantId,
      datasetSpentEpsilon: dsExpenditure.totalEps,
      datasetSpentDelta: dsExpenditure.totalDelta,
      entityExpenditures: entMap,
    });
  }

  public reconcile(snapshot: DatasetLedgerSnapshot | Record<string, unknown>): {
    dataset_id: string;
    tenant_id: string;
    reconciled_entities: number;
  } {
    const snap =
      snapshot instanceof DatasetLedgerSnapshot
        ? snapshot
        : DatasetLedgerSnapshot.fromDict(snapshot);

    let reconciledCount = 0;
    for (const [entityId, [peerEps, peerDelta]] of Object.entries(snap.entityExpenditures)) {
      const exp = this.getEntityExpenditure(snap.tenantId, snap.datasetId, entityId);
      exp.committedEpsilon = Math.max(exp.committedEpsilon, peerEps);
      exp.committedDelta = Math.max(exp.committedDelta, peerDelta);
      reconciledCount++;
    }

    return {
      dataset_id: snap.datasetId,
      tenant_id: snap.tenantId,
      reconciled_entities: reconciledCount,
    };
  }

  public getReservation(reservationId: string): DatasetPrivacyReservation | undefined {
    return this.reservations.get(reservationId);
  }

  public status(): {
    total_registered_datasets: number;
    total_active_reservations: number;
    total_reservations_issued: number;
    total_releases_committed: number;
    total_entities_tracked: number;
  } {
    let active = 0;
    for (const r of this.reservations.values()) {
      if (r.status === DatasetReservationStatus.PENDING) {
        active++;
      }
    }
    return {
      total_registered_datasets: this.datasets.size,
      total_active_reservations: active,
      total_reservations_issued: this.totalReservationsIssued,
      total_releases_committed: this.totalReleasesCommitted,
      total_entities_tracked: this.entityExpenditures.size,
    };
  }

  public reset(): void {
    this.datasets.clear();
    this.entityExpenditures.clear();
    this.reservations.clear();
    this.totalReservationsIssued = 0;
    this.totalReleasesCommitted = 0;
  }
}

// ---------------------------------------------------------------------------
// Global Singleton
// ---------------------------------------------------------------------------

export const globalDatasetPrivacyLedger = new DatasetPrivacyLedger();
