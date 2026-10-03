/**
 * SwarmWatch Multi-Modal Cognitive Grounding & Vector Evidence Fusion (Seam 73 / ADR 0084).
 *
 * Implements deterministic in-process zero-token sparse-to-dense vector embedding,
 * multi-modal evidence registration (text claims, visual watermarks, action receipts,
 * root witness checkpoints, structured telemetry), exact cosine similarity evaluation,
 * multi-modal fusion algebra, fail-closed safety circuit coupling, and tamper-evident
 * grounding receipts.
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class CognitiveGroundingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CognitiveGroundingError";
  }
}

export class UngroundedInferenceError extends CognitiveGroundingError {
  constructor(message: string) {
    super(message);
    this.name = "UngroundedInferenceError";
  }
}

export class GroundingCircuitTrippedError extends CognitiveGroundingError {
  constructor(message: string) {
    super(message);
    this.name = "GroundingCircuitTrippedError";
  }
}

export class EvidenceNotFoundError extends CognitiveGroundingError {
  constructor(message: string) {
    super(message);
    this.name = "EvidenceNotFoundError";
  }
}

export class GroundingModalityMismatchError extends CognitiveGroundingError {
  constructor(message: string) {
    super(message);
    this.name = "GroundingModalityMismatchError";
  }
}

// ==============================================================================
// 2. Domain Types and Models
// ==============================================================================

export type EvidenceModality =
  | "TEXT_CLAIM"
  | "VISUAL_ARTIFACT"
  | "ACTION_RECEIPT"
  | "ROOT_WITNESS"
  | "STRUCTURED_TELEMETRY";

export const EvidenceModality = {
  TEXT_CLAIM: "TEXT_CLAIM" as EvidenceModality,
  VISUAL_ARTIFACT: "VISUAL_ARTIFACT" as EvidenceModality,
  ACTION_RECEIPT: "ACTION_RECEIPT" as EvidenceModality,
  ROOT_WITNESS: "ROOT_WITNESS" as EvidenceModality,
  STRUCTURED_TELEMETRY: "STRUCTURED_TELEMETRY" as EvidenceModality,
} as const;

export type GroundingVerdict =
  | "GROUNDED"
  | "PARTIALLY_GROUNDED"
  | "UNGROUNDED"
  | "DRIFT_DETECTED";

export const GroundingVerdict = {
  GROUNDED: "GROUNDED" as GroundingVerdict,
  PARTIALLY_GROUNDED: "PARTIALLY_GROUNDED" as GroundingVerdict,
  UNGROUNDED: "UNGROUNDED" as GroundingVerdict,
  DRIFT_DETECTED: "DRIFT_DETECTED" as GroundingVerdict,
} as const;

export interface GroundingConfig {
  dimension?: number;
  defaultThreshold?: number;
  partialThreshold?: number;
  driftThreshold?: number;
  maxDriftCountBeforeTrip?: number;
}

export class VectorEvidence {
  evidenceId: string = "";
  tenantId: string = "tenant-default";
  modality: EvidenceModality = "TEXT_CLAIM";
  content: string = "";
  embedding: number[] = [];
  sourceReference: string = "";
  confidenceWeight: number = 1.0;
  createdAtNs: number = 0;
  metadata: Record<string, any> = {};

  constructor(params: {
    evidenceId: string;
    tenantId: string;
    modality: EvidenceModality;
    content: string;
    embedding: number[];
    sourceReference: string;
    confidenceWeight?: number;
    createdAtNs?: number;
    metadata?: Record<string, any>;
  }) {
    this.evidenceId = params.evidenceId;
    this.tenantId = params.tenantId;
    this.modality = params.modality;
    this.content = params.content;
    this.embedding = params.embedding;
    this.sourceReference = params.sourceReference;
    this.confidenceWeight = params.confidenceWeight ?? 1.0;
    this.createdAtNs = params.createdAtNs ?? Date.now() * 1_000_000;
    this.metadata = params.metadata ?? {};
  }

  toJSON(): Record<string, any> {
    return {
      evidence_id: this.evidenceId,
      tenant_id: this.tenantId,
      modality: this.modality,
      content: this.content,
      embedding: this.embedding,
      source_reference: this.sourceReference,
      confidence_weight: this.confidenceWeight,
      created_at_ns: this.createdAtNs,
      metadata: this.metadata,
    };
  }
}

export class GroundingReceipt {
  receiptId: string = "";
  claimId: string = "";
  tenantId: string = "tenant-default";
  verdict: GroundingVerdict = "UNGROUNDED";
  overallScore: number = 0.0;
  fusedModalities: Record<string, number> = {};
  matchedEvidenceIds: string[] = [];
  durationUs: number = 0.0;
  canonicalHash: string = "";
  timestampNs: number = 0;

  constructor(params: {
    receiptId: string;
    claimId: string;
    tenantId: string;
    verdict: GroundingVerdict;
    overallScore: number;
    fusedModalities: Record<string, number>;
    matchedEvidenceIds: string[];
    durationUs: number;
    canonicalHash: string;
    timestampNs?: number;
  }) {
    this.receiptId = params.receiptId;
    this.claimId = params.claimId;
    this.tenantId = params.tenantId;
    this.verdict = params.verdict;
    this.overallScore = params.overallScore;
    this.fusedModalities = params.fusedModalities;
    this.matchedEvidenceIds = params.matchedEvidenceIds;
    this.durationUs = params.durationUs;
    this.canonicalHash = params.canonicalHash;
    this.timestampNs = params.timestampNs ?? Date.now() * 1_000_000;
  }

  toJSON(): Record<string, any> {
    const fused: Record<string, number> = {};
    for (const [k, v] of Object.entries(this.fusedModalities)) {
      fused[k] = Math.round(v * 10000) / 10000;
    }
    return {
      receipt_id: this.receiptId,
      claim_id: this.claimId,
      tenant_id: this.tenantId,
      verdict: this.verdict,
      overall_score: Math.round(this.overallScore * 10000) / 10000,
      fused_modalities: fused,
      matched_evidence_ids: this.matchedEvidenceIds,
      duration_us: Math.round(this.durationUs * 100) / 100,
      canonical_hash: this.canonicalHash,
      timestamp_ns: this.timestampNs,
    };
  }
}

// ==============================================================================
// 3. Deterministic In-Process Sparse-to-Dense Vector Math
// ==============================================================================

const TOKEN_SPLIT_REGEX = /[^a-zA-Z0-9_\-]+/;
const FNV_PRIME = 16777619;
const FNV_OFFSET = 2166136261;

export function hashToken(token: string): number {
  let h = FNV_OFFSET;
  const buf = Buffer.from(token, "utf8");
  for (let i = 0; i < buf.length; i++) {
    h = Math.imul(h ^ buf[i], FNV_PRIME) >>> 0;
  }
  return h;
}

export function computeDeterministicEmbedding(text: string, dimension: number = 64): number[] {
  if (!text || !text.trim()) {
    return new Array(dimension).fill(0.0);
  }

  const normalized = text.toLowerCase().trim();
  const rawWords = normalized.split(TOKEN_SPLIT_REGEX).filter((w) => w.length > 0);

  const tokens: string[] = [...rawWords];
  for (const word of rawWords) {
    if (word.length >= 3) {
      for (let i = 0; i < word.length - 2; i++) {
        tokens.push(word.slice(i, i + 3));
      }
    }
  }

  if (tokens.length === 0) {
    return new Array(dimension).fill(0.0);
  }

  const counts: Record<string, number> = {};
  for (const tok of tokens) {
    counts[tok] = (counts[tok] || 0) + 1;
  }

  const vec = new Array(dimension).fill(0.0);
  for (const [tok, count] of Object.entries(counts)) {
    const thash = hashToken(tok);
    const bucket = thash % dimension;
    const sign = (thash >>> 31) === 0 ? 1.0 : -1.0;
    const weight = 1.0 + Math.log1p(count);
    vec[bucket] += sign * weight;
  }

  let normSq = 0.0;
  for (let i = 0; i < dimension; i++) {
    normSq += vec[i] * vec[i];
  }
  const norm = Math.sqrt(normSq);
  if (norm > 1e-9) {
    for (let i = 0; i < dimension; i++) {
      vec[i] /= norm;
    }
  }

  return vec;
}

export function cosineSimilarity(vecA: number[], vecB: number[]): number {
  if (vecA.length !== vecB.length || vecA.length === 0) {
    return 0.0;
  }
  let dot = 0.0;
  for (let i = 0; i < vecA.length; i++) {
    dot += vecA[i] * vecB[i];
  }
  return Math.max(-1.0, Math.min(1.0, dot));
}

export function computeCanonicalGroundingHash(params: {
  receiptId: string;
  claimId: string;
  tenantId: string;
  verdict: string;
  overallScore: number;
  matchedEvidenceIds: string[];
}): string {
  const canonicalPayload = {
    claim_id: params.claimId,
    matched_evidence_ids: [...params.matchedEvidenceIds].sort(),
    overall_score: Math.round(params.overallScore * 10000) / 10000,
    receipt_id: params.receiptId,
    tenant_id: params.tenantId,
    verdict: params.verdict,
  };
  const rawJson = JSON.stringify(canonicalPayload);
  const digest = crypto.createHash("sha256").update(rawJson, "utf8").digest("hex");
  return `sw:ground:v1:${digest}`;
}

// ==============================================================================
// 4. Vector Evidence Store
// ==============================================================================

export class VectorEvidenceStore {
  dimension: number = 64;
  private evidence: Map<string, Map<string, VectorEvidence>> = new Map();

  constructor(dimension: number = 64) {
    this.dimension = dimension;
  }

  add(evidence: VectorEvidence): void {
    if (!this.evidence.has(evidence.tenantId)) {
      this.evidence.set(evidence.tenantId, new Map());
    }
    this.evidence.get(evidence.tenantId)!.set(evidence.evidenceId, evidence);
  }

  get(evidenceId: string, tenantId: string = "tenant-default"): VectorEvidence | undefined {
    return this.evidence.get(tenantId)?.get(evidenceId);
  }

  listAll(tenantId: string = "tenant-default", modality?: EvidenceModality): VectorEvidence[] {
    const tenantMap = this.evidence.get(tenantId);
    if (!tenantMap) return [];
    const items = Array.from(tenantMap.values());
    if (modality) {
      return items.filter((e) => e.modality === modality);
    }
    return items;
  }

  remove(evidenceId: string, tenantId: string = "tenant-default"): boolean {
    const tenantMap = this.evidence.get(tenantId);
    if (!tenantMap) return false;
    return tenantMap.delete(evidenceId);
  }

  clear(tenantId?: string): void {
    if (tenantId) {
      this.evidence.get(tenantId)?.clear();
    } else {
      this.evidence.clear();
    }
  }

  count(tenantId?: string): number {
    if (tenantId) {
      return this.evidence.get(tenantId)?.size ?? 0;
    }
    let total = 0;
    for (const map of this.evidence.values()) {
      total += map.size;
    }
    return total;
  }
}

// ==============================================================================
// 5. Cognitive Grounding & Vector Evidence Fusion Engine
// ==============================================================================

export class CognitiveGroundingEngine {
  dimension: number = 64;
  defaultThreshold: number = 0.70;
  partialThreshold: number = 0.45;
  driftThreshold: number = 0.15;
  maxDriftCountBeforeTrip: number = 3;

  store: VectorEvidenceStore;

  private circuitTripped: boolean = false;
  private receipts: GroundingReceipt[] = [];
  private consecutiveDriftCounts: Map<string, number> = new Map();
  private totalVerifications: number = 0;
  private verdictCounts: Record<string, number> = {
    GROUNDED: 0,
    PARTIALLY_GROUNDED: 0,
    UNGROUNDED: 0,
    DRIFT_DETECTED: 0,
  };

  constructor(config?: GroundingConfig) {
    this.dimension = config?.dimension ?? 64;
    this.defaultThreshold = config?.defaultThreshold ?? 0.70;
    this.partialThreshold = config?.partialThreshold ?? 0.45;
    this.driftThreshold = config?.driftThreshold ?? 0.15;
    this.maxDriftCountBeforeTrip = config?.maxDriftCountBeforeTrip ?? 3;
    this.store = new VectorEvidenceStore(this.dimension);
  }

  tripCircuit(): void {
    this.circuitTripped = true;
  }

  resetCircuit(): void {
    this.circuitTripped = false;
    this.consecutiveDriftCounts.clear();
  }

  isCircuitTripped(): boolean {
    return this.circuitTripped;
  }

  registerEvidence(params: {
    modality: EvidenceModality;
    content: string;
    sourceReference: string;
    confidenceWeight?: number;
    metadata?: Record<string, any>;
    tenantId?: string;
    evidenceId?: string;
  }): VectorEvidence {
    const evId = params.evidenceId ?? `ev-${crypto.randomBytes(6).toString("hex")}`;
    const tenantId = params.tenantId ?? "tenant-default";
    const embedding = computeDeterministicEmbedding(params.content, this.dimension);

    const evidence = new VectorEvidence({
      evidenceId: evId,
      tenantId,
      modality: params.modality,
      content: params.content,
      embedding,
      sourceReference: params.sourceReference,
      confidenceWeight: Math.max(0.01, Math.min(1.0, params.confidenceWeight ?? 1.0)),
      metadata: params.metadata,
    });

    this.store.add(evidence);
    return evidence;
  }

  verifyGrounding(params: {
    claim: string;
    minThreshold?: number;
    requiredModalities?: EvidenceModality[];
    tenantId?: string;
    strict?: boolean;
    claimId?: string;
  }): GroundingReceipt {
    const t0 = performance.now();
    const cId = params.claimId ?? `claim-${crypto.randomBytes(6).toString("hex")}`;
    const tenantId = params.tenantId ?? "tenant-default";
    const threshold = params.minThreshold ?? this.defaultThreshold;

    if (this.circuitTripped) {
      throw new GroundingCircuitTrippedError(
        `Cognitive grounding rejected for claim '${cId}': Safety circuit is tripped (fail-closed).`
      );
    }

    const allEvidence = this.store.listAll(tenantId);
    if (allEvidence.length === 0) {
      const durationUs = (performance.now() - t0) * 1000.0;
      const receipt = this.mintReceipt({
        claimId: cId,
        tenantId,
        verdict: GroundingVerdict.UNGROUNDED,
        overallScore: 0.0,
        fusedModalities: {},
        matchedEvidenceIds: [],
        durationUs,
      });
      this.recordVerdict(tenantId, GroundingVerdict.UNGROUNDED);
      if (params.strict) {
        throw new UngroundedInferenceError(
          `Claim '${cId}' is UNGROUNDED (evidence store empty, score 0.00 < threshold ${threshold.toFixed(2)}).`
        );
      }
      return receipt;
    }

    const claimVec = computeDeterministicEmbedding(params.claim, this.dimension);

    const evidenceByMod: Map<string, VectorEvidence[]> = new Map();
    for (const ev of allEvidence) {
      if (!evidenceByMod.has(ev.modality)) {
        evidenceByMod.set(ev.modality, []);
      }
      evidenceByMod.get(ev.modality)!.push(ev);
    }

    if (params.requiredModalities && params.requiredModalities.length > 0) {
      for (const reqMod of params.requiredModalities) {
        if (!evidenceByMod.has(reqMod) || evidenceByMod.get(reqMod)!.length === 0) {
          const durationUs = (performance.now() - t0) * 1000.0;
          const receipt = this.mintReceipt({
            claimId: cId,
            tenantId,
            verdict: GroundingVerdict.UNGROUNDED,
            overallScore: 0.0,
            fusedModalities: {},
            matchedEvidenceIds: [],
            durationUs,
          });
          this.recordVerdict(tenantId, GroundingVerdict.UNGROUNDED);
          if (params.strict) {
            throw new GroundingModalityMismatchError(
              `Required evidence modality '${reqMod}' not present in store for tenant '${tenantId}'.`
            );
          }
          return receipt;
        }
      }
    }

    const targetModalities =
      params.requiredModalities && params.requiredModalities.length > 0
        ? params.requiredModalities
        : Array.from(evidenceByMod.keys());

    const fusedModalities: Record<string, number> = {};
    const matchedEvidenceIds: string[] = [];

    for (const mod of targetModalities) {
      const evList = evidenceByMod.get(mod) ?? [];
      if (evList.length === 0) continue;

      let bestSim = -1.0;
      let bestEvId: string | null = null;

      for (const ev of evList) {
        const sim = cosineSimilarity(claimVec, ev.embedding);
        const weightedSim = sim * ev.confidenceWeight;
        if (weightedSim > bestSim) {
          bestSim = weightedSim;
          bestEvId = ev.evidenceId;
        }
      }

      fusedModalities[mod] = Math.max(0.0, bestSim);
      if (bestEvId && bestSim > 0.0) {
        matchedEvidenceIds.push(bestEvId);
      }
    }

    const modVals = Object.values(fusedModalities);
    let overallScore = 0.0;
    if (modVals.length > 0) {
      if (params.requiredModalities && params.requiredModalities.length > 0) {
        overallScore = modVals.reduce((a, b) => a + b, 0) / modVals.length;
      } else {
        overallScore = Math.max(...modVals);
      }
    }

    let verdict: GroundingVerdict;
    if (overallScore >= threshold) {
      verdict = GroundingVerdict.GROUNDED;
    } else if (overallScore >= this.partialThreshold) {
      verdict = GroundingVerdict.PARTIALLY_GROUNDED;
    } else if (overallScore < this.driftThreshold) {
      verdict = GroundingVerdict.DRIFT_DETECTED;
    } else {
      verdict = GroundingVerdict.UNGROUNDED;
    }

    const durationUs = (performance.now() - t0) * 1000.0;

    const receipt = this.mintReceipt({
      claimId: cId,
      tenantId,
      verdict,
      overallScore,
      fusedModalities,
      matchedEvidenceIds,
      durationUs,
    });

    this.recordVerdict(tenantId, verdict);

    if (params.strict && (verdict === GroundingVerdict.UNGROUNDED || verdict === GroundingVerdict.DRIFT_DETECTED)) {
      throw new UngroundedInferenceError(
        `Claim '${cId}' failed grounding verification: verdict=${verdict}, score=${overallScore.toFixed(4)} < threshold=${threshold.toFixed(2)}.`
      );
    }

    return receipt;
  }

  fuseEvidence(params: {
    claim: string;
    evidenceIds: string[];
    modalityWeights?: Record<string, number>;
    tenantId?: string;
  }): GroundingReceipt {
    const t0 = performance.now();
    const claimId = `claim-${crypto.randomBytes(6).toString("hex")}`;
    const tenantId = params.tenantId ?? "tenant-default";

    if (this.circuitTripped) {
      throw new GroundingCircuitTrippedError("Cannot fuse evidence: safety circuit is tripped.");
    }

    const selectedEvidence: VectorEvidence[] = [];
    for (const evId of params.evidenceIds) {
      const ev = this.store.get(evId, tenantId);
      if (!ev) {
        throw new EvidenceNotFoundError(`Evidence '${evId}' not found for tenant '${tenantId}'.`);
      }
      selectedEvidence.push(ev);
    }

    const claimVec = computeDeterministicEmbedding(params.claim, this.dimension);

    const modScores: Record<string, number[]> = {};
    for (const ev of selectedEvidence) {
      if (!modScores[ev.modality]) {
        modScores[ev.modality] = [];
      }
      const sim = cosineSimilarity(claimVec, ev.embedding) * ev.confidenceWeight;
      modScores[ev.modality].push(Math.max(0.0, sim));
    }

    const fusedModalities: Record<string, number> = {};
    for (const [mod, scores] of Object.entries(modScores)) {
      fusedModalities[mod] = Math.max(...scores);
    }

    const weights = params.modalityWeights ?? {};
    let totalWeight = 0;
    let weightedSum = 0;

    for (const [mod, score] of Object.entries(fusedModalities)) {
      const w = weights[mod] ?? 1.0;
      totalWeight += w;
      weightedSum += score * w;
    }

    const overallScore = totalWeight > 0 ? weightedSum / totalWeight : 0.0;

    let verdict: GroundingVerdict;
    if (overallScore >= this.defaultThreshold) {
      verdict = GroundingVerdict.GROUNDED;
    } else if (overallScore >= this.partialThreshold) {
      verdict = GroundingVerdict.PARTIALLY_GROUNDED;
    } else if (overallScore < this.driftThreshold) {
      verdict = GroundingVerdict.DRIFT_DETECTED;
    } else {
      verdict = GroundingVerdict.UNGROUNDED;
    }

    const durationUs = (performance.now() - t0) * 1000.0;

    const receipt = this.mintReceipt({
      claimId,
      tenantId,
      verdict,
      overallScore,
      fusedModalities,
      matchedEvidenceIds: params.evidenceIds,
      durationUs,
    });

    this.recordVerdict(tenantId, verdict);
    return receipt;
  }

  private mintReceipt(params: {
    claimId: string;
    tenantId: string;
    verdict: GroundingVerdict;
    overallScore: number;
    fusedModalities: Record<string, number>;
    matchedEvidenceIds: string[];
    durationUs: number;
  }): GroundingReceipt {
    const receiptId = `rcpt-ground-${crypto.randomBytes(6).toString("hex")}`;
    const canonicalHash = computeCanonicalGroundingHash({
      receiptId,
      claimId: params.claimId,
      tenantId: params.tenantId,
      verdict: params.verdict,
      overallScore: params.overallScore,
      matchedEvidenceIds: params.matchedEvidenceIds,
    });

    const receipt = new GroundingReceipt({
      receiptId,
      claimId: params.claimId,
      tenantId: params.tenantId,
      verdict: params.verdict,
      overallScore: params.overallScore,
      fusedModalities: params.fusedModalities,
      matchedEvidenceIds: params.matchedEvidenceIds,
      durationUs: params.durationUs,
      canonicalHash,
    });

    this.receipts.push(receipt);
    return receipt;
  }

  private recordVerdict(tenantId: string, verdict: GroundingVerdict): void {
    this.totalVerifications++;
    this.verdictCounts[verdict] = (this.verdictCounts[verdict] || 0) + 1;

    if (verdict === GroundingVerdict.UNGROUNDED || verdict === GroundingVerdict.DRIFT_DETECTED) {
      const cnt = (this.consecutiveDriftCounts.get(tenantId) || 0) + 1;
      this.consecutiveDriftCounts.set(tenantId, cnt);
      if (cnt >= this.maxDriftCountBeforeTrip) {
        this.circuitTripped = true;
      }
    } else {
      this.consecutiveDriftCounts.set(tenantId, 0);
    }
  }

  getReceipts(tenantId?: string, limit: number = 100): GroundingReceipt[] {
    let res = this.receipts;
    if (tenantId) {
      res = res.filter((r) => r.tenantId === tenantId);
    }
    return res.slice(-limit).reverse();
  }

  getReceipt(receiptId: string): GroundingReceipt | undefined {
    return this.receipts.find((r) => r.receiptId === receiptId);
  }

  status(): Record<string, any> {
    return {
      circuit_tripped: this.circuitTripped,
      total_evidence_count: this.store.count(),
      total_verifications: this.totalVerifications,
      verdict_counts: { ...this.verdictCounts },
      active_tenants: this.consecutiveDriftCounts.size,
      embedding_dimension: this.dimension,
      default_threshold: this.defaultThreshold,
      status_badge: this.circuitTripped ? "GROUNDING: CIRCUIT TRIPPED" : "GROUNDING: FUSION OK",
    };
  }
}

// Global singleton instance
export const globalCognitiveGroundingEngine = new CognitiveGroundingEngine();
