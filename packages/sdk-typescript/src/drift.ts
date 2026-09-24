/**
 * SwarmWatch Embedding-Free Vector Semantic Drift & Hallucination Divergence Detector
 * (Seam 34 / ADR 0045).
 *
 * TypeScript SDK implementation conforming to Node 22 type-stripping rules.
 * Provides in-process, zero-external-dependency, zero-token-cost mathematical
 * semantic drift detection and echo chamber divergence interception using
 * 64-bit Universal MinHash LSH and Type-Token Ratio dispersion calculus.
 */

import { createHash } from "node:crypto";
import type { SwarmSpan } from "./schema.ts";

export type DivergenceStatus = "nominal" | "warning" | "divergent" | "tripped";

export const DivergenceStatus = {
  NOMINAL: "nominal",
  WARNING: "warning",
  DIVERGENT: "divergent",
  TRIPPED: "tripped",
} as const;

export interface SemanticDriftProfile {
  span_id?: string | null;
  swarm_id: string;
  turn_index: number;
  agent_name: string;
  drift_score: number;
  anchor_similarity: number;
  consecutive_similarity: number;
  lexical_dispersion: number;
  divergence_status: DivergenceStatus;
  top_shift_tokens: string[];
  is_circuit_broken: boolean;
  trip_reason?: string | null;
  timestamp: number;
}

export interface DriftAggregateStats {
  swarm_id: string;
  total_turns: number;
  current_drift_score: number;
  max_drift_score: number;
  mean_drift_score: number;
  current_status: DivergenceStatus;
  is_tripped: boolean;
  trip_reason?: string | null;
  anchor_set: boolean;
}

const MERSENNE_PRIME = 2305843009213693951n; // (1n << 61n) - 1n

const STOP_WORDS = new Set<string>([
  "a", "an", "the", "and", "or", "in", "on", "at", "to", "for", "with",
  "is", "it", "of", "as", "this", "that", "by", "from", "be", "are", "was",
]);

export class MinHashSigner {
  numPermutations: number;
  permutations: Array<[bigint, bigint]>;

  constructor(numPermutations = 64) {
    this.numPermutations = numPermutations;
    this.permutations = this._generatePermutations(numPermutations);
  }

  private _generatePermutations(numPerms: number): Array<[bigint, bigint]> {
    const perms: Array<[bigint, bigint]> = [];
    for (let i = 0; i < numPerms; i++) {
      const seedData = Buffer.from(`minhash_perm_${i}`, "utf-8");
      const h = createHash("sha256").update(seedData).digest();
      const a = (h.readBigUInt64BE(0) % (MERSENNE_PRIME - 1n)) + 1n;
      const b = h.readBigUInt64BE(8) % MERSENNE_PRIME;
      perms.push([a, b]);
    }
    return perms;
  }

  extractShingles(text: string): Set<string> {
    if (!text) return new Set();

    const normalized = text.trim().toLowerCase().replace(/\s+/g, " ");
    const matches = normalized.match(/[a-zA-Z0-9_]+/g) || [];
    const filtered = matches.filter((w) => !STOP_WORDS.has(w));
    const words = filtered.length > 0 ? filtered : matches;

    const shingles = new Set<string>();

    for (let i = 0; i < words.length; i++) {
      shingles.add(`w1:${words[i]}`);
      if (i + 1 < words.length) {
        shingles.add(`w2:${words[i]}_${words[i + 1]}`);
      }
    }

    return shingles;
  }

  private _hashShingle(shingle: string): bigint {
    const digest = createHash("sha256").update(shingle, "utf-8").digest();
    return digest.readBigUInt64BE(0);
  }

  computeSignature(text: string): bigint[] {
    const shingles = this.extractShingles(text);
    if (shingles.size === 0) {
      return new Array(this.numPermutations).fill(0n);
    }

    const hashedShingles: bigint[] = [];
    for (const s of shingles) {
      hashedShingles.push(this._hashShingle(s));
    }

    const signature: bigint[] = [];

    for (let i = 0; i < this.permutations.length; i++) {
      const [a, b] = this.permutations[i];
      let minVal = MERSENNE_PRIME;
      for (let j = 0; j < hashedShingles.length; j++) {
        const val = (a * hashedShingles[j] + b) % MERSENNE_PRIME;
        if (val < minVal) {
          minVal = val;
        }
      }
      signature.push(minVal);
    }

    return signature;
  }

  jaccardSimilarity(sigA: bigint[], sigB: bigint[]): number {
    if (!sigA || !sigB || sigA.length !== sigB.length || sigA.length === 0) {
      return 0.0;
    }
    let matches = 0;
    for (let i = 0; i < sigA.length; i++) {
      if (sigA[i] === sigB[i]) {
        matches++;
      }
    }
    return Math.round((matches / sigA.length) * 10000) / 10000;
  }

  static computeVcr(text: string): { vcr: number; topTokens: string[] } {
    if (!text) return { vcr: 1.0, topTokens: [] };
    const words = text.toLowerCase().match(/[a-zA-Z0-9_]+/g) || [];
    if (words.length === 0) return { vcr: 1.0, topTokens: [] };

    const uniqueWords = Array.from(new Set(words));
    const vcr = Math.round((uniqueWords.length / words.length) * 10000) / 10000;
    return {
      vcr,
      topTokens: uniqueWords.sort().slice(0, 8),
    };
  }
}

interface SwarmDriftState {
  swarmId: string;
  anchorText?: string;
  anchorSig?: bigint[];
  lastSig?: bigint[];
  history: SemanticDriftProfile[];
  consecutiveDivergentCount: number;
  isTripped: boolean;
  tripReason?: string | null;
}

export class SemanticDriftDetector {
  static readonly EXPECTED_OVERLAP_BASELINE = 0.08;

  driftThreshold: number;
  warningThreshold: number;
  minVcrThreshold: number;
  vcrMinTokenLength: number;
  anchorWeight: number;
  localWeight: number;
  consecutiveDivergentLimit: number;
  signer: MinHashSigner;
  private _swarms: Map<string, SwarmDriftState>;

  constructor(options?: {
    driftThreshold?: number;
    warningThreshold?: number;
    minVcrThreshold?: number;
    vcrMinTokenLength?: number;
    anchorWeight?: number;
    localWeight?: number;
    consecutiveDivergentLimit?: number;
  }) {
    this.driftThreshold = options?.driftThreshold ?? 0.72;
    this.warningThreshold = options?.warningThreshold ?? 0.45;
    this.minVcrThreshold = options?.minVcrThreshold ?? 0.20;
    this.vcrMinTokenLength = options?.vcrMinTokenLength ?? 20;
    this.anchorWeight = options?.anchorWeight ?? 0.75;
    this.localWeight = options?.localWeight ?? 0.25;
    this.consecutiveDivergentLimit = options?.consecutiveDivergentLimit ?? 2;
    this.signer = new MinHashSigner();
    this._swarms = new Map();
  }

  private _getOrCreateState(swarmId: string): SwarmDriftState {
    let state = this._swarms.get(swarmId);
    if (!state) {
      state = {
        swarmId,
        history: [],
        consecutiveDivergentCount: 0,
        isTripped: false,
        tripReason: null,
      };
      this._swarms.set(swarmId, state);
    }
    return state;
  }

  setAnchor(swarmId: string, anchorText: string): void {
    const state = this._getOrCreateState(swarmId);
    state.anchorText = anchorText;
    state.anchorSig = this.signer.computeSignature(anchorText);
  }

  evaluateTurn(
    swarmId: string,
    agentName: string,
    text: string,
    options?: {
      spanId?: string | null;
      turnIndex?: number;
    }
  ): SemanticDriftProfile {
    const state = this._getOrCreateState(swarmId);
    const currentTurn = options?.turnIndex ?? state.history.length + 1;
    const sig = this.signer.computeSignature(text);
    const { vcr, topTokens } = MinHashSigner.computeVcr(text);
    const wordsCount = (text.toLowerCase().match(/[a-zA-Z0-9_]+/g) || []).length;

    // First turn establishes root task anchor if not set
    if (!state.anchorSig) {
      state.anchorText = text;
      state.anchorSig = sig;
      state.lastSig = sig;
      const profile: SemanticDriftProfile = {
        span_id: options?.spanId ?? null,
        swarm_id: swarmId,
        turn_index: currentTurn,
        agent_name: agentName,
        drift_score: 0.0,
        anchor_similarity: 1.0,
        consecutive_similarity: 1.0,
        lexical_dispersion: vcr,
        divergence_status: "nominal",
        top_shift_tokens: topTokens,
        is_circuit_broken: false,
        trip_reason: null,
        timestamp: Date.now() / 1000,
      };
      state.history.push(profile);
      return profile;
    }

    const rawAnchorSim = this.signer.jaccardSimilarity(sig, state.anchorSig);
    const rawConsecutiveSim = state.lastSig
      ? this.signer.jaccardSimilarity(sig, state.lastSig)
      : 1.0;

    const normAnchorSim = Math.min(1.0, rawAnchorSim / SemanticDriftDetector.EXPECTED_OVERLAP_BASELINE);
    const normConsecutiveSim = Math.min(
      1.0,
      rawConsecutiveSim / SemanticDriftDetector.EXPECTED_OVERLAP_BASELINE
    );

    const compositeSim = this.anchorWeight * normAnchorSim + this.localWeight * normConsecutiveSim;
    const driftScore = Math.round(Math.max(0.0, Math.min(1.0, 1.0 - compositeSim)) * 10000) / 10000;

    const isEchoChamber = wordsCount >= this.vcrMinTokenLength && vcr < this.minVcrThreshold;
    const isDivergent = driftScore >= this.driftThreshold || isEchoChamber;
    const isWarning = driftScore >= this.warningThreshold;

    let tripTriggered = false;
    let tripReason: string | null = null;
    let status: DivergenceStatus = "nominal";

    if (isEchoChamber) {
      state.consecutiveDivergentCount++;
      if (state.consecutiveDivergentCount >= this.consecutiveDivergentLimit) {
        status = "tripped";
        tripTriggered = true;
        tripReason = `Hallucinatory echo chamber detected: VCR=${vcr.toFixed(2)} across ${wordsCount} tokens`;
      } else {
        status = "divergent";
      }
    } else if (isDivergent) {
      state.consecutiveDivergentCount++;
      if (state.consecutiveDivergentCount >= this.consecutiveDivergentLimit) {
        status = "tripped";
        tripTriggered = true;
        tripReason = `Semantic drift threshold breached: Delta=${driftScore.toFixed(2)} for ${state.consecutiveDivergentCount} consecutive turns`;
      } else {
        status = "divergent";
      }
    } else if (isWarning) {
      state.consecutiveDivergentCount = 0;
      status = "warning";
    } else {
      state.consecutiveDivergentCount = 0;
      status = "nominal";
    }

    if (tripTriggered) {
      state.isTripped = true;
      state.tripReason = tripReason;
    }

    state.lastSig = sig;

    const profile: SemanticDriftProfile = {
      span_id: options?.spanId ?? null,
      swarm_id: swarmId,
      turn_index: currentTurn,
      agent_name: agentName,
      drift_score: driftScore,
      anchor_similarity: Math.round(normAnchorSim * 10000) / 10000,
      consecutive_similarity: Math.round(normConsecutiveSim * 10000) / 10000,
      lexical_dispersion: vcr,
      divergence_status: status,
      top_shift_tokens: topTokens,
      is_circuit_broken: tripTriggered || state.isTripped,
      trip_reason: tripReason || state.tripReason || null,
      timestamp: Date.now() / 1000,
    };

    state.history.push(profile);
    return profile;
  }

  evaluateSpan(span: SwarmSpan): SemanticDriftProfile {
    const swarmId = span.swarm_id || "default-swarm";
    const agentName = span.source_agent || "agent";
    const spanId = span.id;

    const parts: string[] = [];
    if (span.output_payload) {
      if (typeof span.output_payload === "string") parts.push(span.output_payload);
      else parts.push(JSON.stringify(span.output_payload));
    }
    if (span.input_payload) {
      if (typeof span.input_payload === "string") parts.push(span.input_payload);
      else parts.push(JSON.stringify(span.input_payload));
    }

    const text = parts.join(" ").trim() || `action ${span.span_type || "turn"}`;
    return this.evaluateTurn(swarmId, agentName, text, { spanId });
  }

  getTimeline(swarmId: string): SemanticDriftProfile[] {
    const state = this._swarms.get(swarmId);
    return state ? [...state.history] : [];
  }

  getAggregateStats(swarmId: string): DriftAggregateStats {
    const state = this._swarms.get(swarmId);
    if (!state || state.history.length === 0) {
      return {
        swarm_id: swarmId,
        total_turns: 0,
        current_drift_score: 0.0,
        max_drift_score: 0.0,
        mean_drift_score: 0.0,
        current_status: "nominal",
        is_tripped: false,
        trip_reason: null,
        anchor_set: false,
      };
    }

    const driftScores = state.history.map((p) => p.drift_score);
    const current = state.history[state.history.length - 1];
    const sum = driftScores.reduce((acc, v) => acc + v, 0);

    return {
      swarm_id: swarmId,
      total_turns: state.history.length,
      current_drift_score: current.drift_score,
      max_drift_score: Math.max(...driftScores),
      mean_drift_score: Math.round((sum / driftScores.length) * 10000) / 10000,
      current_status: current.divergence_status,
      is_tripped: state.isTripped,
      trip_reason: state.tripReason ?? null,
      anchor_set: state.anchorText !== undefined,
    };
  }

  reset(swarmId?: string): void {
    if (swarmId) {
      this._swarms.delete(swarmId);
    } else {
      this._swarms.clear();
    }
  }
}

export const globalDriftDetector = new SemanticDriftDetector();
