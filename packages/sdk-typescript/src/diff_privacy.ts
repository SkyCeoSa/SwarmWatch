/**
 * SwarmWatch Differential Privacy & Cryptographic State Anonymization (TypeScript Parity).
 * Implements calibrated Laplace mechanism noise injection for quantitative telemetry fields
 * and deterministic HMAC-SHA256 pseudonymization for agent identities and roles (ADR 0049).
 *
 * Conforms strictly to Node 22 type-stripping rules: no TS enums, no parameter properties.
 */

import * as crypto from "node:crypto";

export interface DifferentialPrivacyPolicy {
  epsilon: number;
  totalEpsilonBudget: number;
  tenantSalt: string;
  anonymizeIdentities: boolean;
  anonymizeMetrics: boolean;
  anonymizePayloads: boolean;
  tokenBounds: [number, number];
  durationBounds: [number, number];
  costBounds: [number, number];
  entropyBounds: [number, number];
}

export interface DifferentialPrivacyStatus {
  epsilon: number;
  totalBudget: number;
  spentBudget: number;
  remainingBudget: number;
  budgetSpentPct: number;
  isExhausted: boolean;
  totalAnonymized: number;
  anonymizeIdentities: boolean;
  anonymizeMetrics: boolean;
  anonymizePayloads: boolean;
}

export class LaplaceMechanism {
  private seed: number | null;

  constructor(seed: number | null = null) {
    this.seed = seed;
  }

  /**
   * Draws a single Laplace noise sample: Lap(0, scale)
   * Scale b = Delta f / epsilon must be > 0.
   */
  public sampleNoise(scale: number): number {
    if (scale <= 0.0) {
      return 0.0;
    }
    // Draw uniform u in (1e-12, 1 - 1e-12)
    const u = Math.max(1e-12, Math.min(1.0 - 1e-12, Math.random()));
    const sgn = u >= 0.5 ? 1.0 : -1.0;
    return -sgn * scale * Math.log(1.0 - 2.0 * Math.abs(u - 0.5));
  }

  /**
   * Clamps value to [lowerBound, upperBound], computes sensitivity Delta f = upperBound - lowerBound,
   * and adds calibrated Laplace noise with scale b = Delta f / epsilon.
   */
  public perturb(
    value: number,
    lowerBound: number,
    upperBound: number,
    epsilon: number
  ): number {
    if (epsilon <= 0.0) {
      return value;
    }
    const clamped = Math.max(lowerBound, Math.min(upperBound, Number(value)));
    const sensitivity = upperBound - lowerBound;
    const scale = sensitivity / epsilon;
    const noise = this.sampleNoise(scale);
    return clamped + noise;
  }
}

export class DifferentialPrivacyEngine {
  public policy: DifferentialPrivacyPolicy;
  public laplace: LaplaceMechanism;
  public spentEpsilon: number;
  public totalAnonymized: number;

  constructor(
    policy?: Partial<DifferentialPrivacyPolicy>,
    seed: number | null = null
  ) {
    this.policy = {
      epsilon: policy?.epsilon ?? 1.0,
      totalEpsilonBudget: policy?.totalEpsilonBudget ?? 100.0,
      tenantSalt: policy?.tenantSalt ?? "swarmwatch_default_salt",
      anonymizeIdentities: policy?.anonymizeIdentities ?? true,
      anonymizeMetrics: policy?.anonymizeMetrics ?? true,
      anonymizePayloads: policy?.anonymizePayloads ?? true,
      tokenBounds: policy?.tokenBounds ?? [0, 50000],
      durationBounds: policy?.durationBounds ?? [0.0, 60000.0],
      costBounds: policy?.costBounds ?? [0.0, 5.0],
      entropyBounds: policy?.entropyBounds ?? [0.0, 8.0],
    };
    this.laplace = new LaplaceMechanism(seed);
    this.spentEpsilon = 0.0;
    this.totalAnonymized = 0;
  }

  /**
   * Computes deterministic HMAC-SHA256 pseudonym preserving causal topological consistency.
   */
  public pseudonymize(text: string, prefix: string = "anon_"): string {
    if (!text) {
      return text;
    }
    const h = crypto
      .createHmac("sha256", this.policy.tenantSalt)
      .update(String(text), "utf-8")
      .digest("hex")
      .slice(0, 12);
    return `${prefix}${h}`;
  }

  /**
   * Transforms a causal span object into a privacy-preserving representation.
   */
  public anonymizeSpan(
    span: Record<string, any>,
    epsilon?: number
  ): Record<string, any> {
    const effEps = epsilon !== undefined && epsilon !== null ? epsilon : this.policy.epsilon;
    const out: Record<string, any> = { ...span };

    // Budget check
    const remainingBudget = Math.max(0.0, this.policy.totalEpsilonBudget - this.spentEpsilon);
    const isBudgetExhausted = remainingBudget <= 0.0;

    if (!isBudgetExhausted) {
      const charge = Math.min(remainingBudget, effEps * 0.01);
      this.spentEpsilon = Math.round((this.spentEpsilon + charge) * 1e6) / 1e6;
    }

    this.totalAnonymized += 1;

    // 1. Identities and Roles
    if (this.policy.anonymizeIdentities) {
      if (out.agent) {
        out.agent = this.pseudonymize(String(out.agent), "agent_");
      }
      if (out.source) {
        out.source = this.pseudonymize(String(out.source), "agent_");
      }
      if (out.target) {
        out.target = this.pseudonymize(String(out.target), "agent_");
      }
      if (out.role) {
        out.role = this.pseudonymize(String(out.role), "role_");
      }
    }

    // 2. Numeric Metrics Perturbation (Laplace Mechanism)
    if (this.policy.anonymizeMetrics) {
      if (out.tokens !== undefined && out.tokens !== null) {
        const [lTok, uTok] = this.policy.tokenBounds;
        const noisyTok = this.laplace.perturb(out.tokens, lTok, uTok, effEps);
        out.tokens = Math.max(0, Math.round(noisyTok));
      }
      if (out.duration !== undefined && out.duration !== null) {
        const [lDur, uDur] = this.policy.durationBounds;
        const noisyDur = this.laplace.perturb(out.duration, lDur, uDur, effEps);
        out.duration = Math.max(0.0, Math.round(noisyDur * 100) / 100);
      }
      if (out.cost !== undefined && out.cost !== null) {
        const [lCost, uCost] = this.policy.costBounds;
        const noisyCost = this.laplace.perturb(out.cost, lCost, uCost, effEps);
        out.cost = Math.max(0.0, Math.round(noisyCost * 1e6) / 1e6);
      }
      if (out.entropy !== undefined && out.entropy !== null) {
        const [lEnt, uEnt] = this.policy.entropyBounds;
        const noisyEnt = this.laplace.perturb(out.entropy, lEnt, uEnt, effEps);
        out.entropy = Math.max(0.0, Math.min(8.0, Math.round(noisyEnt * 1e4) / 1e4));
      }
    }

    // 3. Payload Anonymization
    if (this.policy.anonymizePayloads && out.payload !== undefined && out.payload !== null) {
      const rawPayload = String(out.payload);
      const sha = crypto.createHash("sha256").update(rawPayload, "utf-8").digest("hex").slice(0, 8);
      out.payload = `[ANONYMIZED_PAYLOAD: ${rawPayload.length} chars | SHA256:${sha}]`;
    }

    // 4. Differential Privacy Metadata
    out._privacy = {
      epsilon: effEps,
      mechanism: "Laplace(0, Delta_f / epsilon)",
      anonymized: true,
      budget_exhausted: isBudgetExhausted,
    };

    return out;
  }

  /**
   * Anonymizes an array of spans in sequence.
   */
  public anonymizeBatch(
    spans: Record<string, any>[],
    epsilon?: number
  ): Record<string, any>[] {
    return spans.map((s) => this.anonymizeSpan(s, epsilon));
  }

  /**
   * Returns current budget metrics and active policy flags.
   */
  public getStatus(): DifferentialPrivacyStatus {
    const remaining = Math.max(0.0, this.policy.totalEpsilonBudget - this.spentEpsilon);
    const pct = Math.round((this.spentEpsilon / Math.max(0.001, this.policy.totalEpsilonBudget)) * 10000) / 100;
    return {
      epsilon: this.policy.epsilon,
      totalBudget: this.policy.totalEpsilonBudget,
      spentBudget: this.spentEpsilon,
      remainingBudget: Math.round(remaining * 1e6) / 1e6,
      budgetSpentPct: Math.min(100.0, pct),
      isExhausted: remaining <= 0.0,
      totalAnonymized: this.totalAnonymized,
      anonymizeIdentities: this.policy.anonymizeIdentities,
      anonymizeMetrics: this.policy.anonymizeMetrics,
      anonymizePayloads: this.policy.anonymizePayloads,
    };
  }

  /**
   * Resets spent privacy budget back to zero.
   */
  public resetBudget(): void {
    this.spentEpsilon = 0.0;
  }

  /**
   * Updates the active privacy policy.
   */
  public setPolicy(policy: DifferentialPrivacyPolicy): void {
    this.policy = policy;
  }
}

// Global singleton instance
export const globalPrivacyEngine = new DifferentialPrivacyEngine();
