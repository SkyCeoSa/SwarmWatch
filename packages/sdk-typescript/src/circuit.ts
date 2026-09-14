/**
 * SwarmWatch Zero-Token Shannon Entropy Circuit Breaker (TypeScript Edition).
 * Provides fast mathematical loop detection, sliding-window lexical similarity,
 * quota budget enforcement (max_turns, token_budget, cost_budget),
 * counterfactual savings computation (ADR 0006),
 * and a 2-step intervention engine (warning hint -> SwarmCircuitBreakException).
 */

import { computeShannonEntropy, computeLexicalSimilarity } from "./entropy.ts";
import type { CircuitEvent, CounterfactualSavings } from "./schema.ts";

export type CircuitPresetType = "strict" | "balanced" | "lenient";

export interface CircuitPolicyConfig {
  similarity_threshold?: number;
  entropy_threshold?: number;
  window_size?: number;
  max_warnings?: number;
  max_turns?: number | null;
  token_budget?: number | null;
  cost_budget?: number | null;
  cost_per_token?: number;
}

export class CircuitPolicy {
  public similarity_threshold: number;
  public entropy_threshold: number;
  public window_size: number;
  public max_warnings: number;
  public max_turns: number | null;
  public token_budget: number | null;
  public cost_budget: number | null;
  public cost_per_token: number;

  constructor(config: CircuitPolicyConfig = {}) {
    this.similarity_threshold = config.similarity_threshold ?? 0.85;
    this.entropy_threshold = config.entropy_threshold ?? 0.40;
    this.window_size = config.window_size ?? 3;
    this.max_warnings = config.max_warnings ?? 1;
    this.max_turns = config.max_turns ?? null;
    this.token_budget = config.token_budget ?? null;
    this.cost_budget = config.cost_budget ?? null;
    this.cost_per_token = config.cost_per_token ?? 0.00002;
  }

  public static fromPreset(preset: CircuitPresetType | string): CircuitPolicy {
    const val = String(preset).toLowerCase();
    if (val === "strict") {
      return new CircuitPolicy({
        similarity_threshold: 0.70,
        entropy_threshold: 0.55,
        max_warnings: 1,
        window_size: 3,
      });
    } else if (val === "lenient") {
      return new CircuitPolicy({
        similarity_threshold: 0.95,
        entropy_threshold: 0.25,
        max_warnings: 2,
        window_size: 4,
      });
    } else {
      // balanced
      return new CircuitPolicy({
        similarity_threshold: 0.85,
        entropy_threshold: 0.40,
        max_warnings: 1,
        window_size: 3,
      });
    }
  }
}

export interface CircuitDecision {
  decision: "pass" | "warning" | "trip";
  reason?: string | null;
  metric_val: number;
  warning_hint?: string | null;
  counterfactual_savings?: CounterfactualSavings | null;
}

export class SwarmCircuitBreakException extends Error {
  public reason: string;
  public metric_val: number;
  public agent_id: string;
  public swarm_id: string;
  public savings?: CounterfactualSavings | null;

  constructor(
    reason: string,
    metric_val: number,
    agent_id: string = "agent",
    swarm_id: string = "swarm",
    savings?: CounterfactualSavings | null,
  ) {
    super(
      `[SWARMWATCH CIRCUIT TRIP] Swarm '${swarm_id}' agent '${agent_id}' halted: ` +
      `${reason} (metric=${metric_val.toFixed(4)}). Consecutive warning limit exceeded.`
    );
    this.name = "SwarmCircuitBreakException";
    this.reason = reason;
    this.metric_val = metric_val;
    this.agent_id = agent_id;
    this.swarm_id = swarm_id;
    this.savings = savings;
  }
}

export class EntropyCircuitBreaker {
  public policy: CircuitPolicy;
  private history: string[] = [];
  public consecutive_warnings = 0;
  public turn_count = 0;
  public cumulative_tokens = 0;
  public token_history: number[] = [];

  constructor(options?: { policy?: CircuitPolicy; preset?: CircuitPresetType | string }) {
    if (options?.policy) {
      this.policy = options.policy;
    } else if (options?.preset) {
      this.policy = CircuitPolicy.fromPreset(options.preset);
    } else {
      this.policy = CircuitPolicy.fromPreset("balanced");
    }
  }

  public computeSavings(): CounterfactualSavings {
    let turns_averted = 0;
    if (this.policy.max_turns !== null && this.policy.max_turns > this.turn_count) {
      turns_averted = this.policy.max_turns - this.turn_count;
    }

    const avg_tokens = this.token_history.length > 0
      ? this.token_history.reduce((a, b) => a + b, 0) / this.token_history.length
      : 500.0;

    const tokens_saved = Math.floor(turns_averted * avg_tokens);
    const dollars_saved = Number((tokens_saved * this.policy.cost_per_token).toFixed(4));
    const budget_consumed = Number((this.cumulative_tokens * this.policy.cost_per_token).toFixed(4));

    let budget_protected = 0.0;
    if (this.policy.cost_budget !== null) {
      budget_protected = Math.max(0.0, Number((this.policy.cost_budget - budget_consumed).toFixed(4)));
    } else if (this.policy.token_budget !== null) {
      const remaining_tokens = Math.max(0, this.policy.token_budget - this.cumulative_tokens);
      budget_protected = Number((remaining_tokens * this.policy.cost_per_token).toFixed(4));
    }

    return {
      tokens_saved,
      dollars_saved,
      turns_averted,
      budget_consumed,
      budget_protected,
    };
  }

  public evaluate(
    message: string,
    agent_id: string = "agent",
    swarm_id: string = "swarm",
    token_count?: number | null,
  ): CircuitDecision {
    if (!message || !message.trim()) {
      return { decision: "pass", metric_val: 0.0 };
    }

    // 1. Track turn count and token usage
    const tokens = message.toLowerCase().match(/[\p{L}\p{N}]+/gu) || [];
    const effective_tokens = token_count ?? tokens.length;
    this.turn_count += 1;
    this.cumulative_tokens += effective_tokens;
    this.token_history.push(effective_tokens);

    // 2. Check quota constraints: max_turns, token_budget, cost_budget
    if (this.policy.max_turns !== null && this.turn_count > this.policy.max_turns) {
      const savings = this.computeSavings();
      throw new SwarmCircuitBreakException(
        `Maximum turns ceiling exceeded (${this.turn_count} > ${this.policy.max_turns})`,
        this.turn_count,
        agent_id,
        swarm_id,
        savings,
      );
    }

    if (this.policy.token_budget !== null && this.cumulative_tokens > this.policy.token_budget) {
      const savings = this.computeSavings();
      throw new SwarmCircuitBreakException(
        `Session token budget exceeded (${this.cumulative_tokens} > ${this.policy.token_budget})`,
        this.cumulative_tokens,
        agent_id,
        swarm_id,
        savings,
      );
    }

    if (this.policy.cost_budget !== null) {
      const cost = this.cumulative_tokens * this.policy.cost_per_token;
      if (cost > this.policy.cost_budget) {
        const savings = this.computeSavings();
        throw new SwarmCircuitBreakException(
          `Session cost budget exceeded ($${cost.toFixed(4)} > $${this.policy.cost_budget.toFixed(4)})`,
          cost,
          agent_id,
          swarm_id,
          savings,
        );
      }
    }

    // 3. Compute entropy and sliding-window lexical similarity
    const entropy = computeShannonEntropy(message);
    let max_similarity = 0.0;
    for (const prior of this.history) {
      const sim = computeLexicalSimilarity(message, prior);
      if (sim > max_similarity) {
        max_similarity = sim;
      }
    }

    // 4. Check violation conditions
    let violation = false;
    let reason = "";
    let metric_val = 0.0;

    if (max_similarity >= this.policy.similarity_threshold && this.history.length > 0) {
      violation = true;
      reason = `High lexical repetition detected (${max_similarity.toFixed(2)} >= ${this.policy.similarity_threshold.toFixed(2)})`;
      metric_val = max_similarity;
    } else if (entropy <= this.policy.entropy_threshold && tokens.length >= 5) {
      violation = true;
      reason = `Low Shannon entropy detected (${entropy.toFixed(2)} <= ${this.policy.entropy_threshold.toFixed(2)})`;
      metric_val = entropy;
    }

    if (violation) {
      const savings = this.computeSavings();
      if (this.consecutive_warnings < this.policy.max_warnings) {
        this.consecutive_warnings += 1;
        const hint = (
          `[SWARMWATCH CIRCUIT WARNING] Potential repetitive loop detected: ${reason}. ` +
          "Please diversify your approach, try an alternative tool, or summarize current findings."
        );
        this.appendHistory(message);
        return {
          decision: "warning",
          reason,
          metric_val,
          warning_hint: hint,
          counterfactual_savings: savings,
        };
      } else {
        // Consecutive warning limit exceeded -> Trip circuit breaker!
        throw new SwarmCircuitBreakException(
          reason,
          metric_val,
          agent_id,
          swarm_id,
          savings,
        );
      }
    } else {
      // Message is healthy; reset warning streak
      this.consecutive_warnings = 0;
      this.appendHistory(message);
      return {
        decision: "pass",
        metric_val: max_similarity > 0 ? max_similarity : entropy,
      };
    }
  }

  private appendHistory(message: string): void {
    this.history.push(message);
    if (this.history.length > this.policy.window_size) {
      this.history.shift();
    }
  }

  public reset(): void {
    this.history = [];
    this.consecutive_warnings = 0;
    this.turn_count = 0;
    this.cumulative_tokens = 0;
    this.token_history = [];
  }
}
