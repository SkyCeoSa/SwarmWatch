/**
 * SwarmWatch Bounded Model Checking & Multi-Agent Invariant Verification (Seam 42 / ADR 0053).
 * Exhaustively explores agent interaction graphs up to depth bound k to mathematically prove
 * safety and liveness invariants (NO_DEADLOCK, NO_UNBOUNDED_CYCLES, BOUNDED_BUDGET, MUTUAL_EXCLUSION)
 * without remote SAT/SMT solvers or LLM API calls (<1.0ms local execution).
 */

export const VerificationInvariant = {
  NO_DEADLOCK: "NO_DEADLOCK",
  NO_UNBOUNDED_CYCLES: "NO_UNBOUNDED_CYCLES",
  BOUNDED_BUDGET: "BOUNDED_BUDGET",
  MUTUAL_EXCLUSION: "MUTUAL_EXCLUSION",
} as const;

export type VerificationInvariantType = (typeof VerificationInvariant)[keyof typeof VerificationInvariant];

export const VerificationVerdict = {
  VERIFIED: "VERIFIED",
  VIOLATED: "VIOLATED",
  VACUOUS: "VACUOUS",
} as const;

export type VerificationVerdictType = (typeof VerificationVerdict)[keyof typeof VerificationVerdict];

export interface AgentTransition {
  fromAgent: string;
  toAgent: string;
  actionType: string;
  tokenCost: number;
  requiredFlags?: string[];
  setsFlags?: string[];
}

export interface AgentTransitionModel {
  initialAgent: string;
  finalAgents?: Set<string> | string[];
  transitions: AgentTransition[];
  maxTokenBudget?: number;
  mutualExclusionPairs?: Array<[string, string]>;
}

export interface CounterexampleStep {
  stepIndex: number;
  fromAgent: string;
  toAgent: string;
  actionType: string;
  cumulativeTokens: number;
  stateFlags: string[];
}

export interface VerificationResult {
  swarmId: string;
  verdict: VerificationVerdictType;
  invariantsChecked: string[];
  violatedInvariant: string | null;
  maxDepthBound: number;
  totalStatesExplored: number;
  executionTimeMs: number;
  counterexample: CounterexampleStep[];
  summary: string;
}

interface BfsState {
  agent: string;
  depth: number;
  tokens: number;
  flags: Set<string>;
  trace: Array<[string, string, string, number, string[]]>;
}

export class BoundedModelChecker {
  private history: Map<string, VerificationResult[]>;
  private models: Map<string, AgentTransitionModel>;

  constructor() {
    this.history = new Map();
    this.models = new Map();
  }

  registerModel(swarmId: string, model: AgentTransitionModel): void {
    this.models.set(swarmId, model);
  }

  verify(
    model: AgentTransitionModel,
    boundK: number = 10,
    invariants?: string[],
    swarmId: string = "default-swarm"
  ): VerificationResult {
    const t0 = performance.now();
    const checked = invariants && invariants.length > 0 ? invariants : [
      VerificationInvariant.NO_DEADLOCK,
      VerificationInvariant.NO_UNBOUNDED_CYCLES,
      VerificationInvariant.BOUNDED_BUDGET,
      VerificationInvariant.MUTUAL_EXCLUSION,
    ];

    if (!model.transitions || model.transitions.length === 0) {
      const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
      const res: VerificationResult = {
        swarmId,
        verdict: VerificationVerdict.VACUOUS,
        invariantsChecked: checked,
        violatedInvariant: null,
        maxDepthBound: boundK,
        totalStatesExplored: 0,
        executionTimeMs: elapsed,
        counterexample: [],
        summary: "Model contains zero transitions; vacuously verified.",
      };
      this.recordResult(swarmId, res);
      return res;
    }

    const finalSet = new Set<string>(
      Array.isArray(model.finalAgents) ? model.finalAgents : (model.finalAgents ? Array.from(model.finalAgents) : [])
    );
    const maxBudget = model.maxTokenBudget ?? 10000;
    const mutexPairs = model.mutualExclusionPairs ?? [];

    const transitionsBySrc = new Map<string, AgentTransition[]>();
    for (const t of model.transitions) {
      if (!transitionsBySrc.has(t.fromAgent)) {
        transitionsBySrc.set(t.fromAgent, []);
      }
      transitionsBySrc.get(t.fromAgent)!.push(t);
    }

    const queue: BfsState[] = [
      {
        agent: model.initialAgent,
        depth: 0,
        tokens: 0,
        flags: new Set<string>(),
        trace: [],
      },
    ];

    const visited = new Set<string>();
    let totalExplored = 0;

    while (queue.length > 0) {
      const current = queue.shift()!;
      const flagKey = Array.from(current.flags).sort().join(",");
      const stateKey = `${current.agent}|${current.depth}|${current.tokens}|${flagKey}`;

      if (visited.has(stateKey)) {
        continue;
      }
      visited.add(stateKey);
      totalExplored++;

      const isFinal = finalSet.has(current.agent);

      // 1. BOUNDED_BUDGET
      if (checked.includes(VerificationInvariant.BOUNDED_BUDGET)) {
        if (current.tokens > maxBudget) {
          const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
          const res = this.buildViolation(
            swarmId,
            checked,
            VerificationInvariant.BOUNDED_BUDGET,
            boundK,
            totalExplored,
            elapsed,
            current.trace,
            `Path exceeded maximum token budget: ${current.tokens} > ${maxBudget}`
          );
          this.recordResult(swarmId, res);
          return res;
        }
      }

      // 2. MUTUAL_EXCLUSION
      if (checked.includes(VerificationInvariant.MUTUAL_EXCLUSION)) {
        const priorAgents = current.trace.map((step) => step[0]);
        for (const [a1, a2] of mutexPairs) {
          if (
            (current.agent === a1 && priorAgents.includes(a2)) ||
            (current.agent === a2 && priorAgents.includes(a1))
          ) {
            const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
            const res = this.buildViolation(
              swarmId,
              checked,
              VerificationInvariant.MUTUAL_EXCLUSION,
              boundK,
              totalExplored,
              elapsed,
              current.trace,
              `Mutual exclusion violated between incompatible agents '${a1}' and '${a2}'`
            );
            this.recordResult(swarmId, res);
            return res;
          }
        }
      }

      if (isFinal) {
        continue;
      }

      if (current.depth >= boundK) {
        continue;
      }

      const availableTransitions: AgentTransition[] = [];
      const srcTransitions = transitionsBySrc.get(current.agent) ?? [];
      for (const t of srcTransitions) {
        const reqs = t.requiredFlags ?? [];
        if (reqs.every((f) => current.flags.has(f))) {
          availableTransitions.push(t);
        }
      }

      // 3. NO_DEADLOCK
      if (checked.includes(VerificationInvariant.NO_DEADLOCK)) {
        if (availableTransitions.length === 0 && !isFinal) {
          const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
          const res = this.buildViolation(
            swarmId,
            checked,
            VerificationInvariant.NO_DEADLOCK,
            boundK,
            totalExplored,
            elapsed,
            current.trace,
            `Conversational deadlock: agent '${current.agent}' has no transitions and is not FINAL`
          );
          this.recordResult(swarmId, res);
          return res;
        }
      }

      // 4. NO_UNBOUNDED_CYCLES
      if (checked.includes(VerificationInvariant.NO_UNBOUNDED_CYCLES)) {
        const priorAgents = current.trace.map((step) => step[0]);
        const agentCount = priorAgents.filter((a) => a === current.agent).length;
        if (agentCount >= 3) {
          const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
          const res = this.buildViolation(
            swarmId,
            checked,
            VerificationInvariant.NO_UNBOUNDED_CYCLES,
            boundK,
            totalExplored,
            elapsed,
            current.trace,
            `Unbounded delegation cycle detected involving agent '${current.agent}'`
          );
          this.recordResult(swarmId, res);
          return res;
        }
      }

      // Enqueue next states
      for (const t of availableTransitions) {
        const nextTokens = current.tokens + (t.tokenCost || 0);
        const nextFlags = new Set<string>(current.flags);
        if (t.setsFlags) {
          for (const f of t.setsFlags) {
            nextFlags.add(f);
          }
        }
        const step: [string, string, string, number, string[]] = [
          current.agent,
          t.toAgent,
          t.actionType,
          nextTokens,
          Array.from(nextFlags).sort(),
        ];
        queue.push({
          agent: t.toAgent,
          depth: current.depth + 1,
          tokens: nextTokens,
          flags: nextFlags,
          trace: [...current.trace, step],
        });
      }
    }

    const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
    const result: VerificationResult = {
      swarmId,
      verdict: VerificationVerdict.VERIFIED,
      invariantsChecked: checked,
      violatedInvariant: null,
      maxDepthBound: boundK,
      totalStatesExplored: totalExplored,
      executionTimeMs: elapsed,
      counterexample: [],
      summary: `All ${checked.length} invariants mathematically verified up to bound k=${boundK} across ${totalExplored} states.`,
    };
    this.recordResult(swarmId, result);
    return result;
  }

  private buildViolation(
    swarmId: string,
    invariantsChecked: string[],
    violated: string,
    boundK: number,
    totalExplored: number,
    elapsedMs: number,
    trace: Array<[string, string, string, number, string[]]>,
    summary: string
  ): VerificationResult {
    const counterexample: CounterexampleStep[] = trace.map((step, idx) => ({
      stepIndex: idx + 1,
      fromAgent: step[0],
      toAgent: step[1],
      actionType: step[2],
      cumulativeTokens: step[3],
      stateFlags: step[4],
    }));

    return {
      swarmId,
      verdict: VerificationVerdict.VIOLATED,
      invariantsChecked,
      violatedInvariant: violated,
      maxDepthBound: boundK,
      totalStatesExplored: totalExplored,
      executionTimeMs: elapsedMs,
      counterexample,
      summary,
    };
  }

  private recordResult(swarmId: string, result: VerificationResult): void {
    if (!this.history.has(swarmId)) {
      this.history.set(swarmId, []);
    }
    this.history.get(swarmId)!.push(result);
  }

  extractModelFromSpans(spans: Array<Record<string, any>>): AgentTransitionModel {
    if (!spans || spans.length === 0) {
      return {
        initialAgent: "agent-start",
        transitions: [],
      };
    }

    const initialAgent = spans[0].source_agent || spans[0].agent_id || "agent-start";
    const finalAgents = new Set<string>();
    const transitions: AgentTransition[] = [];
    const seenEdges = new Set<string>();

    for (const s of spans) {
      const src = s.source_agent || s.agent_id || "agent";
      const dst = s.target_agent || s.target || "environment";
      const act = s.tool_name || s.action_type || "DELEGATE";
      const tok = Number(s.token_count || 100);

      if (dst === "environment" || dst === "FINAL") {
        finalAgents.add(src);
      }

      const edgeKey = `${src}|${dst}|${act}`;
      if (!seenEdges.has(edgeKey)) {
        seenEdges.add(edgeKey);
        transitions.push({
          fromAgent: src,
          toAgent: dst,
          actionType: act,
          tokenCost: tok,
        });
      }
    }

    if (finalAgents.size === 0 && spans.length > 0) {
      const last = spans[spans.length - 1];
      const lastAgent = last.source_agent || last.target_agent;
      if (lastAgent) {
        finalAgents.add(lastAgent);
      }
    }

    return {
      initialAgent,
      finalAgents,
      transitions,
      maxTokenBudget: 15000,
    };
  }

  getLatestResult(swarmId: string): VerificationResult | null {
    const list = this.history.get(swarmId);
    if (!list || list.length === 0) {
      return null;
    }
    return list[list.length - 1];
  }

  reset(swarmId?: string): void {
    if (swarmId) {
      this.history.delete(swarmId);
      this.models.delete(swarmId);
    } else {
      this.history.clear();
      this.models.clear();
    }
  }
}

export const globalModelChecker = new BoundedModelChecker();
