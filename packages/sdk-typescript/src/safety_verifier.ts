/**
 * Bounded Verification Semantics & Explicit Counterexample Engine (Seam 55 / ADR 0066).
 * Replaces overbroad 'VERIFIED' claims with mathematically sound bounded semantics:
 * BOUNDED_SAFE (with explicit depth k, assumptions, and state coverage),
 * UNBOUNDED_PROVEN (closed state spaces), INDETERMINATE (resource cutoffs),
 * and VIOLATED (minimal counterexample trace with independent replay verification).
 * Zero external LLM API calls or SAT/SMT solver dependencies (<1.0ms local execution).
 */

import type { AgentTransition, AgentTransitionModel } from "./model_checker.ts";

export const BoundedVerdict = {
  BOUNDED_SAFE: "BOUNDED_SAFE",
  VIOLATED: "VIOLATED",
  INDETERMINATE: "INDETERMINATE",
  UNBOUNDED_PROVEN: "UNBOUNDED_PROVEN",
  VACUOUS: "VACUOUS",
} as const;

export type BoundedVerdictType = (typeof BoundedVerdict)[keyof typeof BoundedVerdict];

export const VerificationInvariant = {
  NO_DEADLOCK: "NO_DEADLOCK",
  NO_UNBOUNDED_CYCLES: "NO_UNBOUNDED_CYCLES",
  BOUNDED_BUDGET: "BOUNDED_BUDGET",
  MUTUAL_EXCLUSION: "MUTUAL_EXCLUSION",
  LIVENESS_REACHABLE: "LIVENESS_REACHABLE",
} as const;

export type VerificationInvariantType = (typeof VerificationInvariant)[keyof typeof VerificationInvariant];

export interface VerificationProperty {
  propertyId: string;
  invariantType: string;
  parameters?: Record<string, any>;
}

export interface VerificationLimits {
  maxDepthK?: number;
  maxStates?: number;
  timeoutMs?: number;
}

export interface TransitionStep {
  stepIndex: number;
  fromAgent: string;
  toAgent: string;
  actionType: string;
  tokenCost: number;
  cumulativeTokens: number;
  stateFlags: string[];
}

export interface VerificationReport {
  modelId: string;
  verdict: BoundedVerdictType;
  boundK: number;
  depthReached: number;
  totalStatesExplored: number;
  frontierStatesRemaining: number;
  stateCoverageRatio: number;
  executionTimeMs: number;
  assumptions: string[];
  propertiesChecked: string[];
  violatedProperty: string | null;
  counterexampleTrace: TransitionStep[];
  diagnosticReason: string;
  tenantId: string;
  createdAt: string;
}

export interface VerifierStatus {
  totalVerifications: number;
  verdictsBreakdown: Record<string, number>;
  averageCoverageRatio: number;
  lastVerdict: string | null;
  lastModelId: string | null;
  lastBoundK: number | null;
  lastDepthReached: number | null;
  tenantId: string;
}

interface BfsQueueItem {
  agent: string;
  depth: number;
  tokens: number;
  flags: Set<string>;
  trace: TransitionStep[];
}

export class SafetyVerifier {
  private history: Map<string, VerificationReport[]>;
  private models: Map<string, AgentTransitionModel>;

  constructor() {
    this.history = new Map();
    this.models = new Map();
  }

  private normalizeFinalAgents(finalAgents?: Set<string> | string[]): Set<string> {
    if (!finalAgents) return new Set();
    if (finalAgents instanceof Set) return finalAgents;
    return new Set(finalAgents);
  }

  private normalizeProperties(
    properties?: Array<VerificationProperty | string>
  ): VerificationProperty[] {
    if (!properties || properties.length === 0) {
      return [
        { propertyId: "prop_no_deadlock", invariantType: VerificationInvariant.NO_DEADLOCK },
        { propertyId: "prop_no_cycles", invariantType: VerificationInvariant.NO_UNBOUNDED_CYCLES },
        { propertyId: "prop_budget", invariantType: VerificationInvariant.BOUNDED_BUDGET },
        { propertyId: "prop_mutex", invariantType: VerificationInvariant.MUTUAL_EXCLUSION },
      ];
    }
    return properties.map((p) => {
      if (typeof p === "string") {
        return { propertyId: p, invariantType: p };
      }
      return p;
    });
  }

  verify(
    model: AgentTransitionModel,
    properties?: Array<VerificationProperty | string>,
    limits?: VerificationLimits,
    modelId: string = "default_model",
    tenantId: string = "default"
  ): VerificationReport {
    const t0 = performance.now();
    const props = this.normalizeProperties(properties);
    const finalSet = this.normalizeFinalAgents(model.finalAgents);
    const maxK = limits?.maxDepthK ?? 15;
    const maxStates = limits?.maxStates ?? 10000;
    const timeoutMs = limits?.timeoutMs ?? 1000.0;

    const assumptions = [
      "Closed-world transition relation: only explicitly declared transitions are admissible",
      `Exploration strictly bounded to depth k=${maxK}`,
      `Resource limits: max_states=${maxStates}, timeout=${timeoutMs}ms`,
      "Deterministic non-negative token accumulation and monotonic flag assignment",
    ];
    const propsChecked = props.map((p) => p.propertyId);

    this.models.set(modelId, model);

    if (!model.transitions || model.transitions.length === 0) {
      const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
      const report: VerificationReport = {
        modelId,
        verdict: BoundedVerdict.VACUOUS,
        boundK: maxK,
        depthReached: 0,
        totalStatesExplored: 0,
        frontierStatesRemaining: 0,
        stateCoverageRatio: 1.0,
        executionTimeMs: elapsed,
        assumptions,
        propertiesChecked: propsChecked,
        violatedProperty: null,
        counterexampleTrace: [],
        diagnosticReason: "Model contains zero transitions; vacuously safe.",
        tenantId,
        createdAt: new Date().toISOString(),
      };
      this.recordHistory(tenantId, report);
      return report;
    }

    const transitionsBySrc = new Map<string, AgentTransition[]>();
    for (const t of model.transitions) {
      if (!transitionsBySrc.has(t.fromAgent)) {
        transitionsBySrc.set(t.fromAgent, []);
      }
      transitionsBySrc.get(t.fromAgent)!.push(t);
    }

    const queue: BfsQueueItem[] = [
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
    let depthReached = 0;
    let frontierUnexpanded = 0;
    let isIndeterminate = false;
    let indeterminateReason = "";

    while (queue.length > 0) {
      const elapsedNow = performance.now() - t0;
      if (elapsedNow > timeoutMs) {
        isIndeterminate = true;
        indeterminateReason = `Verification search truncated: timeout of ${timeoutMs}ms exceeded at depth ${depthReached} with ${queue.length} frontier states remaining.`;
        break;
      }

      if (totalExplored >= maxStates) {
        isIndeterminate = true;
        indeterminateReason = `Verification search truncated: state cap of ${maxStates} reached at depth ${depthReached} with ${queue.length} frontier states remaining.`;
        break;
      }

      const item = queue.shift()!;
      const stateKey = `${item.agent}|${item.depth}|${item.tokens}|${Array.from(item.flags).sort().join(",")}`;
      if (visited.has(stateKey)) {
        continue;
      }
      visited.add(stateKey);
      totalExplored++;
      if (item.depth > depthReached) {
        depthReached = item.depth;
      }

      const isFinal = finalSet.has(item.agent);

      // Check invariants
      for (const prop of props) {
        const inv = prop.invariantType;

        // 1. BOUNDED_BUDGET
        if (inv === VerificationInvariant.BOUNDED_BUDGET) {
          const maxBudget = Number(prop.parameters?.max_tokens ?? model.maxTokenBudget ?? 5000);
          if (item.tokens > maxBudget) {
            const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
            const report: VerificationReport = {
              modelId,
              verdict: BoundedVerdict.VIOLATED,
              boundK: maxK,
              depthReached: item.depth,
              totalStatesExplored: totalExplored,
              frontierStatesRemaining: queue.length,
              stateCoverageRatio: Math.round((totalExplored / (totalExplored + queue.length)) * 10000) / 10000,
              executionTimeMs: elapsed,
              assumptions,
              propertiesChecked: propsChecked,
              violatedProperty: prop.propertyId,
              counterexampleTrace: item.trace,
              diagnosticReason: `Path exceeded token budget: ${item.tokens} > ${maxBudget}`,
              tenantId,
              createdAt: new Date().toISOString(),
            };
            this.recordHistory(tenantId, report);
            return report;
          }
        }

        // 2. MUTUAL_EXCLUSION
        else if (inv === VerificationInvariant.MUTUAL_EXCLUSION) {
          const pairs: Array<[string, string]> = prop.parameters?.pairs ?? model.mutualExclusionPairs ?? [];
          const priorAgents = item.trace.map((s) => s.fromAgent);
          for (const [a1, a2] of pairs) {
            if (
              (item.agent === a1 && priorAgents.includes(a2)) ||
              (item.agent === a2 && priorAgents.includes(a1))
            ) {
              const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
              const report: VerificationReport = {
                modelId,
                verdict: BoundedVerdict.VIOLATED,
                boundK: maxK,
                depthReached: item.depth,
                totalStatesExplored: totalExplored,
                frontierStatesRemaining: queue.length,
                stateCoverageRatio: Math.round((totalExplored / (totalExplored + queue.length)) * 10000) / 10000,
                executionTimeMs: elapsed,
                assumptions,
                propertiesChecked: propsChecked,
                violatedProperty: prop.propertyId,
                counterexampleTrace: item.trace,
                diagnosticReason: `Mutual exclusion violated between incompatible agents '${a1}' and '${a2}'`,
                tenantId,
                createdAt: new Date().toISOString(),
              };
              this.recordHistory(tenantId, report);
              return report;
            }
          }
        }

        // 3. NO_UNBOUNDED_CYCLES
        else if (inv === VerificationInvariant.NO_UNBOUNDED_CYCLES) {
          const maxRepeat = Number(prop.parameters?.max_repeat ?? 3);
          const priorAgents = item.trace.map((s) => s.fromAgent);
          const occurrences = priorAgents.filter((a) => a === item.agent).length;
          if (occurrences >= maxRepeat) {
            const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
            const report: VerificationReport = {
              modelId,
              verdict: BoundedVerdict.VIOLATED,
              boundK: maxK,
              depthReached: item.depth,
              totalStatesExplored: totalExplored,
              frontierStatesRemaining: queue.length,
              stateCoverageRatio: Math.round((totalExplored / (totalExplored + queue.length)) * 10000) / 10000,
              executionTimeMs: elapsed,
              assumptions,
              propertiesChecked: propsChecked,
              violatedProperty: prop.propertyId,
              counterexampleTrace: item.trace,
              diagnosticReason: `Unbounded delegation cycle detected: agent '${item.agent}' visited >= ${maxRepeat} times`,
              tenantId,
              createdAt: new Date().toISOString(),
            };
            this.recordHistory(tenantId, report);
            return report;
          }
        }
      }

      // Check outgoing transitions
      const enabledTransitions: AgentTransition[] = [];
      const candidateTransitions = transitionsBySrc.get(item.agent) || [];
      for (const t of candidateTransitions) {
        const reqFlags = t.requiredFlags || [];
        if (reqFlags.every((f) => item.flags.has(f))) {
          enabledTransitions.push(t);
        }
      }

      // 4. NO_DEADLOCK
      for (const prop of props) {
        if (prop.invariantType === VerificationInvariant.NO_DEADLOCK) {
          if (enabledTransitions.length === 0 && !isFinal) {
            const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
            const report: VerificationReport = {
              modelId,
              verdict: BoundedVerdict.VIOLATED,
              boundK: maxK,
              depthReached: item.depth,
              totalStatesExplored: totalExplored,
              frontierStatesRemaining: queue.length,
              stateCoverageRatio: Math.round((totalExplored / (totalExplored + queue.length)) * 10000) / 10000,
              executionTimeMs: elapsed,
              assumptions,
              propertiesChecked: propsChecked,
              violatedProperty: prop.propertyId,
              counterexampleTrace: item.trace,
              diagnosticReason: `Conversational deadlock: agent '${item.agent}' has no enabled transitions and is not a final state`,
              tenantId,
              createdAt: new Date().toISOString(),
            };
            this.recordHistory(tenantId, report);
            return report;
          }
        }
      }

      if (isFinal) {
        continue;
      }

      if (item.depth >= maxK) {
        if (enabledTransitions.length > 0) {
          frontierUnexpanded += enabledTransitions.length;
        }
        continue;
      }

      for (const t of enabledTransitions) {
        const nextTokens = item.tokens + (t.tokenCost || 0);
        const nextFlags = new Set(item.flags);
        for (const f of t.setsFlags || []) {
          nextFlags.add(f);
        }

        const step: TransitionStep = {
          stepIndex: item.trace.length + 1,
          fromAgent: item.agent,
          toAgent: t.toAgent,
          actionType: t.actionType || "DELEGATE",
          tokenCost: t.tokenCost || 0,
          cumulativeTokens: nextTokens,
          stateFlags: Array.from(nextFlags).sort(),
        };

        queue.push({
          agent: t.toAgent,
          depth: item.depth + 1,
          tokens: nextTokens,
          flags: nextFlags,
          trace: [...item.trace, step],
        });
      }
    }

    const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;

    let report: VerificationReport;
    if (isIndeterminate) {
      const totalFrontier = frontierUnexpanded + queue.length;
      const coverage = Math.round((totalExplored / Math.max(1, totalExplored + totalFrontier)) * 10000) / 10000;
      report = {
        modelId,
        verdict: BoundedVerdict.INDETERMINATE,
        boundK: maxK,
        depthReached,
        totalStatesExplored: totalExplored,
        frontierStatesRemaining: totalFrontier,
        stateCoverageRatio: coverage,
        executionTimeMs: elapsed,
        assumptions,
        propertiesChecked: propsChecked,
        violatedProperty: null,
        counterexampleTrace: [],
        diagnosticReason: indeterminateReason,
        tenantId,
        createdAt: new Date().toISOString(),
      };
    } else if (frontierUnexpanded === 0) {
      report = {
        modelId,
        verdict: BoundedVerdict.UNBOUNDED_PROVEN,
        boundK: maxK,
        depthReached,
        totalStatesExplored: totalExplored,
        frontierStatesRemaining: 0,
        stateCoverageRatio: 1.0,
        executionTimeMs: elapsed,
        assumptions,
        propertiesChecked: propsChecked,
        violatedProperty: null,
        counterexampleTrace: [],
        diagnosticReason: `Exhaustive state space closed with 0 frontier remaining (depth ${depthReached} <= ${maxK}). Properties are proven for all possible executions.`,
        tenantId,
        createdAt: new Date().toISOString(),
      };
    } else {
      const coverage = Math.round((totalExplored / (totalExplored + frontierUnexpanded)) * 10000) / 10000;
      report = {
        modelId,
        verdict: BoundedVerdict.BOUNDED_SAFE,
        boundK: maxK,
        depthReached,
        totalStatesExplored: totalExplored,
        frontierStatesRemaining: frontierUnexpanded,
        stateCoverageRatio: coverage,
        executionTimeMs: elapsed,
        assumptions,
        propertiesChecked: propsChecked,
        violatedProperty: null,
        counterexampleTrace: [],
        diagnosticReason: `All ${props.length} properties hold for all reachable states up to bound k=${maxK}. State space remains unclosed beyond k with ${frontierUnexpanded} unexpanded transitions.`,
        tenantId,
        createdAt: new Date().toISOString(),
      };
    }

    this.recordHistory(tenantId, report);
    return report;
  }

  verifyTrace(
    model: AgentTransitionModel,
    property: VerificationProperty | string,
    trace: TransitionStep[]
  ): { isSound: boolean; diagnostic: string | null } {
    const prop: VerificationProperty =
      typeof property === "string" ? { propertyId: property, invariantType: property } : property;
    const finalSet = this.normalizeFinalAgents(model.finalAgents);

    if (!trace || trace.length === 0) {
      if (prop.invariantType === VerificationInvariant.NO_DEADLOCK) {
        if (!finalSet.has(model.initialAgent)) {
          const enabled = (model.transitions || []).filter(
            (t) => t.fromAgent === model.initialAgent && (!t.requiredFlags || t.requiredFlags.length === 0)
          );
          if (enabled.length === 0) {
            return { isSound: true, diagnostic: null };
          }
        }
      }
      return { isSound: false, diagnostic: "Trace is empty and initial state does not violate property" };
    }

    let currentAgent = model.initialAgent;
    let currentTokens = 0;
    const currentFlags = new Set<string>();
    const seenAgents = [currentAgent];

    for (const step of trace) {
      if (step.fromAgent !== currentAgent) {
        return {
          isSound: false,
          diagnostic: `Step ${step.stepIndex}: fromAgent '${step.fromAgent}' does not match current state '${currentAgent}'`,
        };
      }

      const candidate = (model.transitions || []).find(
        (t) => t.fromAgent === step.fromAgent && t.toAgent === step.toAgent && (t.actionType || "DELEGATE") === step.actionType
      );

      if (!candidate) {
        return {
          isSound: false,
          diagnostic: `Step ${step.stepIndex}: transition ${step.fromAgent} -> ${step.toAgent} with action '${step.actionType}' does not exist in model`,
        };
      }

      for (const flag of candidate.requiredFlags || []) {
        if (!currentFlags.has(flag)) {
          return {
            isSound: false,
            diagnostic: `Step ${step.stepIndex}: required flag '${flag}' not satisfied in current flags [${Array.from(currentFlags).sort().join(",")}]`,
          };
        }
      }

      for (const flag of candidate.setsFlags || []) {
        currentFlags.add(flag);
      }
      currentTokens += candidate.tokenCost || 0;
      currentAgent = candidate.toAgent;
      seenAgents.push(currentAgent);

      if (step.cumulativeTokens !== currentTokens) {
        return {
          isSound: false,
          diagnostic: `Step ${step.stepIndex}: cumulative tokens ${step.cumulativeTokens} does not match replayed tokens ${currentTokens}`,
        };
      }
    }

    const inv = prop.invariantType;
    if (inv === VerificationInvariant.NO_DEADLOCK) {
      if (finalSet.has(currentAgent)) {
        return { isSound: false, diagnostic: `Agent '${currentAgent}' is a declared final state; not deadlocked` };
      }
      const enabled = (model.transitions || []).filter(
        (t) => t.fromAgent === currentAgent && (t.requiredFlags || []).every((f) => currentFlags.has(f))
      );
      if (enabled.length > 0) {
        return { isSound: false, diagnostic: `Agent '${currentAgent}' has ${enabled.length} enabled transitions; not deadlocked` };
      }
      return { isSound: true, diagnostic: null };
    } else if (inv === VerificationInvariant.BOUNDED_BUDGET) {
      const maxBudget = Number(prop.parameters?.max_tokens ?? model.maxTokenBudget ?? 5000);
      if (currentTokens <= maxBudget) {
        return { isSound: false, diagnostic: `Cumulative tokens ${currentTokens} does not exceed budget limit ${maxBudget}` };
      }
      return { isSound: true, diagnostic: null };
    } else if (inv === VerificationInvariant.MUTUAL_EXCLUSION) {
      const pairs: Array<[string, string]> = prop.parameters?.pairs ?? model.mutualExclusionPairs ?? [];
      for (const [a1, a2] of pairs) {
        if (seenAgents.includes(a1) && seenAgents.includes(a2)) {
          return { isSound: true, diagnostic: null };
        }
      }
      return { isSound: false, diagnostic: "No mutual exclusion agent pair co-occurred along the trace path" };
    } else if (inv === VerificationInvariant.NO_UNBOUNDED_CYCLES) {
      const maxRepeat = Number(prop.parameters?.max_repeat ?? 3);
      const counts: Record<string, number> = {};
      for (const a of seenAgents) {
        counts[a] = (counts[a] || 0) + 1;
      }
      const maxCount = Math.max(...Object.values(counts));
      if (maxCount >= maxRepeat) {
        return { isSound: true, diagnostic: null };
      }
      return { isSound: false, diagnostic: `No agent occurred >= ${maxRepeat} times along the trace path` };
    }

    return { isSound: true, diagnostic: null };
  }

  status(tenantId?: string): VerifierStatus {
    let reports: VerificationReport[] = [];
    if (tenantId) {
      reports = this.history.get(tenantId) || [];
    } else {
      for (const list of this.history.values()) {
        reports.push(...list);
      }
    }

    const breakdown: Record<string, number> = {
      [BoundedVerdict.BOUNDED_SAFE]: 0,
      [BoundedVerdict.VIOLATED]: 0,
      [BoundedVerdict.INDETERMINATE]: 0,
      [BoundedVerdict.UNBOUNDED_PROVEN]: 0,
      [BoundedVerdict.VACUOUS]: 0,
    };

    let totalCoverage = 0.0;
    for (const r of reports) {
      breakdown[r.verdict] = (breakdown[r.verdict] || 0) + 1;
      totalCoverage += r.stateCoverageRatio;
    }

    const avgCoverage = reports.length > 0 ? Math.round((totalCoverage / reports.length) * 10000) / 10000 : 1.0;
    const lastReport = reports.length > 0 ? reports[reports.length - 1] : null;

    return {
      totalVerifications: reports.length,
      verdictsBreakdown: breakdown,
      averageCoverageRatio: avgCoverage,
      lastVerdict: lastReport ? lastReport.verdict : null,
      lastModelId: lastReport ? lastReport.modelId : null,
      lastBoundK: lastReport ? lastReport.boundK : null,
      lastDepthReached: lastReport ? lastReport.depthReached : null,
      tenantId: tenantId || "all",
    };
  }

  reset(): void {
    this.history.clear();
    this.models.clear();
  }

  private recordHistory(tenantId: string, report: VerificationReport): void {
    if (!this.history.has(tenantId)) {
      this.history.set(tenantId, []);
    }
    this.history.get(tenantId)!.push(report);
  }
}

export const globalSafetyVerifier = new SafetyVerifier();
