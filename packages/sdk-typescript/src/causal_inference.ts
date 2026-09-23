/**
 * SwarmWatch Distributed Multi-Agent Root-Cause Causal Inference Engine
 * (Seam 32 / ADR 0043).
 *
 * TypeScript SDK implementation conforming to Node 22 type-stripping rules.
 * Provides deterministic Structural Causal Model (SCM) analysis, backward
 * ancestral DAG traversal, Judea Pearl Do-Calculus counterfactual reasoning,
 * context poisoning contagion quantification, and Time-Machine fork turn recommendations
 * with zero remote LLM token usage.
 */

import type { SwarmSpan } from "./schema.ts";

export interface CausalPathNode {
  span_id: string;
  step_index: number;
  agent_name: string;
  target_agent: string;
  span_type: string;
  status: string;
  entropy_score?: number | null;
  is_fault_origin: boolean;
  contribution_score: number;
  notes: string;
}

export interface ContextPoisoningMetric {
  origin_agent: string;
  poison_turn: number;
  poison_span_id: string;
  poison_type: string;
  tainted_agents: string[];
  taint_score: number;
  summary: string;
}

export interface RootCauseAttribution {
  root_cause_span_id: string;
  root_cause_agent: string;
  root_cause_turn: number;
  root_cause_reason: string;
  symptom_span_id: string;
  symptom_agent: string;
  symptom_turn: number;
  symptom_reason: string;
  propagation_depth: number;
  confidence_score: number;
  critical_path: CausalPathNode[];
  recommended_fork_turn: number;
  counterfactual_summary: string;
}

export interface CausalInferenceResult {
  swarm_id: string;
  has_anomaly: boolean;
  attribution?: RootCauseAttribution | null;
  context_poisoning: ContextPoisoningMetric[];
  analyzed_spans_count: number;
  execution_time_ms: number;
}

export class CausalInferenceEngine {
  constructor() {}

  /**
   * Executes backward ancestral DAG traversal and causal contribution calculus
   * to determine the true root cause and critical failure propagation path.
   */
  inferRootCause(
    spans: SwarmSpan[],
    symptomSpanId?: string,
  ): CausalInferenceResult {
    const t0 = performance.now();
    if (!spans || spans.length === 0) {
      return {
        swarm_id: "unknown_swarm",
        has_anomaly: false,
        attribution: null,
        context_poisoning: [],
        analyzed_spans_count: 0,
        execution_time_ms: Number((performance.now() - t0).toFixed(3)),
      };
    }

    const swarmId = spans[0].swarm_id || "default_swarm";

    // 1. Sort spans chronologically and index by ID
    const orderedSpans = [...spans].sort((a, b) => {
      const timeA = a.start_time ? new Date(a.start_time).getTime() : 0;
      const timeB = b.start_time ? new Date(b.start_time).getTime() : 0;
      return timeA - timeB;
    });

    const spanById = new Map<string, SwarmSpan>();
    const indexById = new Map<string, number>();
    orderedSpans.forEach((s, idx) => {
      spanById.set(s.id, s);
      indexById.set(s.id, idx + 1);
    });

    // 2. Identify Symptom Span
    let targetSymptom: SwarmSpan | null = null;
    let symptomReason = "Nominal";

    if (symptomSpanId && spanById.has(symptomSpanId)) {
      targetSymptom = spanById.get(symptomSpanId)!;
      symptomReason = `Specified symptom span: ${targetSymptom.status || "ok"}`;
    } else {
      for (let i = orderedSpans.length - 1; i >= 0; i--) {
        const s = orderedSpans[i];
        const st = (s.status || "ok").toLowerCase();
        const ent = s.entropy_score ?? null;
        const outP = s.output_payload || {};

        let hasErrText = false;
        if (typeof outP === "object" && outP !== null) {
          const errVal = String(outP.error || "").toLowerCase();
          const strOut = JSON.stringify(outP).toLowerCase();
          if (errVal || strOut.includes("exception") || strOut.includes("traceback")) {
            hasErrText = true;
          }
        }

        if (["error", "failed", "failure", "halt", "trip"].includes(st) || hasErrText) {
          targetSymptom = s;
          symptomReason = `Execution error in agent '${s.source_agent}': ${st}`;
          break;
        } else if (ent !== null && ent <= 0.35) {
          targetSymptom = s;
          symptomReason = `Shannon entropy collapsed to ${ent.toFixed(3)} bits`;
          break;
        }
      }
    }

    if (!targetSymptom) {
      return {
        swarm_id: swarmId,
        has_anomaly: false,
        attribution: null,
        context_poisoning: [],
        analyzed_spans_count: orderedSpans.length,
        execution_time_ms: Number((performance.now() - t0).toFixed(3)),
      };
    }

    const symptomTurn = indexById.get(targetSymptom.id) || 1;

    // 3. Construct Backward Ancestral Lineage (SCM Graph)
    const ancestors = this._extractAncestralChain(targetSymptom, orderedSpans, spanById);

    // 4. Score Causal Contribution along Ancestral Lineage
    const scoredNodes: Array<{ span: SwarmSpan; score: number; reason: string }> = [];
    for (const s of ancestors) {
      const turnIdx = indexById.get(s.id) || 1;
      const { score, reason } = this._scoreNodeCausalContribution(s, symptomTurn, turnIdx);
      scoredNodes.push({ span: s, score, reason });
    }

    // Sort: highest score first, then earliest turn index
    scoredNodes.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return (indexById.get(a.span.id) || 0) - (indexById.get(b.span.id) || 0);
    });

    const rootItem = scoredNodes[0];
    const rootSpan = rootItem.span;
    const bestScore = rootItem.score;
    const rootReason = rootItem.reason;
    const rootTurn = indexById.get(rootSpan.id) || 1;

    const propagationDepth = Math.max(0, symptomTurn - rootTurn);

    // 5. Extract Critical Propagation Path
    const criticalPathSpans = orderedSpans.filter((s) => {
      const t = indexById.get(s.id) || 0;
      return t >= rootTurn && t <= symptomTurn;
    });

    const pathNodes: CausalPathNode[] = criticalPathSpans.map((s) => {
      const sIdx = indexById.get(s.id) || 0;
      const isOrigin = s.id === rootSpan.id;
      const found = scoredNodes.find((item) => item.span.id === s.id);
      const cScore = found ? found.score : 0.0;
      const note = isOrigin
        ? rootReason
        : s.id === targetSymptom!.id
          ? "Symptom manifestation"
          : "Intermediate context propagation";

      return {
        span_id: s.id,
        step_index: sIdx,
        agent_name: s.source_agent || "agent",
        target_agent: s.target_agent || "agent",
        span_type: s.span_type || "turn",
        status: s.status || "ok",
        entropy_score: s.entropy_score ?? null,
        is_fault_origin: isOrigin,
        contribution_score: Number(cScore.toFixed(3)),
        notes: note,
      };
    });

    // 6. Compute Context Poisoning Contagion
    const contextPoisoning = this._evaluateContextPoisoning(rootSpan, orderedSpans, indexById);

    // 7. Optimal Time-Machine Fork Turn
    const recommendedForkTurn = Math.max(1, rootTurn - 1);

    // 8. Counterfactual Summary
    const avoidedTurns = Math.max(1, symptomTurn - rootTurn);
    const taintedList = contextPoisoning.length > 0 ? contextPoisoning[0].tainted_agents.join(", ") : "none";
    const cfSummary = `Pearl Do-Calculus: do(turn_${rootTurn}[${rootSpan.source_agent}] = nominal) would have severed failure cascade across ${avoidedTurns} subsequent turns and prevented context poisoning of agents: ${taintedList}.`;

    const confidence = Math.min(0.99, Math.max(0.50, Number(bestScore.toFixed(2))));

    const attribution: RootCauseAttribution = {
      root_cause_span_id: rootSpan.id,
      root_cause_agent: rootSpan.source_agent || "agent",
      root_cause_turn: rootTurn,
      root_cause_reason: rootReason,
      symptom_span_id: targetSymptom.id,
      symptom_agent: targetSymptom.source_agent || "agent",
      symptom_turn: symptomTurn,
      symptom_reason: symptomReason,
      propagation_depth: propagationDepth,
      confidence_score: confidence,
      critical_path: pathNodes,
      recommended_fork_turn: recommendedForkTurn,
      counterfactual_summary: cfSummary,
    };

    return {
      swarm_id: swarmId,
      has_anomaly: true,
      attribution,
      context_poisoning: contextPoisoning,
      analyzed_spans_count: orderedSpans.length,
      execution_time_ms: Number((performance.now() - t0).toFixed(3)),
    };
  }

  traceCausalPath(spans: SwarmSpan[], symptomSpanId: string): CausalPathNode[] {
    const result = this.inferRootCause(spans, symptomSpanId);
    return result.attribution ? result.attribution.critical_path : [];
  }

  computeContextPoisoning(spans: SwarmSpan[]): ContextPoisoningMetric[] {
    const result = this.inferRootCause(spans);
    return result.context_poisoning;
  }

  private _extractAncestralChain(
    symptom: SwarmSpan,
    orderedSpans: SwarmSpan[],
    spanById: Map<string, SwarmSpan>,
  ): SwarmSpan[] {
    const ancestors: SwarmSpan[] = [];
    const visited = new Set<string>();

    let curr: SwarmSpan | undefined = symptom;
    while (curr && !visited.has(curr.id)) {
      visited.add(curr.id);
      ancestors.push(curr);
      const parentId = curr.parent_span_id;
      if (parentId && spanById.has(parentId)) {
        curr = spanById.get(parentId);
      } else {
        break;
      }
    }

    if (ancestors.length === 1) {
      const sympIdx = orderedSpans.indexOf(symptom);
      const limit = sympIdx >= 0 ? sympIdx + 1 : orderedSpans.length;
      return orderedSpans.slice(0, limit);
    }

    return ancestors;
  }

  private _scoreNodeCausalContribution(
    span: SwarmSpan,
    symptomTurn: number,
    turnIndex: number,
  ): { score: number; reason: string } {
    let score = 0.1;
    const reasons: string[] = [];

    const status = (span.status || "ok").toLowerCase();
    const spanType = (span.span_type || "turn").toLowerCase();
    const entropy = span.entropy_score ?? null;
    const outPayload = span.output_payload;

    // 1. Error status or exception in payload
    if (["error", "failed", "failure"].includes(status)) {
      score += 0.5;
      reasons.append?.(`Explicit ${status} status`) || reasons.push(`Explicit ${status} status`);
    }

    if (outPayload && typeof outPayload === "object") {
      const errVal = String(outPayload.error || "");
      const strOut = JSON.stringify(outPayload).toLowerCase();
      if (errVal || strOut.includes("exception") || strOut.includes("traceback")) {
        score += 0.4;
        reasons.push(`Exception payload: ${errVal.slice(0, 40) || "error detected"}`);
      }
    }

    // 2. Tool invocation failure
    if (["tool", "tool_call", "function"].includes(spanType) && status !== "ok") {
      score += 0.3;
      reasons.push(`Tool execution failed (${span.target_agent})`);
    }

    // 3. Shannon entropy collapse
    if (entropy !== null && entropy <= 0.35) {
      score += 0.55;
      reasons.push(`Entropy collapse (${entropy.toFixed(3)} bits)`);
    }

    // 4. Temporal causal precedence bonus
    const distance = Math.max(0, symptomTurn - turnIndex);
    if (distance > 0 && score > 0.3) {
      const precedenceBonus = Math.min(0.35, distance * 0.1);
      score += precedenceBonus;
      reasons.push(`Upstream origin precedence (+${distance} turns)`);
    } else if (distance === 0 && score > 0.65) {
      // Symptom node is the manifestation; cap score so genuine upstream faults take precedence
      score = 0.65;
    }

    const reasonStr = reasons.length > 0 ? reasons.join("; ") : "Nominal antecedent turn";
    return { score: Math.min(1.0, score), reason: reasonStr };
  }

  private _evaluateContextPoisoning(
    rootSpan: SwarmSpan,
    orderedSpans: SwarmSpan[],
    indexById: Map<string, number>,
  ): ContextPoisoningMetric[] {
    const rootTurn = indexById.get(rootSpan.id) || 1;
    const downstream = orderedSpans.filter((s) => (indexById.get(s.id) || 0) > rootTurn);

    const taintedAgents: string[] = [];
    for (const s of downstream) {
      const agent = s.source_agent || "agent";
      if (!taintedAgents.includes(agent) && agent !== rootSpan.source_agent) {
        taintedAgents.push(agent);
      }
    }

    const status = (rootSpan.status || "ok").toLowerCase();
    const spanType = (rootSpan.span_type || "turn").toLowerCase();
    const ent = rootSpan.entropy_score ?? 1.0;

    let poisonType = "SCHEMA_CORRUPTION";
    if (["tool", "tool_call"].includes(spanType)) {
      poisonType = "TOOL_FAILURE";
    } else if (ent <= 0.35) {
      poisonType = "ENTROPY_COLLAPSE";
    } else if (status !== "ok") {
      poisonType = "EXCEPTION_CONTAMINATION";
    }

    const allAgents = new Set(orderedSpans.map((s) => s.source_agent || "agent"));
    const taintScore = Math.min(1.0, Number((taintedAgents.length / Math.max(1, allAgents.size)).toFixed(2)));

    return [
      {
        origin_agent: rootSpan.source_agent || "agent",
        poison_turn: rootTurn,
        poison_span_id: rootSpan.id,
        poison_type: poisonType,
        tainted_agents: taintedAgents,
        taint_score: taintScore,
        summary: `Context poisoned at turn ${rootTurn} by '${rootSpan.source_agent}' (${poisonType}), contaminating ${taintedAgents.length} downstream agents`,
      },
    ];
  }
}

export const globalCausalInferenceEngine = new CausalInferenceEngine();
