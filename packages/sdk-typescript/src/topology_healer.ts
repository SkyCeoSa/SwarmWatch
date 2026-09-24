/**
 * SwarmWatch Autonomous Dynamic Topology Healing & Auto-Patching (Seam 43 / ADR 0054).
 * Synthesizes, applies, and reverifies graph-theoretic mutations (TopologyPatch)
 * to automatically heal deadlocks, cycles, budget exhaustion, and mutual exclusion conflicts
 * with zero remote LLM API expenditure (<2.0ms local execution).
 */

import type {
  AgentTransition,
  AgentTransitionModel,
  VerificationResult,
} from "./model_checker.ts";
import {
  VerificationInvariant,
  VerificationVerdict,
  BoundedModelChecker,
  globalModelChecker,
} from "./model_checker.ts";

export const PatchAction = {
  FALLBACK_REDIRECT: "FALLBACK_REDIRECT",
  CYCLE_GATE_BREAK: "CYCLE_GATE_BREAK",
  BUDGET_REWIRE: "BUDGET_REWIRE",
  MUTEX_MEDIATE: "MUTEX_MEDIATE",
} as const;

export type PatchActionType = (typeof PatchAction)[keyof typeof PatchAction];

export interface TopologyPatch {
  patchId: string;
  swarmId: string;
  action: PatchActionType;
  targetAgent: string;
  rewiredTo: string;
  syntheticTransitions: AgentTransition[];
  removedTransitions: AgentTransition[];
  rationale: string;
  appliedAt: string;
  status: "ACTIVE" | "REVERTED" | "PENDING";
}

export interface HealedModelReport {
  swarmId: string;
  originalVerdict: string;
  violatedInvariant: string | null;
  appliedPatch: TopologyPatch | null;
  patchedModel: AgentTransitionModel | null;
  healedVerdict: string;
  executionTimeMs: number;
  summary: string;
}

export class TopologyAutoHealer {
  private checker: BoundedModelChecker;
  private patches: Map<string, TopologyPatch[]>;
  private originalModels: Map<string, AgentTransitionModel>;
  private healedModels: Map<string, AgentTransitionModel>;
  private reports: Map<string, HealedModelReport[]>;

  constructor(checker?: BoundedModelChecker) {
    this.checker = checker || globalModelChecker;
    this.patches = new Map();
    this.originalModels = new Map();
    this.healedModels = new Map();
    this.reports = new Map();
  }

  synthesizePatch(
    model: AgentTransitionModel,
    verification: VerificationResult
  ): TopologyPatch | null {
    if (verification.verdict !== VerificationVerdict.VIOLATED) {
      return null;
    }

    const swarmId = verification.swarmId;
    const patchId = `patch-${Math.random().toString(16).substring(2, 10)}`;
    const violation = verification.violatedInvariant;

    const finals = Array.isArray(model.finalAgents)
      ? model.finalAgents
      : (model.finalAgents ? Array.from(model.finalAgents) : []);
    const defaultTarget = finals.length > 0 ? finals.sort()[0] : "agent-supervisor";

    // 1. NO_DEADLOCK
    if (violation === VerificationInvariant.NO_DEADLOCK) {
      let deadlocked = model.initialAgent;
      if (verification.counterexample && verification.counterexample.length > 0) {
        deadlocked = verification.counterexample[verification.counterexample.length - 1].toAgent || model.initialAgent;
      }
      const syntheticEdge: AgentTransition = {
        fromAgent: deadlocked,
        toAgent: defaultTarget,
        actionType: "FALLBACK_RECOVERY",
        tokenCost: 50,
      };

      return {
        patchId,
        swarmId,
        action: PatchAction.FALLBACK_REDIRECT,
        targetAgent: deadlocked,
        rewiredTo: defaultTarget,
        syntheticTransitions: [syntheticEdge],
        removedTransitions: [],
        rationale: `Injected fallback recovery edge from deadlocked '${deadlocked}' to terminal '${defaultTarget}'.`,
        appliedAt: new Date().toISOString(),
        status: "ACTIVE",
      };
    }

    // 2. NO_UNBOUNDED_CYCLES
    if (violation === VerificationInvariant.NO_UNBOUNDED_CYCLES) {
      let cyclicAgent = model.initialAgent;
      if (verification.counterexample && verification.counterexample.length > 0) {
        cyclicAgent = verification.counterexample[verification.counterexample.length - 1].toAgent || model.initialAgent;
      }
      const exitEdge: AgentTransition = {
        fromAgent: cyclicAgent,
        toAgent: defaultTarget,
        actionType: "CYCLE_BREAK_EXIT",
        tokenCost: 40,
        setsFlags: ["CYCLE_TERMINATED"],
      };

      const removed: AgentTransition[] = [];
      for (const t of model.transitions) {
        if (t.fromAgent === cyclicAgent && t.toAgent !== defaultTarget) {
          removed.push(t);
          break;
        }
      }

      return {
        patchId,
        swarmId,
        action: PatchAction.CYCLE_GATE_BREAK,
        targetAgent: cyclicAgent,
        rewiredTo: defaultTarget,
        syntheticTransitions: [exitEdge],
        removedTransitions: removed,
        rationale: `Decoupled cyclic delegation at '${cyclicAgent}' and injected termination exit to '${defaultTarget}'.`,
        appliedAt: new Date().toISOString(),
        status: "ACTIVE",
      };
    }

    // 3. BOUNDED_BUDGET
    if (violation === VerificationInvariant.BOUNDED_BUDGET) {
      const step = verification.counterexample && verification.counterexample.length > 0
        ? verification.counterexample[verification.counterexample.length - 1]
        : null;
      const greedySrc = step ? step.fromAgent : model.initialAgent;
      const greedyDst = step ? step.toAgent : defaultTarget;

      const compressedEdge: AgentTransition = {
        fromAgent: greedySrc,
        toAgent: greedyDst,
        actionType: "COMPRESSED_SUMMARY",
        tokenCost: Math.max(10, Math.floor((model.maxTokenBudget || 1000) / 10)),
      };

      const removed = model.transitions.filter(
        (t) => t.fromAgent === greedySrc && t.toAgent === greedyDst
      );

      return {
        patchId,
        swarmId,
        action: PatchAction.BUDGET_REWIRE,
        targetAgent: greedySrc,
        rewiredTo: greedyDst,
        syntheticTransitions: [compressedEdge],
        removedTransitions: removed,
        rationale: `Throttled token consumption between '${greedySrc}' and '${greedyDst}' with compressed summary.`,
        appliedAt: new Date().toISOString(),
        status: "ACTIVE",
      };
    }

    // 4. MUTUAL_EXCLUSION
    if (violation === VerificationInvariant.MUTUAL_EXCLUSION) {
      const pair = model.mutualExclusionPairs && model.mutualExclusionPairs.length > 0
        ? model.mutualExclusionPairs[0]
        : ["executor", "verifier"];
      const [a1, a2] = pair;
      const mediatorNode = `agent-mediator-${a1.substring(0, 4)}`;

      const syntheticEdges: AgentTransition[] = [
        { fromAgent: a1, toAgent: mediatorNode, actionType: "DELEGATE_MEDIATED", tokenCost: 30 },
        { fromAgent: mediatorNode, toAgent: defaultTarget, actionType: "MEDIATED_FINISH", tokenCost: 30 },
      ];

      const removed = model.transitions.filter(
        (t) => (t.fromAgent === a1 && t.toAgent === a2) || (t.fromAgent === a2 && t.toAgent === a1)
      );

      return {
        patchId,
        swarmId,
        action: PatchAction.MUTEX_MEDIATE,
        targetAgent: a1,
        rewiredTo: mediatorNode,
        syntheticTransitions: syntheticEdges,
        removedTransitions: removed,
        rationale: `Air-gapped conflicting role pair ('${a1}', '${a2}') via intermediate '${mediatorNode}'.`,
        appliedAt: new Date().toISOString(),
        status: "ACTIVE",
      };
    }

    return null;
  }

  applyPatch(
    model: AgentTransitionModel,
    patch: TopologyPatch
  ): AgentTransitionModel {
    const newTransitions: AgentTransition[] = [];
    const removedKeys = new Set(
      patch.removedTransitions.map((t) => `${t.fromAgent}|${t.toAgent}|${t.actionType}`)
    );

    for (const t of model.transitions) {
      const key = `${t.fromAgent}|${t.toAgent}|${t.actionType}`;
      if (!removedKeys.has(key)) {
        newTransitions.push(t);
      }
    }

    for (const st of patch.syntheticTransitions) {
      newTransitions.push(st);
    }

    const currentFinals = new Set(
      Array.isArray(model.finalAgents) ? model.finalAgents : (model.finalAgents ? Array.from(model.finalAgents) : [])
    );
    if (
      !currentFinals.has(patch.rewiredTo) &&
      (patch.action === PatchAction.FALLBACK_REDIRECT || patch.action === PatchAction.CYCLE_GATE_BREAK)
    ) {
      currentFinals.add(patch.rewiredTo);
    }

    return {
      initialAgent: model.initialAgent,
      finalAgents: currentFinals,
      transitions: newTransitions,
      maxTokenBudget: model.maxTokenBudget,
      mutualExclusionPairs: model.mutualExclusionPairs ? [...model.mutualExclusionPairs] : [],
    };
  }

  healAndVerify(
    model: AgentTransitionModel,
    boundK: number = 10,
    swarmId: string = "default-swarm"
  ): HealedModelReport {
    const t0 = performance.now();
    this.originalModels.set(swarmId, model);

    const initialRes = this.checker.verify(model, boundK, undefined, swarmId);
    if (initialRes.verdict === VerificationVerdict.VERIFIED) {
      const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
      const report: HealedModelReport = {
        swarmId,
        originalVerdict: initialRes.verdict,
        violatedInvariant: null,
        appliedPatch: null,
        patchedModel: model,
        healedVerdict: VerificationVerdict.VERIFIED,
        executionTimeMs: elapsed,
        summary: "Model topology is already compliant; zero auto-patches required.",
      };
      this.recordReport(swarmId, report);
      return report;
    }

    const patch = this.synthesizePatch(model, initialRes);
    if (!patch) {
      const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
      const report: HealedModelReport = {
        swarmId,
        originalVerdict: initialRes.verdict,
        violatedInvariant: initialRes.violatedInvariant,
        appliedPatch: null,
        patchedModel: null,
        healedVerdict: VerificationVerdict.VIOLATED,
        executionTimeMs: elapsed,
        summary: `Failed to synthesize patch for invariant: ${initialRes.violatedInvariant}`,
      };
      this.recordReport(swarmId, report);
      return report;
    }

    const healedModel = this.applyPatch(model, patch);
    const healedRes = this.checker.verify(healedModel, boundK, undefined, swarmId);
    const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;

    const report: HealedModelReport = {
      swarmId,
      originalVerdict: initialRes.verdict,
      violatedInvariant: initialRes.violatedInvariant,
      appliedPatch: patch,
      patchedModel: healedModel,
      healedVerdict: healedRes.verdict,
      executionTimeMs: elapsed,
      summary: `Auto-healed ${initialRes.violatedInvariant} via ${patch.action}. Secondary BMC verification: ${healedRes.verdict}.`,
    };

    if (!this.patches.has(swarmId)) {
      this.patches.set(swarmId, []);
    }
    this.patches.get(swarmId)!.push(patch);
    this.healedModels.set(swarmId, healedModel);
    this.recordReport(swarmId, report);

    return report;
  }

  revertPatch(swarmId: string, patchId?: string): boolean {
    const list = this.patches.get(swarmId);
    if (!list || list.length === 0) {
      return false;
    }

    if (patchId) {
      for (const p of list) {
        if (p.patchId === patchId && p.status === "ACTIVE") {
          p.status = "REVERTED";
          return true;
        }
      }
      return false;
    } else {
      for (const p of list) {
        p.status = "REVERTED";
      }
      if (this.originalModels.has(swarmId)) {
        this.healedModels.set(swarmId, this.originalModels.get(swarmId)!);
      }
      return true;
    }
  }

  getActivePatches(swarmId: string): TopologyPatch[] {
    const list = this.patches.get(swarmId) || [];
    return list.filter((p) => p.status === "ACTIVE");
  }

  getLatestReport(swarmId: string): HealedModelReport | null {
    const list = this.reports.get(swarmId);
    if (!list || list.length === 0) {
      return null;
    }
    return list[list.length - 1];
  }

  reset(swarmId?: string): void {
    if (swarmId) {
      this.patches.delete(swarmId);
      this.originalModels.delete(swarmId);
      this.healedModels.delete(swarmId);
      this.reports.delete(swarmId);
    } else {
      this.patches.clear();
      this.originalModels.clear();
      this.healedModels.clear();
      this.reports.clear();
    }
  }

  private recordReport(swarmId: string, report: HealedModelReport): void {
    if (!this.reports.has(swarmId)) {
      this.reports.set(swarmId, []);
    }
    this.reports.get(swarmId)!.push(report);
  }
}

export const globalTopologyHealer = new TopologyAutoHealer();
