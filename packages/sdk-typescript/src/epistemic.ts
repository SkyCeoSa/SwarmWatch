/**
 * Dynamic Multi-Agent Epistemic Logic & Knowledge Base State Synchronization Engine (Seam 48 / ADR 0059).
 * Modal epistemic logic (S5 / KD45) tracking, fast contradiction detection, and deterministic AGM belief revision.
 */

import * as crypto from "node:crypto";

export const EpistemicTruthValue = {
  TRUE: "TRUE",
  FALSE: "FALSE",
  UNKNOWN: "UNKNOWN",
  CONTRADICTORY: "CONTRADICTORY",
} as const;

export type EpistemicTruthValueType = typeof EpistemicTruthValue[keyof typeof EpistemicTruthValue];

export const EpistemicStatus = {
  CONSISTENT: "CONSISTENT",
  DISSONANT: "DISSONANT",
  STALE: "STALE",
  UNVERIFIED: "UNVERIFIED",
} as const;

export type EpistemicStatusType = typeof EpistemicStatus[keyof typeof EpistemicStatus];

export interface PropositionalClaimOptions {
  claimId?: string;
  agentId: string;
  topic: string;
  predicate: string;
  truthValue?: EpistemicTruthValueType | string;
  confidence?: number;
  timestamp?: string;
  evidenceSpanId?: string;
  metadata?: Record<string, any>;
  assertedAt?: number;
  isActive?: boolean;
}

export class PropositionalClaim {
  public readonly claimId: string;
  public readonly agentId: string;
  public readonly topic: string;
  public readonly predicate: string;
  public readonly truthValue: EpistemicTruthValueType;
  public confidence: number;
  public readonly timestamp: string;
  public readonly evidenceSpanId?: string;
  public readonly metadata?: Record<string, any>;
  public assertedAt: number;
  public isActive: boolean;

  constructor(opts: PropositionalClaimOptions) {
    this.claimId = opts.claimId || `claim_${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`;
    this.agentId = opts.agentId || "agent-unknown";
    this.topic = opts.topic || "general";
    this.predicate = opts.predicate || "predicate";

    let tv: EpistemicTruthValueType = EpistemicTruthValue.UNKNOWN;
    const rawTv = (opts.truthValue || "TRUE").toUpperCase();
    if (rawTv in EpistemicTruthValue) {
      tv = rawTv as EpistemicTruthValueType;
    }
    this.truthValue = tv;

    const conf = opts.confidence !== undefined ? opts.confidence : 1.0;
    this.confidence = Math.max(0.0, Math.min(1.0, conf));
    this.timestamp = opts.timestamp || new Date().toISOString();
    this.evidenceSpanId = opts.evidenceSpanId;
    this.metadata = opts.metadata;
    this.assertedAt = opts.assertedAt !== undefined ? opts.assertedAt : Date.now();
    this.isActive = opts.isActive !== undefined ? opts.isActive : true;
  }

  public toDict(): Record<string, any> {
    return {
      claim_id: this.claimId,
      agent_id: this.agentId,
      topic: this.topic,
      predicate: this.predicate,
      truth_value: this.truthValue,
      confidence: Math.round(this.confidence * 10000) / 10000,
      timestamp: this.timestamp,
      evidence_span_id: this.evidenceSpanId,
      metadata: this.metadata || {},
      asserted_at: this.assertedAt,
      is_active: this.isActive,
    };
  }

  public toJSON(): Record<string, any> {
    return this.toDict();
  }

  public static fromDict(data: Record<string, any>): PropositionalClaim {
    return new PropositionalClaim({
      claimId: data.claim_id,
      agentId: data.agent_id,
      topic: data.topic,
      predicate: data.predicate,
      truthValue: data.truth_value,
      confidence: data.confidence,
      timestamp: data.timestamp,
      evidenceSpanId: data.evidence_span_id,
      metadata: data.metadata,
      assertedAt: data.asserted_at,
      isActive: data.is_active,
    });
  }
}

export class EpistemicConflict {
  public readonly conflictId: string;
  public readonly topic: string;
  public readonly predicate: string;
  public readonly claimA: PropositionalClaim;
  public readonly claimB: PropositionalClaim;
  public readonly severity: number;
  public readonly detectedAt: string;
  public resolved: boolean;
  public resolutionStrategy?: string;
  public winningClaimId?: string;

  constructor(
    conflictId: string,
    topic: string,
    predicate: string,
    claimA: PropositionalClaim,
    claimB: PropositionalClaim,
    severity: number = 1.0,
    detectedAt?: string
  ) {
    this.conflictId = conflictId;
    this.topic = topic;
    this.predicate = predicate;
    this.claimA = claimA;
    this.claimB = claimB;
    this.severity = Math.round(severity * 10000) / 10000;
    this.detectedAt = detectedAt || new Date().toISOString();
    this.resolved = false;
  }

  public get priorAgentId(): string {
    return this.claimA.agentId;
  }

  public get conflictingAgentId(): string {
    return this.claimB.agentId;
  }

  public get priorTruthValue(): string {
    return this.claimA.truthValue;
  }

  public get conflictingTruthValue(): string {
    return this.claimB.truthValue;
  }

  public toDict(): Record<string, any> {
    return {
      conflict_id: this.conflictId,
      topic: this.topic,
      predicate: this.predicate,
      prior_agent_id: this.priorAgentId,
      prior_truth_value: this.priorTruthValue,
      conflicting_agent_id: this.conflictingAgentId,
      conflicting_truth_value: this.conflictingTruthValue,
      claim_a: this.claimA.toDict(),
      claim_b: this.claimB.toDict(),
      severity: this.severity,
      detected_at: this.detectedAt,
      resolved: this.resolved,
      resolution_strategy: this.resolutionStrategy,
      winning_claim_id: this.winningClaimId,
    };
  }

  public toJSON(): Record<string, any> {
    return this.toDict();
  }
}

export class EpistemicKnowledgeBase {
  public readonly swarmId: string;
  private readonly _claims: Map<string, PropositionalClaim> = new Map();
  private readonly _byPredicate: Map<string, Map<string, PropositionalClaim>> = new Map();
  private readonly _conflicts: Map<string, EpistemicConflict> = new Map();
  private _totalAssertions: number = 0;
  private _totalReconciliations: number = 0;

  constructor(swarmId: string = "swarm-live-01") {
    this.swarmId = swarmId;
  }

  public get claims(): PropositionalClaim[] {
    return Array.from(this._claims.values());
  }

  private _makePredicateKey(topic: string, predicate: string): string {
    return `${topic}:::${predicate}`;
  }

  public assertClaim(
    arg: PropositionalClaim | PropositionalClaimOptions
  ): [PropositionalClaim, boolean, EpistemicConflict | null] {
    this._totalAssertions++;

    const claim = arg instanceof PropositionalClaim ? arg : new PropositionalClaim(arg);
    const key = this._makePredicateKey(claim.topic, claim.predicate);

    if (!this._byPredicate.has(key)) {
      this._byPredicate.set(key, new Map());
    }
    const agentMap = this._byPredicate.get(key)!;

    let detectedConflict: EpistemicConflict | null = null;

    // Check for contradiction with existing claims from other agents
    for (const [otherAgent, otherClaim] of agentMap.entries()) {
      if (otherAgent === claim.agentId) {
        continue; // Self-updates supersede prior belief
      }
      if (!otherClaim.isActive) {
        continue;
      }

      const isContradiction =
        (claim.truthValue === EpistemicTruthValue.TRUE && otherClaim.truthValue === EpistemicTruthValue.FALSE) ||
        (claim.truthValue === EpistemicTruthValue.FALSE && otherClaim.truthValue === EpistemicTruthValue.TRUE);

      if (isContradiction) {
        const conflictId = `conf_${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`;
        const severity = Math.min(claim.confidence, otherClaim.confidence);
        detectedConflict = new EpistemicConflict(
          conflictId,
          claim.topic,
          claim.predicate,
          otherClaim,
          claim,
          severity
        );
        this._conflicts.set(conflictId, detectedConflict);
        break;
      }
    }

    // If self-update, remove old claim from active lookup
    if (agentMap.has(claim.agentId)) {
      const oldClaim = agentMap.get(claim.agentId)!;
      oldClaim.isActive = false;
      this._claims.delete(oldClaim.claimId);
    }

    // Register claim
    this._claims.set(claim.claimId, claim);
    agentMap.set(claim.agentId, claim);

    const isConsistent = detectedConflict === null;
    return [claim, isConsistent, detectedConflict];
  }

  public getClaims(topic?: string, agentId?: string, predicate?: string): PropositionalClaim[] {
    const res: PropositionalClaim[] = [];
    for (const c of this._claims.values()) {
      if (topic && c.topic !== topic) continue;
      if (agentId && c.agentId !== agentId) continue;
      if (predicate && c.predicate !== predicate) continue;
      res.push(c);
    }
    return res.sort((a, b) => a.assertedAt - b.assertedAt);
  }

  public getConflicts(unresolvedOnly: boolean = true): EpistemicConflict[] {
    const all = Array.from(this._conflicts.values());
    if (unresolvedOnly) {
      return all.filter((c) => !c.resolved);
    }
    return all;
  }

  public getEpistemicStatus(): EpistemicStatusType {
    const unresolved = this.getConflicts(true);
    return unresolved.length === 0 ? EpistemicStatus.CONSISTENT : EpistemicStatus.DISSONANT;
  }

  public getMutualKnowledge(): Array<Record<string, any>> {
    const mutual: Array<Record<string, any>> = [];

    for (const [, agentMap] of this._byPredicate.entries()) {
      const activeClaims = Array.from(agentMap.values()).filter((c) => c.isActive);
      if (activeClaims.length >= 2) {
        const distinctValues = new Set(activeClaims.map((c) => c.truthValue));
        if (distinctValues.size === 1 && !distinctValues.has(EpistemicTruthValue.UNKNOWN)) {
          const val = activeClaims[0].truthValue;
          const affirming = activeClaims.map((c) => c.agentId);
          const totalConf = activeClaims.reduce((acc, c) => acc + c.confidence, 0);
          mutual.push({
            topic: activeClaims[0].topic,
            predicate: activeClaims[0].predicate,
            truth_value: val,
            agents_count: affirming.length,
            affirming_agents: affirming,
            agents: affirming,
            avg_confidence: Math.round((totalConf / activeClaims.length) * 10000) / 10000,
          });
        }
      }
    }

    return mutual;
  }

  public computeDissonanceScore(): number {
    const totalPredicates = this._byPredicate.size;
    if (totalPredicates === 0) return 0.0;

    const unresolved = this.getConflicts(true);
    if (unresolved.length === 0) return 0.0;

    const conflictSum = unresolved.reduce((acc, c) => acc + c.severity, 0);
    const score = conflictSum / Math.max(1.0, totalPredicates);
    return Math.round(Math.min(1.0, Math.max(0.0, score)) * 10000) / 10000;
  }

  public reconcileBeliefs(strategy: string = "CONFIDENCE_ENTRENCHMENT"): Record<string, any> {
    this._totalReconciliations++;
    const unresolved = this.getConflicts(true);
    let resolvedCount = 0;
    const retractedClaimIds: string[] = [];

    for (const conf of unresolved) {
      const cA = conf.claimA;
      const cB = conf.claimB;
      let winner: PropositionalClaim;

      if (strategy === "CONFIDENCE_ENTRENCHMENT") {
        if (cA.confidence - cB.confidence >= 0.15) {
          winner = cA;
        } else if (cB.confidence - cA.confidence >= 0.15) {
          winner = cB;
        } else {
          winner = cB.assertedAt >= cA.assertedAt ? cB : cA;
        }
      } else {
        // TEMPORAL_RECENCY
        winner = cB.assertedAt >= cA.assertedAt ? cB : cA;
      }

      const loser = winner === cA ? cB : cA;

      // Apply AGM Contraction on loser
      loser.isActive = false;
      const key = this._makePredicateKey(loser.topic, loser.predicate);
      if (this._byPredicate.has(key)) {
        this._byPredicate.get(key)!.delete(loser.agentId);
      }
      retractedClaimIds.push(loser.claimId);

      conf.resolved = true;
      conf.resolutionStrategy = strategy;
      conf.winningClaimId = winner.claimId;
      resolvedCount++;
    }

    const postStatus = this.getEpistemicStatus();
    const postDissonance = this.computeDissonanceScore();

    return {
      swarm_id: this.swarmId,
      strategy: strategy,
      strategy_applied: strategy,
      conflicts_resolved: resolvedCount,
      reconciled_conflicts_count: resolvedCount,
      retracted_claim_ids: retractedClaimIds,
      retracted_claims_count: retractedClaimIds.length,
      remaining_unresolved: this.getConflicts(true).length,
      new_dissonance_score: postDissonance,
      post_reconciliation_dissonance: postDissonance,
      epistemic_status: postStatus,
    };
  }

  public getStatus(): Record<string, any> {
    const dissonance = this.computeDissonanceScore();
    const unresolved = this.getConflicts(true).length;
    const status = unresolved === 0 ? EpistemicStatus.CONSISTENT : EpistemicStatus.DISSONANT;

    const participatingAgents = new Set<string>();
    for (const c of this._claims.values()) {
      participatingAgents.add(c.agentId);
    }

    return {
      swarm_id: this.swarmId,
      status: status,
      epistemic_status: status,
      dissonance_score: dissonance,
      total_claims: this._claims.size,
      total_propositions: this._claims.size,
      total_predicates: this._byPredicate.size,
      participating_agents_count: participatingAgents.size,
      active_agents_count: participatingAgents.size,
      participating_agents: Array.from(participatingAgents).sort(),
      unresolved_conflicts: unresolved,
      unresolved_conflicts_count: unresolved,
      total_assertions_ingested: this._totalAssertions,
      total_reconciliations_run: this._totalReconciliations,
    };
  }

  public reset(): void {
    this._claims.clear();
    this._byPredicate.clear();
    this._conflicts.clear();
    this._totalAssertions = 0;
    this._totalReconciliations = 0;
  }
}

export class EpistemicEngine {
  private readonly _bases: Map<string, EpistemicKnowledgeBase> = new Map();

  public getKnowledgeBase(swarmId: string = "swarm-live-01"): EpistemicKnowledgeBase {
    if (!this._bases.has(swarmId)) {
      this._bases.set(swarmId, new EpistemicKnowledgeBase(swarmId));
    }
    return this._bases.get(swarmId)!;
  }

  public assertProposition(
    swarmId: string,
    agentId: string,
    topic: string,
    predicate: string,
    truthValue: EpistemicTruthValueType | string = EpistemicTruthValue.TRUE,
    confidence: number = 1.0,
    evidenceSpanId?: string,
    metadata?: Record<string, any>
  ): [PropositionalClaim, boolean, EpistemicConflict | null] {
    const kb = this.getKnowledgeBase(swarmId);
    return kb.assertClaim({
      agentId,
      topic,
      predicate,
      truthValue,
      confidence,
      evidenceSpanId,
      metadata,
    });
  }

  public getStatus(swarmId: string = "swarm-live-01"): Record<string, any> {
    return this.getKnowledgeBase(swarmId).getStatus();
  }

  public getPropositions(
    swarmId: string = "swarm-live-01",
    topic?: string,
    agentId?: string
  ): Array<Record<string, any>> {
    return this.getKnowledgeBase(swarmId)
      .getClaims(topic, agentId)
      .map((c) => c.toDict());
  }

  public getConflicts(
    swarmId: string = "swarm-live-01",
    unresolvedOnly: boolean = true
  ): Array<Record<string, any>> {
    return this.getKnowledgeBase(swarmId)
      .getConflicts(unresolvedOnly)
      .map((c) => c.toDict());
  }

  public reconcile(
    swarmId: string = "swarm-live-01",
    strategy: string = "CONFIDENCE_ENTRENCHMENT"
  ): Record<string, any> {
    return this.getKnowledgeBase(swarmId).reconcileBeliefs(strategy);
  }

  public reset(swarmId?: string): void {
    if (swarmId) {
      if (this._bases.has(swarmId)) {
        this._bases.get(swarmId)!.reset();
      }
    } else {
      this._bases.clear();
    }
  }
}

export const globalEpistemicEngine = new EpistemicEngine();
