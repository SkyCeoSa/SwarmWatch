/**
 * SwarmWatch Adaptive Dynamic Quota Re-balancing Engine (TypeScript Parity).
 * Evaluates real-time multi-agent utility combining semantic convergence,
 * lexical diversity, Shannon entropy stability, and fault resilience to dynamically
 * re-allocate token and turn quotas across distributed swarms (ADR 0046).
 *
 * Conforms strictly to Node 22 type-stripping rules: no TS enums, no parameter properties.
 */

import { randomUUID } from "node:crypto";

export const AgentTier = {
  BOOSTED: "boosted",
  NOMINAL: "nominal",
  THROTTLED: "throttled",
  QUARANTINED: "quarantined",
} as const;

export type AgentTierType = typeof AgentTier[keyof typeof AgentTier];

export interface RebalancePolicy {
  weightDrift: number;
  weightVcr: number;
  weightEntropy: number;
  weightFault: number;
  targetEntropy: number;
  sensitivityExponent: number;
  safetyFloorTokens: number;
  surgeThresholdRatio: number;
  epochIntervalTurns: number;
}

export const DEFAULT_REBALANCE_POLICY: RebalancePolicy = {
  weightDrift: 0.45,
  weightVcr: 0.25,
  weightEntropy: 0.20,
  weightFault: 0.10,
  targetEntropy: 0.75,
  sensitivityExponent: 1.5,
  safetyFloorTokens: 500,
  surgeThresholdRatio: 0.85,
  epochIntervalTurns: 5,
};

export interface AgentAllocation {
  agentId: string;
  tier: AgentTierType;
  utilityScore: number;
  allocatedTokens: number;
  consumedTokens: number;
  remainingTokens: number;
  surgeGrantedTokens: number;
  isThrottled: boolean;
  isQuarantined: boolean;
  utilizationRatio: number;
}

export interface RebalancePlan {
  epochId: string;
  swarmId: string;
  timestamp: string;
  totalFleetQuota: number;
  totalConsumedTokens: number;
  reclaimedBufferTokens: number;
  allocations: Record<string, AgentAllocation>;
  justification: string;
}

interface InternalAgentState {
  agentId: string;
  swarmId: string;
  totalTokens: number;
  totalTurns: number;
  totalFaults: number;
  lastDriftScore: number;
  lastVcr: number;
  lastEntropy: number;
  utilityScore: number;
  tier: AgentTierType;
  allocatedTokens: number;
  consumedTokens: number;
  surgeGrantedTokens: number;
  isThrottled: boolean;
  isQuarantined: boolean;
}

export class AdaptiveQuotaRebalancer {
  public policy: RebalancePolicy;
  private swarms: Map<string, Map<string, InternalAgentState>>;
  private fleetQuotas: Map<string, number>;
  private reclaimedBuffers: Map<string, number>;
  private latestPlans: Map<string, RebalancePlan>;
  private swarmTurns: Map<string, number>;

  constructor(policy?: Partial<RebalancePolicy>) {
    this.policy = { ...DEFAULT_REBALANCE_POLICY, ...policy };
    this.swarms = new Map();
    this.fleetQuotas = new Map();
    this.reclaimedBuffers = new Map();
    this.latestPlans = new Map();
    this.swarmTurns = new Map();
  }

  public setFleetQuota(swarmId: string, totalTokens: number): void {
    this.fleetQuotas.set(swarmId, totalTokens);
    if (!this.reclaimedBuffers.has(swarmId)) {
      this.reclaimedBuffers.set(swarmId, 0);
    }
    if (!this.swarmTurns.has(swarmId)) {
      this.swarmTurns.set(swarmId, 0);
    }
  }

  public registerAgent(
    swarmId: string,
    agentId: string,
    initialTokens?: number
  ): AgentAllocation {
    let agents = this.swarms.get(swarmId);
    if (!agents) {
      agents = new Map();
      this.swarms.set(swarmId, agents);
    }

    let state = agents.get(agentId);
    if (!state) {
      const initQuota =
        initialTokens !== undefined
          ? initialTokens
          : this.policy.safetyFloorTokens * 4;
      state = {
        agentId,
        swarmId,
        totalTokens: 0,
        totalTurns: 0,
        totalFaults: 0,
        lastDriftScore: 0.0,
        lastVcr: 1.0,
        lastEntropy: 0.75,
        utilityScore: 1.0,
        tier: AgentTier.NOMINAL,
        allocatedTokens: initQuota,
        consumedTokens: 0,
        surgeGrantedTokens: 0,
        isThrottled: false,
        isQuarantined: false,
      };
      agents.set(agentId, state);
    }

    return this.toAllocation(state);
  }

  public recordTurn(
    swarmId: string,
    agentId: string,
    tokens: number,
    driftScore: number = 0.0,
    vcr: number = 1.0,
    entropy: number = 0.75,
    isFault: boolean = false
  ): AgentAllocation {
    let agents = this.swarms.get(swarmId);
    if (!agents) {
      agents = new Map();
      this.swarms.set(swarmId, agents);
    }

    let state = agents.get(agentId);
    if (!state) {
      state = {
        agentId,
        swarmId,
        totalTokens: 0,
        totalTurns: 0,
        totalFaults: 0,
        lastDriftScore: 0.0,
        lastVcr: 1.0,
        lastEntropy: 0.75,
        utilityScore: 1.0,
        tier: AgentTier.NOMINAL,
        allocatedTokens: this.policy.safetyFloorTokens * 4,
        consumedTokens: 0,
        surgeGrantedTokens: 0,
        isThrottled: false,
        isQuarantined: false,
      };
      agents.set(agentId, state);
    }

    state.totalTokens += tokens;
    state.consumedTokens += tokens;
    state.totalTurns += 1;
    if (isFault) {
      state.totalFaults += 1;
    }

    state.lastDriftScore = Math.max(0.0, Math.min(1.0, driftScore));
    state.lastVcr = Math.max(0.0, Math.min(1.0, vcr));
    state.lastEntropy = Math.max(0.0, Math.min(2.5, entropy));

    state.utilityScore = this.evaluateUtility(
      state.lastDriftScore,
      state.lastVcr,
      state.lastEntropy,
      state.totalFaults / Math.max(1, state.totalTurns)
    );
    state.tier = this.classifyTier(state.utilityScore);
    state.isThrottled =
      state.tier === AgentTier.THROTTLED || state.tier === AgentTier.QUARANTINED;
    state.isQuarantined = state.tier === AgentTier.QUARANTINED;

    // Check surge grant condition for converging boosted agent approaching budget ceiling
    if (
      state.tier === AgentTier.BOOSTED &&
      state.allocatedTokens > 0 &&
      state.consumedTokens / state.allocatedTokens >= this.policy.surgeThresholdRatio
    ) {
      this.requestSurge(swarmId, agentId);
    }

    // Check turn interval for epoch rebalance
    const turns = (this.swarmTurns.get(swarmId) || 0) + 1;
    this.swarmTurns.set(swarmId, turns);
    if (turns % this.policy.epochIntervalTurns === 0) {
      this.computeRebalance(swarmId);
    }

    return this.toAllocation(state);
  }

  public evaluateUtility(
    driftScore: number,
    vcr: number,
    entropy: number,
    faultRate: number
  ): number {
    const driftInv = 1.0 - Math.max(0.0, Math.min(1.0, driftScore));
    const vcrTerm = Math.max(0.0, Math.min(1.0, vcr));

    const diff = Math.abs(entropy - this.policy.targetEntropy);
    const entropyTerm = Math.max(
      0.0,
      1.0 - diff / Math.max(0.1, this.policy.targetEntropy)
    );

    const faultTerm = Math.max(0.0, Math.min(1.0, faultRate));

    const utility =
      this.policy.weightDrift * driftInv +
      this.policy.weightVcr * vcrTerm +
      this.policy.weightEntropy * entropyTerm -
      this.policy.weightFault * faultTerm;

    return Math.max(0.0, Math.min(1.0, utility));
  }

  public classifyTier(utility: number): AgentTierType {
    if (utility >= 0.70) {
      return AgentTier.BOOSTED;
    } else if (utility >= 0.40) {
      return AgentTier.NOMINAL;
    } else if (utility >= 0.20) {
      return AgentTier.THROTTLED;
    } else {
      return AgentTier.QUARANTINED;
    }
  }

  public computeRebalance(
    swarmId: string,
    justification?: string
  ): RebalancePlan {
    const agents = this.swarms.get(swarmId) || new Map();
    const fleetQuota = this.fleetQuotas.get(swarmId) || 100_000;
    let reclaimedBuf = this.reclaimedBuffers.get(swarmId) || 0;

    let totalConsumed = 0;
    for (const a of agents.values()) {
      totalConsumed += a.consumedTokens;
    }
    const remainingFleet = Math.max(0, fleetQuota - totalConsumed);

    const activeCandidates: InternalAgentState[] = [];
    for (const agent of agents.values()) {
      agent.utilityScore = this.evaluateUtility(
        agent.lastDriftScore,
        agent.lastVcr,
        agent.lastEntropy,
        agent.totalFaults / Math.max(1, agent.totalTurns)
      );
      agent.tier = this.classifyTier(agent.utilityScore);
      agent.isThrottled =
        agent.tier === AgentTier.THROTTLED || agent.tier === AgentTier.QUARANTINED;
      agent.isQuarantined = agent.tier === AgentTier.QUARANTINED;

      if (agent.isQuarantined) {
        const unspent = Math.max(0, agent.allocatedTokens - agent.consumedTokens);
        if (unspent > 0) {
          reclaimedBuf += unspent;
          agent.allocatedTokens = agent.consumedTokens;
        }
      } else if (agent.tier === AgentTier.THROTTLED) {
        const targetCap = agent.consumedTokens + this.policy.safetyFloorTokens;
        if (agent.allocatedTokens > targetCap) {
          reclaimedBuf += agent.allocatedTokens - targetCap;
          agent.allocatedTokens = targetCap;
        }
      } else {
        activeCandidates.push(agent);
      }
    }

    const availablePool = remainingFleet + reclaimedBuf;
    const safetyTotal = activeCandidates.length * this.policy.safetyFloorTokens;
    const distributable = Math.max(0, availablePool - safetyTotal);

    const kappa = this.policy.sensitivityExponent;
    const weights: Map<string, number> = new Map();
    let sumWeights = 0;

    for (const agent of activeCandidates) {
      const w = Math.pow(Math.max(0.001, agent.utilityScore), kappa);
      weights.set(agent.agentId, w);
      sumWeights += w;
    }

    if (sumWeights === 0) sumWeights = 1.0;

    let allocatedFromDist = 0;
    for (const agent of activeCandidates) {
      const w = weights.get(agent.agentId) || 0;
      const fraction = w / sumWeights;
      const share = Math.floor(distributable * fraction);
      const newFutureTokens = this.policy.safetyFloorTokens + share;
      agent.allocatedTokens = agent.consumedTokens + newFutureTokens;
      allocatedFromDist += share;
    }

    reclaimedBuf = Math.max(0, distributable - allocatedFromDist);
    this.reclaimedBuffers.set(swarmId, reclaimedBuf);

    const allocations: Record<string, AgentAllocation> = {};
    for (const [aid, state] of agents.entries()) {
      allocations[aid] = this.toAllocation(state);
    }

    const plan: RebalancePlan = {
      epochId: randomUUID(),
      swarmId,
      timestamp: new Date().toISOString(),
      totalFleetQuota: fleetQuota,
      totalConsumedTokens: totalConsumed,
      reclaimedBufferTokens: reclaimedBuf,
      allocations,
      justification:
        justification ||
        `Adaptive epoch rebalance across ${agents.size} agents`,
    };

    this.latestPlans.set(swarmId, plan);
    return plan;
  }

  public requestSurge(
    swarmId: string,
    agentId: string,
    requestedTokens?: number
  ): number {
    const agents = this.swarms.get(swarmId);
    if (!agents) return 0;
    const agent = agents.get(agentId);
    if (!agent || agent.tier !== AgentTier.BOOSTED) return 0;

    const reclaimed = this.reclaimedBuffers.get(swarmId) || 0;
    if (reclaimed <= 0) return 0;

    const ask =
      requestedTokens !== undefined
        ? requestedTokens
        : Math.max(this.policy.safetyFloorTokens, Math.floor(reclaimed / 2));
    const grant = Math.min(ask, reclaimed);

    if (grant > 0) {
      agent.allocatedTokens += grant;
      agent.surgeGrantedTokens += grant;
      this.reclaimedBuffers.set(swarmId, reclaimed - grant);
    }

    return grant;
  }

  public getPlan(swarmId: string): RebalancePlan | null {
    return this.latestPlans.get(swarmId) || null;
  }

  public getAllocations(swarmId: string): Record<string, AgentAllocation> {
    const agents = this.swarms.get(swarmId) || new Map();
    const result: Record<string, AgentAllocation> = {};
    for (const [aid, state] of agents.entries()) {
      result[aid] = this.toAllocation(state);
    }
    return result;
  }

  public getAgentQuota(
    swarmId: string,
    agentId: string
  ): AgentAllocation | null {
    const agents = this.swarms.get(swarmId);
    if (!agents) return null;
    const state = agents.get(agentId);
    return state ? this.toAllocation(state) : null;
  }

  public getAggregateStats(swarmId: string): Record<string, any> {
    const agents = this.swarms.get(swarmId) || new Map();
    const fleetQuota = this.fleetQuotas.get(swarmId) || 100_000;
    const reclaimedBuf = this.reclaimedBuffers.get(swarmId) || 0;
    const plan = this.latestPlans.get(swarmId);

    const tierCounts: Record<string, number> = {
      [AgentTier.BOOSTED]: 0,
      [AgentTier.NOMINAL]: 0,
      [AgentTier.THROTTLED]: 0,
      [AgentTier.QUARANTINED]: 0,
    };

    let totalConsumed = 0;
    for (const a of agents.values()) {
      tierCounts[a.tier] = (tierCounts[a.tier] || 0) + 1;
      totalConsumed += a.consumedTokens;
    }

    return {
      swarmId,
      totalAgents: agents.size,
      fleetQuotaTokens: fleetQuota,
      totalConsumedTokens: totalConsumed,
      reclaimedBufferTokens: reclaimedBuf,
      tierDistribution: tierCounts,
      lastEpochId: plan ? plan.epochId : null,
      lastEpochTimestamp: plan ? plan.timestamp : null,
    };
  }

  public reset(swarmId: string): void {
    this.swarms.delete(swarmId);
    this.fleetQuotas.delete(swarmId);
    this.reclaimedBuffers.delete(swarmId);
    this.latestPlans.delete(swarmId);
    this.swarmTurns.delete(swarmId);
  }

  private toAllocation(state: InternalAgentState): AgentAllocation {
    const remaining = Math.max(0, state.allocatedTokens - state.consumedTokens);
    const ratio =
      state.allocatedTokens > 0
        ? state.consumedTokens / state.allocatedTokens
        : 1.0;

    return {
      agentId: state.agentId,
      tier: state.tier,
      utilityScore: Math.round(state.utilityScore * 10000) / 10000,
      allocatedTokens: state.allocatedTokens,
      consumedTokens: state.consumedTokens,
      remainingTokens: remaining,
      surgeGrantedTokens: state.surgeGrantedTokens,
      isThrottled: state.isThrottled,
      isQuarantined: state.isQuarantined,
      utilizationRatio: Math.round(ratio * 10000) / 10000,
    };
  }
}
