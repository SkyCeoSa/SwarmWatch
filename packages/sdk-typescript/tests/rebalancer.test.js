import test from "node:test";
import assert from "node:assert/strict";
import {
  AdaptiveQuotaRebalancer,
  AgentTier,
  DEFAULT_REBALANCE_POLICY,
} from "../src/index.ts";

test("Adaptive Dynamic Quota Re-balancing Suite (Seam 35 / ADR 0046)", async (t) => {
  await t.test("evaluates composite agent utility U_a with correct weighting and clamping", () => {
    const rebalancer = new AdaptiveQuotaRebalancer();

    // Ideal agent: 0 drift, 1.0 VCR, 0.75 entropy, 0 faults
    const ideal = rebalancer.evaluateUtility(0.0, 1.0, 0.75, 0.0);
    assert.ok(ideal >= 0.89, `Expected ideal utility >= 0.89, got ${ideal}`);

    // Highly divergent/failing agent: 0.90 drift, 0.10 VCR, 0.20 entropy, 0.60 fault rate
    const degraded = rebalancer.evaluateUtility(0.9, 0.1, 0.2, 0.6);
    assert.ok(degraded < 0.20, `Expected degraded utility < 0.20, got ${degraded}`);
  });

  await t.test("classifies operational tiers correctly based on utility thresholds", () => {
    const rebalancer = new AdaptiveQuotaRebalancer();
    assert.equal(rebalancer.classifyTier(0.85), AgentTier.BOOSTED);
    assert.equal(rebalancer.classifyTier(0.70), AgentTier.BOOSTED);
    assert.equal(rebalancer.classifyTier(0.69), AgentTier.NOMINAL);
    assert.equal(rebalancer.classifyTier(0.40), AgentTier.NOMINAL);
    assert.equal(rebalancer.classifyTier(0.39), AgentTier.THROTTLED);
    assert.equal(rebalancer.classifyTier(0.20), AgentTier.THROTTLED);
    assert.equal(rebalancer.classifyTier(0.19), AgentTier.QUARANTINED);
    assert.equal(rebalancer.classifyTier(0.0), AgentTier.QUARANTINED);
  });

  await t.test("registers agents and tracks turns and tokens accurately", () => {
    const rebalancer = new AdaptiveQuotaRebalancer();
    const swarmId = "ts-swarm-01";
    rebalancer.setFleetQuota(swarmId, 50000);

    const alloc = rebalancer.registerAgent(swarmId, "agent-1", 4000);
    assert.equal(alloc.agentId, "agent-1");
    assert.equal(alloc.allocatedTokens, 4000);
    assert.equal(alloc.consumedTokens, 0);
    assert.equal(alloc.remainingTokens, 4000);

    const afterTurn = rebalancer.recordTurn(
      swarmId,
      "agent-1",
      1000,
      0.02,
      0.90,
      0.75,
      false
    );
    assert.equal(afterTurn.consumedTokens, 1000);
    assert.equal(afterTurn.remainingTokens, 3000);
    assert.equal(afterTurn.tier, AgentTier.BOOSTED);
  });

  await t.test("executes epoch rebalance, reclaims unspent budget, and boosts converging agents", () => {
    const rebalancer = new AdaptiveQuotaRebalancer({
      safetyFloorTokens: 500,
      sensitivityExponent: 1.5,
    });
    const swarmId = "ts-swarm-rebalance";
    rebalancer.setFleetQuota(swarmId, 40000);

    // Agent Converging (High utility)
    rebalancer.registerAgent(swarmId, "agent-converging", 5000);
    rebalancer.recordTurn(swarmId, "agent-converging", 2000, 0.02, 0.95, 0.75, false);

    // Agent Nominal
    rebalancer.registerAgent(swarmId, "agent-nominal", 5000);
    rebalancer.recordTurn(swarmId, "agent-nominal", 1000, 0.35, 0.60, 0.70, false);

    // Agent Rogue (High drift, collapsed VCR)
    rebalancer.registerAgent(swarmId, "agent-rogue", 5000);
    rebalancer.recordTurn(swarmId, "agent-rogue", 1000, 0.92, 0.12, 0.20, true);

    const plan = rebalancer.computeRebalance(swarmId, "TypeScript test epoch");
    assert.equal(plan.swarmId, swarmId);
    assert.ok(plan.reclaimedBufferTokens >= 0);

    const allocs = plan.allocations;
    assert.equal(allocs["agent-converging"].tier, AgentTier.BOOSTED);
    assert.ok(
      allocs["agent-rogue"].tier === AgentTier.THROTTLED ||
        allocs["agent-rogue"].tier === AgentTier.QUARANTINED
    );

    // Converging agent receives significantly more tokens than nominal
    assert.ok(
      allocs["agent-converging"].allocatedTokens >
        allocs["agent-nominal"].allocatedTokens
    );

    // Rogue agent allocation is capped
    assert.ok(allocs["agent-rogue"].allocatedTokens <= 1500);
  });

  await t.test("manages surge grants and reclaimed buffer transfers for boosted agents", () => {
    const rebalancer = new AdaptiveQuotaRebalancer({
      safetyFloorTokens: 500,
      surgeThresholdRatio: 0.8,
    });
    const swarmId = "ts-swarm-surge";
    rebalancer.setFleetQuota(swarmId, 25000);

    // Seed reclaimed buffer
    rebalancer.computeRebalance(swarmId);
    // Directly simulate reclaimed buffer
    const plan = rebalancer.getPlan(swarmId);
    assert.ok(plan);

    rebalancer.registerAgent(swarmId, "star-agent", 2000);
    rebalancer.recordTurn(swarmId, "star-agent", 500, 0.0, 0.95, 0.75);

    // Artificially populate buffer
    rebalancer["reclaimedBuffers"].set(swarmId, 3000);

    const granted = rebalancer.requestSurge(swarmId, "star-agent", 1200);
    assert.equal(granted, 1200);

    const updated = rebalancer.getAgentQuota(swarmId, "star-agent");
    assert.ok(updated);
    assert.equal(updated.allocatedTokens, 2000 + 1200);
    assert.equal(updated.surgeGrantedTokens, 1200);
  });

  await t.test("retrieves aggregate stats and resets state cleanly", () => {
    const rebalancer = new AdaptiveQuotaRebalancer();
    const swarmId = "ts-swarm-stats";
    rebalancer.setFleetQuota(swarmId, 30000);
    rebalancer.registerAgent(swarmId, "w-1", 2000);
    rebalancer.recordTurn(swarmId, "w-1", 400);

    const stats = rebalancer.getAggregateStats(swarmId);
    assert.equal(stats.totalAgents, 1);
    assert.equal(stats.totalConsumedTokens, 400);
    assert.equal(stats.fleetQuotaTokens, 30000);

    rebalancer.reset(swarmId);
    const postReset = rebalancer.getAggregateStats(swarmId);
    assert.equal(postReset.totalAgents, 0);
  });
});
