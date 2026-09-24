import test from "node:test";
import assert from "node:assert/strict";
import {
  TopologicalCoalitionEngine,
  globalCoalitionEngine,
} from "../src/index.ts";

test("Real-Time Topological Graph Clustering & Dynamic Agent Coalitions Suite (Seam 36 / ADR 0047)", async (t) => {
  await t.test("records interactions, calculates edge weights with log-token scaling, and ignores self-loops", () => {
    const engine = new TopologicalCoalitionEngine();
    const swarmId = "ts-coalition-01";

    engine.recordInteraction(swarmId, "Agent1", "Agent2", 100);
    engine.recordInteraction(swarmId, "Agent2", "Agent1", 200);
    // Self-loop should be ignored
    engine.recordInteraction(swarmId, "Agent1", "Agent1", 50);

    const res = engine.computeClusters(swarmId);
    assert.equal(res.totalAgents, 2);
    assert.equal(res.totalInteractions, 2);
    assert.ok(res.clusters.length >= 1);
  });

  await t.test("computes node centrality and detects transit bottleneck agents on bridge topology", () => {
    const engine = new TopologicalCoalitionEngine(0.10, 0.40);
    const swarmId = "ts-bridge-swarm";

    // Cluster A talks to Bridge
    engine.recordInteraction(swarmId, "A1", "Bridge", 500);
    engine.recordInteraction(swarmId, "A2", "Bridge", 500);
    // Bridge talks to Cluster C
    engine.recordInteraction(swarmId, "Bridge", "C1", 500);
    engine.recordInteraction(swarmId, "Bridge", "C2", 500);

    const centralities = engine.computeCentrality(swarmId);
    assert.ok("Bridge" in centralities);
    const bridge = centralities["Bridge"];
    assert.equal(bridge.isBottleneck, true);
    assert.ok(bridge.bottleneckScore > centralities["A1"].bottleneckScore);
  });

  await t.test("partitions two weakly-connected subgraphs into distinct modular clusters", () => {
    const engine = new TopologicalCoalitionEngine();
    const swarmId = "ts-cluster-swarm";

    // Dense Cluster 1: Alpha, Beta, Gamma
    for (let i = 0; i < 3; i++) {
      engine.recordInteraction(swarmId, "Alpha", "Beta", 200);
      engine.recordInteraction(swarmId, "Beta", "Gamma", 200);
      engine.recordInteraction(swarmId, "Gamma", "Alpha", 200);
    }

    // Dense Cluster 2: X, Y, Z
    for (let i = 0; i < 3; i++) {
      engine.recordInteraction(swarmId, "X", "Y", 200);
      engine.recordInteraction(swarmId, "Y", "Z", 200);
      engine.recordInteraction(swarmId, "Z", "X", 200);
    }

    // Weak cross-link
    engine.recordInteraction(swarmId, "Gamma", "X", 10);

    const result = engine.computeClusters(swarmId);
    assert.equal(result.totalAgents, 6);
    assert.ok(result.modularityScore > 0.15, `Expected modularity > 0.15, got ${result.modularityScore}`);
    assert.equal(result.clusters.length, 2);
  });

  await t.test("forms dynamic agent coalitions and elects the central coordinator as leader", () => {
    const engine = new TopologicalCoalitionEngine(0.15, 0.30);
    const swarmId = "ts-coalition-form";

    // Coordinator hub interacting with Worker1 and Worker2
    for (let i = 0; i < 4; i++) {
      engine.recordInteraction(swarmId, "Coordinator", "Worker1", 300);
      engine.recordInteraction(swarmId, "Worker1", "Coordinator", 300);
      engine.recordInteraction(swarmId, "Coordinator", "Worker2", 300);
      engine.recordInteraction(swarmId, "Worker2", "Coordinator", 300);
    }

    const result = engine.computeClusters(swarmId);
    assert.ok(result.coalitions.length >= 1);
    const coalition = result.coalitions[0];
    assert.ok(coalition.memberAgents.includes("Coordinator"));
    assert.equal(coalition.leaderAgent, "Coordinator");
    assert.ok(coalition.interactionDensity > 0);
    assert.ok(coalition.totalTokens > 0);
  });

  await t.test("detects conversational deadlock in isolated circular ping-pong loops", () => {
    const engine = new TopologicalCoalitionEngine(0.15, 0.30);
    const swarmId = "ts-deadlock-swarm";

    // Closed loop with no external interaction
    for (let i = 0; i < 5; i++) {
      engine.recordInteraction(swarmId, "LoopAgentA", "LoopAgentB", 100);
      engine.recordInteraction(swarmId, "LoopAgentB", "LoopAgentA", 100);
    }

    const result = engine.computeClusters(swarmId);
    assert.equal(result.coalitions.length, 1);
    const c = result.coalitions[0];
    assert.equal(c.isDeadlocked, true);
  });

  await t.test("resets swarm topological state cleanly", () => {
    const engine = new TopologicalCoalitionEngine();
    const swarmId = "ts-reset-swarm";

    engine.recordInteraction(swarmId, "AgentA", "AgentB", 100);
    assert.equal(engine.computeClusters(swarmId).totalAgents, 2);

    engine.reset(swarmId);
    const clean = engine.computeClusters(swarmId);
    assert.equal(clean.totalAgents, 0);
    assert.equal(clean.totalInteractions, 0);
    assert.equal(clean.coalitions.length, 0);
  });
});
