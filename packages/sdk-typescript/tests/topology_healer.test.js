import test from "node:test";
import assert from "node:assert/strict";
import {
  VerificationVerdict,
  VerificationInvariant,
  PatchAction,
  TopologyAutoHealer,
  globalTopologyHealer,
} from "../src/index.ts";

test("PatchAction defines canonical repair actions", () => {
  assert.equal(PatchAction.FALLBACK_REDIRECT, "FALLBACK_REDIRECT");
  assert.equal(PatchAction.CYCLE_GATE_BREAK, "CYCLE_GATE_BREAK");
  assert.equal(PatchAction.BUDGET_REWIRE, "BUDGET_REWIRE");
  assert.equal(PatchAction.MUTEX_MEDIATE, "MUTEX_MEDIATE");
});

test("TopologyAutoHealer auto-heals deadlocked topologies", () => {
  const healer = new TopologyAutoHealer();
  const swarmId = "swarm-ts-heal-deadlock";

  const model = {
    initialAgent: "agent-planner",
    finalAgents: ["agent-exit"],
    transitions: [
      { fromAgent: "agent-planner", toAgent: "agent-deadlocked", actionType: "DELEGATE", tokenCost: 50 },
    ],
    maxTokenBudget: 5000,
  };

  const report = healer.healAndVerify(model, 5, swarmId);
  assert.equal(report.originalVerdict, VerificationVerdict.VIOLATED);
  assert.equal(report.violatedInvariant, VerificationInvariant.NO_DEADLOCK);
  assert.ok(report.appliedPatch !== null);
  assert.equal(report.appliedPatch.action, PatchAction.FALLBACK_REDIRECT);
  assert.equal(report.appliedPatch.targetAgent, "agent-deadlocked");
  assert.equal(report.healedVerdict, VerificationVerdict.VERIFIED);
  assert.equal(healer.getActivePatches(swarmId).length, 1);
  assert.ok(report.executionTimeMs < 50);
});

test("TopologyAutoHealer auto-heals circular delegation loops", () => {
  const healer = new TopologyAutoHealer();
  const swarmId = "swarm-ts-heal-cycle";

  const model = {
    initialAgent: "agent-ping",
    finalAgents: ["agent-done"],
    transitions: [
      { fromAgent: "agent-ping", toAgent: "agent-pong", actionType: "PING", tokenCost: 40 },
      { fromAgent: "agent-pong", toAgent: "agent-ping", actionType: "PONG", tokenCost: 40 },
    ],
    maxTokenBudget: 100000,
  };

  const report = healer.healAndVerify(model, 10, swarmId);
  assert.equal(report.originalVerdict, VerificationVerdict.VIOLATED);
  assert.equal(report.violatedInvariant, VerificationInvariant.NO_UNBOUNDED_CYCLES);
  assert.ok(report.appliedPatch !== null);
  assert.equal(report.appliedPatch.action, PatchAction.CYCLE_GATE_BREAK);
  assert.equal(report.healedVerdict, VerificationVerdict.VERIFIED);
});

test("TopologyAutoHealer auto-heals token budget exhaustion", () => {
  const healer = new TopologyAutoHealer();
  const swarmId = "swarm-ts-heal-budget";

  const model = {
    initialAgent: "agent-1",
    finalAgents: ["agent-target"],
    transitions: [
      { fromAgent: "agent-1", toAgent: "agent-2", actionType: "HEAVY_1", tokenCost: 400 },
      { fromAgent: "agent-2", toAgent: "agent-target", actionType: "HEAVY_2", tokenCost: 400 },
    ],
    maxTokenBudget: 500,
  };

  const report = healer.healAndVerify(model, 5, swarmId);
  assert.equal(report.originalVerdict, VerificationVerdict.VIOLATED);
  assert.equal(report.violatedInvariant, VerificationInvariant.BOUNDED_BUDGET);
  assert.ok(report.appliedPatch !== null);
  assert.equal(report.appliedPatch.action, PatchAction.BUDGET_REWIRE);
  assert.equal(report.healedVerdict, VerificationVerdict.VERIFIED);
});

test("TopologyAutoHealer auto-heals mutual exclusion role conflicts", () => {
  const healer = new TopologyAutoHealer();
  const swarmId = "swarm-ts-heal-mutex";

  const model = {
    initialAgent: "agent-root",
    finalAgents: ["agent-complete"],
    transitions: [
      { fromAgent: "agent-root", toAgent: "agent-worker", actionType: "DISPATCH", tokenCost: 50 },
      { fromAgent: "agent-worker", toAgent: "agent-verifier", actionType: "VERIFY", tokenCost: 50 },
      { fromAgent: "agent-verifier", toAgent: "agent-complete", actionType: "FINISH", tokenCost: 20 },
    ],
    mutualExclusionPairs: [["agent-worker", "agent-verifier"]],
  };

  const report = healer.healAndVerify(model, 5, swarmId);
  assert.equal(report.originalVerdict, VerificationVerdict.VIOLATED);
  assert.equal(report.violatedInvariant, VerificationInvariant.MUTUAL_EXCLUSION);
  assert.ok(report.appliedPatch !== null);
  assert.equal(report.appliedPatch.action, PatchAction.MUTEX_MEDIATE);
  assert.equal(report.healedVerdict, VerificationVerdict.VERIFIED);
});

test("TopologyAutoHealer handles compliant models and revert lifecycle", () => {
  const healer = new TopologyAutoHealer();
  const swarmId = "swarm-ts-compliant";

  const safeModel = {
    initialAgent: "agent-start",
    finalAgents: ["agent-end"],
    transitions: [
      { fromAgent: "agent-start", toAgent: "agent-end", actionType: "COMPLETE", tokenCost: 100 },
    ],
    maxTokenBudget: 5000,
  };

  const report = healer.healAndVerify(safeModel, 5, swarmId);
  assert.equal(report.originalVerdict, VerificationVerdict.VERIFIED);
  assert.equal(report.appliedPatch, null);
  assert.equal(report.healedVerdict, VerificationVerdict.VERIFIED);

  // Revert test
  const deadlockedModel = {
    initialAgent: "agent-alone",
    finalAgents: ["agent-exit"],
    transitions: [
      { fromAgent: "agent-alone", toAgent: "agent-stuck", actionType: "CALL", tokenCost: 50 },
    ],
  };
  healer.healAndVerify(deadlockedModel, 5, "swarm-revert");
  assert.ok(healer.getActivePatches("swarm-revert").length >= 1);
  healer.revertPatch("swarm-revert");
  assert.equal(healer.getActivePatches("swarm-revert").length, 0);
});
