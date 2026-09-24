import test from "node:test";
import assert from "node:assert/strict";
import {
  VerificationInvariant,
  VerificationVerdict,
  BoundedModelChecker,
  globalModelChecker,
} from "../src/index.ts";

test("VerificationInvariant and VerificationVerdict define canonical invariants", () => {
  assert.equal(VerificationInvariant.NO_DEADLOCK, "NO_DEADLOCK");
  assert.equal(VerificationInvariant.NO_UNBOUNDED_CYCLES, "NO_UNBOUNDED_CYCLES");
  assert.equal(VerificationInvariant.BOUNDED_BUDGET, "BOUNDED_BUDGET");
  assert.equal(VerificationInvariant.MUTUAL_EXCLUSION, "MUTUAL_EXCLUSION");

  assert.equal(VerificationVerdict.VERIFIED, "VERIFIED");
  assert.equal(VerificationVerdict.VIOLATED, "VIOLATED");
  assert.equal(VerificationVerdict.VACUOUS, "VACUOUS");
});

test("BoundedModelChecker verifies safe acyclic multi-agent topology", () => {
  const checker = new BoundedModelChecker();
  const model = {
    initialAgent: "agent-planner",
    finalAgents: new Set(["agent-deployer"]),
    transitions: [
      { fromAgent: "agent-planner", toAgent: "agent-coder", actionType: "CODE", tokenCost: 150 },
      { fromAgent: "agent-coder", toAgent: "agent-tester", actionType: "TEST", tokenCost: 200 },
      { fromAgent: "agent-tester", toAgent: "agent-deployer", actionType: "DEPLOY", tokenCost: 100 },
    ],
    maxTokenBudget: 5000,
  };

  const res = checker.verify(model, 5, undefined, "swarm-ts-safe");
  assert.equal(res.verdict, VerificationVerdict.VERIFIED);
  assert.equal(res.violatedInvariant, null);
  assert.equal(res.counterexample.length, 0);
  assert.ok(res.totalStatesExplored >= 3);
  assert.ok(res.executionTimeMs < 50);
});

test("BoundedModelChecker detects conversational deadlock and builds counterexample trace", () => {
  const checker = new BoundedModelChecker();
  // agent-stuck has no transitions and is not final
  const model = {
    initialAgent: "agent-router",
    finalAgents: ["agent-exit"],
    transitions: [
      { fromAgent: "agent-router", toAgent: "agent-stuck", actionType: "DISPATCH", tokenCost: 50 },
    ],
    maxTokenBudget: 5000,
  };

  const res = checker.verify(model, 5, [VerificationInvariant.NO_DEADLOCK], "swarm-ts-deadlock");
  assert.equal(res.verdict, VerificationVerdict.VIOLATED);
  assert.equal(res.violatedInvariant, VerificationInvariant.NO_DEADLOCK);
  assert.ok(res.counterexample.length >= 1);
  assert.equal(res.counterexample[0].fromAgent, "agent-router");
  assert.equal(res.counterexample[0].toAgent, "agent-stuck");
});

test("BoundedModelChecker detects unbounded circular delegation cycles", () => {
  const checker = new BoundedModelChecker();
  const model = {
    initialAgent: "agent-ping",
    finalAgents: ["agent-end"],
    transitions: [
      { fromAgent: "agent-ping", toAgent: "agent-pong", actionType: "PING", tokenCost: 50 },
      { fromAgent: "agent-pong", toAgent: "agent-ping", actionType: "PONG", tokenCost: 50 },
    ],
    maxTokenBudget: 100000,
  };

  const res = checker.verify(model, 10, [VerificationInvariant.NO_UNBOUNDED_CYCLES], "swarm-ts-cycle");
  assert.equal(res.verdict, VerificationVerdict.VIOLATED);
  assert.equal(res.violatedInvariant, VerificationInvariant.NO_UNBOUNDED_CYCLES);
  assert.ok(res.counterexample.length >= 3);
});

test("BoundedModelChecker detects token budget exhaustion along execution paths", () => {
  const checker = new BoundedModelChecker();
  const model = {
    initialAgent: "agent-start",
    finalAgents: ["agent-finish"],
    transitions: [
      { fromAgent: "agent-start", toAgent: "agent-heavy", actionType: "STAGE_1", tokenCost: 300 },
      { fromAgent: "agent-heavy", toAgent: "agent-finish", actionType: "STAGE_2", tokenCost: 400 },
    ],
    maxTokenBudget: 500,
  };

  const res = checker.verify(model, 5, [VerificationInvariant.BOUNDED_BUDGET], "swarm-ts-budget");
  assert.equal(res.verdict, VerificationVerdict.VIOLATED);
  assert.equal(res.violatedInvariant, VerificationInvariant.BOUNDED_BUDGET);
  assert.ok(res.counterexample.length >= 2);
  assert.equal(res.counterexample[res.counterexample.length - 1].cumulativeTokens, 700);
});

test("BoundedModelChecker detects mutual exclusion violations for conflicting agent pairs", () => {
  const checker = new BoundedModelChecker();
  const model = {
    initialAgent: "agent-lead",
    finalAgents: ["agent-done"],
    transitions: [
      { fromAgent: "agent-lead", toAgent: "agent-executor", actionType: "RUN", tokenCost: 50 },
      { fromAgent: "agent-executor", toAgent: "agent-verifier", actionType: "SELF_VERIFY", tokenCost: 50 },
      { fromAgent: "agent-verifier", toAgent: "agent-done", actionType: "COMPLETE", tokenCost: 20 },
    ],
    mutualExclusionPairs: [["agent-executor", "agent-verifier"]],
  };

  const res = checker.verify(model, 5, [VerificationInvariant.MUTUAL_EXCLUSION], "swarm-ts-mutex");
  assert.equal(res.verdict, VerificationVerdict.VIOLATED);
  assert.equal(res.violatedInvariant, VerificationInvariant.MUTUAL_EXCLUSION);
  assert.ok(res.counterexample.length === 2);
});

test("BoundedModelChecker handles vacuous models and extracts from empirical spans", () => {
  const checker = new BoundedModelChecker();

  // Vacuous empty model
  const vacRes = checker.verify({ initialAgent: "agent-0", transitions: [] });
  assert.equal(vacRes.verdict, VerificationVerdict.VACUOUS);

  // Empirical span extraction
  const spans = [
    { source_agent: "agent-root", target_agent: "agent-middle", action_type: "DELEGATE", token_count: 100 },
    { source_agent: "agent-middle", target_agent: "FINAL", action_type: "COMPLETE", token_count: 50 },
  ];
  const extracted = checker.extractModelFromSpans(spans);
  assert.equal(extracted.initialAgent, "agent-root");
  assert.ok(extracted.finalAgents.has("agent-middle"));
  assert.equal(extracted.transitions.length, 2);

  const empRes = checker.verify(extracted, 5, undefined, "swarm-empirical");
  assert.equal(empRes.verdict, VerificationVerdict.VERIFIED);

  // Reset
  checker.reset("swarm-empirical");
  assert.equal(checker.getLatestResult("swarm-empirical"), null);
});
