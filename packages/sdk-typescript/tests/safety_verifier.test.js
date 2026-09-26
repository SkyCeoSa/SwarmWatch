/**
 * Unit and integration tests for Bounded Verification Semantics & Explicit Counterexample Engine (Seam 55 / ADR 0066).
 * Follows Node 22 native test runner conventions and strict type-stripping rules (no 'as ...' type casts).
 */

import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
  BoundedVerdict,
  VerificationInvariant,
  SafetyVerifier,
  globalSafetyVerifier,
} from "../src/safety_verifier.ts";

describe("SafetyVerifier (Seam 55 / ADR 0066)", () => {
  let verifier;

  beforeEach(() => {
    verifier = new SafetyVerifier();
    globalSafetyVerifier.reset();
  });

  test("VACUOUS verdict when model has zero transitions", () => {
    const model = {
      initialAgent: "Planner",
      finalAgents: [],
      transitions: [],
    };
    const report = verifier.verify(model, undefined, undefined, "empty_model");
    assert.equal(report.verdict, BoundedVerdict.VACUOUS);
    assert.equal(report.totalStatesExplored, 0);
    assert.equal(report.frontierStatesRemaining, 0);
    assert.equal(report.stateCoverageRatio, 1.0);
    assert.ok(report.diagnosticReason.includes("vacuously safe"));
  });

  test("UNBOUNDED_PROVEN verdict on closed finite pipeline", () => {
    const model = {
      initialAgent: "Planner",
      finalAgents: ["Publisher"],
      transitions: [
        { fromAgent: "Planner", toAgent: "Coder", actionType: "DELEGATE", tokenCost: 200 },
        { fromAgent: "Coder", toAgent: "Publisher", actionType: "SUBMIT", tokenCost: 150 },
      ],
      maxTokenBudget: 1000,
    };
    const report = verifier.verify(model, undefined, { maxDepthK: 10 }, "pipeline_model");
    assert.equal(report.verdict, BoundedVerdict.UNBOUNDED_PROVEN);
    assert.equal(report.depthReached, 2);
    assert.equal(report.totalStatesExplored, 3);
    assert.equal(report.frontierStatesRemaining, 0);
    assert.equal(report.stateCoverageRatio, 1.0);
    assert.equal(report.violatedProperty, null);
    assert.equal(report.counterexampleTrace.length, 0);
  });

  test("BOUNDED_SAFE verdict with unclosed frontier beyond depth bound k", () => {
    const model = {
      initialAgent: "AgentA",
      finalAgents: ["EndTarget"],
      transitions: [
        { fromAgent: "AgentA", toAgent: "AgentB", actionType: "STEP_AB", tokenCost: 10 },
        { fromAgent: "AgentB", toAgent: "AgentA", actionType: "STEP_BA", tokenCost: 10 },
      ],
      maxTokenBudget: 50000,
    };
    const report = verifier.verify(
      model,
      [
        { propertyId: "p_deadlock", invariantType: VerificationInvariant.NO_DEADLOCK },
        { propertyId: "p_budget", invariantType: VerificationInvariant.BOUNDED_BUDGET, parameters: { max_tokens: 50000 } },
      ],
      { maxDepthK: 2, maxStates: 100 },
      "unclosed_model"
    );
    assert.equal(report.verdict, BoundedVerdict.BOUNDED_SAFE);
    assert.equal(report.boundK, 2);
    assert.ok(report.frontierStatesRemaining > 0);
    assert.ok(report.stateCoverageRatio < 1.0);
    assert.ok(report.stateCoverageRatio > 0.0);
    assert.ok(report.diagnosticReason.includes("remains unclosed beyond k"));
  });

  test("INDETERMINATE verdict when state cap is reached", () => {
    const transitions = [];
    for (let i = 0; i < 10; i++) {
      transitions.push({ fromAgent: `N${i}`, toAgent: `N${i + 1}A`, actionType: "BRANCH_A", tokenCost: 10 });
      transitions.push({ fromAgent: `N${i}`, toAgent: `N${i + 1}B`, actionType: "BRANCH_B", tokenCost: 10 });
      transitions.push({ fromAgent: `N${i + 1}A`, toAgent: `N${i + 1}`, actionType: "JOIN_A", tokenCost: 10 });
      transitions.push({ fromAgent: `N${i + 1}B`, toAgent: `N${i + 1}`, actionType: "JOIN_B", tokenCost: 10 });
    }

    const model = {
      initialAgent: "N0",
      finalAgents: ["N10"],
      transitions,
    };
    const report = verifier.verify(model, undefined, { maxDepthK: 20, maxStates: 5 }, "branch_model");
    assert.equal(report.verdict, BoundedVerdict.INDETERMINATE);
    assert.ok(report.totalStatesExplored >= 5);
    assert.ok(report.diagnosticReason.includes("state cap of 5 reached"));
  });

  test("VIOLATED verdict and explicit counterexample trace on deadlock", () => {
    const model = {
      initialAgent: "Coordinator",
      finalAgents: ["Done"],
      transitions: [
        { fromAgent: "Coordinator", toAgent: "Worker", actionType: "DELEGATE", tokenCost: 100 },
        { fromAgent: "Worker", toAgent: "StuckAgent", actionType: "HANDOFF", tokenCost: 100 },
      ],
    };
    const report = verifier.verify(model, undefined, { maxDepthK: 5 }, "deadlock_model");
    assert.equal(report.verdict, BoundedVerdict.VIOLATED);
    assert.equal(report.violatedProperty, "prop_no_deadlock");
    assert.equal(report.counterexampleTrace.length, 2);
    assert.equal(report.counterexampleTrace[0].fromAgent, "Coordinator");
    assert.equal(report.counterexampleTrace[0].toAgent, "Worker");
    assert.equal(report.counterexampleTrace[1].fromAgent, "Worker");
    assert.equal(report.counterexampleTrace[1].toAgent, "StuckAgent");
    assert.ok(report.diagnosticReason.includes("Conversational deadlock"));
  });

  test("Independent trace validation and rejection of tampered traces", () => {
    const model = {
      initialAgent: "Coordinator",
      finalAgents: ["Done"],
      transitions: [
        { fromAgent: "Coordinator", toAgent: "Worker", actionType: "DELEGATE", tokenCost: 100 },
        { fromAgent: "Worker", toAgent: "StuckAgent", actionType: "HANDOFF", tokenCost: 100 },
      ],
    };
    const validTrace = [
      {
        stepIndex: 1,
        fromAgent: "Coordinator",
        toAgent: "Worker",
        actionType: "DELEGATE",
        tokenCost: 100,
        cumulativeTokens: 100,
        stateFlags: [],
      },
      {
        stepIndex: 2,
        fromAgent: "Worker",
        toAgent: "StuckAgent",
        actionType: "HANDOFF",
        tokenCost: 100,
        cumulativeTokens: 200,
        stateFlags: [],
      },
    ];

    // 1. Authentic trace is sound
    const res1 = verifier.verifyTrace(model, "NO_DEADLOCK", validTrace);
    assert.equal(res1.isSound, true);
    assert.equal(res1.diagnostic, null);

    // 2. Tampered token arithmetic is rejected
    const tamperedTokens = JSON.parse(JSON.stringify(validTrace));
    tamperedTokens[1].cumulativeTokens = 999;
    const res2 = verifier.verifyTrace(model, "NO_DEADLOCK", tamperedTokens);
    assert.equal(res2.isSound, false);
    assert.ok(res2.diagnostic.includes("cumulative tokens"));

    // 3. Fabricated transition is rejected
    const fabricated = JSON.parse(JSON.stringify(validTrace));
    fabricated[0].actionType = "FABRICATED_ACTION";
    const res3 = verifier.verifyTrace(model, "NO_DEADLOCK", fabricated);
    assert.equal(res3.isSound, false);
    assert.ok(res3.diagnostic.includes("does not exist in model"));

    // 4. Claiming deadlock on a final state is rejected
    const safeModel = {
      initialAgent: "Coordinator",
      finalAgents: ["StuckAgent"],
      transitions: model.transitions,
    };
    const res4 = verifier.verifyTrace(safeModel, "NO_DEADLOCK", validTrace);
    assert.equal(res4.isSound, false);
    assert.ok(res4.diagnostic.includes("declared final state"));
  });

  test("Budget and mutual exclusion property violations", () => {
    const model = {
      initialAgent: "AgentA",
      finalAgents: ["AgentD"],
      transitions: [
        { fromAgent: "AgentA", toAgent: "AgentB", actionType: "CALL_B", tokenCost: 600 },
        { fromAgent: "AgentB", toAgent: "AgentC", actionType: "CALL_C", tokenCost: 600 },
        { fromAgent: "AgentC", toAgent: "AgentD", actionType: "CALL_D", tokenCost: 100 },
      ],
      mutualExclusionPairs: [["AgentA", "AgentC"]],
      maxTokenBudget: 1000,
    };

    // Budget violation
    const reportBudget = verifier.verify(
      model,
      [{ propertyId: "p_budget", invariantType: VerificationInvariant.BOUNDED_BUDGET, parameters: { max_tokens: 1000 } }],
      { maxDepthK: 5 }
    );
    assert.equal(reportBudget.verdict, BoundedVerdict.VIOLATED);
    assert.equal(reportBudget.violatedProperty, "p_budget");
    const traceResBudget = verifier.verifyTrace(
      model,
      { propertyId: "p_budget", invariantType: VerificationInvariant.BOUNDED_BUDGET, parameters: { max_tokens: 1000 } },
      reportBudget.counterexampleTrace
    );
    assert.equal(traceResBudget.isSound, true);

    // Mutex violation
    const reportMutex = verifier.verify(
      model,
      [{ propertyId: "p_mutex", invariantType: VerificationInvariant.MUTUAL_EXCLUSION }],
      { maxDepthK: 5 }
    );
    assert.equal(reportMutex.verdict, BoundedVerdict.VIOLATED);
    assert.equal(reportMutex.violatedProperty, "p_mutex");
    const traceResMutex = verifier.verifyTrace(
      model,
      { propertyId: "p_mutex", invariantType: VerificationInvariant.MUTUAL_EXCLUSION },
      reportMutex.counterexampleTrace
    );
    assert.equal(traceResMutex.isSound, true);
  });
});
