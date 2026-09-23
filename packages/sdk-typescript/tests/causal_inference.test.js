import test from "node:test";
import assert from "node:assert/strict";
import {
  CausalInferenceEngine,
  globalCausalInferenceEngine,
} from "../src/causal_inference.ts";

test("Distributed Multi-Agent Root-Cause Causal Inference (TypeScript / ADR 0043)", async (t) => {
  const engine = new CausalInferenceEngine();

  await t.test("nominal run without anomaly returns has_anomaly: false", () => {
    const spans = [
      {
        id: "span-1",
        swarm_id: "swarm-nom",
        source_agent: "ProductArchitect",
        target_agent: "BackendEngineer",
        span_type: "turn",
        status: "ok",
        entropy_score: 1.85,
        start_time: "2026-09-23T10:00:00Z",
      },
      {
        id: "span-2",
        swarm_id: "swarm-nom",
        source_agent: "BackendEngineer",
        target_agent: "CodeReviewer",
        span_type: "turn",
        status: "ok",
        entropy_score: 1.90,
        start_time: "2026-09-23T10:00:01Z",
      },
    ];

    const res = engine.inferRootCause(spans);
    assert.equal(res.has_anomaly, false);
    assert.equal(res.attribution, null);
    assert.equal(res.context_poisoning.length, 0);
  });

  await t.test("multi-hop failure root-cause attribution isolates true upstream origin", () => {
    const spans = [
      {
        id: "span-1",
        swarm_id: "swarm-fail",
        source_agent: "Researcher",
        target_agent: "query_database",
        span_type: "tool",
        status: "error",
        output_payload: { error: "DatabaseConnectionError: timeout" },
        entropy_score: 1.5,
        start_time: "2026-09-23T10:00:00Z",
      },
      {
        id: "span-2",
        swarm_id: "swarm-fail",
        source_agent: "Planner",
        target_agent: "Coder",
        span_type: "turn",
        status: "ok",
        entropy_score: 1.4,
        start_time: "2026-09-23T10:00:01Z",
      },
      {
        id: "span-3",
        swarm_id: "swarm-fail",
        source_agent: "Coder",
        target_agent: "execute_sql",
        span_type: "tool",
        status: "error",
        output_payload: { error: "ExecutionFailed: missing relation" },
        entropy_score: 1.2,
        start_time: "2026-09-23T10:00:02Z",
      },
    ];

    const res = engine.inferRootCause(spans);
    assert.equal(res.has_anomaly, true);
    assert.ok(res.attribution);
    assert.equal(res.attribution.root_cause_agent, "Researcher");
    assert.equal(res.attribution.root_cause_turn, 1);
    assert.equal(res.attribution.symptom_agent, "Coder");
    assert.equal(res.attribution.symptom_turn, 3);
    assert.equal(res.attribution.propagation_depth, 2);
    assert.equal(res.attribution.recommended_fork_turn, 1);
    assert.ok(res.attribution.confidence_score >= 0.5);
  });

  await t.test("context poisoning contagion correctly measures tainted downstream agents", () => {
    const spans = [
      {
        id: "span-10",
        swarm_id: "swarm-poison",
        source_agent: "Architect",
        target_agent: "tool_fetch",
        span_type: "tool",
        status: "error",
        output_payload: { error: "InvalidSchemaException" },
        start_time: "2026-09-23T10:00:00Z",
      },
      {
        id: "span-11",
        swarm_id: "swarm-poison",
        source_agent: "DeveloperA",
        target_agent: "DeveloperB",
        span_type: "turn",
        status: "ok",
        start_time: "2026-09-23T10:00:01Z",
      },
      {
        id: "span-12",
        swarm_id: "swarm-poison",
        source_agent: "DeveloperB",
        target_agent: "Tester",
        span_type: "turn",
        status: "ok",
        start_time: "2026-09-23T10:00:02Z",
      },
      {
        id: "span-13",
        swarm_id: "swarm-poison",
        source_agent: "Tester",
        target_agent: "Deployer",
        span_type: "turn",
        status: "error",
        output_payload: { error: "TestRunFailed" },
        start_time: "2026-09-23T10:00:03Z",
      },
    ];

    const res = engine.inferRootCause(spans);
    assert.equal(res.has_anomaly, true);
    assert.equal(res.context_poisoning.length, 1);

    const cp = res.context_poisoning[0];
    assert.equal(cp.origin_agent, "Architect");
    assert.equal(cp.poison_turn, 1);
    assert.ok(cp.tainted_agents.includes("DeveloperA"));
    assert.ok(cp.tainted_agents.includes("DeveloperB"));
    assert.ok(cp.tainted_agents.includes("Tester"));
    assert.ok(cp.taint_score > 0);
  });

  await t.test("traceCausalPath extracts ordered critical path nodes", () => {
    const spans = [
      {
        id: "s1",
        swarm_id: "swarm-path",
        source_agent: "AgentAlpha",
        target_agent: "AgentBeta",
        span_type: "turn",
        status: "error",
        output_payload: { error: "CorruptedPayload" },
        start_time: "2026-09-23T10:00:00Z",
      },
      {
        id: "s2",
        swarm_id: "swarm-path",
        source_agent: "AgentBeta",
        target_agent: "AgentGamma",
        span_type: "turn",
        status: "ok",
        start_time: "2026-09-23T10:00:01Z",
      },
      {
        id: "s3",
        swarm_id: "swarm-path",
        source_agent: "AgentGamma",
        target_agent: "System",
        span_type: "turn",
        status: "error",
        output_payload: { error: "FatalAbort" },
        start_time: "2026-09-23T10:00:02Z",
      },
    ];

    const path = engine.traceCausalPath(spans, "s3");
    assert.equal(path.length, 3);
    assert.equal(path[0].agent_name, "AgentAlpha");
    assert.equal(path[0].is_fault_origin, true);
    assert.equal(path[2].agent_name, "AgentGamma");
  });

  await t.test("entropy collapse anomaly triggers root cause attribution on repetitive degenerator", () => {
    const spans = [
      {
        id: "ent-1",
        swarm_id: "swarm-ent",
        source_agent: "Leader",
        target_agent: "Writer",
        span_type: "turn",
        status: "ok",
        entropy_score: 1.8,
        start_time: "2026-09-23T10:00:00Z",
      },
      {
        id: "ent-2",
        swarm_id: "swarm-ent",
        source_agent: "Writer",
        target_agent: "Reviewer",
        span_type: "turn",
        status: "ok",
        entropy_score: 0.12, // severe collapse
        start_time: "2026-09-23T10:00:01Z",
      },
      {
        id: "ent-3",
        swarm_id: "swarm-ent",
        source_agent: "Reviewer",
        target_agent: "Leader",
        span_type: "turn",
        status: "error",
        output_payload: { error: "RepetitionLoopDetected" },
        start_time: "2026-09-23T10:00:02Z",
      },
    ];

    const res = engine.inferRootCause(spans);
    assert.equal(res.has_anomaly, true);
    assert.equal(res.attribution?.root_cause_agent, "Writer");
    assert.equal(res.attribution?.root_cause_turn, 2);
    assert.equal(res.attribution?.recommended_fork_turn, 1);
  });
});
