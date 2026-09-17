/**
 * Tests for SwarmWatch Incident Post-Mortem & Financial ROI Report Generator (TypeScript SDK).
 * Conforms to ADR 0020 & Seam 11 parity specifications.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  IncidentReportGenerator,
  InMemoryStorageSink,
} from "../src/index.ts";

test("Incident Post-Mortem & Financial ROI Report Generator (Seam 11 / ADR 0020)", async (t) => {
  await t.test("analyzes healthy completed swarm with zero financial loss", () => {
    const swarmId = "swarm-healthy-101";
    const spans = [
      {
        id: "span-1",
        swarm_id: swarmId,
        source_agent: "UserProxy",
        target_agent: "Planner",
        span_type: "agent_call",
        start_time: "2026-09-17T10:00:00.000Z",
        end_time: "2026-09-17T10:00:01.000Z",
        token_count: 500,
        entropy_score: 0.88,
        input_payload: { query: "Design distributed trading architecture" },
        output_payload: { plan: "Step 1: Ingestion, Step 2: Risk Check" },
      },
      {
        id: "span-2",
        swarm_id: swarmId,
        source_agent: "Planner",
        target_agent: "RiskAssessor",
        span_type: "agent_call",
        start_time: "2026-09-17T10:00:01.500Z",
        end_time: "2026-09-17T10:00:03.000Z",
        token_count: 850,
        entropy_score: 0.92,
        input_payload: { plan: "Verify drawdown threshold" },
        output_payload: { risk_level: "LOW" },
      },
    ];

    const generator = IncidentReportGenerator.fromSpans(spans);
    const postMortem = generator.analyze(swarmId);

    assert.ok(postMortem, "Post-mortem must not be null");
    assert.strictEqual(postMortem.swarm_id, swarmId);
    assert.strictEqual(postMortem.status, "COMPLETED_SUCCESSFULLY");
    assert.strictEqual(postMortem.circuit_tripped, false);
    assert.strictEqual(postMortem.total_spans, 2);
    assert.strictEqual(postMortem.total_tokens, 1350);
    assert.strictEqual(postMortem.roi.dollars_saved, 0.0);
    assert.strictEqual(postMortem.roi.tokens_saved, 0);
    assert.strictEqual(postMortem.roi.turns_prevented, 0);
    assert.strictEqual(postMortem.roi.tokens_used, 1350);
    assert.strictEqual(postMortem.root_cause, null);
    assert.ok(postMortem.recommendations[0].includes("healthy"));
  });

  await t.test("analyzes circuit-tripped swarm with counterfactual financial ROI", () => {
    const swarmId = "swarm-tripped-202";
    const sink = new InMemoryStorageSink();

    const spans = [
      {
        id: "span-a",
        swarm_id: swarmId,
        source_agent: "Architect",
        target_agent: "Coder",
        span_type: "agent_call",
        start_time: "2026-09-17T12:00:00.000Z",
        end_time: "2026-09-17T12:00:01.000Z",
        token_count: 400,
        entropy_score: 0.85,
        input_payload: { prompt: "Write DB migration" },
        output_payload: { code: "CREATE TABLE t1;" },
      },
      {
        id: "span-b",
        swarm_id: swarmId,
        source_agent: "Coder",
        target_agent: "DatabaseAgent",
        span_type: "agent_call",
        start_time: "2026-09-17T12:00:01.200Z",
        end_time: "2026-09-17T12:00:02.000Z",
        token_count: 350,
        entropy_score: 0.15,
        input_payload: { sql: "SELECT 1; SELECT 1; SELECT 1;" },
        output_payload: { error: "Deadlock detected" },
      },
    ];

    const circuitEvent = {
      id: "event-trip-1",
      swarm_id: swarmId,
      span_id: "span-b",
      event_type: "trip",
      reason: "Entropy collapse H_norm=0.15 <= 0.35 threshold",
      metric_val: 0.15,
      counterfactual_savings: {
        dollars_saved: 4.85,
        tokens_saved: 242500,
        turns_averted: 45,
        budget_consumed: 750,
        budget_protected: 242500,
      },
      created_at: "2026-09-17T12:00:02.050Z",
    };

    sink.saveSpans(spans);
    sink.recordCircuitEvent(circuitEvent);

    const generator = IncidentReportGenerator.fromSink(sink);
    const postMortem = generator.analyze(swarmId);

    assert.ok(postMortem);
    assert.strictEqual(postMortem.status, "HALTED_BY_CIRCUIT_BREAKER");
    assert.strictEqual(postMortem.circuit_tripped, true);
    assert.strictEqual(postMortem.roi.dollars_saved, 4.85);
    assert.strictEqual(postMortem.roi.tokens_saved, 242500);
    assert.strictEqual(postMortem.roi.turns_prevented, 45);
    assert.strictEqual(postMortem.roi.tokens_used, 750);

    assert.ok(postMortem.root_cause);
    assert.strictEqual(postMortem.root_cause.source_agent, "Coder");
    assert.strictEqual(postMortem.root_cause.target_agent, "DatabaseAgent");
    assert.strictEqual(postMortem.root_cause.trigger_span_id, "span-b");
    assert.strictEqual(postMortem.root_cause.trigger_entropy, 0.15);
    assert.ok(postMortem.recommendations[0].includes("collapsed in vocabulary diversity"));
  });

  await t.test("constructs chronological entropy trajectory timeline", () => {
    const swarmId = "swarm-timeline-303";
    const spans = [
      {
        id: "span-t1",
        swarm_id: swarmId,
        source_agent: "A1",
        target_agent: "A2",
        start_time: "2026-09-17T14:00:00.000Z",
        end_time: "2026-09-17T14:00:01.000Z",
        token_count: 100,
        entropy_score: 0.95,
      },
      {
        id: "span-t2",
        swarm_id: swarmId,
        source_agent: "A2",
        target_agent: "A3",
        start_time: "2026-09-17T14:00:01.500Z",
        end_time: "2026-09-17T14:00:02.500Z",
        token_count: 150,
        entropy_score: 0.60,
      },
      {
        id: "span-t3",
        swarm_id: swarmId,
        source_agent: "A3",
        target_agent: "A1",
        start_time: "2026-09-17T14:00:03.000Z",
        end_time: "2026-09-17T14:00:04.000Z",
        token_count: 200,
        entropy_score: 0.22,
      },
    ];

    const generator = IncidentReportGenerator.fromSpans(spans);
    const postMortem = generator.analyze(swarmId);

    assert.ok(postMortem);
    assert.strictEqual(postMortem.entropy_trajectory.length, 3);
    assert.strictEqual(postMortem.entropy_trajectory[0].step_index, 1);
    assert.strictEqual(postMortem.entropy_trajectory[0].entropy_score, 0.95);
    assert.strictEqual(postMortem.entropy_trajectory[1].step_index, 2);
    assert.strictEqual(postMortem.entropy_trajectory[1].entropy_score, 0.60);
    assert.strictEqual(postMortem.entropy_trajectory[2].step_index, 3);
    assert.strictEqual(postMortem.entropy_trajectory[2].entropy_score, 0.22);
  });

  await t.test("tallies neutralized security credentials from SensitiveDataMasker", () => {
    const swarmId = "swarm-security-404";
    const spans = [
      {
        id: "span-sec-1",
        swarm_id: swarmId,
        source_agent: "DevOps",
        target_agent: "CloudDeployer",
        start_time: "2026-09-17T15:00:00.000Z",
        end_time: "2026-09-17T15:00:01.000Z",
        input_payload: {
          key: "[REDACTED_API_KEY]",
          aws: "[REDACTED_AWS_KEY]",
        },
        output_payload: {
          db: "[REDACTED_DATABASE_DSN]",
          extra_key: "[REDACTED_API_KEY]",
        },
      },
    ];

    const generator = IncidentReportGenerator.fromSpans(spans);
    const postMortem = generator.analyze(swarmId);

    assert.ok(postMortem);
    assert.strictEqual(postMortem.security_summary.total_redacted, 4);
    assert.strictEqual(postMortem.security_summary.redacted_categories["API_KEY"], 2);
    assert.strictEqual(postMortem.security_summary.redacted_categories["AWS_KEY"], 1);
    assert.strictEqual(postMortem.security_summary.redacted_categories["DATABASE_DSN"], 1);
  });

  await t.test("formats reports across markdown, json, and terminal representations", () => {
    const swarmId = "swarm-formats-505";
    const spans = [
      {
        id: "span-f1",
        swarm_id: swarmId,
        source_agent: "AgentA",
        target_agent: "AgentB",
        start_time: "2026-09-17T16:00:00.000Z",
        end_time: "2026-09-17T16:00:01.000Z",
        token_count: 500,
        entropy_score: 0.85,
        input_payload: { key: "[REDACTED_API_KEY]" },
        output_payload: { res: "OK" },
      },
    ];

    const circuitEvent = {
      id: "event-trip-f",
      swarm_id: swarmId,
      span_id: "span-f1",
      event_type: "trip",
      reason: "Infinite tool recursion detected",
      metric_val: 1.0,
      counterfactual_savings: {
        dollars_saved: 3.70,
        tokens_saved: 185000,
        turns_averted: 35,
        budget_consumed: 500,
        budget_protected: 185000,
      },
      created_at: "2026-09-17T16:00:01.050Z",
    };

    const generator = IncidentReportGenerator.fromSpans(spans, [circuitEvent]);

    // 1. Markdown Format
    const md = generator.generateReport(swarmId, "markdown");
    assert.ok(typeof md === "string");
    assert.ok(md.includes(`# SwarmWatch Incident Post-Mortem: ${swarmId}`));
    assert.ok(md.includes("Counterfactual Dollars Saved"));
    assert.ok(md.includes("$3.70"));
    assert.ok(md.includes("Infinite tool recursion detected"));
    assert.ok(md.includes("Tool invocation loop detected"));

    // 2. JSON Format
    const json = generator.generateReport(swarmId, "json");
    assert.strictEqual(typeof json, "object");
    assert.strictEqual(json.swarm_id, swarmId);
    assert.strictEqual(json.roi.dollars_saved, 3.70);
    assert.strictEqual(json.security_summary.total_redacted, 1);

    // 3. Terminal Format
    const term = generator.generateReport(swarmId, "terminal");
    assert.ok(typeof term === "string");
    assert.ok(term.includes("SWARMWATCH INCIDENT POST-MORTEM"));
    assert.ok(term.includes("Dollars Saved:       $3.70"));
    assert.ok(term.includes("Tokens Prevented:    185,000 tokens"));
  });

  await t.test("high-throughput benchmark: executes in < 2ms per incident report", () => {
    const swarmId = "swarm-bench-606";
    const spans = Array.from({ length: 20 }, (_, i) => ({
      id: `span-bench-${i}`,
      swarm_id: swarmId,
      source_agent: `Agent-${i % 3}`,
      target_agent: `Agent-${(i + 1) % 3}`,
      start_time: new Date(Date.now() + i * 100).toISOString(),
      end_time: new Date(Date.now() + i * 100 + 50).toISOString(),
      token_count: 200,
      entropy_score: 0.75,
      input_payload: { arg: `query-${i}`, secret: "[REDACTED_API_KEY]" },
      output_payload: { res: `result-${i}` },
    }));

    const generator = IncidentReportGenerator.fromSpans(spans);

    const start = performance.now();
    const iterations = 50;
    for (let i = 0; i < iterations; i++) {
      generator.generateReport(swarmId, "markdown");
    }
    const elapsed = performance.now() - start;
    const avgMs = elapsed / iterations;

    assert.ok(
      avgMs < 2.0,
      `Report generation benchmark exceeded 2ms (measured: ${avgMs.toFixed(3)}ms)`
    );
  });
});
