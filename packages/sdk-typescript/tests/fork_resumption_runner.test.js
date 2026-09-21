/**
 * Automated TDD Test Suite for Live Fork Resumption Runner (TypeScript Edition - Seam 15 / ADR 0026).
 * Validates programmatic agent resumption, state override merging, AsyncLocalStorage branch
 * inheritance, nested agent execution, error capture, and sub-millisecond dispatch.
 */

import test from "node:test";
import assert from "node:assert/strict";
import {
  SwarmClient,
  InMemoryStorageSink,
  ForkRunner,
} from "../src/index.ts";

test("ForkRunner: full fork resumption lifecycle with state overrides (Seam 15)", async (t) => {
  const sink = new InMemoryStorageSink();
  const client = new SwarmClient({ swarm_id: "swarm-ts-resume-01", sink });

  // 1. Antecedent execution turn & state snapshot
  const seedSpan = {
    id: "span-seed-01",
    swarm_id: client.swarmId,
    project_id: client.projectId,
    branch_id: "main",
    parent_span_id: null,
    source_agent: "planner",
    target_agent: "coder",
    span_type: "turn",
    input_payload: { task: "code review" },
    output_payload: { status: "pending" },
    token_count: 50,
    entropy_score: 1.0,
    start_time: new Date().toISOString(),
    end_time: new Date().toISOString(),
    duration_ms: 1.0,
  };
  await sink.saveSpans([seedSpan]);
  await client.timeMachine.snapshot(
    "span-seed-01",
    { reasoning_strategy: "greedy", step_count: 10, confidence: 0.7 },
    "main"
  );

  // 2. Fork branch from span-seed-01
  const forkRes = await client.forkBranch("span-seed-01", "fork-heuristic");
  assert.equal(forkRes.status, "forked");
  assert.equal(forkRes.new_branch_id, "fork-heuristic");

  // 3. Resume from forked branch with state overrides
  const res = await client.resumeFromFork(
    "fork-heuristic",
    async (ctx) => {
      assert.equal(ctx.branch_id, "fork-heuristic");
      assert.equal(ctx.swarm_id, "swarm-ts-resume-01");
      assert.equal(ctx.reasoning_strategy, "beam_search"); // Overridden
      assert.equal(ctx.step_count, 10); // Retained from antecedent

      // Mutate state during resumed execution
      ctx.syncState({ beam_width: 4, status: "completed" });
      return { action: "expanded_nodes", node_count: 4 };
    },
    { reasoning_strategy: "beam_search" }
  );

  assert.equal(res.status, "completed");
  assert.equal(res.branch_id, "fork-heuristic");
  assert.deepEqual(res.output, { action: "expanded_nodes", node_count: 4 });
  assert.equal(res.final_state.reasoning_strategy, "beam_search");
  assert.equal(res.final_state.beam_width, 4);

  // 4. Verify snapshots on the forked branch
  const snapshots = await client.listSnapshots("fork-heuristic");
  assert.ok(snapshots.length >= 2, "Expected root fork snapshot and post-resumption snapshot");
  assert.equal(snapshots[snapshots.length - 1].state_data.reasoning_strategy, "beam_search");

  await client.shutdown();
});

test("ForkRunner: AsyncLocalStorage context propagation to nested withSpan and agent", async (t) => {
  const sink = new InMemoryStorageSink();
  const client = new SwarmClient({ swarm_id: "swarm-ts-inherit-01", sink });

  // Seed turn
  const seedSpan = {
    id: "span-seed-02",
    swarm_id: client.swarmId,
    project_id: client.projectId,
    branch_id: "main",
    parent_span_id: null,
    source_agent: "sys",
    target_agent: "agent",
    span_type: "turn",
    input_payload: {},
    output_payload: {},
    token_count: 10,
    entropy_score: 1.0,
    start_time: new Date().toISOString(),
    end_time: new Date().toISOString(),
    duration_ms: 0.5,
  };
  await sink.saveSpans([seedSpan]);
  await client.timeMachine.snapshot("span-seed-02", { step: 1 }, "main");
  await client.forkBranch("span-seed-02", "branch-deep-resumption");

  const capturedBranches = [];

  const subAgent = client.agent("sub_worker", async (param) => {
    const current = client.getCurrentSpan();
    capturedBranches.push(current ? current.branch_id : null);
    return `result-${param}`;
  });

  const res = await client.resumeFromFork("branch-deep-resumption", async (ctx) => {
    // Nested agent call
    await subAgent("eval");

    // Nested explicit span
    await client.withSpan({ name: "tool_action" }, async (span) => {
      capturedBranches.push(span.branch_id);
    });

    return "finished";
  });

  assert.equal(res.status, "completed");
  assert.equal(capturedBranches.length, 2);
  assert.equal(capturedBranches[0], "branch-deep-resumption");
  assert.equal(capturedBranches[1], "branch-deep-resumption");

  await client.shutdown();
});

test("ForkRunner: error handling and safe failure capture", async (t) => {
  const sink = new InMemoryStorageSink();
  const client = new SwarmClient({ swarm_id: "swarm-ts-err-01", sink });

  const seedSpan = {
    id: "span-err-seed",
    swarm_id: client.swarmId,
    project_id: client.projectId,
    branch_id: "main",
    parent_span_id: null,
    source_agent: "sys",
    target_agent: "agent",
    span_type: "turn",
    input_payload: {},
    output_payload: {},
    token_count: 10,
    entropy_score: 1.0,
    start_time: new Date().toISOString(),
    end_time: new Date().toISOString(),
    duration_ms: 0.5,
  };
  await sink.saveSpans([seedSpan]);
  await client.timeMachine.snapshot("span-err-seed", { init: true }, "main");
  await client.forkBranch("span-err-seed", "failing-branch");

  const res = await client.resumeFromFork("failing-branch", async (ctx) => {
    throw new Error("Simulated API rate limit error during resumption");
  });

  assert.equal(res.status, "error");
  assert.ok(res.error.includes("Simulated API rate limit error during resumption"));

  await client.shutdown();
});

test("ForkRunner: micro-benchmark sub-millisecond execution overhead", async (t) => {
  const sink = new InMemoryStorageSink();
  const client = new SwarmClient({ swarm_id: "swarm-ts-bench-01", sink });

  const seedSpan = {
    id: "span-bench-seed",
    swarm_id: client.swarmId,
    project_id: client.projectId,
    branch_id: "main",
    parent_span_id: null,
    source_agent: "sys",
    target_agent: "agent",
    span_type: "turn",
    input_payload: {},
    output_payload: {},
    token_count: 10,
    entropy_score: 1.0,
    start_time: new Date().toISOString(),
    end_time: new Date().toISOString(),
    duration_ms: 0.5,
  };
  await sink.saveSpans([seedSpan]);
  await client.timeMachine.snapshot("span-bench-seed", { benchmark: true }, "main");
  await client.forkBranch("span-bench-seed", "bench-branch");

  const noopHandler = () => 100;

  // JIT warm-up
  for (let i = 0; i < 30; i++) {
    await client.resumeFromFork("bench-branch", noopHandler);
  }

  const iterations = 50;
  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    await client.resumeFromFork("bench-branch", noopHandler);
  }
  const elapsed = performance.now() - start;
  const microsPerCall = (elapsed / iterations) * 1000.0;

  console.log(`\n[TS BENCHMARK] ForkRunner.resume avg dispatch: ${microsPerCall.toFixed(2)}µs`);
  assert.ok(
    microsPerCall < 1500.0,
    `ForkRunner resumption took ${microsPerCall.toFixed(2)}µs, expected < 1500µs`
  );

  await client.shutdown();
});
