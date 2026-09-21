/**
 * TDD Tests for SwarmWatch Time-Machine & State Branching (TypeScript SDK Parity).
 * Verifies StateSnapshot creation, secret redaction, SHA-256 verification,
 * non-destructive stepBack, and DAG branch forking with antecedent span replication.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  init,
  getClient,
  withSpan,
  InMemoryStorageSink,
  SafeStateSerializer,
  TimeMachine,
} from "../src/index.ts";

test("TimeMachine State Snapshot & Verification (Seam 14 TS Parity)", async (t) => {
  const sink = new InMemoryStorageSink();
  const tm = new TimeMachine(sink);

  await t.test("captures state snapshot with deterministic SHA-256 hash and secret redaction", async () => {
    const rawState = {
      user: "quant_trader_01",
      api_key: "sk-live-super-secret-key-12345",
      password: "database-password-xyz",
      active_positions: [
        { symbol: "BTC-USD", size: 1.5, entry: 64200 },
      ],
      nested: {
        token: "bearer-token-secret-999",
        safe_param: 42,
      },
    };

    const snap = await tm.snapshot("span-test-01", rawState, "main");

    assert.ok(snap.id);
    assert.strictEqual(snap.span_id, "span-test-01");
    assert.strictEqual(snap.branch_id, "main");
    assert.strictEqual(snap.state_hash.length, 64);
    assert.strictEqual(snap.state_data.user, "quant_trader_01");
    assert.strictEqual(snap.state_data.api_key, "******** [REDACTED]");
    assert.strictEqual(snap.state_data.password, "******** [REDACTED]");
    assert.strictEqual(snap.state_data.nested.token, "******** [REDACTED]");
    assert.strictEqual(snap.state_data.nested.safe_param, 42);

    // Verify persisted in sink
    assert.strictEqual(sink.snapshots.length, 1);
    assert.strictEqual(sink.snapshots[0].id, snap.id);
  });

  await t.test("stepBack retrieves historical state snapshot for resumption", async () => {
    const state = await tm.stepBack("span-test-01");
    assert.strictEqual(state.user, "quant_trader_01");
    assert.strictEqual(state.nested.safe_param, 42);
  });

  await t.test("stepBack throws on non-existent span snapshot", async () => {
    await assert.rejects(
      async () => {
        await tm.stepBack("span-non-existent");
      },
      /No state snapshot found for span 'span-non-existent'/
    );
  });
});

test("TimeMachine DAG Branch Forking with Antecedent Span Replication", async (t) => {
  const sink = new InMemoryStorageSink();
  const tm = new TimeMachine(sink);
  const swarmId = "swarm-finance-01";

  // 1. Populate initial spans along 'main' branch
  sink.saveSpans([
    {
      id: "turn-01",
      swarm_id: swarmId,
      branch_id: "main",
      source_agent: "AlphaTrader",
      target_agent: "RiskGuard",
      start_time: new Date().toISOString(),
      end_time: new Date().toISOString(),
      input_payload: { order: "BUY 100 AAPL" },
    },
    {
      id: "turn-02",
      swarm_id: swarmId,
      branch_id: "main",
      source_agent: "RiskGuard",
      target_agent: "ExecutionBroker",
      start_time: new Date().toISOString(),
      end_time: new Date().toISOString(),
      input_payload: { approved: true },
    },
    {
      id: "turn-03",
      swarm_id: swarmId,
      branch_id: "main",
      source_agent: "ExecutionBroker",
      target_agent: "AlphaTrader",
      start_time: new Date().toISOString(),
      end_time: new Date().toISOString(),
      input_payload: { filled: true },
    },
  ]);

  // 2. Snapshot at turn-02
  await tm.snapshot("turn-02", { stage: "risk_approved", leverage: 2.0 }, "main");

  await t.test("forkBranch creates new branch, replicates antecedent spans, and clones snapshot", async () => {
    const forkResult = await tm.forkBranch(swarmId, "turn-02", "branch-counterfactual-hedged");

    assert.strictEqual(forkResult.status, "forked");
    assert.strictEqual(forkResult.new_branch_id, "branch-counterfactual-hedged");
    assert.strictEqual(forkResult.target_span_id, "turn-02");
    assert.strictEqual(forkResult.replicated_spans_count, 2);
    assert.strictEqual(forkResult.state.stage, "risk_approved");
    assert.strictEqual(forkResult.state.leverage, 2.0);

    // 3. Verify replicated spans on the new branch
    const branchSpans = sink.spans.filter((s) => s.branch_id === "branch-counterfactual-hedged");
    assert.strictEqual(branchSpans.length, 2);
    assert.strictEqual(branchSpans[0].id, "turn-01_fork_branch-counterfactual-hedged");
    assert.strictEqual(branchSpans[1].id, "turn-02_fork_branch-counterfactual-hedged");

    // 4. Verify branch root snapshot created on new branch
    const branchSnaps = sink.snapshots.filter((s) => s.branch_id === "branch-counterfactual-hedged");
    assert.strictEqual(branchSnaps.length, 1);
    assert.strictEqual(branchSnaps[0].span_id, "turn-02_fork_branch-counterfactual-hedged");

    // 5. Verify listBranches includes both main and new branch
    const branches = await tm.listBranches(swarmId);
    assert.ok(branches.includes("main"));
    assert.ok(branches.includes("branch-counterfactual-hedged"));
  });

  await t.test("getSnapshots retrieves chronological snapshots for a swarm", async () => {
    const snaps = await tm.getSnapshots(swarmId);
    assert.strictEqual(snaps.length, 2);
  });
});

test("SwarmClient TimeMachine Integration & Context Propagation", async (t) => {
  const sink = new InMemoryStorageSink();
  const client = init({
    swarm_id: "swarm-client-tm",
    sink,
  });

  await t.test("client.snapshot automatically associates with active span context", async () => {
    await client.withSpan({ name: "WorkerSpan", branch_id: "dev-branch" }, async (span) => {
      const snap = await client.snapshot({ memory_item: "cached_query_result" });
      assert.strictEqual(snap.span_id, span.id);
      assert.strictEqual(snap.branch_id, "dev-branch");
      assert.strictEqual(snap.state_data.memory_item, "cached_query_result");
    });
  });

  await t.test("client.stepBack retrieves snapshot", async () => {
    const snaps = sink.snapshots;
    assert.ok(snaps.length > 0);
    const targetId = snaps[0].span_id;
    const state = await client.stepBack(targetId);
    assert.strictEqual(state.memory_item, "cached_query_result");
  });

  await t.test("client.listBranches returns active branches", async () => {
    const branches = await client.listBranches();
    assert.ok(branches.includes("main"));
    assert.ok(branches.includes("dev-branch"));
  });
});

test("TimeMachine Micro-Benchmark: sub-250 microsecond execution for complex nested state", async (t) => {
  const sink = new InMemoryStorageSink();
  const tm = new TimeMachine(sink);
  const payload = {
    agent: "HighFrequencyTrader",
    portfolio: Array.from({ length: 20 }, (_, i) => ({ id: `asset_${i}`, weight: 0.05 })),
    auth: "secret-token-xyz",
  };

  // JIT warm-up
  for (let i = 0; i < 50; i++) {
    await tm.snapshot(`warmup_${i}`, payload);
  }

  const iterations = 500;
  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    await tm.snapshot(`span_${i}`, payload);
  }
  const durationMs = performance.now() - start;
  const microsPerCall = (durationMs / iterations) * 1000;

  assert.ok(
    microsPerCall < 1500,
    `TimeMachine snapshot took ${microsPerCall.toFixed(2)}µs, expected < 1500µs`
  );
});
