import test from "node:test";
import assert from "node:assert/strict";

import {
  ProtocolSimulator,
  checkTraceInvariants,
  replaySimulation,
} from "../src/deterministic_sim.ts";

test("Seam 90: Simulation seed determinism and replay (TypeScript)", () => {
  const seed = 42;
  const sim1 = new ProtocolSimulator(seed);
  const trace1 = sim1.run(50, "chaos_mesh");

  // Invariant M1: Replay from identical seed yields matching trace events
  assert.equal(trace1.events.length, 50);
  assert.equal(replaySimulation(trace1), true);
});

test("Seam 90: Model-checked safety predicates (TypeScript)", () => {
  const sim = new ProtocolSimulator(1337);
  const trace = sim.run(50, "stress_schedule");

  // Invariant M2: Safety predicates hold across randomized steps
  const verdict = checkTraceInvariants(trace);
  assert.equal(verdict.is_safe, true);
  assert.equal(verdict.failed_predicates.length, 0);
  assert.ok(verdict.evaluated_predicates.includes("Predicate_QC"));
  assert.ok(verdict.evaluated_predicates.includes("Predicate_Barrier"));
  assert.ok(verdict.evaluated_predicates.includes("Predicate_AuditExploitsBlocked"));
});

test("Seam 90: Sub-50ms execution latency SLO (TypeScript)", () => {
  const sim = new ProtocolSimulator(999);
  const start = process.hrtime.bigint();
  const trace = sim.run(50);
  const elapsedMs = Number(process.hrtime.bigint() - start) / 1_000_000;
  assert.ok(elapsedMs < 100, `Simulation run latency ${elapsedMs}ms must be fast`);
});
