import test from "node:test";
import assert from "node:assert/strict";

import {
  HybridLogicalClock,
  compareHlc,
  formatHlc,
  parseHlc,
  BoundedSkewViolationError,
} from "../src/hlc.ts";

test("Seam 83: HLC local monotonicity (TypeScript)", () => {
  let fakeTime = 1_000_000_000;
  const clock = new HybridLogicalClock("node-a", 500_000_000, () => fakeTime);

  const t1 = clock.now();
  assert.equal(t1.physical_ns, fakeTime);
  assert.equal(t1.logical_counter, 0);
  assert.equal(t1.node_id, "node-a");

  const t2 = clock.now();
  assert.equal(t2.physical_ns, fakeTime);
  assert.equal(t2.logical_counter, 1);
  assert.equal(compareHlc(t1, t2), -1);

  fakeTime = 1_000_000_500;
  const t3 = clock.now();
  assert.equal(t3.physical_ns, fakeTime);
  assert.equal(t3.logical_counter, 0);
  assert.equal(compareHlc(t2, t3), -1);
});

test("Seam 83: Causal message observation (TypeScript)", () => {
  const timeA = 1_000_000_000;
  const timeB = 1_000_000_100;

  const clockA = new HybridLogicalClock("node-a", 500_000_000, () => timeA);
  const clockB = new HybridLogicalClock("node-b", 500_000_000, () => timeB);

  const msgTs = clockA.now();
  const recvTs = clockB.observe(msgTs);

  // Invariant H1: msgTs < recvTs
  assert.equal(compareHlc(msgTs, recvTs), -1);
  assert.ok(recvTs.physical_ns >= msgTs.physical_ns);
});

test("Seam 83: Bounded skew violation rejected (TypeScript)", () => {
  const localTime = 1_000_000_000;
  const maxDrift = 500_000_000;

  const clock = new HybridLogicalClock("node-local", maxDrift, () => localTime);

  const maliciousRemote = {
    physical_ns: localTime + 1_000_000_000,
    logical_counter: 0,
    node_id: "node-adversary",
  };

  assert.throws(() => {
    clock.observe(maliciousRemote);
  }, BoundedSkewViolationError);
});

test("Seam 83: Strict total ordering tie-breaking (TypeScript)", () => {
  const tAlpha = { physical_ns: 100, logical_counter: 5, node_id: "cluster-alpha" };
  const tBeta = { physical_ns: 100, logical_counter: 5, node_id: "cluster-beta" };

  assert.equal(compareHlc(tAlpha, tBeta), -1);
  assert.equal(compareHlc(tBeta, tAlpha), 1);
  assert.equal(compareHlc(tAlpha, tAlpha), 0);
});

test("Seam 83: Sub-250us execution latency SLO (TypeScript)", () => {
  const clock = new HybridLogicalClock("perf-node");

  // Warmup
  for (let i = 0; i < 100; i++) clock.now();

  const startNs = process.hrtime.bigint();
  const iterations = 1000;
  for (let i = 0; i < iterations; i++) {
    clock.now();
  }
  const avgUs = Number(process.hrtime.bigint() - startNs) / (iterations * 1000);
  assert.ok(avgUs < 10, `Average HLC tick latency ${avgUs}us must be well within SLO`);
});
