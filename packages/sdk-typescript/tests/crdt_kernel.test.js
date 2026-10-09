import test from "node:test";
import assert from "node:assert/strict";

import {
  DeltaCrdtKernel,
  mergeLwwProps,
} from "../src/crdt_kernel.ts";

test("Seam 84: Algebraic commutativity and idempotence (TypeScript)", () => {
  const ts1 = { physical_ns: 100, logical_counter: 1, node_id: "cluster-a" };
  const ts2 = { physical_ns: 100, logical_counter: 2, node_id: "cluster-b" };

  const propsA = {
    status: { value: "ACTIVE", timestamp: ts1 },
    weight: { value: 10, timestamp: ts1 },
  };
  const propsB = {
    status: { value: "SUSPENDED", timestamp: ts2 },
    priority: { value: "HIGH", timestamp: ts2 },
  };

  // Commutativity: merge(A, B) == merge(B, A)
  const ab = mergeLwwProps(propsA, propsB);
  const ba = mergeLwwProps(propsB, propsA);

  assert.deepEqual(Object.keys(ab).sort(), ["priority", "status", "weight"]);
  assert.deepEqual(Object.keys(ba).sort(), ["priority", "status", "weight"]);

  for (const k of Object.keys(ab)) {
    assert.deepEqual(ab[k].value, ba[k].value);
    assert.deepEqual(ab[k].timestamp, ba[k].timestamp);
  }

  // Idempotence: merge(A, A) == A
  const aa = mergeLwwProps(propsA, propsA);
  for (const k of Object.keys(propsA)) {
    assert.deepEqual(aa[k].value, propsA[k].value);
    assert.deepEqual(aa[k].timestamp, propsA[k].timestamp);
  }
});

test("Seam 84: Causal delivery and dangling edge buffering (TypeScript)", () => {
  const replica = new DeltaCrdtKernel("cluster-target");

  // Edge arriving BEFORE vertices are created
  const remote = new DeltaCrdtKernel("cluster-remote");
  const edgeDelta = remote.putEdge("node-101", "node-102", "CONNECTS_TO", {
    bandwidth: "10Gbps",
  });

  // Invariant C2: Merge edge delta into target replica
  const applied = replica.mergeDelta(edgeDelta);
  assert.equal(applied, 0); // Not applied yet
  assert.equal(replica.pendingBufferCount(), 1); // Buffered safely without circuit trip!
  assert.equal(replica.getEdge("node-101", "node-102", "CONNECTS_TO"), null);

  // Now vertices arrive
  const v1Delta = remote.putVertex("node-101", { role: "WORKER" });
  const v2Delta = remote.putVertex("node-102", { role: "LEADER" });

  replica.mergeDelta(v1Delta);
  assert.equal(replica.pendingBufferCount(), 1); // Still waiting for node-102

  replica.mergeDelta(v2Delta);
  // Both endpoints now present: buffered edge is causally drained!
  assert.equal(replica.pendingBufferCount(), 0);
  const edge = replica.getEdge("node-101", "node-102", "CONNECTS_TO");
  assert.notEqual(edge, null);
  assert.equal(edge.properties.bandwidth, "10Gbps");
});

test("Seam 84: State digest convergence under reordered deltas (TypeScript)", () => {
  const source = new DeltaCrdtKernel("primary");
  const d1 = source.putVertex("v-alpha", { name: "Alpha", score: 95 });
  const d2 = source.putVertex("v-beta", { name: "Beta", score: 88 });
  const d3 = source.putEdge("v-alpha", "v-beta", "PARENT_OF", { weight: 1.0 });

  const replica1 = new DeltaCrdtKernel("replica-1");
  const replica2 = new DeltaCrdtKernel("replica-2");

  // Order 1: d1 -> d2 -> d3
  replica1.mergeDelta(d1);
  replica1.mergeDelta(d2);
  replica1.mergeDelta(d3);

  // Order 2: d3 (buffered!) -> d2 -> d1 (triggers drain)
  replica2.mergeDelta(d3);
  replica2.mergeDelta(d2);
  replica2.mergeDelta(d1);

  // Invariant C3: identical deterministic Merkle digests
  assert.equal(replica1.pendingBufferCount(), 0);
  assert.equal(replica2.pendingBufferCount(), 0);
  assert.equal(replica1.getStateDigest(), replica2.getStateDigest());
});

test("Seam 84: Sub-250us execution latency SLO (TypeScript)", () => {
  const kernel = new DeltaCrdtKernel("perf-cluster");

  // Warmup
  for (let i = 0; i < 50; i++) kernel.putVertex(`warmup-${i}`, { v: i });

  const startNs = process.hrtime.bigint();
  const count = 500;
  for (let i = 0; i < count; i++) {
    kernel.putVertex(`v-${i}`, { index: i, val: i * 2 });
  }
  const avgUs = Number(process.hrtime.bigint() - startNs) / (count * 1000);
  assert.ok(avgUs < 250, `Average putVertex latency ${avgUs}us must be within 250us SLO`);
});
