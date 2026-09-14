/**
 * Tests for SwarmClient Developer Surface, TelemetryTransport,
 * SafeStateSerializer, and Context Propagation (TypeScript SDK).
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  init,
  getClient,
  withSpan,
  agent,
  syncState,
  InMemoryStorageSink,
  TelemetryTransport,
  SafeStateSerializer,
} from "../src/index.ts";

test("SafeStateSerializer & State Hashing", async (t) => {
  const serializer = new SafeStateSerializer();

  await t.test("redacts sensitive keys", () => {
    const raw = {
      user: "alice",
      api_key: "sk-secret-12345",
      password: "my-password",
      nested: {
        token: "jwt-token-999",
        normal: "hello",
      },
    };
    const sanitized = serializer.sanitize(raw);
    assert.strictEqual(sanitized.user, "alice");
    assert.strictEqual(sanitized.api_key, "******** [REDACTED]");
    assert.strictEqual(sanitized.password, "******** [REDACTED]");
    assert.strictEqual(sanitized.nested.token, "******** [REDACTED]");
    assert.strictEqual(sanitized.nested.normal, "hello");
  });

  await t.test("breaks circular references safely", () => {
    const objA = { name: "A" };
    const objB = { name: "B", a: objA };
    objA.b = objB; // circular loop

    const sanitized = serializer.sanitize(objA);
    assert.strictEqual(sanitized.name, "A");
    assert.strictEqual(sanitized.b.name, "B");
    assert.strictEqual(sanitized.b.a, "[CIRCULAR_REFERENCE]");
  });

  await t.test("computes deterministic SHA-256 hash", () => {
    const data1 = { a: 1, b: 2 };
    const data2 = { a: 1, b: 2 };
    const hash1 = serializer.computeHash(data1);
    const hash2 = serializer.computeHash(data2);
    assert.strictEqual(hash1, hash2);
    assert.strictEqual(hash1.length, 64);
  });
});

test("TelemetryTransport Bounded Ring Buffer", async (t) => {
  await t.test("buffers spans and flushes to sink asynchronously", async () => {
    const sink = new InMemoryStorageSink();
    const transport = new TelemetryTransport(sink, { batch_size: 5, flush_interval_ms: 100 });
    transport.start();

    const dummySpan = {
      id: "span-1",
      swarm_id: "swarm-test",
      source_agent: "agentA",
      target_agent: "agentB",
      start_time: new Date().toISOString(),
      end_time: new Date().toISOString(),
    };

    const queued = transport.emitSpan(dummySpan);
    assert.strictEqual(queued, true);

    await transport.flush();
    assert.strictEqual(sink.spans.length, 1);
    assert.strictEqual(sink.spans[0].id, "span-1");

    await transport.shutdown();
  });

  await t.test("drops spans when queue capacity is reached without blocking", async () => {
    const sink = new InMemoryStorageSink();
    const transport = new TelemetryTransport(sink, { max_queue_size: 2 });

    const spanA = { id: "1", swarm_id: "s", source_agent: "a", target_agent: "b", start_time: "", end_time: "" };
    const spanB = { id: "2", swarm_id: "s", source_agent: "a", target_agent: "b", start_time: "", end_time: "" };
    const spanC = { id: "3", swarm_id: "s", source_agent: "a", target_agent: "b", start_time: "", end_time: "" };

    assert.strictEqual(transport.emitSpan(spanA), true);
    assert.strictEqual(transport.emitSpan(spanB), true);
    // Queue is now full (max_queue_size=2)
    assert.strictEqual(transport.emitSpan(spanC), false);
    assert.strictEqual(transport.droppedSpans, 1);
  });
});

test("SwarmClient & Async Context Propagation", async (t) => {
  await t.test("tracks nested spans with parent-child relationship", async () => {
    const sink = new InMemoryStorageSink();
    const client = init({ swarm_id: "test-nesting", sink });

    await client.withSpan({ name: "parent_task", source: "orchestrator", target: "worker" }, async (parentSpan) => {
      assert.strictEqual(parentSpan.parent_span_id, null);

      // Nested child span
      await client.withSpan({ name: "child_task", source: "worker", target: "database" }, async (childSpan) => {
        assert.strictEqual(childSpan.parent_span_id, parentSpan.id);
      });
    });

    await client.flush();
    assert.strictEqual(sink.spans.length, 2);
    const child = sink.spans.find((s) => s.source_agent === "worker");
    const parent = sink.spans.find((s) => s.source_agent === "orchestrator");
    assert.ok(child && parent);
    assert.strictEqual(child.parent_span_id, parent.id);

    await client.shutdown();
  });

  await t.test("agent() decorator wraps async function and captures return value", async () => {
    const sink = new InMemoryStorageSink();
    const client = init({ swarm_id: "test-agent", sink });

    const runAgent = client.agent("researcher", async (query) => {
      return { answer: `Analysis for ${query}` };
    });

    const result = await runAgent("market trend");
    assert.deepStrictEqual(result, { answer: "Analysis for market trend" });

    await client.flush();
    assert.strictEqual(sink.spans.length, 1);
    assert.strictEqual(sink.spans[0].source_agent, "researcher");
    assert.strictEqual(sink.spans[0].output_payload.answer, "Analysis for market trend");

    await client.shutdown();
  });

  await t.test("syncState() records sanitized state snapshots", async () => {
    const sink = new InMemoryStorageSink();
    const client = init({ swarm_id: "test-state", sink });

    const snapshot = client.syncState({ memory: "stored fact", api_key: "confidential" });
    assert.strictEqual(snapshot.state_data.memory, "stored fact");
    assert.strictEqual(snapshot.state_data.api_key, "******** [REDACTED]");
    assert.strictEqual(sink.snapshots.length, 1);

    await client.shutdown();
  });
});
