/**
 * Tests for SwarmWatch Distributed Redis Circuit Breaker & Fleet State Synchronization (TypeScript SDK).
 * Validates atomic quota ledgers, sub-millisecond Pub/Sub trip broadcasting,
 * dual-coordinator synchronization, and quota ceilings (ADR 0033).
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  CircuitPolicy,
  SwarmCircuitBreakException,
  MockRedisClient,
  RedisFleetBackend,
  RedisCircuitCoordinator,
} from "../src/index.ts";

test("MockRedisClient operations", async (t) => {
  await t.test("ping returns true", async () => {
    const client = new MockRedisClient();
    const ok = await client.ping();
    assert.strictEqual(ok, true);
  });

  await t.test("hashes and atomic increments", async () => {
    const client = new MockRedisClient();
    await client.hset("metrics:01", "tokens", "100");
    const val = await client.hget("metrics:01", "tokens");
    assert.strictEqual(val, "100");

    const inc1 = await client.hincrby("metrics:01", "tokens", 50);
    assert.strictEqual(inc1, 150);

    const incFloat = await client.hincrbyfloat("metrics:01", "cost", 0.025);
    assert.strictEqual(Math.round(incFloat * 1000) / 1000, 0.025);

    const incFloat2 = await client.hincrbyfloat("metrics:01", "cost", 0.05);
    assert.strictEqual(Math.round(incFloat2 * 1000) / 1000, 0.075);
  });

  await t.test("sets and members", async () => {
    const client = new MockRedisClient();
    await client.sadd("workers", "w1", "w2");
    const members = await client.smembers("workers");
    assert.strictEqual(members.size, 2);
    assert.ok(members.has("w1"));
    assert.ok(members.has("w2"));

    await client.srem("workers", "w1");
    const after = await client.smembers("workers");
    assert.strictEqual(after.size, 1);
    assert.ok(!after.has("w1"));
    assert.ok(after.has("w2"));
  });

  await t.test("pub/sub channels", async () => {
    const client = new MockRedisClient();
    const received = [];

    await client.subscribe("swarmwatch:trips", (channel, message) => {
      received.push({ channel, message });
    });

    const count = await client.publish("swarmwatch:trips", "trip-event-1");
    assert.strictEqual(count, 1);
    assert.strictEqual(received.length, 1);
    assert.strictEqual(received[0].channel, "swarmwatch:trips");
    assert.strictEqual(received[0].message, "trip-event-1");
  });
});

test("RedisFleetBackend Quota Trips", async (t) => {
  await t.test("records turns and accumulates tokens", async () => {
    const client = new MockRedisClient();
    const backend = new RedisFleetBackend(client);
    const policy = new CircuitPolicy({ max_turns: 10, token_budget: 1000 });

    const m1 = await backend.recordTurnAtomic("w1", "swarm-ts-1", 150, 0.015, 1, policy);
    assert.strictEqual(m1.total_tokens, 150);
    assert.strictEqual(m1.total_turns, 1);
    assert.strictEqual(m1.is_tripped, false);

    const m2 = await backend.recordTurnAtomic("w2", "swarm-ts-1", 200, 0.02, 1, policy);
    assert.strictEqual(m2.total_tokens, 350);
    assert.strictEqual(m2.total_turns, 2);
    assert.strictEqual(m2.active_workers.size, 2);
  });

  await t.test("trips when max turns breached", async () => {
    const client = new MockRedisClient();
    const backend = new RedisFleetBackend(client);
    const policy = new CircuitPolicy({ max_turns: 3 });

    await backend.recordTurnAtomic("w1", "swarm-turns", 50, 0.005, 2, policy);
    assert.strictEqual(await backend.isTripped("swarm-turns"), false);

    // Turn 3 breaches quota
    await assert.rejects(
      async () => {
        await backend.recordTurnAtomic("w2", "swarm-turns", 50, 0.005, 1, policy);
      },
      (err) => err instanceof SwarmCircuitBreakException && err.message.includes("max turns quota reached")
    );

    assert.strictEqual(await backend.isTripped("swarm-turns"), true);
    const metrics = await backend.getMetrics("swarm-turns");
    assert.ok(metrics !== null);
    assert.strictEqual(metrics.is_tripped, true);
    assert.strictEqual(metrics.tripped_by, "w2");
  });

  await t.test("trips when token budget breached", async () => {
    const client = new MockRedisClient();
    const backend = new RedisFleetBackend(client);
    const policy = new CircuitPolicy({ token_budget: 400 });

    await backend.recordTurnAtomic("w1", "swarm-tokens", 300, 0.03, 1, policy);
    assert.strictEqual(await backend.isTripped("swarm-tokens"), false);

    await assert.rejects(
      async () => {
        await backend.recordTurnAtomic("w2", "swarm-tokens", 150, 0.015, 1, policy);
      },
      (err) => err instanceof SwarmCircuitBreakException && err.message.includes("token budget exceeded")
    );

    assert.strictEqual(await backend.isTripped("swarm-tokens"), true);
  });

  await t.test("trips when cost budget breached", async () => {
    const client = new MockRedisClient();
    const backend = new RedisFleetBackend(client);
    const policy = new CircuitPolicy({ cost_budget: 0.5 });

    await backend.recordTurnAtomic("w1", "swarm-cost", 1000, 0.35, 1, policy);
    assert.strictEqual(await backend.isTripped("swarm-cost"), false);

    await assert.rejects(
      async () => {
        await backend.recordTurnAtomic("w2", "swarm-cost", 1000, 0.20, 1, policy);
      },
      (err) => err instanceof SwarmCircuitBreakException && err.message.includes("cost budget exceeded")
    );

    assert.strictEqual(await backend.isTripped("swarm-cost"), true);
  });

  await t.test("resets swarm state cleanly", async () => {
    const client = new MockRedisClient();
    const backend = new RedisFleetBackend(client);
    const policy = new CircuitPolicy({ max_turns: 1 });

    await assert.rejects(async () => {
      await backend.recordTurnAtomic("w1", "swarm-reset", 10, 0.001, 1, policy);
    });

    assert.strictEqual(await backend.isTripped("swarm-reset"), true);
    await backend.resetSwarm("swarm-reset");
    assert.strictEqual(await backend.isTripped("swarm-reset"), false);
    assert.strictEqual(await backend.getMetrics("swarm-reset"), null);
  });
});

test("RedisCircuitCoordinator Synchronization & Fast-Path Cache", async (t) => {
  await t.test("fast-path in-memory trip cache blocks without hit to backend", async () => {
    const client = new MockRedisClient();
    const coordinator = new RedisCircuitCoordinator(
      new CircuitPolicy({ max_turns: 2 }),
      client
    );

    await coordinator.recordTurn("w1", "swarm-cache", 100, 0.01, 1);
    assert.strictEqual(await coordinator.isTripped("swarm-cache"), false);

    await assert.rejects(async () => {
      await coordinator.recordTurn("w2", "swarm-cache", 100, 0.01, 1);
    });

    assert.strictEqual(await coordinator.isTripped("swarm-cache"), true);

    // Next call halts immediately
    await assert.rejects(
      async () => {
        await coordinator.recordTurn("w1", "swarm-cache", 10, 0.001, 1);
      },
      (err) => err instanceof SwarmCircuitBreakException && err.message.includes("TRIPPED for swarm")
    );
  });

  await t.test("cross-coordinator real-time Pub/Sub synchronization", async () => {
    const client = new MockRedisClient();
    const coordA = new RedisCircuitCoordinator(
      new CircuitPolicy({ token_budget: 250 }),
      client
    );
    const coordB = new RedisCircuitCoordinator(
      new CircuitPolicy({ token_budget: 250 }),
      client
    );

    const eventsB = [];
    coordB.subscribe((ev) => eventsB.push(ev));

    await coordA.recordTurn("worker-a", "swarm-multi", 150, 0.015, 1);
    assert.strictEqual(await coordA.isTripped("swarm-multi"), false);
    assert.strictEqual(await coordB.isTripped("swarm-multi"), false);

    // Coord A breaches token budget
    await assert.rejects(async () => {
      await coordA.recordTurn("worker-a", "swarm-multi", 150, 0.015, 1);
    });

    assert.strictEqual(await coordA.isTripped("swarm-multi"), true);
    // Coord B must be tripped immediately via Pub/Sub broadcast
    assert.strictEqual(await coordB.isTripped("swarm-multi"), true);

    const tripEvent = eventsB.find((e) => e.event_type === "trip_broadcast");
    assert.ok(tripEvent !== undefined);
    assert.strictEqual(tripEvent.swarm_id, "swarm-multi");
    assert.strictEqual(tripEvent.worker_id, "worker-a");

    // Coord B halts worker-b immediately
    await assert.rejects(
      async () => {
        await coordB.recordTurn("worker-b", "swarm-multi", 10, 0.001, 1);
      },
      (err) => err instanceof SwarmCircuitBreakException && err.message.includes("TRIPPED for swarm")
    );
  });

  await t.test("manual trip and reset across multiple coordinators", async () => {
    const client = new MockRedisClient();
    const c1 = new RedisCircuitCoordinator(new CircuitPolicy(), client);
    const c2 = new RedisCircuitCoordinator(new CircuitPolicy(), client);

    await c1.trip("swarm-manual-ts", "Manual Operator Pause", "operator-charlie");
    assert.strictEqual(await c1.isTripped("swarm-manual-ts"), true);
    assert.strictEqual(await c2.isTripped("swarm-manual-ts"), true);

    await c1.resetSwarm("swarm-manual-ts");
    assert.strictEqual(await c1.isTripped("swarm-manual-ts"), false);
    await c2.resetSwarm("swarm-manual-ts");
    assert.strictEqual(await c2.isTripped("swarm-manual-ts"), false);
  });
});
