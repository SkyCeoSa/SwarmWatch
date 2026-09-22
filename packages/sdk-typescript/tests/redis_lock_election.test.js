/**
 * SwarmWatch Distributed Redis Lock & Leader Election Test Suite (TypeScript Edition / ADR 0035).
 */

import { test } from "node:test";
import assert from "node:assert";
import {
  MockRedisClient,
  RedisDistributedLock,
  RedisLeaderElector,
} from "../src/index.ts";

test("RedisDistributedLock: Mutual Exclusion & CAS Semantics", async (t) => {
  const redis = new MockRedisClient();
  const lockA = new RedisDistributedLock(redis, "test:lock:resource", 1000);
  const lockB = new RedisDistributedLock(redis, "test:lock:resource", 1000);

  await t.test("acquires lock successfully and sets unique token", async () => {
    const acquired = await lockA.acquire({ blocking: false });
    assert.strictEqual(acquired, true);
    assert.ok(lockA.token);
    assert.strictEqual(lockA.isHeld, true);
    assert.strictEqual(await lockA.isLocked(), true);
  });

  await t.test("prevents second client from acquiring held lock", async () => {
    const acquired = await lockB.acquire({ blocking: false });
    assert.strictEqual(acquired, false);
    assert.strictEqual(lockB.isHeld, false);
  });

  await t.test("extends held lock TTL", async () => {
    const extended = await lockA.extend(2000);
    assert.strictEqual(extended, true);
  });

  await t.test("releases lock cleanly with CAS token verification", async () => {
    const released = await lockA.release();
    assert.strictEqual(released, true);
    assert.strictEqual(lockA.isHeld, false);
    assert.strictEqual(await lockA.isLocked(), false);

    // Now lockB can acquire
    const acquiredB = await lockB.acquire({ blocking: false });
    assert.strictEqual(acquiredB, true);
    await lockB.release();
  });
});

test("RedisLeaderElector: Lease-Based Leadership & Automatic Standby Failover", async (t) => {
  const redis = new MockRedisClient();
  const electorA = new RedisLeaderElector({
    client: redis,
    nodeId: "srv-node-alpha",
    electionKey: "test:leader:key",
    rosterKey: "test:roster:key",
    leaseTtlMs: 800,
    heartbeatIntervalMs: 150,
  });

  const electorB = new RedisLeaderElector({
    client: redis,
    nodeId: "srv-node-beta",
    electionKey: "test:leader:key",
    rosterKey: "test:roster:key",
    leaseTtlMs: 800,
    heartbeatIntervalMs: 150,
  });

  let electedA = false;
  let steppedDownA = false;
  let electedB = false;

  electorA.onElected(() => { electedA = true; });
  electorA.onSteppedDown(() => { steppedDownA = true; });
  electorB.onElected(() => { electedB = true; });

  await t.test("electorA starts and becomes LEADER", async () => {
    await electorA.start();
    // Allow tick to execute
    await new Promise((r) => setTimeout(r, 100));

    assert.strictEqual(electorA.isLeader, true);
    assert.strictEqual(electorA.role, "LEADER");
    assert.strictEqual(electedA, true);

    const statusA = await electorA.getLeaderStatus();
    assert.strictEqual(statusA.leader_node_id, "srv-node-alpha");
    assert.strictEqual(statusA.is_leader, true);
    assert.strictEqual(statusA.local_role, "LEADER");
  });

  await t.test("electorB starts and assumes STANDBY role", async () => {
    await electorB.start();
    await new Promise((r) => setTimeout(r, 100));

    assert.strictEqual(electorB.isLeader, false);
    assert.strictEqual(electorB.role, "STANDBY");
    assert.strictEqual(electedB, false);

    const statusB = await electorB.getLeaderStatus();
    assert.strictEqual(statusB.leader_node_id, "srv-node-alpha");
    assert.strictEqual(statusB.is_leader, false);
    assert.strictEqual(statusB.local_role, "STANDBY");
    assert.strictEqual(statusB.nodes_count, 2);
  });

  await t.test("electorA voluntarily steps down and electorB promotes to LEADER", async () => {
    const stepped = await electorA.stepDown();
    assert.strictEqual(stepped, true);
    assert.strictEqual(electorA.isLeader, false);
    assert.strictEqual(steppedDownA, true);

    // Wait for electorB's tick to discover vacant lease and acquire
    await new Promise((r) => setTimeout(r, 350));

    assert.strictEqual(electorB.isLeader, true);
    assert.strictEqual(electorB.role, "LEADER");
    assert.strictEqual(electedB, true);

    const statusFinal = await electorB.getLeaderStatus();
    assert.strictEqual(statusFinal.leader_node_id, "srv-node-beta");
    assert.strictEqual(statusFinal.is_leader, true);
  });

  // Teardown
  await electorA.stop();
  await electorB.stop();
});
