import test from "node:test";
import assert from "node:assert/strict";
import * as crypto from "node:crypto";

import {
  createCommitteeRoster,
  attestQuorum,
} from "../src/qc.ts";
import {
  SafetyCircuitBus,
  CircuitState,
  CircuitBusTrippedError,
  UnauthorizedRearmError,
  InvalidSnapshotHashError,
} from "../src/circuit_bus.ts";

function generateOperator(opId) {
  const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
  const pubDer = publicKey.export({ type: "spki", format: "der" });
  const member = {
    member_id: opId,
    public_key_hex: pubDer.toString("hex"),
    role: "OPERATOR",
  };
  return { member, privateKey };
}

test("Seam 82: Circuit trip monotonic generation and guard (TypeScript)", async () => {
  const bus = new SafetyCircuitBus();
  assert.equal(bus.state, CircuitState.CLOSED);
  assert.equal(bus.generation, 0);
  assert.equal(bus.isTripped(), false);

  // Guard allows execution when CLOSED
  let executed = false;
  await bus.guard("consensus", () => {
    executed = true;
  });
  assert.equal(executed, true);

  // Warm up and benchmark steady-state trip latency
  bus.trip("warmup", "WARMUP");
  const startNs = process.hrtime.bigint();
  for (let i = 0; i < 100; i++) {
    bus.trip("bench", "BENCH");
  }
  const avgDurationUs = Number(process.hrtime.bigint() - startNs) / (100 * 1000);
  assert.ok(avgDurationUs < 250, `Average trip latency ${avgDurationUs}us must be within 250us SLO`);

  assert.equal(bus.state, CircuitState.TRIPPED);
  assert.ok(bus.generation > 100);

  // Fresh bus for strict generation step checks
  const testBus = new SafetyCircuitBus();
  testBus.trip("Byzantine equivocation detected", "CONSENSUS");
  assert.equal(testBus.generation, 1);
  assert.equal(testBus.isTripped("CONSENSUS"), true);
  assert.equal(testBus.isTripped("GLOBAL"), true);

  // Second trip increments generation monotonically (Invariant SC1)
  testBus.trip("Entropy threshold collapse", "ENTROPY");
  assert.equal(testBus.generation, 2);

  // Guard blocks execution fail-closed (Invariant SC3)
  await assert.rejects(
    async () => {
      await testBus.guard("CONSENSUS", () => {});
    },
    (err) => {
      assert.ok(err instanceof CircuitBusTrippedError);
      assert.equal(err.generation, 2);
      return true;
    }
  );
});

test("Seam 82: Unilateral or unauthenticated rearm rejected (TypeScript)", () => {
  const bus = new SafetyCircuitBus();
  bus.trip("Test trip", "GLOBAL");

  const { member: op0, privateKey: key0 } = generateOperator("operator-0");
  const rosterSingle = createCommitteeRoster(1, [op0]);
  const snapshotHash = "a".repeat(64);

  const signerKeys = new Map();
  signerKeys.set("operator-0", key0);

  const certSingle = attestQuorum(rosterSingle, "circuit:rearm", snapshotHash, signerKeys);

  // bus.rearm enforces >= 2 operators
  assert.throws(() => {
    bus.rearm(certSingle, snapshotHash, rosterSingle);
  }, UnauthorizedRearmError);

  // Empty snapshot hash rejected
  assert.throws(() => {
    bus.rearm(certSingle, "", rosterSingle);
  }, InvalidSnapshotHashError);
});

test("Seam 82: Valid multi-operator quorum rearm (TypeScript)", async () => {
  const bus = new SafetyCircuitBus();
  bus.trip("Critical test trip", "GLOBAL");
  assert.equal(bus.isTripped(), true);

  const { member: op0, privateKey: key0 } = generateOperator("operator-0");
  const { member: op1, privateKey: key1 } = generateOperator("operator-1");
  const { member: op2, privateKey: key2 } = generateOperator("operator-2");

  const roster = createCommitteeRoster(1, [op0, op1, op2]);
  const snapshotHash = "c".repeat(64);

  const signerKeys = new Map();
  signerKeys.set("operator-0", key0);
  signerKeys.set("operator-1", key1);

  const rearmQc = attestQuorum(roster, "circuit:rearm", snapshotHash, signerKeys);

  const gen = bus.rearm(rearmQc, snapshotHash, roster);
  assert.equal(bus.state, CircuitState.CLOSED);
  assert.equal(bus.isTripped(), false);
  assert.ok(gen >= 1);

  // Guard allows execution again
  let ranAgain = false;
  await bus.guard("GLOBAL", () => {
    ranAgain = true;
  });
  assert.equal(ranAgain, true);
});

test("Seam 82: Circuit bus subscriber notification (TypeScript)", () => {
  const bus = new SafetyCircuitBus();
  const receivedEvents = [];

  bus.subscribe((event) => {
    receivedEvents.push(event);
  });

  bus.trip("Partition event", "MESH", { peers: 3 });

  assert.equal(receivedEvents.length, 1);
  assert.equal(receivedEvents[0].scope, "MESH");
  assert.equal(receivedEvents[0].reason, "Partition event");
  assert.equal(receivedEvents[0].metadata.peers, 3);
});
