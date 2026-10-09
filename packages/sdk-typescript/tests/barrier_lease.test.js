import test from "node:test";
import assert from "node:assert/strict";
import * as crypto from "node:crypto";

import { createCommitteeRoster } from "../src/qc.ts";
import {
  LeasedBarrierCoordinator,
  BarrierDecision,
  InvalidTargetEpochError,
} from "../src/barrier_lease.ts";

function generateClusterValidator(clusterId) {
  const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
  const pubDer = publicKey.export({ type: "spki", format: "der" });
  const member = {
    member_id: clusterId,
    public_key_hex: pubDer.toString("hex"),
    role: "VALIDATOR",
  };
  return { member, privateKey };
}

test("Seam 85: Barrier lease successful cutover (TypeScript)", () => {
  const barrier = new LeasedBarrierCoordinator(1);
  assert.equal(barrier.currentEpoch, 1);

  // Operation at epoch 1 is admitted, epoch 2 is rejected
  assert.equal(barrier.admitOperation(1), true);
  assert.equal(barrier.admitOperation(2), false);

  const { member: c0, privateKey: k0 } = generateClusterValidator("cluster-alpha");
  const { member: c1, privateKey: k1 } = generateClusterValidator("cluster-beta");
  const { member: c2, privateKey: k2 } = generateClusterValidator("cluster-gamma");

  const roster = createCommitteeRoster(1, [c0, c1, c2]);

  const signerKeys = new Map();
  signerKeys.set("cluster-alpha", k0);
  signerKeys.set("cluster-beta", k1);

  const startNs = process.hrtime.bigint();
  const result = barrier.cutover({
    target_epoch: 2,
    roster,
    signer_keys: signerKeys,
    lease_ms: 1000,
    participants: ["cluster-alpha", "cluster-beta"],
    reported_quiescence: {
      "cluster-alpha": true,
      "cluster-beta": true,
    },
  });
  const durationUs = Number(process.hrtime.bigint() - startNs) / 1000;
  assert.ok(durationUs < 10000, `Execution latency ${durationUs}us must be fast`);

  assert.equal(result.decision, BarrierDecision.COMMITTED);
  assert.equal(result.epoch, 2);
  assert.equal(barrier.currentEpoch, 2);
  assert.notEqual(result.certificate, null);
  assert.equal(result.abort_proof, null);

  // Now epoch 2 is admitted!
  assert.equal(barrier.admitOperation(2), true);
});

test("Seam 85: Barrier lease atomic abort on silent cluster (TypeScript)", () => {
  const barrier = new LeasedBarrierCoordinator(5);

  const { member: c0, privateKey: k0 } = generateClusterValidator("cluster-alpha");
  const { member: c1, privateKey: k1 } = generateClusterValidator("cluster-beta");
  const roster = createCommitteeRoster(5, [c0, c1]);

  const signerKeys = new Map();
  signerKeys.set("cluster-alpha", k0);

  const result = barrier.cutover({
    target_epoch: 6,
    roster,
    signer_keys: signerKeys,
    lease_ms: 500,
    participants: ["cluster-alpha", "cluster-beta"],
    reported_quiescence: {
      "cluster-alpha": true,
      "cluster-beta": false, // Silent!
    },
  });

  // Invariant L2: Atomic abort, epoch conserved at 5
  assert.equal(result.decision, BarrierDecision.ABORTED);
  assert.equal(result.epoch, 5);
  assert.equal(barrier.currentEpoch, 5);
  assert.notEqual(result.abort_proof, null);
  assert.ok(result.abort_proof.failed_participants.includes("cluster-beta"));
  assert.equal(barrier.state, "OPEN");
});

test("Seam 85: Barrier lease timeout abort (TypeScript)", () => {
  const barrier = new LeasedBarrierCoordinator(10);

  const { member: c0, privateKey: k0 } = generateClusterValidator("cluster-alpha");
  const roster = createCommitteeRoster(10, [c0]);
  const signerKeys = new Map();
  signerKeys.set("cluster-alpha", k0);

  const result = barrier.cutover({
    target_epoch: 11,
    roster,
    signer_keys: signerKeys,
    lease_ms: 100,
    force_timeout: true,
  });

  assert.equal(result.decision, BarrierDecision.ABORTED);
  assert.equal(result.epoch, 10);
  assert.equal(barrier.currentEpoch, 10);
  assert.ok(result.abort_proof.reason.includes("expired"));
});

test("Seam 85: Invalid target epoch rejected (TypeScript)", () => {
  const barrier = new LeasedBarrierCoordinator(3);
  const { member: c0, privateKey: k0 } = generateClusterValidator("cluster-alpha");
  const roster = createCommitteeRoster(3, [c0]);
  const signerKeys = new Map();
  signerKeys.set("cluster-alpha", k0);

  assert.throws(() => {
    barrier.cutover({
      target_epoch: 5, // Jumping to 5 from 3
      roster,
      signer_keys: signerKeys,
    });
  }, InvalidTargetEpochError);
});
