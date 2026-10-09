import test from "node:test";
import assert from "node:assert/strict";
import * as crypto from "node:crypto";

import {
  createCommitteeRoster,
  attestQuorum,
  verifyQuorumCertificate,
  rotateCommittee,
  computeByzantineQuorum,
  InsufficientQuorumError,
} from "../src/qc.ts";

function generateTestMember(memberId) {
  const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
  const pubDer = publicKey.export({ type: "spki", format: "der" });
  const member = {
    member_id: memberId,
    public_key_hex: pubDer.toString("hex"),
  };
  return { member, privateKey };
}

test("Seam 81: Byzantine threshold math (TypeScript)", () => {
  assert.deepEqual(computeByzantineQuorum(4), { f: 1, q: 3 });
  assert.deepEqual(computeByzantineQuorum(7), { f: 2, q: 5 });
  assert.deepEqual(computeByzantineQuorum(10), { f: 3, q: 7 });
  assert.deepEqual(computeByzantineQuorum(3), { f: 0, q: 2 });
});

test("Seam 81: Committee Key Ceremony and Quorum Attestation (TypeScript)", () => {
  const members = [];
  const keys = new Map();

  for (let i = 0; i < 4; i++) {
    const { member, privateKey } = generateTestMember(`node-${i}`);
    members.push(member);
    keys.set(`node-${i}`, privateKey);
  }

  const roster = createCommitteeRoster(1, members);
  assert.equal(roster.epoch, 1);
  assert.equal(roster.quorum_threshold, 3);
  assert.equal(roster.fault_tolerance_f, 1);
  assert.equal(roster.committee_root.length, 64);

  // Sign with 3 members (meets Q=3)
  const signerKeys = new Map();
  for (let i = 0; i < 3; i++) {
    signerKeys.set(`node-${i}`, keys.get(`node-${i}`));
  }

  const startNs = process.hrtime.bigint();
  const cert = attestQuorum(roster, "barrier:cutover", "abcd".repeat(16), signerKeys);
  const durationUs = Number(process.hrtime.bigint() - startNs) / 1000;
  assert.ok(durationUs < 10000, `Execution latency ${durationUs}us must be fast`);

  assert.equal(cert.epoch, 1);
  assert.equal(cert.quorum_required, 3);
  assert.equal(Object.keys(cert.signatures).length, 3);
  assert.equal(cert.committee_root, roster.committee_root);

  // Verification against roster
  const verdict = verifyQuorumCertificate(cert, roster);
  assert.equal(verdict.is_valid, true);
  assert.equal(verdict.threshold_met, true);
  assert.equal(verdict.verified_signers.length, 3);
});

test("Seam 81: Insufficient quorum rejected (TypeScript)", () => {
  const members = [];
  const keys = new Map();

  for (let i = 0; i < 4; i++) {
    const { member, privateKey } = generateTestMember(`node-${i}`);
    members.push(member);
    keys.set(`node-${i}`, privateKey);
  }

  const roster = createCommitteeRoster(1, members);
  const signerKeys = new Map();
  for (let i = 0; i < 2; i++) {
    signerKeys.set(`node-${i}`, keys.get(`node-${i}`));
  }

  assert.throws(() => {
    attestQuorum(roster, "barrier:cutover", "abcd".repeat(16), signerKeys);
  }, InsufficientQuorumError);
});

test("Seam 81: Tampered signature rejected (TypeScript)", () => {
  const members = [];
  const keys = new Map();

  for (let i = 0; i < 4; i++) {
    const { member, privateKey } = generateTestMember(`node-${i}`);
    members.push(member);
    keys.set(`node-${i}`, privateKey);
  }

  const roster = createCommitteeRoster(1, members);
  const signerKeys = new Map();
  for (let i = 0; i < 3; i++) {
    signerKeys.set(`node-${i}`, keys.get(`node-${i}`));
  }

  const cert = attestQuorum(roster, "barrier:cutover", "abcd".repeat(16), signerKeys);
  const tamperedCert = {
    ...cert,
    signatures: {
      ...cert.signatures,
      "node-0": "00".repeat(64),
    },
  };

  const verdict = verifyQuorumCertificate(tamperedCert, roster);
  assert.equal(verdict.is_valid, false);
  assert.equal(verdict.threshold_met, false);
  assert.equal(verdict.verified_signers.length, 2);
});

test("Seam 81: Key rotation ceremony to epoch e+1 (TypeScript)", () => {
  const membersE1 = [];
  const keysE1 = new Map();

  for (let i = 0; i < 4; i++) {
    const { member, privateKey } = generateTestMember(`node-e1-${i}`);
    membersE1.push(member);
    keysE1.set(`node-e1-${i}`, privateKey);
  }

  const rosterE1 = createCommitteeRoster(1, membersE1);

  const membersE2 = [];
  for (let i = 0; i < 4; i++) {
    const { member } = generateTestMember(`node-e2-${i}`);
    membersE2.push(member);
  }

  const signersE1 = new Map();
  for (let i = 0; i < 3; i++) {
    signersE1.set(`node-e1-${i}`, keysE1.get(`node-e1-${i}`));
  }

  const { nextRoster, rotationQc } = rotateCommittee(rosterE1, membersE2, signersE1);

  assert.equal(nextRoster.epoch, 2);
  assert.equal(rotationQc.domain, "committee:rotation");
  assert.equal(rotationQc.epoch, 1);
  assert.equal(rotationQc.digest, nextRoster.committee_root);

  const verdict = verifyQuorumCertificate(rotationQc, rosterE1);
  assert.equal(verdict.is_valid, true);
  assert.equal(verdict.threshold_met, true);
});
