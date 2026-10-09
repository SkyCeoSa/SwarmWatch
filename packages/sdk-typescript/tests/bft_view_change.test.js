import test from "node:test";
import assert from "node:assert/strict";
import * as crypto from "node:crypto";

import { createCommitteeRoster } from "../src/qc.ts";
import {
  BftViewCoordinator,
  getPrimary,
  InsufficientViewVotesError,
} from "../src/bft_view_change.ts";

function generateNode(nodeId) {
  const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
  const pubDer = publicKey.export({ type: "spki", format: "der" });
  const member = {
    member_id: nodeId,
    public_key_hex: pubDer.toString("hex"),
    role: "VALIDATOR",
  };
  return { member, privateKey };
}

test("Seam 87: Deterministic primary rotation (TypeScript)", () => {
  const members = [];
  for (let i = 0; i < 4; i++) {
    const { member } = generateNode(`node-${i}`);
    members.push(member);
  }
  const roster = createCommitteeRoster(1, members);

  // Invariant V3: primary for view v is sorted_members[v % 4]
  assert.equal(getPrimary(0, roster), "node-0");
  assert.equal(getPrimary(1, roster), "node-1");
  assert.equal(getPrimary(2, roster), "node-2");
  assert.equal(getPrimary(3, roster), "node-3");
  assert.equal(getPrimary(4, roster), "node-0");
});

test("Seam 87: BFT view-change installation (TypeScript)", () => {
  const nodes = [];
  const keys = new Map();
  for (let i = 0; i < 4; i++) {
    const { member, privateKey } = generateNode(`node-${i}`);
    nodes.push(member);
    keys.set(`node-${i}`, privateKey);
  }
  const roster = createCommitteeRoster(1, nodes);

  const coordinator = new BftViewCoordinator(0);
  assert.equal(coordinator.currentView, 0);

  // 3 nodes cast votes for view 1
  const votes = [];
  for (let i = 0; i < 3; i++) {
    const nodeId = `node-${i}`;
    const vote = coordinator.createVote(
      1,
      10,
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      nodeId,
      keys.get(nodeId)
    );
    votes.push(vote);
  }

  const startNs = process.hrtime.bigint();
  const cert = coordinator.installNewView(1, votes, roster);
  const durationUs = Number(process.hrtime.bigint() - startNs) / 1000;
  assert.ok(durationUs < 10000, `Execution latency ${durationUs}us must be fast`);

  assert.equal(cert.view, 1);
  assert.equal(cert.primary_id, "node-1");
  assert.equal(coordinator.currentView, 1);
  assert.equal(cert.votes.length, 3);
});

test("Seam 87: Insufficient view votes rejected (TypeScript)", () => {
  const nodes = [];
  const keys = new Map();
  for (let i = 0; i < 4; i++) {
    const { member, privateKey } = generateNode(`node-${i}`);
    nodes.push(member);
    keys.set(`node-${i}`, privateKey);
  }
  const roster = createCommitteeRoster(1, nodes);
  const coordinator = new BftViewCoordinator(0);

  const votes = [];
  for (let i = 0; i < 2; i++) {
    const nodeId = `node-${i}`;
    const vote = coordinator.createVote(
      1,
      5,
      "abcd".repeat(16),
      nodeId,
      keys.get(nodeId)
    );
    votes.push(vote);
  }

  assert.throws(() => {
    coordinator.installNewView(1, votes, roster);
  }, InsufficientViewVotesError);
});

test("Seam 87: Third-party accountable slashing (TypeScript)", () => {
  const { member: nodeM, privateKey: keyM } = generateNode("malicious-node");
  const { member: nodeB } = generateNode("honest-node");
  const roster = createCommitteeRoster(1, [nodeM, nodeB]);

  const coordinator = new BftViewCoordinator(0);

  const view = 0;
  const seq = 42;
  const digestA = "11".repeat(32);
  const digestB = "22".repeat(32);

  const payloadA = Buffer.from(`sw:bft:msg:${view}:${seq}:${digestA}`, "utf-8");
  const payloadB = Buffer.from(`sw:bft:msg:${view}:${seq}:${digestB}`, "utf-8");

  const sigA = crypto.sign(null, payloadA, keyM).toString("hex");
  const sigB = crypto.sign(null, payloadB, keyM).toString("hex");

  const proof = {
    slashed_node_id: "malicious-node",
    view,
    sequence: seq,
    digest_a: digestA,
    sig_a_hex: sigA,
    digest_b: digestB,
    sig_b_hex: sigB,
    proven_at_ns: Date.now() * 1_000_000,
  };

  assert.equal(coordinator.isSlashed("malicious-node"), false);
  const slashed = coordinator.verifyAndApplySlashing(proof, roster);
  assert.equal(slashed, true);
  assert.equal(coordinator.isSlashed("malicious-node"), true);
});
