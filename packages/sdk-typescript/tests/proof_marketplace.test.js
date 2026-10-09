import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import { SafetyCircuitBus } from "../src/circuit_bus.ts";
import {
  ProofMarketplaceCoordinator,
  InsufficientStakeError,
  VerifierSlashedError,
  InvalidReceiptSignatureError,
  InsufficientVerificationQuorumError,
  computeBidDigest,
  computeReceiptDigest,
} from "../src/proof_marketplace.ts";

function generateTestVerifier(verifierId, stake = 500.0, minStake = 100.0) {
  const keypair = crypto.generateKeyPairSync("ed25519");
  const pubHex = keypair.publicKey.export({ type: "spki", format: "der" }).subarray(12).toString("hex");
  const node = {
    verifier_id: verifierId,
    public_key: pubHex,
    bonded_stake: stake,
    min_stake: minStake,
    reputation_score: 1.0,
    is_slashed: false,
    is_active: true,
  };
  return { node, keypair };
}

test("Seam 93: Verifier staking and min_stake CPM1 (TypeScript)", () => {
  const coordinator = new ProofMarketplaceCoordinator("market-test");
  const { node } = generateTestVerifier("v-01", 500.0, 100.0);
  coordinator.registerVerifier(node);

  assert.ok(coordinator.getVerifier("v-01"));
  assert.equal(node.bonded_stake, 500.0);

  // Bond additional stake
  coordinator.bondStake("v-01", 200.0);
  assert.equal(node.bonded_stake, 700.0);

  // Unbond stake safely
  coordinator.unbondStake("v-01", 500.0);
  assert.equal(node.bonded_stake, 200.0);

  // Unbonding below min_stake fails
  assert.throws(
    () => coordinator.unbondStake("v-01", 150.0),
    InsufficientStakeError
  );

  // Registering with insufficient stake fails
  const poor = generateTestVerifier("v-poor", 50.0, 100.0);
  assert.throws(
    () => coordinator.registerVerifier(poor.node),
    InsufficientStakeError
  );
});

test("Seam 93: Bounty creation and double-auction matching CPM2 (TypeScript)", () => {
  const coordinator = new ProofMarketplaceCoordinator("market-test");

  const v1 = generateTestVerifier("v-1", 300.0);
  const v2 = generateTestVerifier("v-2", 400.0);
  const v3 = generateTestVerifier("v-3", 500.0);
  coordinator.registerVerifier(v1.node);
  coordinator.registerVerifier(v2.node);
  coordinator.registerVerifier(v3.node);

  const bounty = {
    bounty_id: "bounty-100",
    proof_type: "ZK_SNARK_ROLLUP",
    proof_digest: "a".repeat(64),
    max_bounty: 50.0,
    required_quorum: 2,
    deadline_epoch: 1,
    status: "OPEN",
  };
  coordinator.createBounty(bounty);

  // Submit valid bids
  const digest1 = computeBidDigest("bounty-100", "v-1", 25.0);
  const sig1 = crypto.sign(null, Buffer.from(digest1, "hex"), v1.keypair.privateKey).toString("hex");
  coordinator.submitBid({
    bid_id: "bid-1",
    bounty_id: "bounty-100",
    verifier_id: "v-1",
    fee_bid: 25.0,
    staked_collateral: 300.0,
    signature: sig1,
  });

  const digest2 = computeBidDigest("bounty-100", "v-2", 20.0);
  const sig2 = crypto.sign(null, Buffer.from(digest2, "hex"), v2.keypair.privateKey).toString("hex");
  coordinator.submitBid({
    bid_id: "bid-2",
    bounty_id: "bounty-100",
    verifier_id: "v-2",
    fee_bid: 20.0,
    staked_collateral: 400.0,
    signature: sig2,
  });

  // Forged signature rejected
  assert.throws(
    () =>
      coordinator.submitBid({
        bid_id: "bid-3",
        bounty_id: "bounty-100",
        verifier_id: "v-3",
        fee_bid: 15.0,
        staked_collateral: 500.0,
        signature: "ff".repeat(64),
      }),
    InvalidReceiptSignatureError
  );

  // Match bounty: selects top 2 lowest fee bids (v-2 with 20.0, v-1 with 25.0)
  const matched = coordinator.matchBounty("bounty-100");
  assert.equal(matched.length, 2);
  assert.deepEqual(matched, ["v-2", "v-1"]);
});

test("Seam 93: K-of-N quorum settlement and payout CPM3 (TypeScript)", () => {
  const coordinator = new ProofMarketplaceCoordinator("market-test");

  const verifiers = [];
  for (let i = 0; i < 3; i++) {
    const v = generateTestVerifier(`node-${i}`, 300.0);
    coordinator.registerVerifier(v.node);
    verifiers.push(v);
  }

  const bounty = {
    bounty_id: "bounty-200",
    proof_type: "EPISTEMIC_DISPUTE_PROOF",
    proof_digest: "b".repeat(64),
    max_bounty: 60.0,
    required_quorum: 3,
    deadline_epoch: 1,
    status: "OPEN",
  };
  coordinator.createBounty(bounty);

  // Submit bids
  for (let i = 0; i < 3; i++) {
    const bDig = computeBidDigest("bounty-200", `node-${i}`, 15.0);
    const bSig = crypto.sign(null, Buffer.from(bDig, "hex"), verifiers[i].keypair.privateKey).toString("hex");
    coordinator.submitBid({
      bid_id: `bid-${i}`,
      bounty_id: "bounty-200",
      verifier_id: `node-${i}`,
      fee_bid: 15.0,
      staked_collateral: 300.0,
      signature: bSig,
    });
  }

  coordinator.matchBounty("bounty-200");

  // Submit 3 matching True receipts
  for (let i = 0; i < 3; i++) {
    const rDig = computeReceiptDigest("bounty-200", `node-${i}`, true, "b".repeat(64));
    const rSig = crypto.sign(null, Buffer.from(rDig, "hex"), verifiers[i].keypair.privateKey).toString("hex");
    coordinator.submitReceipt({
      receipt_id: `rcpt-${i}`,
      bounty_id: "bounty-200",
      verifier_id: `node-${i}`,
      verdict: true,
      proof_hash: "b".repeat(64),
      signature: rSig,
    });
  }

  const certificate = coordinator.settleBounty("bounty-200");
  assert.equal(certificate.finalized_verdict, true);
  assert.equal(certificate.bounty_id, "bounty-200");
  assert.equal(certificate.participating_verifiers.length, 3);
  assert.equal(certificate.slashed_verifiers.length, 0);
  assert.equal(certificate.cleared_fee, 15.0);
  assert.equal(certificate.payout_distributions["node-0"], 5.0);
});

test("Seam 93: Equivocation and fraud slashing CPM1 (TypeScript)", () => {
  const coordinator = new ProofMarketplaceCoordinator("market-test");

  const v1 = generateTestVerifier("v-good-1", 400.0);
  const v2 = generateTestVerifier("v-good-2", 400.0);
  const v3 = generateTestVerifier("v-malicious", 500.0);
  coordinator.registerVerifier(v1.node);
  coordinator.registerVerifier(v2.node);
  coordinator.registerVerifier(v3.node);

  const bounty = {
    bounty_id: "bounty-300",
    proof_type: "CAUSAL_STATE_PROOF",
    proof_digest: "c".repeat(64),
    max_bounty: 100.0,
    required_quorum: 2,
    deadline_epoch: 1,
    status: "OPEN",
  };
  coordinator.createBounty(bounty);

  // Submit valid receipts: v-good-1 and v-good-2 vote True, v-malicious votes False
  const rDig1 = computeReceiptDigest("bounty-300", "v-good-1", true, "c".repeat(64));
  coordinator.submitReceipt({
    receipt_id: "r-1",
    bounty_id: "bounty-300",
    verifier_id: "v-good-1",
    verdict: true,
    proof_hash: "c".repeat(64),
    signature: crypto.sign(null, Buffer.from(rDig1, "hex"), v1.keypair.privateKey).toString("hex"),
  });

  const rDig2 = computeReceiptDigest("bounty-300", "v-good-2", true, "c".repeat(64));
  coordinator.submitReceipt({
    receipt_id: "r-2",
    bounty_id: "bounty-300",
    verifier_id: "v-good-2",
    verdict: true,
    proof_hash: "c".repeat(64),
    signature: crypto.sign(null, Buffer.from(rDig2, "hex"), v2.keypair.privateKey).toString("hex"),
  });

  const rDig3 = computeReceiptDigest("bounty-300", "v-malicious", false, "c".repeat(64));
  coordinator.submitReceipt({
    receipt_id: "r-3",
    bounty_id: "bounty-300",
    verifier_id: "v-malicious",
    verdict: false,
    proof_hash: "c".repeat(64),
    signature: crypto.sign(null, Buffer.from(rDig3, "hex"), v3.keypair.privateKey).toString("hex"),
  });

  const cert = coordinator.settleBounty("bounty-300");
  assert.equal(cert.finalized_verdict, true);
  assert.ok(cert.slashed_verifiers.includes("v-malicious"));

  // Check that v-malicious was permanently slashed
  assert.equal(v3.node.is_slashed, true);
  assert.equal(v3.node.is_active, false);
  assert.equal(v3.node.bonded_stake, 0.0);

  // Slashed verifier cannot bond stake
  assert.throws(
    () => coordinator.bondStake("v-malicious", 100.0),
    VerifierSlashedError
  );
});

test("Seam 93: Solvency breach trips SafetyCircuitBus CPM4 (TypeScript)", () => {
  const circuitBus = new SafetyCircuitBus();
  const coordinator = new ProofMarketplaceCoordinator("market-solvent", 1000.0, circuitBus);

  const n1 = generateTestVerifier("v-node-1", 600.0);
  const n2 = generateTestVerifier("v-node-2", 600.0);
  coordinator.registerVerifier(n1.node);
  coordinator.registerVerifier(n2.node);

  assert.equal(circuitBus.isTripped(), false);

  // Slashing node 1 leaves only 600.0 stake (< 1000.0 required solvency)
  coordinator.slashVerifier("v-node-1", 1.0, "Equivocation");

  assert.equal(circuitBus.isTripped(), true);
  const status = circuitBus.getStatus();
  assert.equal(status.last_trip_event.reason, "PROOF_MARKETPLACE_SOLVENCY_BREACH");
});

test("Seam 93: Sub-250 microsecond execution latency SLO (TypeScript)", () => {
  const coordinator = new ProofMarketplaceCoordinator("market-bench");
  const node = generateTestVerifier("v-bench", 1000.0);
  coordinator.registerVerifier(node.node);

  const start = performance.now();
  const iterations = 200;
  for (let i = 0; i < iterations; i++) {
    coordinator.createBounty({
      bounty_id: `b-bench-${i}`,
      proof_type: "ZK_PROOF",
      proof_digest: "0".repeat(64),
      max_bounty: 10.0,
      required_quorum: 1,
      deadline_epoch: 1,
      status: "OPEN",
    });
  }
  const elapsedMs = performance.now() - start;
  const avgLatencyUs = (elapsedMs / iterations) * 1000;
  assert.ok(avgLatencyUs < 250.0, `Average latency ${avgLatencyUs.toFixed(2)}µs exceeds 250µs SLO`);
});
