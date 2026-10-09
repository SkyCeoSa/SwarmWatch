/**
 * Tests for SwarmWatch Real-Time Epistemic Dispute Resolution & Dynamic Slashing Escalation Tribunal (Seam 97 / ADR 0108).
 * Verifies TRB1 (Multi-Tiered Dispute Escalation), TRB2 (Severity-Graded Slashing Matrix),
 * TRB3 (Super-Majority Arbiter Finality), TRB4 (Fail-Closed Safety Circuit Latching),
 * and sub-250 microsecond execution latency SLO in TypeScript.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';

import { SafetyCircuitBus } from '../src/circuit_bus.ts';
import { generateNodeKeypair, signBftPayload } from '../src/byzantine_oracle.ts';
import {
  SlashingEscalationTribunal,
  DisputeNotFoundError,
  InvalidDisputeTierError,
  InsufficientArbiterQuorumError,
  TribunalCircuitTrippedError,
  computeVerdictDigest,
} from '../src/slashing_tribunal.ts';

function setupCleanTribunal() {
  const bus = new SafetyCircuitBus();
  const tribunal = new SlashingEscalationTribunal('test-tribunal', 3, bus);

  const k1 = generateNodeKeypair();
  const k2 = generateNodeKeypair();
  const k3 = generateNodeKeypair();

  tribunal.registerArbiter({ arbiterId: 'arb-01', publicKey: k1.publicKeyHex, bondedStake: 1000.0, isActive: true });
  tribunal.registerArbiter({ arbiterId: 'arb-02', publicKey: k2.publicKeyHex, bondedStake: 1000.0, isActive: true });
  tribunal.registerArbiter({ arbiterId: 'arb-03', publicKey: k3.publicKeyHex, bondedStake: 1000.0, isActive: true });

  const keys = { 'arb-01': k1.privateKey, 'arb-02': k2.privateKey, 'arb-03': k3.privateKey };
  return { tribunal, bus, keys };
}

function makeSignedVotes(tribunal, disputeId, ruling, slashedAmount, recipientId, hlcTs, arbiterIds, keys) {
  const claim = tribunal.getDispute(disputeId);
  assert(claim !== undefined);
  const digest = computeVerdictDigest(disputeId, claim.tier, ruling, slashedAmount, recipientId, hlcTs);
  const votes = {};
  for (const arbId of arbiterIds) {
    votes[arbId] = signBftPayload(keys[arbId], digest);
  }
  return votes;
}

test('TRB1: Dispute lifecycle and monotonic escalation', () => {
  const { tribunal } = setupCleanTribunal();

  // 1. File Tier 1 Dispute
  const claim = tribunal.fileDispute('agent-alice', 'agent-bob', 'MEDIUM', 'e'.repeat(64), 20.0);
  assert.equal(claim.tier, 1);
  assert.equal(claim.disputeBond, 20.0);
  assert.equal(claim.status, 'OPEN');

  // 2. Escalate to Tier 2 (Committee)
  const claim2 = tribunal.escalateDispute(claim.disputeId, 20.0);
  assert.equal(claim2.tier, 2);
  assert.equal(claim2.disputeBond, 40.0);
  assert.equal(claim2.status, 'ESCALATED');

  // 3. Escalate to Tier 3 (Tribunal Appellate)
  const claim3 = tribunal.escalateDispute(claim.disputeId, 40.0);
  assert.equal(claim3.tier, 3);
  assert.equal(claim3.disputeBond, 80.0);

  // 4. Attempt invalid escalation beyond Tier 3
  assert.throws(
    () => tribunal.escalateDispute(claim.disputeId, 80.0),
    InvalidDisputeTierError
  );

  // 5. Non-existent dispute escalation raises DisputeNotFoundError
  assert.throws(
    () => tribunal.escalateDispute('non-existent-dispute', 50.0),
    DisputeNotFoundError
  );
});

test('TRB2: Severity-graded dynamic slashing matrix', () => {
  const { tribunal, keys } = setupCleanTribunal();

  // Test LOW severity: 5% slash
  const dLow = tribunal.fileDispute('alice', 'bob', 'LOW', '', 10.0);
  const hlcTsLow = 'hlc:100:0:trb';
  const votesLow = makeSignedVotes(tribunal, dLow.disputeId, 'RESOLVED_CLAIMANT', 5.0, 'alice', hlcTsLow, ['arb-01', 'arb-02', 'arb-03'], keys);
  const recLow = tribunal.adjudicateDispute(dLow.disputeId, 'RESOLVED_CLAIMANT', 100.0, votesLow, hlcTsLow);
  assert.equal(recLow.slashedAmount, 5.0);
  assert.equal(recLow.recipientId, 'alice');

  // Test MEDIUM severity: 25% slash
  const dMed = tribunal.fileDispute('charlie', 'dave', 'MEDIUM', '', 10.0);
  const hlcTsMed = 'hlc:101:0:trb';
  const votesMed = makeSignedVotes(tribunal, dMed.disputeId, 'RESOLVED_CLAIMANT', 25.0, 'charlie', hlcTsMed, ['arb-01', 'arb-02', 'arb-03'], keys);
  const recMed = tribunal.adjudicateDispute(dMed.disputeId, 'RESOLVED_CLAIMANT', 100.0, votesMed, hlcTsMed);
  assert.equal(recMed.slashedAmount, 25.0);

  // Test CRITICAL severity: 100% slash
  const dCrit = tribunal.fileDispute('eve', 'mallory', 'CRITICAL', '', 10.0);
  const hlcTsCrit = 'hlc:102:0:trb';
  const votesCrit = makeSignedVotes(tribunal, dCrit.disputeId, 'RESOLVED_CLAIMANT', 100.0, 'eve', hlcTsCrit, ['arb-01', 'arb-02', 'arb-03'], keys);
  const recCrit = tribunal.adjudicateDispute(dCrit.disputeId, 'RESOLVED_CLAIMANT', 100.0, votesCrit, hlcTsCrit);
  assert.equal(recCrit.slashedAmount, 100.0);

  // Test Defendant wins: 0% slash
  const dDef = tribunal.fileDispute('frank', 'grace', 'CRITICAL', '', 10.0);
  const hlcTsDef = 'hlc:103:0:trb';
  const votesDef = makeSignedVotes(tribunal, dDef.disputeId, 'RESOLVED_DEFENDANT', 0.0, 'grace', hlcTsDef, ['arb-01', 'arb-02', 'arb-03'], keys);
  const recDef = tribunal.adjudicateDispute(dDef.disputeId, 'RESOLVED_DEFENDANT', 100.0, votesDef, hlcTsDef);
  assert.equal(recDef.slashedAmount, 0.0);
  assert.equal(recDef.recipientId, 'grace');
});

test('TRB3: Super-majority arbiter quorum finality', () => {
  const { tribunal, keys } = setupCleanTribunal();
  const d = tribunal.fileDispute('user1', 'user2', 'MEDIUM', '', 10.0);

  // Only 1 out of 3 votes provided (quorum requires 3)
  const hlcTs = 'hlc:100:0:trb';
  const votesInsufficient = makeSignedVotes(tribunal, d.disputeId, 'RESOLVED_CLAIMANT', 25.0, 'user1', hlcTs, ['arb-01'], keys);
  assert.throws(
    () => tribunal.adjudicateDispute(d.disputeId, 'RESOLVED_CLAIMANT', 100.0, votesInsufficient, hlcTs),
    InsufficientArbiterQuorumError
  );

  // 3 out of 3 votes succeeds
  const votesFull = makeSignedVotes(tribunal, d.disputeId, 'RESOLVED_CLAIMANT', 25.0, 'user1', hlcTs, ['arb-01', 'arb-02', 'arb-03'], keys);
  const receipt = tribunal.adjudicateDispute(d.disputeId, 'RESOLVED_CLAIMANT', 100.0, votesFull, hlcTs);
  assert.equal(receipt.verdict, 'RESOLVED_CLAIMANT');
  assert.equal(Object.keys(receipt.arbiterSignatures).length, 3);
});

test('TRB4: Fail-closed surge circuit latching', () => {
  const { tribunal, bus } = setupCleanTribunal();
  assert.equal(bus.isTripped(), false);

  // Surge threshold is 3 critical disputes
  tribunal.fileDispute('c1', 'd1', 'CRITICAL', '', 10.0);
  tribunal.fileDispute('c2', 'd2', 'CRITICAL', '', 10.0);
  assert.equal(bus.isTripped(), false);

  // Third critical dispute triggers surge trip
  tribunal.fileDispute('c3', 'd3', 'CRITICAL', '', 10.0);
  assert.equal(bus.isTripped(), true);
  assert.equal(bus.getStatus().last_trip_event?.reason, 'TRIBUNAL_SYSTEMIC_DISPUTE_SURGE');

  // Subsequent dispute filing fails fail-closed
  assert.throws(
    () => tribunal.fileDispute('c4', 'd4', 'LOW', '', 10.0),
    TribunalCircuitTrippedError
  );
});

test('TRB-ADV: Adversarial forged tribunal signatures rejected', () => {
  const { tribunal, keys } = setupCleanTribunal();
  const d = tribunal.fileDispute('adv_c', 'adv_d', 'MEDIUM', '', 10.0);
  const hlcTs = 'hlc:100:0:trb';
  const digest = computeVerdictDigest(d.disputeId, d.tier, 'RESOLVED_CLAIMANT', 25.0, 'adv_c', hlcTs);

  // 1. Literal test string pattern
  const forgedLiteral = {
    'arb-01': `sig:fake:${digest.slice(0, 16)}`,
    'arb-02': `sig:fake:${digest.slice(0, 16)}`,
    'arb-03': `sig:fake:${digest.slice(0, 16)}`,
  };
  assert.throws(
    () => tribunal.adjudicateDispute(d.disputeId, 'RESOLVED_CLAIMANT', 100.0, forgedLiteral, hlcTs),
    InsufficientArbiterQuorumError
  );

  // 2. Corrupted signature bytes
  const realSig = signBftPayload(keys['arb-01'], digest);
  const corruptedSig = 'ff' + realSig.slice(2);
  const forgedCorrupted = {
    'arb-01': corruptedSig,
    'arb-02': corruptedSig,
    'arb-03': corruptedSig,
  };
  assert.throws(
    () => tribunal.adjudicateDispute(d.disputeId, 'RESOLVED_CLAIMANT', 100.0, forgedCorrupted, hlcTs),
    InsufficientArbiterQuorumError
  );
});

test('Performance SLO: Sub-250us execution latency and zero token cost', () => {
  const { tribunal } = setupCleanTribunal();

  const t0 = performance.now();
  const d = tribunal.fileDispute('alice', 'bob', 'LOW', '', 10.0);
  tribunal.escalateDispute(d.disputeId, 10.0);
  const t1 = performance.now();

  const elapsedUs = (t1 - t0) * 1000.0;
  assert.ok(elapsedUs < 250.0, `Execution latency ${elapsedUs.toFixed(2)}us exceeded 250us SLO.`);

  const status = tribunal.getStatus();
  assert.equal(status.total_disputes_count, 1);
  assert.equal(status.open_disputes_count, 1);
});
