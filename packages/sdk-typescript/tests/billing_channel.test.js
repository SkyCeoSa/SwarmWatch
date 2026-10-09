/**
 * Tests for SwarmWatch Autonomous Cloud SaaS Billing, Usage Metering & Micro-Payment Channel Ledger (Seam 95 / ADR 0106).
 * Verifies MPC1 (Balance Conservation), MPC2 (Monotonic Nonces & Signatures),
 * MPC3 (Dispute Window & Challenge Slashing), MPC4 (Usage Metering & Circuit Latching),
 * and sub-250 microsecond execution latency SLO in TypeScript.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { createHmac } from 'node:crypto';

import { SafetyCircuitBus } from '../src/circuit_bus.ts';
import { generateNodeKeypair, signBftPayload } from '../src/byzantine_oracle.ts';
import {
  MicroPaymentChannelLedger,
  ChannelBalanceConservationError,
  StaleChannelNonceError,
  InvalidChannelSignatureError,
  ChannelDisputeActiveError,
  BillingChannelExhaustedError,
  computeChannelCommitmentDigest,
} from '../src/billing_channel.ts';

function makeSignedCommitment(
  channelId,
  nonce,
  agentBalance,
  providerBalance,
  agentPriv,
  providerPriv,
  hlcTs = 'hlc:100:0:node'
) {
  const digest = computeChannelCommitmentDigest(
    channelId,
    nonce,
    agentBalance,
    providerBalance,
    hlcTs
  );
  return {
    channelId,
    nonce,
    agentBalance,
    providerBalance,
    commitmentDigest: digest,
    agentSignature: signBftPayload(agentPriv, digest),
    providerSignature: signBftPayload(providerPriv, digest),
    hlcTimestamp: hlcTs,
  };
}

test('Seam 95: Channel opening and deposit balance conservation MPC1 (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const ledger = new MicroPaymentChannelLedger('test-ledger-ts', bus);
  const agentKp = generateNodeKeypair();
  const providerKp = generateNodeKeypair();

  // Negative deposit rejected
  assert.throws(() => {
    ledger.openChannel('tenant-ts', agentKp.publicKeyHex, providerKp.publicKeyHex, -5.0);
  }, ChannelBalanceConservationError);

  const channel = ledger.openChannel('tenant-ts', agentKp.publicKeyHex, providerKp.publicKeyHex, 100.0, 5.0, 'chan-ts-1');
  assert.equal(channel.channelId, 'chan-ts-1');
  assert.equal(channel.agentBalance, 100.0);
  assert.equal(channel.providerBalance, 0.0);
  assert.equal(channel.status, 'OPEN');

  // Valid update
  const comm = makeSignedCommitment('chan-ts-1', 1, 85.0, 15.0, agentKp.privateKey, providerKp.privateKey);
  const updated = ledger.submitStateUpdate(comm);
  assert.equal(updated.nonce, 1);
  assert.equal(channel.agentBalance, 85.0);
  assert.equal(channel.providerBalance, 15.0);

  // Conservation breach (85 + 20 != 100)
  const breach = makeSignedCommitment('chan-ts-1', 2, 85.0, 20.0, agentKp.privateKey, providerKp.privateKey);
  assert.throws(() => {
    ledger.submitStateUpdate(breach);
  }, ChannelBalanceConservationError);
});

test('Seam 95: Monotonic nonces and dual signature verification MPC2 (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const ledger = new MicroPaymentChannelLedger('test-ledger-ts', bus);
  const agentKp = generateNodeKeypair();
  const providerKp = generateNodeKeypair();

  ledger.openChannel('tenant-ts-2', agentKp.publicKeyHex, providerKp.publicKeyHex, 50.0, 5.0, 'chan-ts-2');

  const comm1 = makeSignedCommitment('chan-ts-2', 5, 40.0, 10.0, agentKp.privateKey, providerKp.privateKey);
  ledger.submitStateUpdate(comm1);

  // Stale nonce
  const stale = makeSignedCommitment('chan-ts-2', 5, 35.0, 15.0, agentKp.privateKey, providerKp.privateKey);
  assert.throws(() => {
    ledger.submitStateUpdate(stale);
  }, StaleChannelNonceError);

  // Forged signature
  const forged = makeSignedCommitment('chan-ts-2', 6, 35.0, 15.0, agentKp.privateKey, providerKp.privateKey);
  forged.agentSignature = '00'.repeat(64);
  assert.throws(() => {
    ledger.submitStateUpdate(forged);
  }, InvalidChannelSignatureError);
});

test('Seam 95: Dispute window and adversarial fraud challenge slashing MPC3 (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const ledger = new MicroPaymentChannelLedger('test-ledger-ts', bus);
  const agentKp = generateNodeKeypair();
  const providerKp = generateNodeKeypair();

  ledger.openChannel('tenant-ts-3', agentKp.publicKeyHex, providerKp.publicKeyHex, 200.0, 5.0, 'chan-ts-3');

  const comm10 = makeSignedCommitment('chan-ts-3', 10, 150.0, 50.0, agentKp.privateKey, providerKp.privateKey);
  ledger.submitStateUpdate(comm10);

  const closing = ledger.initiateClosure('chan-ts-3');
  assert.equal(closing.status, 'CLOSING');

  // Normal updates blocked during CLOSING
  const normalComm = makeSignedCommitment('chan-ts-3', 11, 140.0, 60.0, agentKp.privateKey, providerKp.privateKey);
  assert.throws(() => {
    ledger.submitStateUpdate(normalComm);
  }, ChannelDisputeActiveError);

  // Valid challenge with higher nonce 15
  const higherComm = makeSignedCommitment('chan-ts-3', 15, 100.0, 100.0, agentKp.privateKey, providerKp.privateKey);
  const cert = ledger.challengeClosure('chan-ts-3', higherComm);

  assert.equal(cert.settlementType, 'CHEATER_SLASHED');
  assert.equal(cert.finalAgentBalance, 0.0);
  assert.equal(cert.finalProviderBalance, 200.0); // Full deposit slashed
  const chan = ledger.getChannel('chan-ts-3');
  assert.equal(chan.status, 'SLASHED');
});

test('Seam 95: Real-time usage metering and circuit latching MPC4 (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const ledger = new MicroPaymentChannelLedger('test-ledger-ts', bus);
  const agentKp = generateNodeKeypair();
  const providerKp = generateNodeKeypair();

  ledger.openChannel('tenant-ts-4', agentKp.publicKeyHex, providerKp.publicKeyHex, 10.0, 3.0, 'chan-ts-4');

  const evt1 = ledger.recordMeterEvent('chan-ts-4', 'TELEMETRY_SPAN', 5000, 0.001);
  assert.equal(evt1.totalCharge, 5.0);
  assert.equal(bus.isTripped(), false);

  // Exceeding reserve threshold 3.0 trips circuit
  const evt2 = ledger.recordMeterEvent('chan-ts-4', 'TELEMETRY_SPAN', 3000, 0.001);
  assert.equal(evt2.totalCharge, 3.0);
  const chan = ledger.getChannel('chan-ts-4');
  assert.equal(chan.agentBalance, 2.0);
  assert.equal(bus.isTripped(), true);
  const tripEvt = bus.getStatus().last_trip_event;
  assert.equal(tripEvt.reason, 'BILLING_CHANNEL_DEPLETED');
});

test('Seam 95: Adversarial forged channel signatures rejected (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const ledger = new MicroPaymentChannelLedger('test-ledger-ts-adv', bus);
  const agentKp = generateNodeKeypair();
  const providerKp = generateNodeKeypair();

  ledger.openChannel('tenant-ts-adv', agentKp.publicKeyHex, providerKp.publicKeyHex, 100.0, 5.0, 'chan-adv');
  const digest = computeChannelCommitmentDigest('chan-adv', 1, 90.0, 10.0, 'hlc:1:0:node');

  // Attempt 1: Forged HMAC signature is rejected
  const forgedHmac = createHmac('sha256', agentKp.publicKeyHex).update(digest).digest('hex');
  const commHmac = {
    channelId: 'chan-adv', nonce: 1, agentBalance: 90.0, providerBalance: 10.0,
    commitmentDigest: digest, agentSignature: forgedHmac,
    providerSignature: signBftPayload(providerKp.privateKey, digest), hlcTimestamp: 'hlc:1:0:node'
  };
  assert.throws(() => {
    ledger.submitStateUpdate(commHmac);
  }, InvalidChannelSignatureError);

  // Attempt 2: Forged test string "sig:<pubkey>:<digest>" is rejected
  const forgedDirect = `sig:${agentKp.publicKeyHex}:${digest.slice(0, 16)}`;
  const commDirect = {
    channelId: 'chan-adv', nonce: 1, agentBalance: 90.0, providerBalance: 10.0,
    commitmentDigest: digest, agentSignature: forgedDirect,
    providerSignature: signBftPayload(providerKp.privateKey, digest), hlcTimestamp: 'hlc:1:0:node'
  };
  assert.throws(() => {
    ledger.submitStateUpdate(commDirect);
  }, InvalidChannelSignatureError);
});

test('Seam 95: Sub-250 microsecond execution latency SLO (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const ledger = new MicroPaymentChannelLedger('test-ledger-perf', bus);
  const agentKp = generateNodeKeypair();
  const providerKp = generateNodeKeypair();

  ledger.openChannel('tenant-perf', agentKp.publicKeyHex, providerKp.publicKeyHex, 10000.0, 1.0, 'chan-perf');

  const nOps = 100;
  const start = performance.now();
  for (let i = 1; i <= nOps; i++) {
    ledger.recordMeterEvent('chan-perf', 'METER_UNIT', 1, 0.0001);
  }
  const elapsedMs = performance.now() - start;
  const avgUs = (elapsedMs / nOps) * 1000;
  assert.ok(avgUs < 250.0, `Average latency ${avgUs.toFixed(2)}µs exceeded 250µs SLO`);
});
