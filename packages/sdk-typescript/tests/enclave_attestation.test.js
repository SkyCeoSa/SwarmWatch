/**
 * Tests for SwarmWatch Autonomous Agent Capability Attestation & Hardware Secure Enclave (SGX/TEE) Remote Witness (Seam 98 / ADR 0109).
 * Verifies TEE1 (Measurement Integrity), TEE2 (Nonce Freshness & Anti-Replay),
 * TEE3 (Lease-Bounded Capability Delegation), TEE4 (Enclave Quarantine & Safety Circuit Latching),
 * and sub-250 microsecond execution latency SLO in TypeScript.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';

import { SafetyCircuitBus } from '../src/circuit_bus.ts';
import { generateNodeKeypair, signBftPayload } from '../src/byzantine_oracle.ts';
import {
  EnclaveAttestationCoordinator,
  EnclaveMeasurementMismatchError,
  StaleAttestationReportError,
  UnauthorizedCapabilityError,
  EnclaveCircuitTrippedError,
  EnclaveAttestationError,
  computeQuoteDigest,
} from '../src/enclave_attestation.ts';

function setupCleanEnclave() {
  const bus = new SafetyCircuitBus();
  const coord = new EnclaveAttestationCoordinator('test-enclave-coord', 3, 5.0, 60.0, bus);
  const meas = {
    enclaveId: 'enc-01',
    teeType: 'SGX',
    codeHash: 'c'.repeat(64),
    configHash: 'f'.repeat(64),
    signerId: 'intel-ias-signer',
    isApproved: true,
  };
  const mhash = coord.registerApprovedMeasurement(meas);
  const { privateKey, publicKeyHex } = generateNodeKeypair();
  return { coord, bus, mhash, privateKey, publicKeyHex };
}

function makeSignedQuote(enclaveId, teeType, measurementHash, nonce, privateKey, publicKeyHex, timestampNs, userData = 'udata') {
  const digest = computeQuoteDigest(enclaveId, teeType, measurementHash, nonce, userData, timestampNs);
  const sig = signBftPayload(privateKey, digest);
  return {
    quoteId: `q-${nonce.slice(0, 8)}`,
    enclaveId,
    teeType,
    measurementHash,
    challengeNonce: nonce,
    publicKey: publicKeyHex,
    signature: sig,
    timestampNs,
    userDataDigest: userData,
  };
}

test('TEE1: Measurement registration and quote verification', () => {
  const { coord, mhash, privateKey, publicKeyHex } = setupCleanEnclave();

  const chal = coord.issueChallenge('enc-01');
  const nowNs = Date.now() * 1_000_000;
  const quote = makeSignedQuote('enc-01', 'SGX', mhash, chal.nonce, privateKey, publicKeyHex, nowNs);

  const token = coord.verifyAndAttest(quote, 'agent-007', ['TASK_EXECUTE', 'SETTLEMENT_READ']);
  assert.equal(token.enclaveId, 'enc-01');
  assert.equal(token.agentId, 'agent-007');
  assert.ok(token.capabilities.includes('TASK_EXECUTE'));

  // Unapproved / altered measurement fails fail-closed (TEE1)
  const chal2 = coord.issueChallenge('enc-01');
  const badMhash = '0'.repeat(64);
  const quoteBadMeas = makeSignedQuote('enc-01', 'SGX', badMhash, chal2.nonce, privateKey, publicKeyHex, nowNs);
  assert.throws(
    () => coord.verifyAndAttest(quoteBadMeas, 'agent-007', ['TASK_EXECUTE']),
    EnclaveMeasurementMismatchError
  );
});

test('TEE2: Challenge nonce freshness and anti-replay', () => {
  const { coord, mhash, privateKey, publicKeyHex } = setupCleanEnclave();

  const chal = coord.issueChallenge('enc-01');
  const nowNs = Date.now() * 1_000_000;
  const quote = makeSignedQuote('enc-01', 'SGX', mhash, chal.nonce, privateKey, publicKeyHex, nowNs);

  // First verification succeeds
  coord.verifyAndAttest(quote, 'agent-007', ['TASK_EXECUTE']);

  // Replay of the same quote with consumed nonce fails (TEE2)
  assert.throws(
    () => coord.verifyAndAttest(quote, 'agent-007', ['TASK_EXECUTE']),
    StaleAttestationReportError
  );

  // Unknown challenge nonce fails
  const quoteFakeNonce = makeSignedQuote('enc-01', 'SGX', mhash, 'fake-nonce'.repeat(4), privateKey, publicKeyHex, nowNs);
  assert.throws(
    () => coord.verifyAndAttest(quoteFakeNonce, 'agent-007', ['TASK_EXECUTE']),
    StaleAttestationReportError
  );
});

test('TEE3: Lease-bounded capability delegation and validation', () => {
  const { coord, mhash, privateKey, publicKeyHex } = setupCleanEnclave();

  const chal = coord.issueChallenge('enc-01');
  const nowNs = Date.now() * 1_000_000;
  const quote = makeSignedQuote('enc-01', 'SGX', mhash, chal.nonce, privateKey, publicKeyHex, nowNs);

  const token = coord.verifyAndAttest(quote, 'agent-007', ['TASK_EXECUTE', 'FUNDS_TRANSFER']);

  // Valid capability succeeds
  assert.equal(coord.validateCapability(token, 'TASK_EXECUTE'), true);
  assert.equal(coord.validateCapability(token, 'FUNDS_TRANSFER'), true);

  // Unauthorized capability raises UnauthorizedCapabilityError
  assert.throws(
    () => coord.validateCapability(token, 'ADMIN_CLUSTER_REARM'),
    UnauthorizedCapabilityError
  );

  // Expired token raises UnauthorizedCapabilityError
  token.expiresAtNs = Date.now() * 1_000_000 - 1000; // simulate past expiration
  assert.throws(
    () => coord.validateCapability(token, 'TASK_EXECUTE'),
    UnauthorizedCapabilityError
  );
});

test('TEE4: Consecutive failures quarantine and safety circuit latching', () => {
  const { coord, bus, mhash, publicKeyHex } = setupCleanEnclave();
  assert.equal(bus.isTripped(), false);

  // Repeated signature validation failures (threshold = 3)
  for (let i = 0; i < 3; i++) {
    const chal = coord.issueChallenge('enc-01');
    const nowNs = Date.now() * 1_000_000;
    const badQuote = {
      quoteId: `q-bad-${i}`,
      enclaveId: 'enc-01',
      teeType: 'SGX',
      measurementHash: mhash,
      challengeNonce: chal.nonce,
      publicKey: publicKeyHex,
      signature: 'bad-signature'.repeat(5),
      timestampNs: nowNs,
      userDataDigest: '',
    };
    try {
      coord.verifyAndAttest(badQuote, 'agent-bad', ['READ']);
    } catch (e) {}
  }

  // Safety circuit is now tripped and enclave quarantined
  assert.equal(bus.isTripped(), true);
  assert.equal(bus.getStatus().last_trip_event?.reason, 'TEE_HARDWARE_ATTESTATION_BREACH');

  // Subsequent challenge issuance fails fail-closed
  assert.throws(
    () => coord.issueChallenge('enc-01'),
    EnclaveCircuitTrippedError
  );
});

test('TEE-ADV: Adversarial forged enclave quote signatures rejected', () => {
  const { coord, mhash, privateKey, publicKeyHex } = setupCleanEnclave();
  const chal = coord.issueChallenge('enc-01');
  const nowNs = Date.now() * 1_000_000;
  const digest = computeQuoteDigest('enc-01', 'SGX', mhash, chal.nonce, 'udata', nowNs);

  // 1. Literal test string format
  const quoteLiteral = {
    quoteId: 'q-adv-1',
    enclaveId: 'enc-01',
    teeType: 'SGX',
    measurementHash: mhash,
    challengeNonce: chal.nonce,
    publicKey: publicKeyHex,
    signature: `sig:${publicKeyHex}:${digest.slice(0, 16)}`,
    timestampNs: nowNs,
    userDataDigest: 'udata',
  };
  assert.throws(
    () => coord.verifyAndAttest(quoteLiteral, 'agent-adv', ['READ']),
    EnclaveAttestationError
  );

  // 2. Corrupted token signature in capability verification
  const validChal = coord.issueChallenge('enc-01');
  const validQuote = makeSignedQuote('enc-01', 'SGX', mhash, validChal.nonce, privateKey, publicKeyHex, nowNs);
  const token = coord.verifyAndAttest(validQuote, 'agent-adv', ['READ']);
  token.tokenSignature = 'ff' + token.tokenSignature.slice(2);
  assert.throws(
    () => coord.validateCapability(token, 'READ'),
    UnauthorizedCapabilityError
  );
});

test('Performance SLO: Sub-250us execution latency and zero token cost', () => {
  const { coord, mhash, privateKey, publicKeyHex } = setupCleanEnclave();

  // Warmup and setup token
  const chal = coord.issueChallenge('enc-01');
  const nowNs = Date.now() * 1_000_000;
  const quote = makeSignedQuote('enc-01', 'SGX', mhash, chal.nonce, privateKey, publicKeyHex, nowNs);
  const token = coord.verifyAndAttest(quote, 'agent-perf', ['EXECUTE']);

  const iterations = 50;
  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    coord.validateCapability(token, 'EXECUTE');
  }
  const t1 = performance.now();

  const elapsedUs = ((t1 - t0) * 1000.0) / iterations;
  assert.ok(elapsedUs < 250.0, `Execution latency ${elapsedUs.toFixed(2)}us exceeded 250us SLO.`);

  const status = coord.getStatus();
  assert.equal(status.total_attestations, 1);
  assert.equal(status.active_capability_tokens, 1);
  assert.equal(status.circuit_tripped, false);
});

