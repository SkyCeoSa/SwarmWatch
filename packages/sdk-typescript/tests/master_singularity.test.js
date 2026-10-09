/**
 * Tests for SwarmWatch Global Master Singularity & Autonomous Self-Governing Sovereign Registry (Seam 100 / ADR 0111).
 * Verifies SNG1 (Monotonic Epoch Progression), SNG2 (Constitutional Quorum Ratification),
 * SNG3 (Proof-of-Solvency & Invariant Convergence), SNG4 (Emergency Halt & Containment),
 * and sub-250 microsecond execution latency SLO in TypeScript.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import crypto, { createHash, createHmac } from 'node:crypto';

import { SafetyCircuitBus } from '../src/circuit_bus.ts';
import { generateNodeKeypair, signBftPayload } from '../src/byzantine_oracle.ts';
import {
  MasterSingularityCoordinator,
  SingularityError,
  StaleSingularityEpochError,
  InsufficientGovernanceQuorumError,
  SingularitySolvencyBreachError,
  SingularityQuarantinedError,
  SingularityCircuitTrippedError,
} from '../src/master_singularity.ts';

function setupCleanSingularity() {
  const bus = new SafetyCircuitBus();
  const coord = new MasterSingularityCoordinator({
    singularityId: 'test-singularity-root',
    minSolvencyThreshold: 500_000,
    circuitBus: bus,
  });

  const keys = {};
  for (let i = 1; i <= 3; i++) {
    const gid = `gov-${i}`;
    const kp = generateNodeKeypair();
    keys[gid] = kp;
    coord.registerGovernor({ governorId: gid, publicKeyHex: kp.publicKeyHex });
  }

  return { coord, bus, keys };
}

function signForGovernor(keys, governorId, digest) {
  return signBftPayload(keys[governorId].privateKey, digest);
}

test('SNG1: Monotonic epoch progression and state tree evolution', () => {
  const { coord } = setupCleanSingularity();

  assert.equal(coord.currentEpoch, 0);
  assert.equal(coord.currentStateRoot, '0'.repeat(64));

  // Non-monotonic or skipped epoch fails with StaleSingularityEpochError (SNG1)
  assert.throws(
    () =>
      coord.commitSubsystemState(
        2, // Expected 1
        'mesh-0',
        'fed-0',
        'snark-0',
        'fence-0',
        1_000_000,
      ),
    StaleSingularityEpochError
  );

  // Valid epoch 1 transition
  const c1 = coord.commitSubsystemState(
    1,
    'mesh-1',
    'fed-1',
    'snark-1',
    'fence-1',
    1_000_000,
  );
  assert.equal(coord.currentEpoch, 1);
  assert.equal(c1.epoch, 1);
  assert.equal(coord.currentStateRoot, c1.stateDigest);

  // Attempting to re-commit epoch 1 or older fails
  assert.throws(
    () =>
      coord.commitSubsystemState(
        1,
        'mesh-1',
        'fed-1',
        'snark-1',
        'fence-1',
        1_000_000,
      ),
    StaleSingularityEpochError
  );
});

test('SNG2: Byzantine constitutional quorum ratification', () => {
  const { coord, keys } = setupCleanSingularity();

  // Submit proposal targeting epoch 1
  const prop = coord.submitProposal(
    'prop-constitutional-reweight',
    'gov-1',
    'Increase minimum solvency threshold',
    { minSolvencyThreshold: 750_000 },
    1,
  );
  assert.equal(prop.status, 'PROPOSED');

  // With 3 active governors, required quorum is 3. Vote with only 2
  const sig1 = signForGovernor(keys, 'gov-1', prop.payloadDigest);
  const sig2 = signForGovernor(keys, 'gov-2', prop.payloadDigest);
  coord.voteProposal(prop.proposalId, 'gov-1', sig1);
  coord.voteProposal(prop.proposalId, 'gov-2', sig2);

  // Ratification fails with InsufficientGovernanceQuorumError (SNG2)
  assert.throws(
    () => coord.ratifyProposal(prop.proposalId),
    InsufficientGovernanceQuorumError
  );

  // Third governor votes
  const sig3 = signForGovernor(keys, 'gov-3', prop.payloadDigest);
  coord.voteProposal(prop.proposalId, 'gov-3', sig3);

  // Now ratification succeeds
  const ratified = coord.ratifyProposal(prop.proposalId);
  assert.equal(ratified.status, 'RATIFIED');
  assert.ok(ratified.ratifiedAtNs);
});

test('SNG3: Universal proof-of-solvency and certificate issuance', () => {
  const { coord } = setupCleanSingularity();

  // Solvency breach: total stake below threshold fails with SingularitySolvencyBreachError (SNG3)
  assert.throws(
    () =>
      coord.commitSubsystemState(
        1,
        'mesh-1',
        'fed-1',
        'snark-1',
        'fence-1',
        200_000, // Below 500_000
      ),
    SingularitySolvencyBreachError
  );

  // Solvency breach: active breaches > 0 fails with SingularitySolvencyBreachError (SNG3)
  assert.throws(
    () =>
      coord.commitSubsystemState(
        1,
        'mesh-1',
        'fed-1',
        'snark-1',
        'fence-1',
        1_000_000,
        2, // breachCount = 2
      ),
    SingularitySolvencyBreachError
  );

  // Successful commitment with adequate solvency
  coord.commitSubsystemState(
    1,
    'mesh-1',
    'fed-1',
    'snark-1',
    'fence-1',
    1_000_000,
    0,
  );

  // Issue singularity certificate
  const cert = coord.issueSingularityCertificate(1);
  assert.equal(cert.epoch, 1);
  assert.ok(cert.certificateId.startsWith('sng-cert-1-'));
  assert.equal(cert.masterSingularitySignature.length, 128);
});

test('SNG4: Universal emergency halt and recovery ceremony', () => {
  const { coord, bus, keys } = setupCleanSingularity();

  // Trigger emergency halt (SNG4)
  coord.emergencyHalt('CRITICAL_BYZANTINE_COLLAPSE');
  assert.equal(coord.isQuarantined, true);
  assert.equal(bus.isTripped(), true);

  // Mutating operations raise SingularityQuarantinedError or SingularityCircuitTrippedError
  assert.throws(
    () =>
      coord.submitProposal(
        'prop-blocked',
        'gov-1',
        'Blocked proposal',
        {},
        1,
      ),
    (err) => err instanceof SingularityQuarantinedError || err instanceof SingularityCircuitTrippedError
  );

  // Recovery ceremony with insufficient signatures fails
  const recovDigest = coord.getRecoveryDigest();

  const recovSigs = {
    'gov-1': signForGovernor(keys, 'gov-1', recovDigest),
    'gov-2': signForGovernor(keys, 'gov-2', recovDigest),
  };
  assert.equal(coord.executeRecoveryCeremony(recovSigs), false);
  assert.equal(coord.isQuarantined, true);

  // Full super-majority quorum participates
  recovSigs['gov-3'] = signForGovernor(keys, 'gov-3', recovDigest);
  assert.equal(coord.executeRecoveryCeremony(recovSigs), true);
  assert.equal(coord.isQuarantined, false);
  assert.equal(bus.isTripped(), false);
});

test('SNG-ADV: Adversarial forged governor signatures strictly rejected in TypeScript', () => {
  const { coord, keys } = setupCleanSingularity();

  const prop = coord.submitProposal(
    'prop-malicious-disable-guardrails',
    'attacker',
    'Disable guardrails',
    { guardrails: 'disabled' },
    1,
  );

  // Attempt 1: Forged HMAC of governor ID is rejected
  const forgedHmac = createHmac('sha256', 'gov-1').update(prop.payloadDigest).digest('hex');
  assert.equal(coord.voteProposal(prop.proposalId, 'gov-1', forgedHmac), false);

  // Attempt 2: Forged test signature string is rejected
  const forgedDirect = `sig:${keys['gov-1'].publicKeyHex}:${prop.payloadDigest.slice(0, 16)}`;
  assert.equal(coord.voteProposal(prop.proposalId, 'gov-1', forgedDirect), false);

  // Attempt 3: Corrupted signature byte is rejected
  const validSig = signForGovernor(keys, 'gov-1', prop.payloadDigest);
  const corruptedSig = validSig.slice(0, -2) + (validSig.slice(-2) === '00' ? 'ff' : '00');
  assert.equal(coord.voteProposal(prop.proposalId, 'gov-1', corruptedSig), false);

  // Attempt 4: Registering governor with invalid public key is rejected
  assert.throws(() => {
    coord.registerGovernor({ governorId: 'evil-gov', publicKeyHex: 'invalid-hex' });
  }, SingularityError);

  // Attempt 5: Cannot lift emergency lockdown with forged HMAC signatures
  coord.emergencyHalt('ADVERSARIAL_TEST');
  const recovDigest = createHash('sha256')
    .update(`RECOVERY:${coord.singularityId}:${coord.currentEpoch}`)
    .digest('hex');
  const forgedRecov = {
    'gov-1': createHmac('sha256', 'gov-1').update(recovDigest).digest('hex'),
    'gov-2': createHmac('sha256', 'gov-2').update(recovDigest).digest('hex'),
    'gov-3': createHmac('sha256', 'gov-3').update(recovDigest).digest('hex'),
  };
  assert.equal(coord.executeRecoveryCeremony(forgedRecov), false);
  assert.equal(coord.isQuarantined, true);
});

test('SLO: Execution latency remains under 250us', () => {
  const { coord } = setupCleanSingularity();

  // Pre-commit epoch 1
  coord.commitSubsystemState(
    1,
    'mesh-1',
    'fed-1',
    'snark-1',
    'fence-1',
    1_000_000,
  );

  // Benchmark issueSingularityCertificate over 50 iterations
  const iterations = 50;
  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    coord.issueSingularityCertificate(1);
  }
  const end = performance.now();

  const avgLatencyUs = ((end - start) * 1000) / iterations;
  assert.ok(
    avgLatencyUs < 250.0,
    `Average latency ${avgLatencyUs.toFixed(2)}us exceeds 250us SLO.`
  );
});
