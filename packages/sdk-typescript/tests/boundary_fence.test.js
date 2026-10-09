/**
 * Tests for SwarmWatch Sovereign Cross-Border Data Boundary Fence & Zero-Egress Encryption Vault (Seam 99 / ADR 0110).
 * Verifies BND1 (Geographic Residency Policy Constraint), BND2 (Zero-Egress Vault Encryption),
 * BND3 (Cryptographic Egress Export Passport), BND4 (Breach Containment & Safety Circuit Latching),
 * and sub-250 microsecond execution latency SLO in TypeScript.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';

import { SafetyCircuitBus } from '../src/circuit_bus.ts';
import {
  DataBoundaryFence,
  CrossBorderResidencyViolationError,
  InvalidEgressPassportError,
  FenceCircuitTrippedError,
} from '../src/boundary_fence.ts';

function setupCleanFence() {
  const bus = new SafetyCircuitBus();
  const fence = new DataBoundaryFence('test-boundary-fence', 3, 30.0, bus);

  fence.registerResidencyZone({ zoneId: 'EU_DE', jurisdiction: 'Germany', complianceTags: ['GDPR'] });
  fence.registerResidencyZone({ zoneId: 'US_EAST', jurisdiction: 'United States', complianceTags: ['SOC2'] });
  fence.registerResidencyZone({ zoneId: 'CH_ZH', jurisdiction: 'Switzerland', complianceTags: ['FADP'] });

  return { fence, bus };
}

test('BND1: Geofenced residency and treaty enforcement', () => {
  const { fence } = setupCleanFence();

  // 1. Intra-zone transfer needs no treaty or passport
  const envLocal = fence.sealData('tenant-acme', 'EU_DE', 'local-sensitive-payload');
  const resLocal = fence.inspectEgress(envLocal, 'EU_DE');
  assert.equal(resLocal.allowed, true);
  assert.equal(resLocal.reason, 'LOCAL_INTRA_ZONE_TRANSFER');

  // 2. Cross-border transfer without active treaty raises CrossBorderResidencyViolationError (BND1)
  assert.throws(
    () => fence.issueEgressPassport('tenant-acme', 'EU_DE', 'US_EAST', envLocal.dataDigest),
    CrossBorderResidencyViolationError
  );

  // 3. Register treaty EU_DE -> US_EAST
  fence.registerEgressTreaty({
    treatyId: 'trt-eu-us',
    sourceZone: 'EU_DE',
    destZone: 'US_EAST',
    isBidirectional: true,
    isActive: true,
  });

  // 4. Now passport issuance succeeds
  const passport = fence.issueEgressPassport('tenant-acme', 'EU_DE', 'US_EAST', envLocal.dataDigest);
  assert.equal(passport.sourceZone, 'EU_DE');
  assert.equal(passport.destZone, 'US_EAST');
});

test('BND2: Zero-egress vault envelope encryption and unsealing', () => {
  const { fence } = setupCleanFence();

  const rawData = 'sensitive-customer-pii-record-48291';
  const envelope = fence.sealData('tenant-alpha', 'EU_DE', rawData);
  assert.equal(envelope.originZone, 'EU_DE');
  assert.equal(envelope.tenantId, 'tenant-alpha');
  assert.ok(envelope.ciphertextHex.length > 0);

  // Unsealing recovers plaintext
  const recovered = fence.unsealData(envelope);
  assert.equal(recovered.toString('utf-8'), rawData);

  // Tampered envelope fails decryption
  const tampered = {
    ...envelope,
    ciphertextHex: 'ff'.repeat(Buffer.from(envelope.ciphertextHex, 'hex').length),
  };
  assert.throws(
    () => fence.unsealData(tampered),
    /failed/i
  );
});

test('BND3: Cryptographic egress passport validation and anti-replay', () => {
  const { fence } = setupCleanFence();
  fence.registerEgressTreaty({ treatyId: 't1', sourceZone: 'EU_DE', destZone: 'CH_ZH', isActive: true });

  const env = fence.sealData('tenant-bank', 'EU_DE', 'swiss-banking-telemetry');
  const passport = fence.issueEgressPassport('tenant-bank', 'EU_DE', 'CH_ZH', env.dataDigest);

  // First inspection succeeds and consumes passport
  const res = fence.inspectEgress(env, 'CH_ZH', passport);
  assert.equal(res.allowed, true);
  assert.equal(res.reason, 'CROSS_BORDER_PASSPORT_VERIFIED');

  // Replay of consumed passport fails fail-closed (BND3)
  assert.throws(
    () => fence.inspectEgress(env, 'CH_ZH', passport),
    InvalidEgressPassportError
  );

  // Route mismatch passport fails
  const passport2 = fence.issueEgressPassport('tenant-bank', 'EU_DE', 'CH_ZH', env.dataDigest);
  passport2.destZone = 'US_EAST'; // mismatch
  assert.throws(
    () => fence.inspectEgress(env, 'CH_ZH', passport2),
    InvalidEgressPassportError
  );
});

test('BND4: Breach containment quarantine and safety circuit latching', () => {
  const { fence, bus } = setupCleanFence();
  assert.equal(bus.isTripped(), false);

  const env = fence.sealData('tenant-bad', 'EU_DE', 'data-payload');

  // 3 consecutive breaches on EU_DE (threshold = 3)
  for (let i = 0; i < 3; i++) {
    try {
      fence.inspectEgress(env, 'US_EAST', undefined);
    } catch (e) {}
  }

  // Safety circuit is now tripped and zone quarantined
  assert.equal(bus.isTripped(), true);
  assert.equal(bus.getStatus().last_trip_event?.reason, 'BOUNDARY_FENCE_EGRESS_BREACH');

  // Subsequent operations on quarantined zone fail fail-closed
  assert.throws(
    () => fence.sealData('tenant-any', 'EU_DE', 'any-data'),
    FenceCircuitTrippedError
  );
});

test('Performance SLO: Sub-250us execution latency and zero token cost', () => {
  const { fence } = setupCleanFence();
  fence.registerEgressTreaty({ treatyId: 't-perf', sourceZone: 'EU_DE', destZone: 'US_EAST', isActive: true });

  const env = fence.sealData('tenant-perf', 'EU_DE', 'telemetry-span-record');
  const passport = fence.issueEgressPassport('tenant-perf', 'EU_DE', 'US_EAST', env.dataDigest);

  // Warmup
  fence.inspectEgress(env, 'US_EAST', passport);

  const latenciesUs = [];
  for (let i = 0; i < 20; i++) {
    passport.isConsumed = false;
    const t0 = performance.now();
    fence.inspectEgress(env, 'US_EAST', passport);
    const t1 = performance.now();
    latenciesUs.push((t1 - t0) * 1000.0);
  }

  latenciesUs.sort((a, b) => a - b);
  const medianUs = latenciesUs[Math.floor(latenciesUs.length / 2)];
  assert.ok(medianUs < 250.0, `Execution latency ${medianUs.toFixed(2)}us exceeded 250us SLO.`);

  const status = fence.getStatus();
  assert.ok(status.total_sealed_envelopes >= 1);
  assert.ok(status.total_inspections >= 1);
  assert.equal(status.circuit_tripped, false);
});

test('Adversarial: Forged passport signatures rejected fail-closed', () => {
  const { fence } = setupCleanFence();
  fence.registerEgressTreaty({ treatyId: 't-adv', sourceZone: 'EU_DE', destZone: 'US_EAST', isActive: true });

  const env = fence.sealData('tenant-adv', 'EU_DE', 'confidential-payload');
  const passport = fence.issueEgressPassport('tenant-adv', 'EU_DE', 'US_EAST', env.dataDigest);

  // 1. String signature template attack
  passport.passportSignature = `sig:${fence.fenceId}:${passport.payloadDigest.slice(0, 16)}`;
  assert.throws(
    () => fence.inspectEgress(env, 'US_EAST', passport),
    InvalidEgressPassportError
  );

  // 2. Random hex signature attack
  passport.passportSignature = 'baddecaf'.repeat(8);
  assert.throws(
    () => fence.inspectEgress(env, 'US_EAST', passport),
    InvalidEgressPassportError
  );
});
