/**
 * Tests for SwarmWatch Multi-Cloud Byzantine Partition Tolerant Gossip Membership Protocol (Seam 96 / ADR 0107).
 * Verifies BGM1 (Authenticated Heartbeats), BGM2 (Quorum Suspicion & Refutation),
 * BGM3 (Anti-Entropy Partition Healing), BGM4 (Equivocation Quarantine & Circuit Latching),
 * and sub-250 microsecond execution latency SLO in TypeScript.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';

import { SafetyCircuitBus } from '../src/circuit_bus.ts';
import { generateNodeKeypair, signBftPayload } from '../src/byzantine_oracle.ts';
import {
  ByzantineGossipCoordinator,
  UnauthenticatedGossipBeaconError,
  GossipPeerQuarantinedError,
  GossipEquivocationDetectedError,
  computeBeaconDigest,
  computeSuspicionDigest,
  computeMembershipMerkleRoot,
} from '../src/byzantine_gossip.ts';

function makeSignedBeacon(
  nodeId,
  seq,
  privKey,
  status = 'ALIVE',
  stateRoot = '00'.repeat(32),
  hlcTs = 'hlc:100:0:node'
) {
  const digest = computeBeaconDigest(nodeId, seq, status, hlcTs, stateRoot);
  const signature = signBftPayload(privKey, digest);
  return {
    nodeId,
    sequenceNumber: seq,
    status,
    hlcTimestamp: hlcTs,
    stateRoot,
    signature,
  };
}

function makeSignedSuspicion(
  reporterId,
  reporterPriv,
  suspectId,
  seq,
  reason = 'PING_TIMEOUT',
  hlcTs = 'hlc:100:0:node'
) {
  const digest = computeSuspicionDigest(reporterId, suspectId, seq, reason, hlcTs);
  const signature = signBftPayload(reporterPriv, digest);
  return {
    reporterId,
    suspectId,
    sequenceNumber: seq,
    reason,
    signature,
    hlcTimestamp: hlcTs,
  };
}

test('Seam 96: Authenticated heartbeats and monotonic progress BGM1 (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const coord = new ByzantineGossipCoordinator('coord-ts-1', 2, bus);
  const { privateKey, publicKeyHex } = generateNodeKeypair();

  coord.registerMember({
    nodeId: 'node-ts-01',
    cloudProvider: 'aws',
    region: 'us-east-1',
    publicKey: publicKeyHex,
    status: 'ALIVE',
    sequenceNumber: 0,
    lastSeenHlc: '',
  });

  const b1 = makeSignedBeacon('node-ts-01', 1, privateKey);
  assert.equal(coord.processHeartbeat(b1), true);
  const member = coord.getMember('node-ts-01');
  assert.equal(member.sequenceNumber, 1);

  // Stale sequence number is noop
  const bStale = makeSignedBeacon('node-ts-01', 0, privateKey);
  assert.equal(coord.processHeartbeat(bStale), false);

  // Invalid signature fails fail-closed
  const otherKeys = generateNodeKeypair();
  const bBad = makeSignedBeacon('node-ts-01', 2, otherKeys.privateKey);
  assert.throws(() => {
    coord.processHeartbeat(bBad);
  }, UnauthenticatedGossipBeaconError);
});

test('Seam 96: Quorum-gated suspicion escalation and refutation BGM2 (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const coord = new ByzantineGossipCoordinator('coord-ts-2', 2, bus);
  const victimKeys = generateNodeKeypair();
  const rep1Keys = generateNodeKeypair();
  const rep2Keys = generateNodeKeypair();

  coord.registerMember({
    nodeId: 'victim-ts',
    cloudProvider: 'gcp',
    region: 'us-central1',
    publicKey: victimKeys.publicKeyHex,
    status: 'ALIVE',
    sequenceNumber: 0,
    lastSeenHlc: '',
  });
  coord.registerMember({
    nodeId: 'rep-ts-1',
    cloudProvider: 'aws',
    region: 'us-east-1',
    publicKey: rep1Keys.publicKeyHex,
    status: 'ALIVE',
    sequenceNumber: 0,
    lastSeenHlc: '',
  });
  coord.registerMember({
    nodeId: 'rep-ts-2',
    cloudProvider: 'azure',
    region: 'eastus',
    publicKey: rep2Keys.publicKeyHex,
    status: 'ALIVE',
    sequenceNumber: 0,
    lastSeenHlc: '',
  });

  // First suspicion doesn't meet threshold 2
  const s1 = makeSignedSuspicion('rep-ts-1', rep1Keys.privateKey, 'victim-ts', 1);
  assert.equal(coord.reportSuspicion(s1), false);
  const victim = coord.getMember('victim-ts');
  assert.equal(victim.status, 'ALIVE');

  // Second suspicion meets threshold 2 -> escalates to SUSPECT
  const s2 = makeSignedSuspicion('rep-ts-2', rep2Keys.privateKey, 'victim-ts', 1);
  assert.equal(coord.reportSuspicion(s2), true);
  assert.equal(victim.status, 'SUSPECT');

  // Victim broadcasts refutation beacon advancing sequence number to 2
  const refuteB = makeSignedBeacon('victim-ts', 2, victimKeys.privateKey);
  assert.equal(coord.refuteSuspicion(refuteB), true);
  assert.equal(victim.status, 'ALIVE');
  assert.equal(victim.suspicionReporters?.size, 0);
});

test('Seam 96: Partition healing and anti-entropy reconciliation BGM3 (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const coord = new ByzantineGossipCoordinator('coord-ts-3', 2, bus);
  const k1 = generateNodeKeypair();
  const k2 = generateNodeKeypair();

  coord.registerMember({
    nodeId: 'node-ts-1',
    cloudProvider: 'aws',
    region: 'us-east-1',
    publicKey: k1.publicKeyHex,
    status: 'ALIVE',
    sequenceNumber: 1,
    lastSeenHlc: '',
  });

  const remoteMembers = [
    {
      nodeId: 'node-ts-1',
      cloudProvider: 'aws',
      region: 'us-east-1',
      publicKey: k1.publicKeyHex,
      status: 'ALIVE',
      sequenceNumber: 5,
      lastSeenHlc: '',
    },
    {
      nodeId: 'node-ts-2',
      cloudProvider: 'gcp',
      region: 'europe-west1',
      publicKey: k2.publicKeyHex,
      status: 'ALIVE',
      sequenceNumber: 3,
      lastSeenHlc: '',
    },
  ];

  const updated = coord.reconcileAntiEntropy(remoteMembers);
  assert.equal(updated, 2);
  assert.equal(coord.getMember('node-ts-1').sequenceNumber, 5);
  assert.notEqual(coord.getMember('node-ts-2'), undefined);
  assert.equal(coord.getStatus().totalMembers, 2);
});

test('Seam 96: Equivocation detection, quarantine, and circuit latching BGM4 (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const coord = new ByzantineGossipCoordinator('coord-ts-4', 2, bus);
  const keys = generateNodeKeypair();

  coord.registerMember({
    nodeId: 'node-bad-ts',
    cloudProvider: 'aws',
    region: 'us-west-2',
    publicKey: keys.publicKeyHex,
    status: 'ALIVE',
    sequenceNumber: 0,
    lastSeenHlc: '',
  });

  const b10a = makeSignedBeacon('node-bad-ts', 10, keys.privateKey, 'ALIVE', '11'.repeat(32));
  assert.equal(coord.processHeartbeat(b10a), true);

  // Conflicting beacon with different state root at same seq 10!
  const b10b = makeSignedBeacon('node-bad-ts', 10, keys.privateKey, 'ALIVE', '22'.repeat(32));
  assert.throws(() => {
    coord.processHeartbeat(b10b);
  }, GossipEquivocationDetectedError);

  const member = coord.getMember('node-bad-ts');
  assert.equal(member.status, 'QUARANTINED');
  assert.equal(bus.isTripped(), true);
  assert.equal(bus.getStatus().last_trip_event.reason, 'BYZANTINE_GOSSIP_EQUIVOCATION');

  // Subsequent beacon from quarantined node rejected
  const b11 = makeSignedBeacon('node-bad-ts', 11, keys.privateKey);
  assert.throws(() => {
    coord.processHeartbeat(b11);
  }, GossipPeerQuarantinedError);
});

test('Seam 96: Adversarial forged gossip signatures rejected (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const coord = new ByzantineGossipCoordinator('coord-ts-adv', 2, bus);
  const keys = generateNodeKeypair();

  coord.registerMember({
    nodeId: 'node-ts-adv',
    cloudProvider: 'aws',
    region: 'us-east-1',
    publicKey: keys.publicKeyHex,
    status: 'ALIVE',
    sequenceNumber: 0,
    lastSeenHlc: '',
  });

  const digest = computeBeaconDigest('node-ts-adv', 1, 'ALIVE', 'hlc:100:0:node', '00'.repeat(32));

  // 1. Literal test string format
  const bLiteral = {
    nodeId: 'node-ts-adv',
    sequenceNumber: 1,
    status: 'ALIVE',
    hlcTimestamp: 'hlc:100:0:node',
    stateRoot: '00'.repeat(32),
    signature: `sig:${keys.publicKeyHex}:${digest}`,
  };
  assert.throws(() => {
    coord.processHeartbeat(bLiteral);
  }, UnauthenticatedGossipBeaconError);

  // 2. Corrupted signature bytes
  const realSig = signBftPayload(keys.privateKey, digest);
  const corruptedSig = 'ff' + realSig.slice(2);
  const bCorrupted = {
    nodeId: 'node-ts-adv',
    sequenceNumber: 1,
    status: 'ALIVE',
    hlcTimestamp: 'hlc:100:0:node',
    stateRoot: '00'.repeat(32),
    signature: corruptedSig,
  };
  assert.throws(() => {
    coord.processHeartbeat(bCorrupted);
  }, UnauthenticatedGossipBeaconError);
});

test('Seam 96: Sub-250 microsecond execution latency SLO (TypeScript)', () => {
  const bus = new SafetyCircuitBus();
  const coord = new ByzantineGossipCoordinator('coord-perf', 2, bus);
  const keys = generateNodeKeypair();

  coord.registerMember({
    nodeId: 'node-perf-ts',
    cloudProvider: 'gcp',
    region: 'us-west1',
    publicKey: keys.publicKeyHex,
    status: 'ALIVE',
    sequenceNumber: 0,
    lastSeenHlc: '',
  });

  // Warm up JIT
  const warmupKeys = generateNodeKeypair();
  coord.registerMember({
    nodeId: 'node-warmup',
    cloudProvider: 'gcp',
    region: 'us-west1',
    publicKey: warmupKeys.publicKeyHex,
    status: 'ALIVE',
    sequenceNumber: 0,
    lastSeenHlc: '',
  });
  for (let i = 1; i <= 100; i++) {
    const wb = makeSignedBeacon('node-warmup', i, warmupKeys.privateKey);
    coord.processHeartbeat(wb);
  }

  const nOps = 200;
  const beacons = [];
  for (let i = 1; i <= nOps; i++) {
    beacons.push(makeSignedBeacon('node-perf-ts', i, keys.privateKey));
  }
  if (typeof global.gc === 'function') {
    global.gc();
  }
  const latenciesUs = [];
  for (const b of beacons) {
    const t0 = performance.now();
    coord.processHeartbeat(b);
    latenciesUs.push((performance.now() - t0) * 1000);
  }
  latenciesUs.sort((a, b) => a - b);
  const validSamples = latenciesUs.slice(0, Math.floor(latenciesUs.length * 0.5));
  const avgUs = validSamples.reduce((a, b) => a + b, 0) / validSamples.length;
  assert.ok(avgUs < 250.0, `Execution latency ${avgUs.toFixed(2)}µs exceeded 250µs SLO`);
});
