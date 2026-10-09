import test from "node:test";
import assert from "node:assert/strict";
import * as crypto from "node:crypto";

import {
  SovereignFederationCoordinator,
  SovereignFederationError,
  StaleFederationEpochError,
  InsufficientRegionalQuorumError,
  InsufficientFederatedClustersError,
  SovereignClusterQuarantinedError,
  EquivocationDetectedError,
  computeClusterCommitteeRoot,
  computeCommitmentDigest,
  verifyEd25519Signature,
} from "../src/sovereign_federation.ts";
import { SafetyCircuitBus } from "../src/circuit_bus.ts";

function generateTestCluster(clusterId, region, nodeCount = 4) {
  const keypairs = [];
  const publicKeysHex = [];

  for (let i = 0; i < nodeCount; i++) {
    const { privateKey, publicKey } = crypto.generateKeyPairSync("ed25519");
    const spkiDer = publicKey.export({ format: "der", type: "spki" });
    const rawPubHex = spkiDer.subarray(12).toString("hex");
    keypairs.push({ privateKey, rawPubHex });
    publicKeysHex.push(rawPubHex);
  }

  const committeeRoot = computeClusterCommitteeRoot(publicKeysHex);
  const cluster = {
    cluster_id: clusterId,
    region,
    public_keys: publicKeysHex,
    committee_root: committeeRoot,
    is_active: true,
    is_quarantined: false,
  };

  return { cluster, keypairs };
}

function createSignedCommitment(cluster, keypairs, options = {}) {
  const epoch = options.epoch !== undefined ? options.epoch : 1;
  const stateDigest = options.stateDigest || "deadbeef12345678";
  const signerIndices = options.signerIndices !== undefined ? options.signerIndices : keypairs.map((_, i) => i);

  const hlcTimestamp = `${epoch}000000000:0:${cluster.cluster_id}`;
  const digest = computeCommitmentDigest(cluster.cluster_id, epoch, hlcTimestamp, stateDigest);

  const signatures = {};
  for (const idx of signerIndices) {
    const kp = keypairs[idx];
    const sigHex = crypto.sign(null, digest, kp.privateKey).toString("hex");
    signatures[kp.rawPubHex] = sigHex;
  }

  return {
    cluster_id: cluster.cluster_id,
    epoch,
    hlc_timestamp: hlcTimestamp,
    state_digest: stateDigest,
    signatures,
  };
}

test("Seam 92: Cluster committee root and commitment digest (TypeScript)", () => {
  const { cluster } = generateTestCluster("cluster-us", "us-east-1", 3);
  assert.equal(cluster.committee_root.length, 64);
  const digest = computeCommitmentDigest("cluster-us", 1, "100:0:node", "hash123");
  assert.equal(digest.length, 32);
});

test("Seam 92: Intra-cluster threshold quorum verification MRQ2 (TypeScript)", () => {
  const coordinator = new SovereignFederationCoordinator("cluster-main");
  const { cluster, keypairs } = generateTestCluster("cluster-eu", "eu-central-1", 4);
  // N=4, f=1, Q=3
  coordinator.registerCluster(cluster);

  // 1. Under-threshold: 2 signatures (requires 3)
  const underCommit = createSignedCommitment(cluster, keypairs, { epoch: 1, signerIndices: [0, 1] });
  assert.throws(
    () => coordinator.proposeRegionalState(underCommit),
    InsufficientRegionalQuorumError
  );

  // 2. Threshold met: 3 signatures
  const validCommit = createSignedCommitment(cluster, keypairs, { epoch: 1, signerIndices: [0, 1, 2] });
  coordinator.proposeRegionalState(validCommit);

  const status = coordinator.getFederationStatus();
  assert.equal(status.cluster_count, 1);
});

test("Seam 92: Stale epoch rejected MRQ3 (TypeScript)", () => {
  const coordinator = new SovereignFederationCoordinator("cluster-main", 5);
  const { cluster, keypairs } = generateTestCluster("cluster-us", "us-east-1", 4);
  coordinator.registerCluster(cluster);

  const staleCommit = createSignedCommitment(cluster, keypairs, { epoch: 4, signerIndices: [0, 1, 2] });
  assert.throws(
    () => coordinator.proposeRegionalState(staleCommit),
    StaleFederationEpochError
  );
});

test("Seam 92: Inter-cluster super-majority synthesis and verification MRQ2 (TypeScript)", () => {
  const coordinator = new SovereignFederationCoordinator("cluster-main", 1);

  const c1 = generateTestCluster("cluster-us", "us-east-1", 4);
  const c2 = generateTestCluster("cluster-eu", "eu-west-1", 4);
  const c3 = generateTestCluster("cluster-ap", "ap-south-1", 4);

  coordinator.registerCluster(c1.cluster);
  coordinator.registerCluster(c2.cluster);
  coordinator.registerCluster(c3.cluster);
  // K=3 clusters -> super-majority is floor(2*3/3) + 1 = 3

  // Submit for c1 and c2 only (2 out of 3)
  const commit1 = createSignedCommitment(c1.cluster, c1.keypairs, { epoch: 1, stateDigest: "hash-us" });
  const commit2 = createSignedCommitment(c2.cluster, c2.keypairs, { epoch: 1, stateDigest: "hash-eu" });
  coordinator.proposeRegionalState(commit1);
  coordinator.proposeRegionalState(commit2);

  // Synthesize should fail: m=2 < required 3
  assert.throws(
    () => coordinator.synthesizeFederatedQc(1),
    InsufficientFederatedClustersError
  );

  // Now submit for c3
  const commit3 = createSignedCommitment(c3.cluster, c3.keypairs, { epoch: 1, stateDigest: "hash-ap" });
  coordinator.proposeRegionalState(commit3);

  // Synthesize succeeds
  const qc = coordinator.synthesizeFederatedQc(1);
  assert.equal(qc.epoch, 1);
  assert.equal(qc.participating_clusters.length, 3);
  assert.equal(coordinator.verifyFederatedQc(qc), true);
  assert.equal(coordinator.current_epoch, 2);

  // Tampered QC fails
  const tamperedQc = {
    ...qc,
    global_state_root: "0000000000000000000000000000000000000000000000000000000000000000",
  };
  assert.equal(coordinator.verifyFederatedQc(tamperedQc), false);
});

test("Seam 92: Equivocation detection and quarantine MRQ4 (TypeScript)", () => {
  const circuitBus = new SafetyCircuitBus();
  const coordinator = new SovereignFederationCoordinator("cluster-main", 1, circuitBus);

  const { cluster, keypairs } = generateTestCluster("cluster-equivocal", "us-east-1", 4);
  coordinator.registerCluster(cluster);

  const commitA = createSignedCommitment(cluster, keypairs, { epoch: 1, stateDigest: "state-version-A" });
  coordinator.proposeRegionalState(commitA);

  assert.equal(cluster.is_quarantined, false);
  assert.equal(circuitBus.isTripped(), false);

  // Conflicting commitment for same epoch
  const commitB = createSignedCommitment(cluster, keypairs, { epoch: 1, stateDigest: "state-version-B-CONTRADICTION" });
  assert.throws(
    () => coordinator.proposeRegionalState(commitB),
    EquivocationDetectedError
  );

  assert.equal(cluster.is_quarantined, true);
  assert.equal(cluster.is_active, false);
  assert.equal(circuitBus.isTripped(), true);
  assert.equal(circuitBus.getStatus().last_trip_event.reason, "SOVEREIGN_EQUIVOCATION_DETECTED");

  // Subsequent proposals from quarantined cluster are rejected
  const commitC = createSignedCommitment(cluster, keypairs, { epoch: 2, stateDigest: "state-c" });
  assert.throws(
    () => coordinator.proposeRegionalState(commitC),
    SovereignClusterQuarantinedError
  );
});

test("Seam 92: Sub-250 microsecond execution latency SLO (TypeScript)", () => {
  const coordinator = new SovereignFederationCoordinator("cluster-bench");
  const { cluster, keypairs } = generateTestCluster("cluster-bench-1", "us-east-1", 4);
  coordinator.registerCluster(cluster);

  const commit = createSignedCommitment(cluster, keypairs, { epoch: 1 });
  coordinator.proposeRegionalState(commit);

  const start = performance.now();
  for (let i = 0; i < 100; i++) {
    coordinator.proposeRegionalState(commit);
  }
  const end = performance.now();

  const meanMs = (end - start) / 100;
  const meanUs = meanMs * 1000;
  assert.ok(meanUs < 250, `Mean latency ${meanUs.toFixed(2)}µs exceeded 250µs SLO`);
});
