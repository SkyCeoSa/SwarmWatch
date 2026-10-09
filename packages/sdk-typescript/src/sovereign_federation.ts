/**
 * SwarmWatch Multi-Region Quorum State Synchronization & Sovereign Cluster Federation (Seam 92 / ADR 0103).
 *
 * Implements sovereign cluster identity isolation (MRQ1), two-tier federated quorum certification (MRQ2),
 * causal epoch monotonicity with gap-buffering (MRQ3), and interactive equivocation challenge with
 * automated quarantine and fail-closed safety circuit latching (MRQ4).
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";
import { SafetyCircuitBus } from "./circuit_bus.ts";
import { HybridLogicalClock } from "./hlc.ts";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class SovereignFederationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SovereignFederationError";
  }
}

export class StaleFederationEpochError extends SovereignFederationError {
  constructor(message: string) {
    super(message);
    this.name = "StaleFederationEpochError";
  }
}

export class InsufficientRegionalQuorumError extends SovereignFederationError {
  constructor(message: string) {
    super(message);
    this.name = "InsufficientRegionalQuorumError";
  }
}

export class InsufficientFederatedClustersError extends SovereignFederationError {
  constructor(message: string) {
    super(message);
    this.name = "InsufficientFederatedClustersError";
  }
}

export class SovereignClusterQuarantinedError extends SovereignFederationError {
  constructor(message: string) {
    super(message);
    this.name = "SovereClusterQuarantinedError";
  }
}

export class EquivocationDetectedError extends SovereignFederationError {
  constructor(message: string) {
    super(message);
    this.name = "EquivocationDetectedError";
  }
}

// ==============================================================================
// 2. Domain Models & Cryptographic Helpers
// ==============================================================================

export interface SovereignCluster {
  cluster_id: string;
  region: string;
  public_keys: string[];
  committee_root: string;
  is_active: boolean;
  is_quarantined: boolean;
}

export interface RegionalStateCommitment {
  cluster_id: string;
  epoch: number;
  hlc_timestamp: string;
  state_digest: string;
  signatures: Record<string, string>; // public_key_hex -> ed25519_signature_hex
}

export interface FederatedQuorumCertificate {
  federated_qc_id: string;
  epoch: number;
  global_state_root: string;
  participating_clusters: string[];
  regional_commitments: Record<string, RegionalStateCommitment>;
  created_at_hlc: string;
}

export interface SovereignSlashingProof {
  proof_id: string;
  offending_cluster_id: string;
  epoch: number;
  commitment_a_hash: string;
  commitment_b_hash: string;
  evidence: string;
  created_at_ns: number;
}

export function computeClusterCommitteeRoot(publicKeys: string[]): string {
  const sortedKeys = [...publicKeys].sort();
  const raw = sortedKeys.join(":");
  return crypto.createHash("sha256").update(`sw:sovereign:root:v1:${raw}`, "utf8").digest("hex");
}

export function computeCommitmentDigest(
  clusterId: string,
  epoch: number,
  hlcTimestamp: string,
  stateDigest: string
): Buffer {
  const raw = `sw:sovereign:commit:v1:${clusterId.trim()}:${epoch}:${hlcTimestamp.trim()}:${stateDigest.trim()}`;
  return crypto.createHash("sha256").update(raw, "utf8").digest();
}

export function verifyEd25519Signature(
  publicKeyHex: string,
  signatureHex: string,
  messageDigest: Buffer
): boolean {
  try {
    const rawKey = Buffer.from(publicKeyHex, "hex");
    const sig = Buffer.from(signatureHex, "hex");

    // Standard Ed25519 SPKI DER prefix: 302a300506032b6570032100 (12 bytes)
    const spkiPrefix = Buffer.from("302a300506032b6570032100", "hex");
    const spkiDer = Buffer.concat([spkiPrefix, rawKey]);

    const keyObj = crypto.createPublicKey({
      key: spkiDer,
      format: "der",
      type: "spki",
    });

    return crypto.verify(null, messageDigest, keyObj, sig);
  } catch {
    return false;
  }
}

// ==============================================================================
// 3. Sovereign Federation Coordinator Deep Module
// ==============================================================================

export class SovereignFederationCoordinator {
  public local_cluster_id: string;
  public current_epoch: number;
  public circuit_bus?: SafetyCircuitBus;

  private _hlc: HybridLogicalClock;
  private _clusters: Map<string, SovereignCluster> = new Map();
  private _commitments: Map<number, Map<string, RegionalStateCommitment>> = new Map();
  private _certificates: Map<number, FederatedQuorumCertificate> = new Map();
  private _slashing_proofs: Map<string, SovereignSlashingProof> = new Map();

  constructor(
    localClusterId: string = "cluster-local",
    currentEpoch: number = 1,
    circuitBus?: SafetyCircuitBus
  ) {
    this.local_cluster_id = localClusterId;
    this.current_epoch = currentEpoch;
    this.circuit_bus = circuitBus;
    this._hlc = new HybridLogicalClock(`fed-${localClusterId}`);
  }

  bindCircuitBus(circuitBus: SafetyCircuitBus): void {
    this.circuit_bus = circuitBus;
  }

  registerCluster(cluster: SovereignCluster): void {
    this._clusters.set(cluster.cluster_id, cluster);
  }

  getCluster(clusterId: string): SovereignCluster | undefined {
    return this._clusters.get(clusterId);
  }

  proposeRegionalState(commitment: RegionalStateCommitment): void {
    // 1. Cluster Validation & Quarantine Check (MRQ1)
    const cluster = this._clusters.get(commitment.cluster_id);
    if (!cluster) {
      throw new SovereignFederationError(`Cluster '${commitment.cluster_id}' is not registered in federation.`);
    }

    if (cluster.is_quarantined) {
      throw new SovereignClusterQuarantinedError(
        `Cluster '${commitment.cluster_id}' is quarantined due to prior equivocation.`
      );
    }

    // 2. Circuit Bus Health Check
    if (this.circuit_bus && this.circuit_bus.isTripped()) {
      throw new SovereignFederationError("Federation rejected: Safety circuit bus is currently TRIPPED.");
    }

    // 3. Epoch Monotonicity Check (MRQ3)
    if (commitment.epoch < this.current_epoch) {
      throw new StaleFederationEpochError(
        `Commitment epoch ${commitment.epoch} is stale (current epoch: ${this.current_epoch}).`
      );
    }

    // 4. Equivocation / Idempotence Fast-Path Check (MRQ4)
    if (!this._commitments.has(commitment.epoch)) {
      this._commitments.set(commitment.epoch, new Map());
    }

    const epochCommits = this._commitments.get(commitment.epoch)!;
    if (epochCommits.has(commitment.cluster_id)) {
      const existing = epochCommits.get(commitment.cluster_id)!;
      if (existing.state_digest !== commitment.state_digest) {
        const proof = this.challengeEquivocation(existing, commitment);
        throw new EquivocationDetectedError(
          `Equivocation detected for cluster '${commitment.cluster_id}' at epoch ${commitment.epoch}. Slashing proof: ${proof.proof_id}.`
        );
      }
      return;
    }

    // 5. Intra-Cluster Threshold Quorum Verification (MRQ2)
    const nK = cluster.public_keys.length;
    if (nK === 0) {
      throw new InsufficientRegionalQuorumError(`Cluster '${commitment.cluster_id}' has zero registered keys.`);
    }

    const fK = Math.floor((nK - 1) / 3);
    const qK = Math.ceil((nK + fK + 1) / 2);

    const messageDigest = computeCommitmentDigest(
      commitment.cluster_id,
      commitment.epoch,
      commitment.hlc_timestamp,
      commitment.state_digest
    );

    let validSignaturesCount = 0;
    const knownKeys = new Set(cluster.public_keys);
    for (const [pubHex, sigHex] of Object.entries(commitment.signatures)) {
      if (knownKeys.has(pubHex)) {
        if (verifyEd25519Signature(pubHex, sigHex, messageDigest)) {
          validSignaturesCount += 1;
        }
      }
    }

    if (validSignaturesCount < qK) {
      throw new InsufficientRegionalQuorumError(
        `Commitment from '${commitment.cluster_id}' carries ${validSignaturesCount} valid signatures, requiring threshold ${qK}.`
      );
    }

    epochCommits.set(commitment.cluster_id, { ...commitment });
  }

  synthesizeFederatedQc(epoch: number): FederatedQuorumCertificate {
    const activeClusters = Array.from(this._clusters.values()).filter(
      (c) => c.is_active && !c.is_quarantined
    );
    const k = activeClusters.length;
    if (k === 0) {
      throw new InsufficientFederatedClustersError("Zero active sovereign clusters available in federation.");
    }

    const requiredSupermajority = Math.floor((2 * k) / 3) + 1;

    const epochCommits = this._commitments.get(epoch) || new Map();
    const participatingCommits: Record<string, RegionalStateCommitment> = {};

    for (const [cid, commit] of epochCommits.entries()) {
      const cl = this._clusters.get(cid);
      if (cl && cl.is_active && !cl.is_quarantined) {
        participatingCommits[cid] = commit;
      }
    }

    const sortedCids = Object.keys(participatingCommits).sort();
    const m = sortedCids.length;

    if (m < requiredSupermajority) {
      throw new InsufficientFederatedClustersError(
        `Epoch ${epoch} has ${m} participating clusters, requiring super-majority ${requiredSupermajority} of ${k}.`
      );
    }

    // Compute canonical global state root
    const rootMaterial = sortedCids.map((cid) => `${cid}:${participatingCommits[cid].state_digest}`).join(":");
    const globalStateRoot = crypto
      .createHash("sha256")
      .update(`sw:fedroot:v1:${epoch}:${rootMaterial}`, "utf8")
      .digest("hex");

    const qcMaterial = `${epoch}:${globalStateRoot}:${sortedCids.join(":")}`;
    const federatedQcId = `sw:fedqc:v1:${crypto
      .createHash("sha256")
      .update(qcMaterial, "utf8")
      .digest("hex")}`;

    const createdHlc = this._hlc.now().toString();
    const qc: FederatedQuorumCertificate = {
      federated_qc_id: federatedQcId,
      epoch,
      global_state_root: globalStateRoot,
      participating_clusters: sortedCids,
      regional_commitments: participatingCommits,
      created_at_hlc: createdHlc,
    };

    this._certificates.set(epoch, qc);
    if (epoch >= this.current_epoch) {
      this.current_epoch = epoch + 1;
    }

    return qc;
  }

  verifyFederatedQc(qc: FederatedQuorumCertificate): boolean {
    const activeClusters = Array.from(this._clusters.values()).filter(
      (c) => c.is_active && !c.is_quarantined
    );
    const k = activeClusters.length;
    const requiredM = k > 0 ? Math.floor((2 * k) / 3) + 1 : 1;

    if (qc.participating_clusters.length < requiredM) {
      return false;
    }

    for (const cid of qc.participating_clusters) {
      const cluster = this._clusters.get(cid);
      if (!cluster || cluster.is_quarantined) {
        return false;
      }

      const commit = qc.regional_commitments[cid];
      if (!commit || commit.epoch !== qc.epoch) {
        return false;
      }

      const nK = cluster.public_keys.length;
      const fK = Math.floor((nK - 1) / 3);
      const qK = Math.ceil((nK + fK + 1) / 2);

      const digest = computeCommitmentDigest(
        cid,
        commit.epoch,
        commit.hlc_timestamp,
        commit.state_digest
      );

      let validSigs = 0;
      const knownKeys = new Set(cluster.public_keys);
      for (const [pub, sig] of Object.entries(commit.signatures)) {
        if (knownKeys.has(pub) && verifyEd25519Signature(pub, sig, digest)) {
          validSigs += 1;
        }
      }

      if (validSigs < qK) {
        return false;
      }
    }

    // Check global state root
    const sortedCids = [...qc.participating_clusters].sort();
    const rootMaterial = sortedCids.map((cid) => `${cid}:${qc.regional_commitments[cid].state_digest}`).join(":");
    const expectedRoot = crypto
      .createHash("sha256")
      .update(`sw:fedroot:v1:${qc.epoch}:${rootMaterial}`, "utf8")
      .digest("hex");

    if (expectedRoot.length !== qc.global_state_root.length) {
      return false;
    }
    return crypto.timingSafeEqual(Buffer.from(expectedRoot), Buffer.from(qc.global_state_root));
  }

  challengeEquivocation(
    commitmentA: RegionalStateCommitment,
    commitmentB: RegionalStateCommitment
  ): SovereignSlashingProof {
    if (commitmentA.cluster_id !== commitmentB.cluster_id) {
      throw new SovereignFederationError("Commitments must originate from the same cluster to prove equivocation.");
    }

    if (commitmentA.epoch !== commitmentB.epoch) {
      throw new SovereignFederationError("Commitments must belong to the same epoch to prove equivocation.");
    }

    if (commitmentA.state_digest === commitmentB.state_digest) {
      throw new SovereignFederationError("Commitments must have conflicting digests to prove equivocation.");
    }

    const cid = commitmentA.cluster_id;
    const cluster = this._clusters.get(cid);
    if (!cluster) {
      throw new SovereignFederationError(`Cluster '${cid}' not registered.`);
    }

    const hashA = crypto
      .createHash("sha256")
      .update(
        JSON.stringify({
          cluster_id: commitmentA.cluster_id,
          epoch: commitmentA.epoch,
          state_digest: commitmentA.state_digest,
          signatures: commitmentA.signatures,
        }),
        "utf8"
      )
      .digest("hex");

    const hashB = crypto
      .createHash("sha256")
      .update(
        JSON.stringify({
          cluster_id: commitmentB.cluster_id,
          epoch: commitmentB.epoch,
          state_digest: commitmentB.state_digest,
          signatures: commitmentB.signatures,
        }),
        "utf8"
      )
      .digest("hex");

    const proofId = `sw:slash:v1:${crypto
      .createHash("sha256")
      .update(`${cid}:${commitmentA.epoch}:${hashA}:${hashB}`, "utf8")
      .digest("hex")}`;

    const proof: SovereignSlashingProof = {
      proof_id: proofId,
      offending_cluster_id: cid,
      epoch: commitmentA.epoch,
      commitment_a_hash: hashA,
      commitment_b_hash: hashB,
      evidence: `Conflicting state digests at epoch ${commitmentA.epoch}: '${commitmentA.state_digest}' vs '${commitmentB.state_digest}'`,
      created_at_ns: Date.now() * 1_000_000,
    };

    // Quarantine cluster permanently
    cluster.is_quarantined = true;
    cluster.is_active = false;
    this._slashing_proofs.set(proofId, proof);

    // Trip Safety Circuit Bus fail-closed
    if (this.circuit_bus) {
      this.circuit_bus.trip(
        "SOVEREIGN_EQUIVOCATION_DETECTED",
        `federation:${cid}`,
        {
          slashing_proof_id: proofId,
          cluster_id: cid,
          epoch: commitmentA.epoch,
        }
      );
    }

    return proof;
  }

  getFederationStatus(): Record<string, unknown> {
    const clustersInfo = Array.from(this._clusters.values()).map((c) => ({
      cluster_id: c.cluster_id,
      region: c.region,
      key_count: c.public_keys.length,
      is_active: c.is_active,
      is_quarantined: c.is_quarantined,
    }));

    return {
      local_cluster_id: this.local_cluster_id,
      current_epoch: this.current_epoch,
      cluster_count: this._clusters.size,
      quarantined_count: Array.from(this._clusters.values()).filter((c) => c.is_quarantined).length,
      certificates_count: this._certificates.size,
      slashing_proofs_count: this._slashing_proofs.size,
      clusters: clustersInfo,
    };
  }

  getStatus(): Record<string, unknown> {
    return this.getFederationStatus();
  }
}

export const globalSovereignFederation: SovereignFederationCoordinator = new SovereignFederationCoordinator();
export const globalSovereignCoordinator: SovereignFederationCoordinator = globalSovereignFederation;
export { EquivocationDetectedError as SovereignEquivocationDetectedError };
