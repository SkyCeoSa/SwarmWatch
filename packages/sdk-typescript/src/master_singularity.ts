/**
 * SwarmWatch Global Master Singularity & Autonomous Self-Governing Sovereign Registry (Seam 100 / ADR 0111)
 *
 * Provides the global sovereign root anchor for the entire federated multi-agent universe:
 * - Invariant SNG1: Monotonic Singularity Epoch & Sovereign State Tree Evolution (E_{k+1} > E_k)
 * - Invariant SNG2: Byzantine Constitutional Quorum Ratification (Q >= floor(2K/3) + 1)
 * - Invariant SNG3: Autonomous Universal Proof-of-Solvency & Invariant Convergence
 * - Invariant SNG4: Universal Fail-Closed Circuit Latching & Emergency Quarantine (SINGULARITY_GLOBAL_HALT)
 *
 * Zero-token cost guarantee ($0.00 USD), sub-250 microsecond execution latency SLO.
 */

import crypto, { createHash, timingSafeEqual, createHmac } from 'node:crypto';
import { SafetyCircuitBus, globalSafetyCircuitBus } from './circuit_bus.ts';
import { HybridLogicalClock } from './hlc.ts';
import { generateNodeKeypair, signBftPayload, verifyBftSignature } from './byzantine_oracle.ts';

export class SingularityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SingularityError';
  }
}

export class GovernorKeyOverwriteError extends SingularityError {
  constructor(message: string) {
    super(message);
    this.name = 'GovernorKeyOverwriteError';
  }
}

export class StaleSingularityEpochError extends SingularityError {
  constructor(message: string) {
    super(message);
    this.name = 'StaleSingularityEpochError';
  }
}

export class InsufficientGovernanceQuorumError extends SingularityError {
  constructor(message: string) {
    super(message);
    this.name = 'InsufficientGovernanceQuorumError';
  }
}

export class SingularitySolvencyBreachError extends SingularityError {
  constructor(message: string) {
    super(message);
    this.name = 'SingularitySolvencyBreachError';
  }
}

export class SingularityQuarantinedError extends SingularityError {
  constructor(message: string) {
    super(message);
    this.name = 'SingularityQuarantinedError';
  }
}

export class SingularityCircuitTrippedError extends SingularityError {
  constructor(message: string) {
    super(message);
    this.name = 'SingularityCircuitTrippedError';
  }
}

export interface SingularityGovernor {
  governorId: string;
  publicKeyHex: string;
  votingWeight?: number;
  isActive?: boolean;
  isQuarantined?: boolean;
}

export interface ConstitutionalProposal {
  proposalId: string;
  proposerId: string;
  title: string;
  constitutionPayload: Record<string, any>;
  payloadDigest: string;
  targetEpoch: number;
  status?: string; // 'PROPOSED' | 'RATIFIED' | 'REJECTED' | 'APPLIED'
  createdAtNs?: number;
  ratifiedAtNs?: number | null;
  signatures?: Record<string, string>;
}

export interface SingularityStateCommitment {
  epoch: number;
  prevStateRoot: string;
  meshRelayRoot: string;
  federationRoot: string;
  recursiveSnarkRoot: string;
  boundaryFenceRoot: string;
  stateDigest: string;
  totalStakedSolvency: number;
  committedAtNs: number;
}

export interface SingularityCertificate {
  certificateId: string;
  epoch: number;
  stateDigest: string;
  ratifiedProposalId: string | null;
  governorQuorumCount: number;
  governorSignatures: Record<string, string>;
  certifiedAtNs: number;
  masterSingularitySignature: string;
}

export interface SingularityStatus {
  singularityId: string;
  currentEpoch: number;
  currentStateRoot: string;
  totalGovernorsCount: number;
  activeGovernorsCount: number;
  totalProposalsCount: number;
  ratifiedProposalsCount: number;
  totalCertificatesIssued: number;
  isQuarantined: boolean;
  circuitTripped: boolean;
}

export function computeSingularityDigest(
  epoch: number,
  prevRoot: string,
  meshRoot: string,
  fedRoot: string,
  snarkRoot: string,
  fenceRoot: string,
): string {
  const raw = `${epoch}:${prevRoot}:${meshRoot}:${fedRoot}:${snarkRoot}:${fenceRoot}`;
  return createHash('sha256').update(raw).digest('hex');
}

export function computeProposalDigest(
  proposalId: string,
  proposerId: string,
  targetEpoch: number,
  payloadObj: Record<string, any>,
): string {
  const sortedKeys = Object.keys(payloadObj).sort();
  const sortedObj: Record<string, any> = {};
  for (const k of sortedKeys) {
    sortedObj[k] = payloadObj[k];
  }
  const payloadCanonical = JSON.stringify(sortedObj);
  const raw = `${proposalId}:${proposerId}:${targetEpoch}:${payloadCanonical}`;
  return createHash('sha256').update(raw).digest('hex');
}

export function verifyGovernorSignature(
  publicKeyHex: string,
  digest: string,
  signatureHex: string,
): boolean {
  if (!publicKeyHex || !signatureHex || !digest) {
    return false;
  }
  return verifyBftSignature(publicKeyHex, digest, signatureHex);
}

export function verifySingularityCertificate(
  publicKeyHex: string,
  certificate: SingularityCertificate,
): boolean {
  if (!publicKeyHex || !certificate || !certificate.masterSingularitySignature) {
    return false;
  }
  const expectedMsg = `${certificate.certificateId}:${certificate.stateDigest}`;
  return verifyBftSignature(publicKeyHex, expectedMsg, certificate.masterSingularitySignature);
}

export class MasterSingularityCoordinator {
  public readonly singularityId: string;
  public readonly minSolvencyThreshold: number;
  public circuitBus?: SafetyCircuitBus;
  public clock: HybridLogicalClock;

  public currentEpoch: number = 0;
  public currentStateRoot: string = '0'.repeat(64);
  public isQuarantined: boolean = false;
  public activeLockdownId: string | null = null;
  public activeLockdownNonce: number = 0;
  private _consumedRecoveryFingerprints: Set<string> = new Set();
  private _privateKeyHex: string;
  public publicKeyHex: string;

  private _governors: Map<string, SingularityGovernor> = new Map();
  private _proposals: Map<string, ConstitutionalProposal> = new Map();
  private _commitments: Map<number, SingularityStateCommitment> = new Map();
  private _certificates: Map<number, SingularityCertificate> = new Map();

  constructor(options?: {
    singularityId?: string;
    minSolvencyThreshold?: number;
    circuitBus?: SafetyCircuitBus;
  }) {
    this.singularityId = options?.singularityId ?? 'singularity-master-root';
    this.minSolvencyThreshold = options?.minSolvencyThreshold ?? 1_000_000;
    this.circuitBus = options?.circuitBus ?? globalSafetyCircuitBus;
    this.clock = new HybridLogicalClock('master-singularity-root');

    const kp = generateNodeKeypair();
    this._privateKeyHex = kp.privateKey;
    this.publicKeyHex = kp.publicKeyHex;
  }

  public reset(): void {
    this.currentEpoch = 0;
    this.currentStateRoot = '0'.repeat(64);
    this.isQuarantined = false;
    this.activeLockdownId = null;
    this.activeLockdownNonce = 0;
    this._consumedRecoveryFingerprints.clear();
    this._governors.clear();
    this._proposals.clear();
    this._commitments.clear();
    this._certificates.clear();
  }

  public registerGovernor(governor: SingularityGovernor): void {
    this._checkSafety();
    if (!governor.publicKeyHex || governor.publicKeyHex.length !== 64) {
      throw new SingularityError('Governor publicKeyHex must be a valid 64-character hex Ed25519 public key.');
    }
    if (!/^[0-9a-fA-F]{64}$/.test(governor.publicKeyHex)) {
      throw new SingularityError('Governor publicKeyHex must be valid hexadecimal.');
    }
    if (typeof governor.votingWeight === 'number' && (governor.votingWeight < 1 || !Number.isInteger(governor.votingWeight))) {
      throw new SingularityError(`Governor votingWeight must be a positive integer >= 1, got ${governor.votingWeight}.`);
    }
    if (this._governors.has(governor.governorId)) {
      throw new GovernorKeyOverwriteError(`Governor '${governor.governorId}' already registered. Overwriting governor key or weight is strictly forbidden.`);
    }
    if (this.currentEpoch > 0 && this._governors.size > 0 && !this._governors.has(governor.governorId)) {
      throw new SingularityError('Cannot register new governor during active epoch without constitutional ratification.');
    }
    this._governors.set(governor.governorId, {
      ...governor,
      votingWeight: governor.votingWeight ?? 1,
      isActive: governor.isActive ?? true,
      isQuarantined: governor.isQuarantined ?? false,
    });
  }

  public submitProposal(
    proposalId: string,
    proposerId: string,
    title: string,
    constitutionPayload: Record<string, any>,
    targetEpoch: number,
  ): ConstitutionalProposal {
    this._checkSafety();
    if (this._proposals.has(proposalId)) {
      throw new SingularityError(`Proposal '${proposalId}' already exists and cannot be overwritten.`);
    }
    if (targetEpoch <= this.currentEpoch) {
      raiseStaleEpoch(`Target epoch ${targetEpoch} must be greater than current epoch ${this.currentEpoch} (SNG1).`);
    }

    const pDigest = computeProposalDigest(proposalId, proposerId, targetEpoch, constitutionPayload);
    const proposal: ConstitutionalProposal = {
      proposalId,
      proposerId,
      title,
      constitutionPayload,
      payloadDigest: pDigest,
      targetEpoch,
      status: 'PROPOSED',
      createdAtNs: Number(process.hrtime.bigint()),
      ratifiedAtNs: null,
      signatures: {},
    };
    this._proposals.set(proposalId, proposal);
    return proposal;
  }

  public voteProposal(proposalId: string, governorId: string, signatureHex: string): boolean {
    this._checkSafety();
    const proposal = this._proposals.get(proposalId);
    if (!proposal) {
      return false;
    }

    const governor = this._governors.get(governorId);
    if (!governor || governor.isActive === false || governor.isQuarantined === true) {
      return false;
    }

    if (!verifyGovernorSignature(governor.publicKeyHex, proposal.payloadDigest, signatureHex)) {
      return false;
    }

    if (!proposal.signatures) {
      proposal.signatures = {};
    }
    proposal.signatures[governorId] = signatureHex;
    return true;
  }

  public ratifyProposal(proposalId: string): ConstitutionalProposal {
    this._checkSafety();
    const proposal = this._proposals.get(proposalId);
    if (!proposal) {
      throw new InsufficientGovernanceQuorumError(`Proposal '${proposalId}' not found.`);
    }

    const activeGovernors = Array.from(this._governors.values()).filter(
      (g) => g.isActive !== false && g.isQuarantined !== true,
    );
    const totalK = activeGovernors.length;
    if (totalK === 0) {
      throw new InsufficientGovernanceQuorumError('Zero active sovereign governors registered.');
    }

    const totalWeight = activeGovernors.reduce((acc, g) => acc + (g.votingWeight ?? 1), 0);
    const requiredWeight = Math.floor((2 * totalWeight) / 3) + 1;
    let votedWeight = 0;
    for (const gid of Object.keys(proposal.signatures ?? {})) {
      const gov = this._governors.get(gid);
      if (gov && gov.isActive !== false && gov.isQuarantined !== true) {
        votedWeight += (gov.votingWeight ?? 1);
      }
    }

    if (votedWeight < requiredWeight) {
      throw new InsufficientGovernanceQuorumError(
        `Ratification requires >= ${requiredWeight} weighted governor votes, got ${votedWeight} (SNG2).`,
      );
    }

    proposal.status = 'RATIFIED';
    proposal.ratifiedAtNs = Number(process.hrtime.bigint());
    return proposal;
  }

  public commitSubsystemState(
    epoch: number,
    meshRoot: string,
    fedRoot: string,
    snarkRoot: string,
    fenceRoot: string,
    totalStakedSolvency: number,
    breachCount: number = 0,
  ): SingularityStateCommitment {
    this._checkSafety();
    if (epoch !== this.currentEpoch + 1) {
      throw new StaleSingularityEpochError(
        `Epoch transition must advance monotonically from ${this.currentEpoch} to ${this.currentEpoch + 1}, got ${epoch} (SNG1).`,
      );
    }

    if (totalStakedSolvency < this.minSolvencyThreshold) {
      throw new SingularitySolvencyBreachError(
        `Aggregate solvency ${totalStakedSolvency} falls below minimum threshold ${this.minSolvencyThreshold} (SNG3).`,
      );
    }

    if (breachCount > 0) {
      throw new SingularitySolvencyBreachError(
        `Active subsystem breaches detected (${breachCount} breaches) - invariant convergence failed (SNG3).`,
      );
    }

    const stateDigest = computeSingularityDigest(
      epoch,
      this.currentStateRoot,
      meshRoot,
      fedRoot,
      snarkRoot,
      fenceRoot,
    );

    const commitment: SingularityStateCommitment = {
      epoch,
      prevStateRoot: this.currentStateRoot,
      meshRelayRoot: meshRoot,
      federationRoot: fedRoot,
      recursiveSnarkRoot: snarkRoot,
      boundaryFenceRoot: fenceRoot,
      stateDigest,
      totalStakedSolvency,
      committedAtNs: Number(process.hrtime.bigint()),
    };

    this._commitments.set(epoch, commitment);
    this.currentEpoch = epoch;
    this.currentStateRoot = stateDigest;
    return commitment;
  }

  public issueSingularityCertificate(epoch: number, proposalId?: string): SingularityCertificate {
    this._checkSafety();
    const commitment = this._commitments.get(epoch);
    if (!commitment) {
      throw new StaleSingularityEpochError(`No state commitment registered for epoch ${epoch}.`);
    }

    let ratifiedPid: string | null = null;
    let governorSigs: Record<string, string> = {};

    if (proposalId) {
      const proposal = this._proposals.get(proposalId);
      if (!proposal || proposal.status !== 'RATIFIED') {
        throw new InsufficientGovernanceQuorumError(`Proposal '${proposalId}' is not ratified.`);
      }
      ratifiedPid = proposal.proposalId;
      governorSigs = { ...(proposal.signatures ?? {}) };
    }

    const certId = `sng-cert-${epoch}-${commitment.stateDigest.slice(0, 12)}`;
    const msSig = signBftPayload(this._privateKeyHex, `${certId}:${commitment.stateDigest}`);

    const certificate: SingularityCertificate = {
      certificateId: certId,
      epoch,
      stateDigest: commitment.stateDigest,
      ratifiedProposalId: ratifiedPid,
      governorQuorumCount: Object.keys(governorSigs).length,
      governorSignatures: governorSigs,
      certifiedAtNs: Number(process.hrtime.bigint()),
      masterSingularitySignature: msSig,
    };

    this._certificates.set(epoch, certificate);
    return certificate;
  }

  public verifySingularityCertificate(cert: SingularityCertificate): boolean {
    return verifyBftSignature(this.publicKeyHex, `${cert.certificateId}:${cert.stateDigest}`, cert.masterSingularitySignature);
  }

  public emergencyHalt(reason: string = 'GLOBAL_EMERGENCY_HALT'): string {
    this.isQuarantined = true;
    this.activeLockdownNonce += 1;
    this.activeLockdownId = `halt-${this.currentEpoch}-${this.activeLockdownNonce}`;
    if (this.circuitBus) {
      this.circuitBus.trip('SINGULARITY_GLOBAL_HALT', this.singularityId, {
        reason,
        quarantinedEpoch: this.currentEpoch,
        lockdownId: this.activeLockdownId,
      });
    }
    return this.activeLockdownId;
  }

  public getRecoveryDigest(): string {
    const raw = `RECOVERY:${this.singularityId}:${this.currentEpoch}:${this.activeLockdownId ?? 0}`;
    return createHash('sha256').update(raw).digest('hex');
  }

  public executeRecoveryCeremony(governorSignatures: Record<string, string>, lockdownId?: string): boolean {
    if (!this.isQuarantined) {
      return false;
    }
    const sigEntries = Object.entries(governorSignatures);
    if (sigEntries.length === 0) {
      return false;
    }

    const sortedSigVals = Object.values(governorSignatures).sort().join(':');
    const sigFingerprint = createHash('sha256').update(sortedSigVals).digest('hex');
    if (this._consumedRecoveryFingerprints.has(sigFingerprint)) {
      return false; // Replay detected
    }

    const activeGovernors = Array.from(this._governors.values()).filter(
      (g) => g.isActive !== false && g.isQuarantined !== true,
    );
    const totalK = activeGovernors.length;
    if (totalK === 0) {
      return false;
    }

    const totalWeight = activeGovernors.reduce((acc, g) => acc + (g.votingWeight ?? 1), 0);
    if (totalWeight <= 0) {
      return false;
    }
    const requiredWeight = Math.floor((2 * totalWeight) / 3) + 1;
    let votedWeight = 0;

    const recoveryDigest = this.getRecoveryDigest();

    for (const [gid, sig] of sigEntries) {
      const gov = this._governors.get(gid);
      if (gov && gov.isActive !== false && gov.isQuarantined !== true) {
        if (verifyGovernorSignature(gov.publicKeyHex, recoveryDigest, sig)) {
          votedWeight += (gov.votingWeight ?? 1);
        }
      }
    }

    if (votedWeight >= requiredWeight) {
      this.isQuarantined = false;
      this._consumedRecoveryFingerprints.add(sigFingerprint);
      this.activeLockdownId = null;
      if (this.circuitBus && this.circuitBus.isTripped()) {
        this.circuitBus.resetForTest();
      }
      return true;
    }
    return false;
  }

  public getStatus(): SingularityStatus {
    const activeGovernors = Array.from(this._governors.values()).filter(
      (g) => g.isActive !== false && g.isQuarantined !== true,
    ).length;
    const ratifiedProposals = Array.from(this._proposals.values()).filter(
      (p) => p.status === 'RATIFIED',
    ).length;

    return {
      singularityId: this.singularityId,
      currentEpoch: this.currentEpoch,
      currentStateRoot: this.currentStateRoot,
      totalGovernorsCount: this._governors.size,
      activeGovernorsCount: activeGovernors,
      totalProposalsCount: this._proposals.size,
      ratifiedProposalsCount: ratifiedProposals,
      totalCertificatesIssued: this._certificates.size,
      isQuarantined: this.isQuarantined,
      circuitTripped: this.circuitBus ? this.circuitBus.isTripped() : false,
    };
  }

  private _checkSafety(): void {
    if (this.isQuarantined) {
      throw new SingularityQuarantinedError(
        'Master Singularity is in emergency quarantine isolation (SNG4).',
      );
    }
    if (this.circuitBus && this.circuitBus.isTripped()) {
      throw new SingularityCircuitTrippedError(
        'Safety circuit bus is tripped. Singularity operations halted.',
      );
    }
  }
}

function raiseStaleEpoch(msg: string): never {
  throw new StaleSingularityEpochError(msg);
}

// Authoritative Global Singletons
export const globalMasterSingularity = new MasterSingularityCoordinator();
export const globalSingularityCoordinator = globalMasterSingularity;
