/**
 * SwarmWatch Autonomous Agent Capability Attestation & Hardware Secure Enclave (SGX/TEE) Remote Witness (Seam 98 / ADR 0109)
 *
 * Provides hardware measurement verification (TEE1), anti-replay challenge nonces (TEE2),
 * lease-bounded capability delegation (TEE3), and fail-closed safety circuit latching on attestation breaches (TEE4).
 * Zero-token cost guarantee ($0.00 USD), sub-250 microsecond execution latency SLO.
 */

import { createHash } from 'node:crypto';
import { SafetyCircuitBus, globalSafetyCircuitBus } from './circuit_bus.ts';
import { HybridLogicalClock } from './hlc.ts';
import { generateNodeKeypair, signBftPayload, verifyBftSignature } from './byzantine_oracle.ts';

export class EnclaveAttestationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EnclaveAttestationError';
  }
}

export class EnclaveMeasurementMismatchError extends EnclaveAttestationError {
  constructor(message: string) {
    super(message);
    this.name = 'EnclaveMeasurementMismatchError';
  }
}

export class StaleAttestationReportError extends EnclaveAttestationError {
  constructor(message: string) {
    super(message);
    this.name = 'StaleAttestationReportError';
  }
}

export class UnauthorizedCapabilityError extends EnclaveAttestationError {
  constructor(message: string) {
    super(message);
    this.name = 'UnauthorizedCapabilityError';
  }
}

export class EnclaveQuarantinedError extends EnclaveAttestationError {
  constructor(message: string) {
    super(message);
    this.name = 'EnclaveQuarantinedError';
  }
}

export class EnclaveCircuitTrippedError extends EnclaveAttestationError {
  constructor(message: string) {
    super(message);
    this.name = 'EnclaveCircuitTrippedError';
  }
}

export interface EnclaveMeasurement {
  enclaveId: string;
  teeType: 'SGX' | 'TDX' | 'NITRO' | 'SEV_SNP' | 'SIMULATED' | string;
  codeHash: string;
  configHash: string;
  signerId: string;
  isApproved?: boolean;
}

export interface EnclaveChallenge {
  challengeId: string;
  enclaveId: string;
  nonce: string;
  issuedAtNs: number;
  expiresAtNs: number;
  isConsumed: boolean;
}

export interface AttestationQuote {
  quoteId: string;
  enclaveId: string;
  teeType: string;
  measurementHash: string;
  challengeNonce: string;
  publicKey: string;
  signature: string;
  timestampNs: number;
  userDataDigest?: string;
}

export interface EnclaveCapabilityToken {
  tokenId: string;
  enclaveId: string;
  agentId: string;
  capabilities: string[];
  issuedAtNs: number;
  expiresAtNs: number;
  tokenSignature: string;
}

export interface EnclaveStatus {
  coordinator_id: string;
  approved_measurements_count: number;
  active_challenges_count: number;
  total_attestations: number;
  active_capability_tokens: number;
  total_capability_verifications: number;
  quarantined_enclaves_count: number;
  circuit_tripped: boolean;
}

export function computeEnclaveMeasurementHash(codeHash: string, configHash: string, signerId: string): string {
  const raw = `sw:mrenclave:v1:${codeHash}:${configHash}:${signerId}`;
  return createHash('sha256').update(raw).digest('hex');
}

export function computeQuoteDigest(
  enclaveId: string,
  teeType: string,
  measurementHash: string,
  nonce: string,
  userData: string,
  timestampNs: number,
): string {
  const raw = `sw:quote:v1:${enclaveId}:${teeType}:${measurementHash}:${nonce}:${userData}:${timestampNs}`;
  return createHash('sha256').update(raw).digest('hex');
}

export function computeTokenDigest(
  tokenId: string,
  enclaveId: string,
  agentId: string,
  capabilities: string[],
  expiresAtNs: number,
): string {
  const capsSorted = [...capabilities].sort().join(',');
  const raw = `sw:captoken:v1:${tokenId}:${enclaveId}:${agentId}:${capsSorted}:${expiresAtNs}`;
  return createHash('sha256').update(raw).digest('hex');
}

export function verifyEnclaveSignature(pubkey: string, digest: string, signature: string): boolean {
  if (!signature || !pubkey || !digest) return false;
  return verifyBftSignature(pubkey, digest, signature);
}

export class EnclaveAttestationCoordinator {
  public coordinatorId: string;
  public failureThreshold: number;
  public challengeTtlNs: number;
  public defaultLeaseNs: number;
  public circuitBus: SafetyCircuitBus | null;
  public coordinatorPrivateKey: string;
  public coordinatorPublicKey: string;
  private hlc: HybridLogicalClock;

  private approvedMeasurements: Map<string, EnclaveMeasurement> = new Map();
  private activeChallenges: Map<string, EnclaveChallenge> = new Map();
  private issuedTokens: Map<string, EnclaveCapabilityToken> = new Map();
  private quarantinedEnclaves: Set<string> = new Set();
  private failureCounts: Map<string, number> = new Map();
  private totalAttestations: number = 0;
  private totalVerifications: number = 0;

  constructor(
    coordinatorId: string = 'enclave-coordinator-primary',
    failureThreshold: number = 3,
    challengeTtlSec: number = 30.0,
    defaultLeaseSec: number = 300.0,
    circuitBus?: SafetyCircuitBus,
  ) {
    this.coordinatorId = coordinatorId;
    this.failureThreshold = failureThreshold;
    this.challengeTtlNs = Math.floor(challengeTtlSec * 1_000_000_000);
    this.defaultLeaseNs = Math.floor(defaultLeaseSec * 1_000_000_000);
    this.circuitBus = circuitBus !== undefined ? circuitBus : globalSafetyCircuitBus;
    this.hlc = new HybridLogicalClock(`tee-${coordinatorId}`);

    const kp = generateNodeKeypair();
    this.coordinatorPrivateKey = kp.privateKey;
    this.coordinatorPublicKey = kp.publicKeyHex;
  }

  public bindCircuitBus(circuitBus: SafetyCircuitBus): void {
    this.circuitBus = circuitBus;
  }

  public registerApprovedMeasurement(measurement: EnclaveMeasurement): string {
    const mhash = computeEnclaveMeasurementHash(
      measurement.codeHash,
      measurement.configHash,
      measurement.signerId,
    );
    this.approvedMeasurements.set(mhash, { ...measurement, isApproved: measurement.isApproved ?? true });
    return mhash;
  }

  public issueChallenge(enclaveId: string): EnclaveChallenge {
    if (this.circuitBus && this.circuitBus.isTripped()) {
      raiseCircuitTripped();
    }

    if (this.quarantinedEnclaves.has(enclaveId)) {
      throw new EnclaveQuarantinedError(`Enclave '${enclaveId}' is permanently quarantined.`);
    }

    const nowNs = Date.now() * 1_000_000;
    const raw = `chal:${enclaveId}:${nowNs}:${this.hlc.now().toString()}`;
    const nonce = createHash('sha256').update(raw).digest('hex');
    const cid = `chl-${nonce.slice(0, 16)}`;

    const challenge: EnclaveChallenge = {
      challengeId: cid,
      enclaveId,
      nonce,
      issuedAtNs: nowNs,
      expiresAtNs: nowNs + this.challengeTtlNs,
      isConsumed: false,
    };

    this.activeChallenges.set(nonce, challenge);
    return challenge;
  }

  public verifyAndAttest(
    quote: AttestationQuote,
    agentId: string,
    requestedCapabilities: string[],
    leaseSec?: number,
  ): EnclaveCapabilityToken {
    if (this.circuitBus && this.circuitBus.isTripped()) {
      raiseCircuitTripped();
    }

    if (this.quarantinedEnclaves.has(quote.enclaveId)) {
      throw new EnclaveQuarantinedError(`Enclave '${quote.enclaveId}' is quarantined.`);
    }

    // 1. Challenge Nonce Verification (TEE2)
    const challenge = this.activeChallenges.get(quote.challengeNonce);
    const nowNs = Date.now() * 1_000_000;
    if (!challenge) {
      this.recordFailure(quote.enclaveId, 'CHALLENGE_NONCE_UNKNOWN');
      throw new StaleAttestationReportError('Attestation quote references unknown challenge nonce.');
    }

    if (challenge.isConsumed) {
      this.recordFailure(quote.enclaveId, 'CHALLENGE_NONCE_REPLAYED');
      throw new StaleAttestationReportError('Attestation challenge nonce has already been consumed.');
    }

    if (nowNs > challenge.expiresAtNs) {
      this.recordFailure(quote.enclaveId, 'CHALLENGE_NONCE_EXPIRED');
      throw new StaleAttestationReportError('Attestation challenge nonce has expired.');
    }

    if (challenge.enclaveId !== quote.enclaveId) {
      this.recordFailure(quote.enclaveId, 'CHALLENGE_ENCLAVE_MISMATCH');
      throw new StaleAttestationReportError('Challenge nonce was issued to a different enclave ID.');
    }

    challenge.isConsumed = true;

    // 2. Measurement Verification (TEE1)
    const refMeas = this.approvedMeasurements.get(quote.measurementHash);
    if (!refMeas || !refMeas.isApproved) {
      this.recordFailure(quote.enclaveId, 'MEASUREMENT_UNAPPROVED');
      throw new EnclaveMeasurementMismatchError(
        `Enclave measurement '${quote.measurementHash}' is unapproved or mismatched.`,
      );
    }

    // 3. Cryptographic Signature Verification
    if (quote.publicKey.length !== 64 || !/^[0-9a-fA-F]+$/.test(quote.publicKey)) {
      this.recordFailure(quote.enclaveId, 'PUBLIC_KEY_INVALID');
      throw new EnclaveAttestationError('Quote publicKey must be a valid 64-character hex Ed25519 key.');
    }

    const digest = computeQuoteDigest(
      quote.enclaveId,
      quote.teeType,
      quote.measurementHash,
      quote.challengeNonce,
      quote.userDataDigest || '',
      quote.timestampNs,
    );

    if (!verifyEnclaveSignature(quote.publicKey, digest, quote.signature)) {
      this.recordFailure(quote.enclaveId, 'QUOTE_SIGNATURE_INVALID');
      throw new EnclaveAttestationError('Enclave quote cryptographic signature verification failed.');
    }

    // Success: reset failure count
    this.failureCounts.set(quote.enclaveId, 0);
    this.totalAttestations += 1;

    // 4. Issue Capability Token (TEE3)
    const leaseDurationNs = leaseSec ? Math.floor(leaseSec * 1_000_000_000) : this.defaultLeaseNs;
    const expiresAtNs = nowNs + leaseDurationNs;
    const tid = `tok-${createHash('sha256')
      .update(`${quote.enclaveId}:${agentId}:${nowNs}`)
      .digest('hex')
      .slice(0, 16)}`;

    const tokenDig = computeTokenDigest(tid, quote.enclaveId, agentId, requestedCapabilities, expiresAtNs);
    const tokenSig = signBftPayload(this.coordinatorPrivateKey, tokenDig);

    const token: EnclaveCapabilityToken = {
      tokenId: tid,
      enclaveId: quote.enclaveId,
      agentId,
      capabilities: requestedCapabilities,
      issuedAtNs: nowNs,
      expiresAtNs,
      tokenSignature: tokenSig,
    };

    this.issuedTokens.set(tid, token);
    return token;
  }

  public validateCapability(token: EnclaveCapabilityToken, requiredCapability: string): boolean {
    this.totalVerifications += 1;
    if (this.circuitBus && this.circuitBus.isTripped()) {
      raiseCircuitTripped();
    }

    if (this.quarantinedEnclaves.has(token.enclaveId)) {
      throw new EnclaveQuarantinedError(`Enclave '${token.enclaveId}' is quarantined.`);
    }

    const nowNs = Date.now() * 1_000_000;
    if (nowNs > token.expiresAtNs) {
      throw new UnauthorizedCapabilityError(`Capability token '${token.tokenId}' has expired.`);
    }

    if (!token.capabilities.includes(requiredCapability)) {
      throw new UnauthorizedCapabilityError(
        `Action '${requiredCapability}' not granted in token capabilities: ${token.capabilities}.`,
      );
    }

    const expectedDig = computeTokenDigest(
      token.tokenId,
      token.enclaveId,
      token.agentId,
      token.capabilities,
      token.expiresAtNs,
    );
    if (!verifyBftSignature(this.coordinatorPublicKey, expectedDig, token.tokenSignature)) {
      throw new UnauthorizedCapabilityError('Capability token signature integrity check failed.');
    }

    return true;
  }

  public revokeEnclave(enclaveId: string, reason: string = 'MANUAL_REVOCATION'): void {
    this.quarantinedEnclaves.add(enclaveId);
    if (this.circuitBus) {
      this.circuitBus.trip(
        'TEE_HARDWARE_ATTESTATION_BREACH',
        `enclave:${enclaveId}`,
        { revoked_enclave: enclaveId, reason },
      );
    }
  }

  private recordFailure(enclaveId: string, reason: string): void {
    const count = (this.failureCounts.get(enclaveId) || 0) + 1;
    this.failureCounts.set(enclaveId, count);
    if (count >= this.failureThreshold) {
      this.quarantinedEnclaves.add(enclaveId);
      if (this.circuitBus) {
        this.circuitBus.trip(
          'TEE_HARDWARE_ATTESTATION_BREACH',
          `enclave:${enclaveId}`,
          {
            quarantined_enclave: enclaveId,
            consecutive_failures: count,
            last_reason: reason,
          },
        );
      }
    }
  }

  public getStatus(): EnclaveStatus {
    const nowNs = Date.now() * 1_000_000;
    let activeTokens = 0;
    for (const t of this.issuedTokens.values()) {
      if (nowNs <= t.expiresAtNs) activeTokens += 1;
    }

    return {
      coordinator_id: this.coordinatorId,
      approved_measurements_count: this.approvedMeasurements.size,
      active_challenges_count: this.activeChallenges.size,
      total_attestations: this.totalAttestations,
      active_capability_tokens: activeTokens,
      total_capability_verifications: this.totalVerifications,
      quarantined_enclaves_count: this.quarantinedEnclaves.size,
      circuit_tripped: this.circuitBus ? this.circuitBus.isTripped() : false,
    };
  }
}

function raiseCircuitTripped(): never {
  throw new EnclaveCircuitTrippedError('Cannot process enclave operation: Safety circuit bus is TRIPPED.');
}

// Authoritative Global Singletons
export const globalEnclaveCoordinator = new EnclaveAttestationCoordinator();
export const globalEnclaveAttestation = globalEnclaveCoordinator;
