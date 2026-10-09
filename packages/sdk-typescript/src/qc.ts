/**
 * SwarmWatch Committee Key Ceremony & Threshold Quorum Certificates (Seam 81 / ADR 0092).
 *
 * Implements threshold multi-party quorum certificates (QCs), certified epoch key ceremonies,
 * Ed25519 cryptographic signatures, deterministic Merkle committee roots, domain-separated
 * attestations, and fail-closed Byzantine verification across distributed seams.
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class QuorumCertificateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "QuorumCertificateError";
  }
}

export class InsufficientQuorumError extends QuorumCertificateError {
  constructor(message: string) {
    super(message);
    this.name = "InsufficientQuorumError";
  }
}

export class InvalidCommitteeRootError extends QuorumCertificateError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidCommitteeRootError";
  }
}

export class ForgedCertificateSignatureError extends QuorumCertificateError {
  constructor(message: string) {
    super(message);
    this.name = "ForgedCertificateSignatureError";
  }
}

export class UnauthorizedSignerError extends QuorumCertificateError {
  constructor(message: string) {
    super(message);
    this.name = "UnauthorizedSignerError";
  }
}

export class KeyRotationViolationError extends QuorumCertificateError {
  constructor(message: string) {
    super(message);
    this.name = "KeyRotationViolationError";
  }
}

// ==============================================================================
// 2. Domain Types & Interfaces
// ==============================================================================

export interface CommitteeMember {
  readonly member_id: string;
  readonly public_key_hex: string;
  readonly role?: string;
}

export interface CommitteeRoster {
  readonly epoch: number;
  readonly members: Record<string, CommitteeMember>;
  readonly committee_root: string;
  readonly quorum_threshold: number;
  readonly fault_tolerance_f: number;
  readonly created_at_ns: number;
}

export interface ThresholdQuorumCertificate {
  readonly cert_id: string;
  readonly domain: string;
  readonly epoch: number;
  readonly digest: string;
  readonly committee_root: string;
  readonly quorum_required: number;
  readonly signatures: Record<string, string>; // member_id -> signature_hex
  readonly signers: readonly string[];         // sorted list of member IDs
  readonly issued_at_ns: number;
}

export interface QuorumVerdict {
  readonly is_valid: boolean;
  readonly verified_signers: readonly string[];
  readonly threshold_met: boolean;
  readonly error_reason?: string;
}

// ==============================================================================
// 3. Cryptographic Math & Helpers
// ==============================================================================

export function canonicalJsonString(data: Record<string, unknown>): string {
  const sortedKeys = Object.keys(data).sort();
  const sortedObj: Record<string, unknown> = {};
  for (const k of sortedKeys) {
    sortedObj[k] = data[k];
  }
  return JSON.stringify(sortedObj);
}

export function computeByzantineQuorum(n: number): { f: number; q: number } {
  if (n <= 0) return { f: 0, q: 0 };
  if (n < 4) {
    return { f: 0, q: Math.max(1, Math.ceil((n + 1) / 2)) };
  }
  const f = Math.floor((n - 1) / 3);
  const q = Math.ceil((n + f + 1) / 2);
  return { f, q };
}

export function computeMerkleRoot(leafHashes: string[]): string {
  if (leafHashes.length === 0) {
    return crypto.createHash("sha256").update("sw:merkle:empty").digest("hex");
  }

  let currentLevel = [...leafHashes].sort();
  while (currentLevel.length > 1) {
    const nextLevel: string[] = [];
    for (let i = 0; i < currentLevel.length; i += 2) {
      const left = currentLevel[i];
      const right = i + 1 < currentLevel.length ? currentLevel[i + 1] : currentLevel[i];
      const combined = crypto
        .createHash("sha256")
        .update(`${left}:${right}`, "utf-8")
        .digest("hex");
      nextLevel.push(combined);
    }
    currentLevel = nextLevel;
  }
  return currentLevel[0];
}

export function computeCommitteeRoot(members: CommitteeMember[]): string {
  const sortedMembers = [...members].sort((a, b) => a.member_id.localeCompare(b.member_id));
  const leafDigests = sortedMembers.map((m) =>
    crypto
      .createHash("sha256")
      .update(`${m.member_id}:${m.public_key_hex}`, "utf-8")
      .digest("hex")
  );
  return computeMerkleRoot(leafDigests);
}

export function formatAttestationPayload(
  domain: string,
  epoch: number,
  digest: string,
  committeeRoot: string
): Buffer {
  const domainTag = `sw:qc:v1:${domain}`;
  const raw = `${domainTag}:${epoch}:${digest}:${committeeRoot}`;
  return crypto.createHash("sha256").update(raw, "utf-8").digest();
}

// ==============================================================================
// 4. Committee Key Ceremony & Quorum Attestation Kernel
// ==============================================================================

export function createCommitteeRoster(epoch: number, members: CommitteeMember[]): CommitteeRoster {
  if (!members || members.length === 0) {
    throw new Error("Committee members list cannot be empty");
  }

  const memberMap: Record<string, CommitteeMember> = {};
  for (const m of members) {
    if (memberMap[m.member_id]) {
      throw new Error(`Duplicate member ID '${m.member_id}' in committee ceremony`);
    }
    memberMap[m.member_id] = m;
  }

  const { f, q } = computeByzantineQuorum(members.length);
  const root = computeCommitteeRoot(members);

  return {
    epoch,
    members: memberMap,
    committee_root: root,
    quorum_threshold: q,
    fault_tolerance_f: f,
    created_at_ns: Date.now() * 1_000_000,
  };
}

export function attestQuorum(
  roster: CommitteeRoster,
  domain: string,
  digest: string,
  signerKeys: Map<string, crypto.KeyObject | string>
): ThresholdQuorumCertificate {
  if (!signerKeys || signerKeys.size === 0) {
    throw new InsufficientQuorumError("Cannot attest quorum with zero signer keys");
  }

  const payload = formatAttestationPayload(domain, roster.epoch, digest, roster.committee_root);
  const signatures: Record<string, string> = {};
  const signers: string[] = [];

  for (const [memberId, privateKey] of signerKeys.entries()) {
    const member = roster.members[memberId];
    if (!member) {
      throw new UnauthorizedSignerError(`Signer '${memberId}' is not in epoch ${roster.epoch} roster`);
    }

    let privKeyObj: crypto.KeyObject;
    if (typeof privateKey === "string") {
      privKeyObj = crypto.createPrivateKey({
        key: Buffer.from(privateKey, "hex"),
        format: "der",
        type: "pkcs8",
      });
    } else {
      privKeyObj = privateKey;
    }

    const sig = crypto.sign(null, payload, privKeyObj);
    signatures[memberId] = sig.toString("hex");
    signers.push(memberId);
  }

  signers.sort();

  if (signers.length < roster.quorum_threshold) {
    throw new InsufficientQuorumError(
      `Collected ${signers.length} signatures, required quorum is ${roster.quorum_threshold}`
    );
  }

  const certMaterial = `${domain}:${roster.epoch}:${digest}:${roster.committee_root}:${signers.join(":")}`;
  const certId = crypto.createHash("sha256").update(certMaterial, "utf-8").digest("hex");

  return {
    cert_id: certId,
    domain,
    epoch: roster.epoch,
    digest,
    committee_root: roster.committee_root,
    quorum_required: roster.quorum_threshold,
    signatures,
    signers,
    issued_at_ns: Date.now() * 1_000_000,
  };
}

export function verifyQuorumCertificate(
  cert: ThresholdQuorumCertificate,
  roster?: CommitteeRoster,
  expectedRoot?: string,
  expectedEpoch?: number
): QuorumVerdict {
  if (expectedEpoch !== undefined && cert.epoch !== expectedEpoch) {
    return {
      is_valid: false,
      verified_signers: [],
      threshold_met: false,
      error_reason: `Certificate epoch ${cert.epoch} does not match expected ${expectedEpoch}`,
    };
  }

  if (expectedRoot !== undefined && cert.committee_root !== expectedRoot) {
    return {
      is_valid: false,
      verified_signers: [],
      threshold_met: false,
      error_reason: `Certificate committee root ${cert.committee_root} does not match expected ${expectedRoot}`,
    };
  }

  let effectiveThreshold: number;
  if (roster) {
    if (cert.committee_root !== roster.committee_root) {
      return {
        is_valid: false,
        verified_signers: [],
        threshold_met: false,
        error_reason: "Certificate committee root does not match provided roster root",
      };
    }
    effectiveThreshold = roster.quorum_threshold;
  } else {
    if (!expectedRoot || cert.committee_root !== expectedRoot) {
      return {
        is_valid: false,
        verified_signers: [],
        threshold_met: false,
        error_reason: "Cannot verify certificate without trusted roster or matching expected_root",
      };
    }
    effectiveThreshold = cert.quorum_required;
  }

  if (!cert.signatures || Object.keys(cert.signatures).length === 0) {
    return {
      is_valid: false,
      verified_signers: [],
      threshold_met: false,
      error_reason: "Certificate contains no cryptographic signatures",
    };
  }

  const payload = formatAttestationPayload(cert.domain, cert.epoch, cert.digest, cert.committee_root);
  const verifiedSigners: string[] = [];

  for (const [memberId, sigHex] of Object.entries(cert.signatures)) {
    if (!roster) {
      return {
        is_valid: false,
        verified_signers: [],
        threshold_met: false,
        error_reason: "Missing public key material for member verification",
      };
    }

    const member = roster.members[memberId];
    if (!member) continue;

    try {
      const pubKeyDer = Buffer.from(member.public_key_hex, "hex");
      const pubKeyObj = crypto.createPublicKey({
        key: pubKeyDer,
        format: "der",
        type: "spki",
      });

      const sigBuf = Buffer.from(sigHex, "hex");
      const isValid = crypto.verify(null, payload, pubKeyObj, sigBuf);
      if (isValid) {
        verifiedSigners.push(memberId);
      }
    } catch {
      continue;
    }
  }

  const thresholdMet = verifiedSigners.length >= effectiveThreshold;
  const isValid = thresholdMet && verifiedSigners.length >= cert.quorum_required;

  return {
    is_valid: isValid,
    verified_signers: verifiedSigners,
    threshold_met: thresholdMet,
    error_reason: isValid ? undefined : `Quorum threshold not met: ${verifiedSigners.length}/${effectiveThreshold}`,
  };
}

export function rotateCommittee(
  currentRoster: CommitteeRoster,
  newMembers: CommitteeMember[],
  signerKeys: Map<string, crypto.KeyObject | string>
): { nextRoster: CommitteeRoster; rotationQc: ThresholdQuorumCertificate } {
  const nextEpoch = currentRoster.epoch + 1;
  const nextRoster = createCommitteeRoster(nextEpoch, newMembers);

  const rotationDigest = nextRoster.committee_root;
  const rotationQc = attestQuorum(currentRoster, "committee:rotation", rotationDigest, signerKeys);

  return { nextRoster, rotationQc };
}
