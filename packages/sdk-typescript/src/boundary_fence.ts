/**
 * SwarmWatch Sovereign Cross-Border Data Boundary Fence & Zero-Egress Encryption Vault (Seam 99 / ADR 0110)
 *
 * Provides geofenced residency boundaries (BND1), zero-egress vault envelope encryption (BND2),
 * cryptographic egress export passports (BND3), and fail-closed safety circuit containment (BND4).
 * Zero-token cost guarantee ($0.00 USD), sub-250 microsecond execution latency SLO.
 */

import { createHash, randomBytes, createCipheriv, createDecipheriv, createHmac } from 'node:crypto';
import { SafetyCircuitBus, globalSafetyCircuitBus } from './circuit_bus.ts';
import { HybridLogicalClock } from './hlc.ts';
import { generateNodeKeypair, signBftPayload, verifyBftSignature } from './byzantine_oracle.ts';

export class BoundaryFenceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BoundaryFenceError';
  }
}

export class CrossBorderResidencyViolationError extends BoundaryFenceError {
  constructor(message: string) {
    super(message);
    this.name = 'CrossBorderResidencyViolationError';
  }
}

export class UnsealedDataEgressError extends BoundaryFenceError {
  constructor(message: string) {
    super(message);
    this.name = 'UnsealedDataEgressError';
  }
}

export class InvalidEgressPassportError extends BoundaryFenceError {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidEgressPassportError';
  }
}

export class FenceZoneQuarantinedError extends BoundaryFenceError {
  constructor(message: string) {
    super(message);
    this.name = 'FenceZoneQuarantinedError';
  }
}

export class FenceCircuitTrippedError extends BoundaryFenceError {
  constructor(message: string) {
    super(message);
    this.name = 'FenceCircuitTrippedError';
  }
}

export interface ResidencyZone {
  zoneId: string; // e.g. EU_DE, US_EAST, CH_ZH, JP_TYO
  jurisdiction: string;
  complianceTags?: string[];
  isQuarantined?: boolean;
}

export interface EgressTreaty {
  treatyId: string;
  sourceZone: string;
  destZone: string;
  isBidirectional?: boolean;
  maxClassification?: string;
  isActive?: boolean;
}

export interface VaultEnvelope {
  envelopeId: string;
  tenantId: string;
  originZone: string;
  ciphertextHex: string;
  tagHex: string;
  ivHex: string;
  dataDigest: string;
  createdAtNs: number;
}

export interface EgressExportPassport {
  passportId: string;
  tenantId: string;
  sourceZone: string;
  destZone: string;
  payloadDigest: string;
  issuedAtNs: number;
  expiresAtNs: number;
  passportSignature: string;
  isConsumed?: boolean;
}

export interface BoundaryInspectionResult {
  allowed: boolean;
  sourceZone: string;
  destZone: string;
  envelopeId: string;
  reason: string;
  inspectedAtNs: number;
}

export interface FenceStatus {
  fence_id: string;
  total_zones_count: number;
  quarantined_zones_count: number;
  active_treaties_count: number;
  total_sealed_envelopes: number;
  active_passports_count: number;
  total_inspections: number;
  circuit_tripped: boolean;
}

export function computePassportDigest(
  passportId: string,
  tenantId: string,
  srcZone: string,
  dstZone: string,
  payloadDigest: string,
  expiresAtNs: number,
): string {
  const raw = `sw:pass:v1:${passportId}:${tenantId}:${srcZone}:${dstZone}:${payloadDigest}:${expiresAtNs}`;
  return createHash('sha256').update(raw).digest('hex');
}

export function verifyPassportSignature(pubkey: string, digest: string, signature: string): boolean {
  if (!signature || !pubkey || !digest) return false;
  return verifyBftSignature(pubkey, digest, signature);
}

export class DataBoundaryFence {
  public fenceId: string;
  public breachThreshold: number;
  public passportTtlNs: number;
  public circuitBus: SafetyCircuitBus | null;
  public vaultMasterKey: Buffer;
  public authorityPrivateKey: string;
  public authorityPublicKey: string;
  private hlc: HybridLogicalClock;

  private zones: Map<string, ResidencyZone> = new Map();
  private treaties: Map<string, EgressTreaty> = new Map();
  private passports: Map<string, EgressExportPassport> = new Map();
  private envelopes: Map<string, VaultEnvelope> = new Map();
  private breachCounts: Map<string, number> = new Map();
  private inspectionsCount: number = 0;
  private sealedCount: number = 0;

  constructor(
    fenceId: string = 'boundary-fence-primary',
    breachThreshold: number = 3,
    passportTtlSec: number = 60.0,
    circuitBus?: SafetyCircuitBus,
    vaultMasterKey?: Buffer,
  ) {
    this.fenceId = fenceId;
    this.breachThreshold = breachThreshold;
    this.passportTtlNs = Math.floor(passportTtlSec * 1_000_000_000);
    this.circuitBus = circuitBus !== undefined ? circuitBus : globalSafetyCircuitBus;
    this.hlc = new HybridLogicalClock(`fnc-${fenceId}`);

    this.vaultMasterKey = vaultMasterKey ?? randomBytes(32);
    const kp = generateNodeKeypair();
    this.authorityPrivateKey = kp.privateKey;
    this.authorityPublicKey = kp.publicKeyHex;
  }

  public bindCircuitBus(circuitBus: SafetyCircuitBus): void {
    this.circuitBus = circuitBus;
  }

  public registerResidencyZone(zone: ResidencyZone): void {
    this.zones.set(zone.zoneId, { ...zone, isQuarantined: zone.isQuarantined ?? false });
  }

  public registerEgressTreaty(treaty: EgressTreaty): void {
    this.treaties.set(treaty.treatyId, { ...treaty, isActive: treaty.isActive ?? true });
  }

  public sealData(tenantId: string, zoneId: string, plaintext: string | Buffer, key?: Buffer): VaultEnvelope {
    if (this.circuitBus && this.circuitBus.isTripped()) {
      raiseCircuitTripped();
    }

    const zone = this.zones.get(zoneId);
    if (zone && zone.isQuarantined) {
      throw new FenceZoneQuarantinedError(`Zone '${zoneId}' is quarantined.`);
    }

    const dataBytes = typeof plaintext === 'string' ? Buffer.from(plaintext, 'utf-8') : plaintext;
    const dataDigest = createHash('sha256').update(dataBytes).digest('hex');
    const nowNs = Date.now() * 1_000_000;

    const encKey = key || createHmac('sha256', this.vaultMasterKey).update(`${zoneId}:${tenantId}`).digest();
    const iv = randomBytes(12);

    try {
      const cipher = createCipheriv('aes-256-gcm', encKey, iv);
      cipher.setAAD(Buffer.from(`${tenantId}:${zoneId}`, 'utf-8'));
      const ciphertext = Buffer.concat([cipher.update(dataBytes), cipher.final()]);
      const tag = cipher.getAuthTag();

      const eid = `vlt-${createHash('sha256')
        .update(`${tenantId}:${zoneId}:${nowNs}:${dataDigest}`)
        .digest('hex')
        .slice(0, 16)}`;

      const envelope: VaultEnvelope = {
        envelopeId: eid,
        tenantId,
        originZone: zoneId,
        ciphertextHex: ciphertext.toString('hex'),
        tagHex: tag.toString('hex'),
        ivHex: iv.toString('hex'),
        dataDigest,
        createdAtNs: nowNs,
      };

      this.envelopes.set(eid, envelope);
      this.sealedCount += 1;
      return envelope;
    } catch (err: any) {
      throw new BoundaryFenceError(`Vault envelope AES-256-GCM encryption failed: ${err?.message ?? err}`);
    }
  }

  public unsealData(envelope: VaultEnvelope, key?: Buffer): Buffer {
    const encKey = key || createHmac('sha256', this.vaultMasterKey).update(`${envelope.originZone}:${envelope.tenantId}`).digest();
    const ciphertext = Buffer.from(envelope.ciphertextHex, 'hex');
    const tag = Buffer.from(envelope.tagHex, 'hex');
    const iv = Buffer.from(envelope.ivHex, 'hex');

    if (tag.length !== 16) {
      throw new BoundaryFenceError(
        `Invalid GCM authentication tag length: ${tag.length} bytes, expected exactly 16 bytes (PoC-N)`
      );
    }

    try {
      const decipher = createDecipheriv('aes-256-gcm', encKey, iv, { authTagLength: 16 });
      decipher.setAAD(Buffer.from(`${envelope.tenantId}:${envelope.originZone}`, 'utf-8'));
      decipher.setAuthTag(tag);
      return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
    } catch (err: any) {
      throw new BoundaryFenceError(`Vault envelope AES-256-GCM authentication/decryption failed: ${err?.message ?? err}`);
    }
  }

  public issueEgressPassport(
    tenantId: string,
    sourceZone: string,
    destZone: string,
    payloadDigest: string,
    ttlSec?: number,
  ): EgressExportPassport {
    if (this.circuitBus && this.circuitBus.isTripped()) {
      raiseCircuitTripped();
    }

    const src = this.zones.get(sourceZone);
    if (src && src.isQuarantined) {
      throw new FenceZoneQuarantinedError(`Source zone '${sourceZone}' is quarantined.`);
    }

    const dst = this.zones.get(destZone);
    if (dst && dst.isQuarantined) {
      throw new FenceZoneQuarantinedError(`Destination zone '${destZone}' is quarantined.`);
    }

    if (sourceZone !== destZone) {
      const treaty = this.findActiveTreaty(sourceZone, destZone);
      if (!treaty) {
        this.recordBreach(sourceZone, 'NO_ACTIVE_EGRESS_TREATY');
        throw new CrossBorderResidencyViolationError(
          `Cross-border transfer from '${sourceZone}' to '${destZone}' violates data residency (no active treaty).`,
        );
      }
    }

    const nowNs = Date.now() * 1_000_000;
    const durationNs = ttlSec ? Math.floor(ttlSec * 1_000_000_000) : this.passportTtlNs;
    const expiresAtNs = nowNs + durationNs;
    const pid = `pas-${createHash('sha256')
      .update(`${tenantId}:${sourceZone}:${destZone}:${nowNs}`)
      .digest('hex')
      .slice(0, 16)}`;

    const digest = computePassportDigest(pid, tenantId, sourceZone, destZone, payloadDigest, expiresAtNs);
    const sig = signBftPayload(this.authorityPrivateKey, digest);

    const passport: EgressExportPassport = {
      passportId: pid,
      tenantId,
      sourceZone,
      destZone,
      payloadDigest,
      issuedAtNs: nowNs,
      expiresAtNs,
      passportSignature: sig,
      isConsumed: false,
    };

    this.passports.set(pid, passport);
    return passport;
  }

  public inspectEgress(
    envelope: VaultEnvelope,
    destZone: string,
    passport?: EgressExportPassport,
  ): BoundaryInspectionResult {
    this.inspectionsCount += 1;
    const nowNs = Date.now() * 1_000_000;

    if (this.circuitBus && this.circuitBus.isTripped()) {
      raiseCircuitTripped();
    }

    const sourceZone = envelope.originZone;
    const srcZone = this.zones.get(sourceZone);
    if (srcZone && srcZone.isQuarantined) {
      throw new FenceZoneQuarantinedError(`Source zone '${sourceZone}' is under quarantine.`);
    }

    if (sourceZone === destZone) {
      return {
        allowed: true,
        sourceZone,
        destZone,
        envelopeId: envelope.envelopeId,
        reason: 'LOCAL_INTRA_ZONE_TRANSFER',
        inspectedAtNs: nowNs,
      };
    }

    if (!passport) {
      this.recordBreach(sourceZone, 'EGRESS_PASSPORT_MISSING');
      throw new InvalidEgressPassportError(
        `Cross-border transfer to '${destZone}' requires an egress export passport.`,
      );
    }

    if (passport.isConsumed) {
      this.recordBreach(sourceZone, 'PASSPORT_REPLAY_ATTEMPT');
      throw new InvalidEgressPassportError(`Egress passport '${passport.passportId}' has already been consumed.`);
    }

    if (nowNs > passport.expiresAtNs) {
      this.recordBreach(sourceZone, 'PASSPORT_EXPIRED');
      throw new InvalidEgressPassportError(`Egress passport '${passport.passportId}' has expired.`);
    }

    if (passport.sourceZone !== sourceZone || passport.destZone !== destZone) {
      this.recordBreach(sourceZone, 'PASSPORT_ZONE_MISMATCH');
      throw new InvalidEgressPassportError('Egress passport route does not match envelope route.');
    }

    if (passport.payloadDigest !== envelope.dataDigest) {
      this.recordBreach(sourceZone, 'PASSPORT_PAYLOAD_MISMATCH');
      throw new InvalidEgressPassportError('Egress passport payload digest does not match envelope digest.');
    }

    const pDig = computePassportDigest(
      passport.passportId,
      passport.tenantId,
      passport.sourceZone,
      passport.destZone,
      passport.payloadDigest,
      passport.expiresAtNs,
    );

    if (!verifyPassportSignature(this.authorityPublicKey, pDig, passport.passportSignature)) {
      this.recordBreach(sourceZone, 'PASSPORT_SIGNATURE_INVALID');
      throw new InvalidEgressPassportError('Egress passport signature verification failed.');
    }

    passport.isConsumed = true;
    return {
      allowed: true,
      sourceZone,
      destZone,
      envelopeId: envelope.envelopeId,
      reason: 'CROSS_BORDER_PASSPORT_VERIFIED',
      inspectedAtNs: nowNs,
    };
  }

  public quarantineZone(zoneId: string, reason: string = 'MANUAL_QUARANTINE'): void {
    const zone = this.zones.get(zoneId);
    if (zone) {
      zone.isQuarantined = true;
    }
    if (this.circuitBus) {
      this.circuitBus.trip(
        'BOUNDARY_FENCE_EGRESS_BREACH',
        `zone:${zoneId}`,
        { quarantined_zone: zoneId, reason },
      );
    }
  }

  private findActiveTreaty(src: string, dst: string): EgressTreaty | undefined {
    for (const t of this.treaties.values()) {
      if (!t.isActive) continue;
      if (t.sourceZone === src && t.destZone === dst) return t;
      if (t.isBidirectional && t.sourceZone === dst && t.destZone === src) return t;
    }
    return undefined;
  }

  private recordBreach(zoneId: string, reason: string): void {
    const count = (this.breachCounts.get(zoneId) || 0) + 1;
    this.breachCounts.set(zoneId, count);
    if (count >= this.breachThreshold) {
      this.quarantineZone(zoneId, `BREACH_THRESHOLD_EXCEEDED:${reason}`);
    }
  }

  public getStatus(): FenceStatus {
    const nowNs = Date.now() * 1_000_000;
    let activeTreaties = 0;
    for (const t of this.treaties.values()) {
      if (t.isActive) activeTreaties += 1;
    }

    let quarantinedZones = 0;
    for (const z of this.zones.values()) {
      if (z.isQuarantined) quarantinedZones += 1;
    }

    let activePassports = 0;
    for (const p of this.passports.values()) {
      if (!p.isConsumed && nowNs <= p.expiresAtNs) activePassports += 1;
    }

    return {
      fence_id: this.fenceId,
      total_zones_count: this.zones.size,
      quarantined_zones_count: quarantinedZones,
      active_treaties_count: activeTreaties,
      total_sealed_envelopes: this.sealedCount,
      active_passports_count: activePassports,
      total_inspections: this.inspectionsCount,
      circuit_tripped: this.circuitBus ? this.circuitBus.isTripped() : false,
    };
  }
}

function raiseCircuitTripped(): never {
  throw new FenceCircuitTrippedError('Cannot process fence operation: Safety circuit bus is TRIPPED.');
}

// Authoritative Global Singletons
export const globalBoundaryFence = new DataBoundaryFence();
export const globalDataFence = globalBoundaryFence;
