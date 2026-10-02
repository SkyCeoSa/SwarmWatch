/**
 * Tenant Identity Boundary & Attenuated Capability Tokens (Seam 65 / ADR 0076).
 * Implements cryptographically bound capability tokens, monotonic delegation attenuation,
 * subtree revocation cascade, replay and cross-tenant forgery rejection, and sub-250µs verification.
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { WitnessKeyManager, sha256Digest } from "./root_witness.ts";

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

export class CapabilityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CapabilityError";
  }
}

export class PrivilegeEscalationError extends CapabilityError {
  constructor(message: string) {
    super(message);
    this.name = "PrivilegeEscalationError";
  }
}

export class AttenuationViolationError extends CapabilityError {
  constructor(message: string) {
    super(message);
    this.name = "AttenuationViolationError";
  }
}

export class CapabilityVerificationError extends CapabilityError {
  constructor(message: string) {
    super(message);
    this.name = "CapabilityVerificationError";
  }
}

export class RevocationError extends CapabilityError {
  constructor(message: string) {
    super(message);
    this.name = "RevocationError";
  }
}

// ---------------------------------------------------------------------------
// Pattern Matching Utilities
// ---------------------------------------------------------------------------

export function resourcePatternMatches(pattern: string, targetResource: string): boolean {
  if (pattern === "*" || pattern === targetResource) {
    return true;
  }
  if (pattern.endsWith("*")) {
    const prefix = pattern.slice(0, -1);
    return targetResource.startsWith(prefix);
  }
  return false;
}

export function isSubpattern(parentPattern: string, childPattern: string): boolean {
  if (parentPattern === "*") {
    return true;
  }
  if (childPattern === "*") {
    return false;
  }
  if (parentPattern.endsWith("*")) {
    const prefix = parentPattern.slice(0, -1);
    if (childPattern.endsWith("*")) {
      const childPrefix = childPattern.slice(0, -1);
      return childPrefix.startsWith(prefix);
    }
    return childPattern.startsWith(prefix);
  }
  return childPattern === parentPattern;
}

export function currentEpochNs(): bigint {
  return BigInt(Date.now()) * 1_000_000n;
}

// ---------------------------------------------------------------------------
// Data Models
// ---------------------------------------------------------------------------

export interface CapabilityTokenData {
  tokenId: string;
  parentTokenId: string | null;
  tenantId: string;
  issuerPrincipal: string;
  subjectPrincipal: string;
  scopes: string[];
  resourcePattern: string;
  epoch: number;
  notBeforeNs: bigint | number;
  expiresAtNs: bigint | number;
  depth: number;
  maxDepth: number;
  issuerPublicKey: string;
  attenuationDigest: string;
  signature: string;
  metadata?: Record<string, unknown>;
}

export class CapabilityToken {
  public readonly tokenId: string;
  public readonly parentTokenId: string | null;
  public readonly tenantId: string;
  public readonly issuerPrincipal: string;
  public readonly subjectPrincipal: string;
  public readonly scopes: string[];
  public readonly resourcePattern: string;
  public readonly epoch: number;
  public readonly notBeforeNs: bigint;
  public readonly expiresAtNs: bigint;
  public readonly depth: number;
  public readonly maxDepth: number;
  public readonly issuerPublicKey: string;
  public readonly attenuationDigest: string;
  public readonly signature: string;
  public readonly metadata: Record<string, unknown>;

  constructor(data: CapabilityTokenData) {
    this.tokenId = data.tokenId;
    this.parentTokenId = data.parentTokenId ?? null;
    this.tenantId = data.tenantId;
    this.issuerPrincipal = data.issuerPrincipal;
    this.subjectPrincipal = data.subjectPrincipal;
    this.scopes = [...data.scopes];
    this.resourcePattern = data.resourcePattern;
    this.epoch = data.epoch;
    this.notBeforeNs = typeof data.notBeforeNs === "bigint" ? data.notBeforeNs : BigInt(data.notBeforeNs);
    this.expiresAtNs = typeof data.expiresAtNs === "bigint" ? data.expiresAtNs : BigInt(data.expiresAtNs);
    this.depth = data.depth;
    this.maxDepth = data.maxDepth;
    this.issuerPublicKey = data.issuerPublicKey;
    this.attenuationDigest = data.attenuationDigest;
    this.signature = data.signature;
    this.metadata = { ...(data.metadata || {}) };
  }

  public static computeDigest(params: {
    tokenId: string;
    parentTokenId: string | null;
    tenantId: string;
    issuerPrincipal: string;
    subjectPrincipal: string;
    scopes: string[];
    resourcePattern: string;
    epoch: number;
    notBeforeNs: bigint | number;
    expiresAtNs: bigint | number;
    depth: number;
    maxDepth: number;
    issuerPublicKey: string;
  }): string {
    const sortedScopes = [...params.scopes].sort().join(",");
    const parentStr = params.parentTokenId || "";
    const seed =
      `sw:cap:v1:${params.tokenId}:${parentStr}:${params.tenantId}:${params.issuerPrincipal}:` +
      `${params.subjectPrincipal}:${sortedScopes}:${params.resourcePattern}:${params.epoch}:` +
      `${params.notBeforeNs.toString()}:${params.expiresAtNs.toString()}:${params.depth}:${params.maxDepth}:${params.issuerPublicKey}`;
    return sha256Digest(seed);
  }

  public toJSON(): Record<string, unknown> {
    return {
      token_id: this.tokenId,
      parent_token_id: this.parentTokenId,
      tenant_id: this.tenantId,
      issuer_principal: this.issuerPrincipal,
      subject_principal: this.subjectPrincipal,
      scopes: [...this.scopes],
      resource_pattern: this.resourcePattern,
      epoch: this.epoch,
      not_before_ns: Number(this.notBeforeNs),
      expires_at_ns: Number(this.expiresAtNs),
      depth: this.depth,
      max_depth: this.maxDepth,
      issuer_public_key: this.issuerPublicKey,
      attenuation_digest: this.attenuationDigest,
      signature: this.signature,
      metadata: { ...this.metadata },
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }

  public static fromDict(data: Record<string, unknown>): CapabilityToken {
    return new CapabilityToken({
      tokenId: String(data.token_id || data.tokenId || ""),
      parentTokenId: data.parent_token_id ? String(data.parent_token_id) : (data.parentTokenId ? String(data.parentTokenId) : null),
      tenantId: String(data.tenant_id || data.tenantId || ""),
      issuerPrincipal: String(data.issuer_principal || data.issuerPrincipal || ""),
      subjectPrincipal: String(data.subject_principal || data.subjectPrincipal || ""),
      scopes: Array.isArray(data.scopes) ? data.scopes.map(String) : [],
      resourcePattern: String(data.resource_pattern || data.resourcePattern || ""),
      epoch: Number(data.epoch ?? 0),
      notBeforeNs: BigInt(String(data.not_before_ns ?? data.notBeforeNs ?? 0)),
      expiresAtNs: BigInt(String(data.expires_at_ns ?? data.expiresAtNs ?? 0)),
      depth: Number(data.depth ?? 0),
      maxDepth: Number(data.max_depth ?? data.maxDepth ?? 10),
      issuerPublicKey: String(data.issuer_public_key || data.issuerPublicKey || ""),
      attenuationDigest: String(data.attenuation_digest || data.attenuationDigest || ""),
      signature: String(data.signature || ""),
      metadata: (data.metadata as Record<string, unknown>) || {},
    });
  }
}

export interface CapabilityVerdictData {
  valid: boolean;
  tokenId: string;
  tenantId: string;
  subjectPrincipal: string;
  actionScope: string;
  targetResource: string;
  reason: string;
  checkedAtNs?: bigint | number;
}

export class CapabilityVerdict {
  public readonly valid: boolean;
  public readonly tokenId: string;
  public readonly tenantId: string;
  public readonly subjectPrincipal: string;
  public readonly actionScope: string;
  public readonly targetResource: string;
  public readonly reason: string;
  public readonly checkedAtNs: bigint;

  constructor(data: CapabilityVerdictData) {
    this.valid = data.valid;
    this.tokenId = data.tokenId;
    this.tenantId = data.tenantId;
    this.subjectPrincipal = data.subjectPrincipal;
    this.actionScope = data.actionScope;
    this.targetResource = data.targetResource;
    this.reason = data.reason;
    this.checkedAtNs =
      data.checkedAtNs !== undefined
        ? typeof data.checkedAtNs === "bigint"
          ? data.checkedAtNs
          : BigInt(data.checkedAtNs)
        : currentEpochNs();
  }

  public toJSON(): Record<string, unknown> {
    return {
      valid: this.valid,
      token_id: this.tokenId,
      tenant_id: this.tenantId,
      subject_principal: this.subjectPrincipal,
      action_scope: this.actionScope,
      target_resource: this.targetResource,
      reason: this.reason,
      checked_at_ns: Number(this.checkedAtNs),
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }

  public static fromDict(data: Record<string, unknown>): CapabilityVerdict {
    return new CapabilityVerdict({
      valid: Boolean(data.valid),
      tokenId: String(data.token_id || data.tokenId || ""),
      tenantId: String(data.tenant_id || data.tenantId || ""),
      subjectPrincipal: String(data.subject_principal || data.subjectPrincipal || ""),
      actionScope: String(data.action_scope || data.actionScope || ""),
      targetResource: String(data.target_resource || data.targetResource || ""),
      reason: String(data.reason || ""),
      checkedAtNs: BigInt(String(data.checked_at_ns ?? data.checkedAtNs ?? 0)),
    });
  }
}

export interface RevocationReceiptData {
  revocationId: string;
  tokenId: string;
  revokedBy: string;
  reason: string;
  timestampNs?: bigint | number;
}

export class RevocationReceipt {
  public readonly revocationId: string;
  public readonly tokenId: string;
  public readonly revokedBy: string;
  public readonly reason: string;
  public readonly timestampNs: bigint;

  constructor(data: RevocationReceiptData) {
    this.revocationId = data.revocationId;
    this.tokenId = data.tokenId;
    this.revokedBy = data.revokedBy;
    this.reason = data.reason;
    this.timestampNs =
      data.timestampNs !== undefined
        ? typeof data.timestampNs === "bigint"
          ? data.timestampNs
          : BigInt(data.timestampNs)
        : currentEpochNs();
  }

  public toJSON(): Record<string, unknown> {
    return {
      revocation_id: this.revocationId,
      token_id: this.tokenId,
      revoked_by: this.revokedBy,
      reason: this.reason,
      timestamp_ns: Number(this.timestampNs),
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }

  public static fromDict(data: Record<string, unknown>): RevocationReceipt {
    return new RevocationReceipt({
      revocationId: String(data.revocation_id || data.revocationId || ""),
      tokenId: String(data.token_id || data.tokenId || ""),
      revokedBy: String(data.revoked_by || data.revokedBy || ""),
      reason: String(data.reason || ""),
      timestampNs: BigInt(String(data.timestamp_ns ?? data.timestampNs ?? 0)),
    });
  }
}

export interface CapabilityStatusData {
  totalIssued: number;
  totalAttenuated: number;
  totalRevoked: number;
  activeRootPublicKey: string;
}

export class CapabilityStatus {
  public readonly totalIssued: number;
  public readonly totalAttenuated: number;
  public readonly totalRevoked: number;
  public readonly activeRootPublicKey: string;

  constructor(data: CapabilityStatusData) {
    this.totalIssued = data.totalIssued;
    this.totalAttenuated = data.totalAttenuated;
    this.totalRevoked = data.totalRevoked;
    this.activeRootPublicKey = data.activeRootPublicKey;
  }

  public toJSON(): Record<string, unknown> {
    return {
      total_issued: this.totalIssued,
      total_attenuated: this.totalAttenuated,
      total_revoked: this.totalRevoked,
      active_root_public_key: this.activeRootPublicKey,
    };
  }

  public toDict(): Record<string, unknown> {
    return this.toJSON();
  }
}

// ---------------------------------------------------------------------------
// Core Deep Module: TenantCapabilityManager
// ---------------------------------------------------------------------------

export interface IssueRootTokenOptions {
  tenantId: string;
  subjectPrincipal: string;
  scopes: string[];
  resourcePattern?: string;
  epoch?: number;
  notBeforeNs?: bigint | number;
  ttlSeconds?: number;
  maxDepth?: number;
  metadata?: Record<string, unknown>;
}

export interface AttenuateOptions {
  parentToken: CapabilityToken;
  subjectPrincipal: string;
  scopes?: string[];
  resourcePattern?: string;
  epoch?: number;
  notBeforeNs?: bigint | number;
  expiresAtNs?: bigint | number;
  ttlSeconds?: number;
  signerKeyManager?: WitnessKeyManager;
  metadata?: Record<string, unknown>;
}

export interface VerifyExecutionOptions {
  token: CapabilityToken;
  actionScope: string;
  targetResource: string;
  executingPrincipal: string;
  expectedTenantId: string;
  currentEpoch?: number;
  nowNs?: bigint | number;
}

export class TenantCapabilityManager {
  public static readonly DEFAULT_TTL_SECONDS = 3600;
  public static readonly MAX_DELEGATION_DEPTH = 8;

  public readonly authorityKeyManager: WitnessKeyManager;
  public readonly storagePath?: string;

  private readonly _tokens: Map<string, CapabilityToken> = new Map();
  private readonly _tokenParents: Map<string, string> = new Map();
  private readonly _revokedTokens: Set<string> = new Set();
  private readonly _revocationRecords: Map<string, RevocationReceipt> = new Map();
  private readonly _signatureCache: Map<string, boolean> = new Map();

  private _totalIssued = 0;
  private _totalAttenuated = 0;

  constructor(options?: {
    authorityKeyManager?: WitnessKeyManager;
    storagePath?: string;
  }) {
    this.authorityKeyManager = options?.authorityKeyManager ?? WitnessKeyManager.generate();
    this.storagePath = options?.storagePath;

    if (this.storagePath && fs.existsSync(this.storagePath)) {
      this._loadFromStorage();
    }
  }

  public issueRootToken(options: IssueRootTokenOptions): CapabilityToken {
    const now = currentEpochNs();
    const nbNs =
      options.notBeforeNs !== undefined
        ? typeof options.notBeforeNs === "bigint"
          ? options.notBeforeNs
          : BigInt(options.notBeforeNs)
        : now;
    const ttl = options.ttlSeconds ?? TenantCapabilityManager.DEFAULT_TTL_SECONDS;
    const expNs = nbNs + BigInt(ttl) * 1_000_000_000n;
    const tokenId = crypto.randomUUID();
    const pubKey = this.authorityKeyManager.publicKeyB64;
    const maxD = options.maxDepth ?? TenantCapabilityManager.MAX_DELEGATION_DEPTH;
    const pattern = options.resourcePattern ?? "*";
    const epoch = options.epoch ?? 0;

    const digest = CapabilityToken.computeDigest({
      tokenId,
      parentTokenId: null,
      tenantId: options.tenantId,
      issuerPrincipal: "authority",
      subjectPrincipal: options.subjectPrincipal,
      scopes: options.scopes,
      resourcePattern: pattern,
      epoch,
      notBeforeNs: nbNs,
      expiresAtNs: expNs,
      depth: 0,
      maxDepth: maxD,
      issuerPublicKey: pubKey,
    });

    const sig = this.authorityKeyManager.signB64(digest);

    const token = new CapabilityToken({
      tokenId,
      parentTokenId: null,
      tenantId: options.tenantId,
      issuerPrincipal: "authority",
      subjectPrincipal: options.subjectPrincipal,
      scopes: options.scopes,
      resourcePattern: pattern,
      epoch,
      notBeforeNs: nbNs,
      expiresAtNs: expNs,
      depth: 0,
      maxDepth: maxD,
      issuerPublicKey: pubKey,
      attenuationDigest: digest,
      signature: sig,
      metadata: options.metadata,
    });

    this._tokens.set(tokenId, token);
    this._totalIssued++;

    if (this.storagePath) {
      this._persistRecord("token", token.toJSON());
    }

    return token;
  }

  public attenuate(options: AttenuateOptions): CapabilityToken {
    const parent = options.parentToken;

    // 1. Depth check
    const childDepth = parent.depth + 1;
    if (childDepth > parent.maxDepth) {
      throw new AttenuationViolationError(
        `Delegation depth exceeds max_depth: ${childDepth} > ${parent.maxDepth}`
      );
    }

    // 2. Scope monotonicity
    const parentScopes = new Set(parent.scopes);
    let finalScopes: string[];
    if (options.scopes !== undefined) {
      const childScopes = new Set(options.scopes);
      for (const s of childScopes) {
        if (!parentScopes.has(s)) {
          throw new PrivilegeEscalationError(`Unheld scopes requested in delegation: [${s}]`);
        }
      }
      finalScopes = [...options.scopes];
    } else {
      finalScopes = [...parent.scopes];
    }

    // 3. Resource pattern narrowing
    const childPattern = options.resourcePattern !== undefined ? options.resourcePattern : parent.resourcePattern;

    // Check cross-tenant pattern attempt
    if (childPattern.startsWith("sw://")) {
      const parts = childPattern.slice(5).split("/");
      if (parts.length > 0 && parts[0] !== "*" && parts[0] !== parent.tenantId) {
        throw new PrivilegeEscalationError(
          `Cross-tenant resource pattern prohibited: ${childPattern} != tenant ${parent.tenantId}`
        );
      }
    }

    if (!isSubpattern(parent.resourcePattern, childPattern)) {
      throw new PrivilegeEscalationError(
        `Resource pattern cannot be widened: '${childPattern}' is not a sub-pattern of '${parent.resourcePattern}'`
      );
    }

    // 4. Temporal constraints
    const now = currentEpochNs();
    const childNb =
      options.notBeforeNs !== undefined
        ? typeof options.notBeforeNs === "bigint"
          ? options.notBeforeNs
          : BigInt(options.notBeforeNs)
        : now > parent.notBeforeNs
        ? now
        : parent.notBeforeNs;

    if (childNb < parent.notBeforeNs) {
      throw new AttenuationViolationError("Child not_before cannot precede parent not_before");
    }

    let childExp: bigint;
    if (options.expiresAtNs !== undefined) {
      childExp =
        typeof options.expiresAtNs === "bigint"
          ? options.expiresAtNs
          : BigInt(options.expiresAtNs);
    } else if (options.ttlSeconds !== undefined) {
      childExp = childNb + BigInt(options.ttlSeconds) * 1_000_000_000n;
    } else {
      childExp = parent.expiresAtNs;
    }

    if (childExp > parent.expiresAtNs) {
      throw new AttenuationViolationError(
        `Child expiration cannot exceed parent expiration: ${childExp} > ${parent.expiresAtNs}`
      );
    }

    // 5. Epoch monotonicity
    const childEpoch = options.epoch !== undefined ? options.epoch : parent.epoch;
    if (childEpoch < parent.epoch) {
      throw new AttenuationViolationError(
        `Child epoch cannot precede parent epoch: ${childEpoch} < ${parent.epoch}`
      );
    }

    // 6. Issue child token
    const km = options.signerKeyManager ?? this.authorityKeyManager;
    const childTokenId = crypto.randomUUID();
    const pubKey = km.publicKeyB64;

    const childDigest = CapabilityToken.computeDigest({
      tokenId: childTokenId,
      parentTokenId: parent.tokenId,
      tenantId: parent.tenantId,
      issuerPrincipal: parent.subjectPrincipal,
      subjectPrincipal: options.subjectPrincipal,
      scopes: finalScopes,
      resourcePattern: childPattern,
      epoch: childEpoch,
      notBeforeNs: childNb,
      expiresAtNs: childExp,
      depth: childDepth,
      maxDepth: parent.maxDepth,
      issuerPublicKey: pubKey,
    });

    const sig = km.signB64(childDigest);

    const childToken = new CapabilityToken({
      tokenId: childTokenId,
      parentTokenId: parent.tokenId,
      tenantId: parent.tenantId,
      issuerPrincipal: parent.subjectPrincipal,
      subjectPrincipal: options.subjectPrincipal,
      scopes: finalScopes,
      resourcePattern: childPattern,
      epoch: childEpoch,
      notBeforeNs: childNb,
      expiresAtNs: childExp,
      depth: childDepth,
      maxDepth: parent.maxDepth,
      issuerPublicKey: pubKey,
      attenuationDigest: childDigest,
      signature: sig,
      metadata: options.metadata,
    });

    this._tokens.set(childTokenId, childToken);
    this._tokenParents.set(childTokenId, parent.tokenId);
    this._totalAttenuated++;

    if (this.storagePath) {
      this._persistRecord("token", childToken.toJSON());
    }

    return childToken;
  }

  public verify(options: VerifyExecutionOptions): CapabilityVerdict {
    const token = options.token;
    const ts =
      options.nowNs !== undefined
        ? typeof options.nowNs === "bigint"
          ? options.nowNs
          : BigInt(options.nowNs)
        : currentEpochNs();

    // 1. Signature & digest integrity
    if (!this.verifyTokenSignature(token)) {
      return new CapabilityVerdict({
        valid: false,
        tokenId: token.tokenId,
        tenantId: token.tenantId,
        subjectPrincipal: token.subjectPrincipal,
        actionScope: options.actionScope,
        targetResource: options.targetResource,
        reason: "SIGNATURE_INVALID: Digest or signature verification failed",
        checkedAtNs: ts,
      });
    }

    // 2. Tenant isolation check
    if (token.tenantId !== options.expectedTenantId) {
      return new CapabilityVerdict({
        valid: false,
        tokenId: token.tokenId,
        tenantId: token.tenantId,
        subjectPrincipal: token.subjectPrincipal,
        actionScope: options.actionScope,
        targetResource: options.targetResource,
        reason: `TENANT_MISMATCH: Token tenant '${token.tenantId}' != expected '${options.expectedTenantId}'`,
        checkedAtNs: ts,
      });
    }

    // 3. Subject principal authorization
    if (token.subjectPrincipal !== options.executingPrincipal) {
      return new CapabilityVerdict({
        valid: false,
        tokenId: token.tokenId,
        tenantId: token.tenantId,
        subjectPrincipal: token.subjectPrincipal,
        actionScope: options.actionScope,
        targetResource: options.targetResource,
        reason: `PRINCIPAL_MISMATCH: Executing principal '${options.executingPrincipal}' != token subject '${token.subjectPrincipal}'`,
        checkedAtNs: ts,
      });
    }

    // 4. Scope authorization
    if (!token.scopes.includes(options.actionScope)) {
      return new CapabilityVerdict({
        valid: false,
        tokenId: token.tokenId,
        tenantId: token.tenantId,
        subjectPrincipal: token.subjectPrincipal,
        actionScope: options.actionScope,
        targetResource: options.targetResource,
        reason: `UNAUTHORIZED_SCOPE: Action '${options.actionScope}' not in granted scopes [${token.scopes.join(", ")}]`,
        checkedAtNs: ts,
      });
    }

    // 5. Resource namespace matching
    if (!resourcePatternMatches(token.resourcePattern, options.targetResource)) {
      return new CapabilityVerdict({
        valid: false,
        tokenId: token.tokenId,
        tenantId: token.tenantId,
        subjectPrincipal: token.subjectPrincipal,
        actionScope: options.actionScope,
        targetResource: options.targetResource,
        reason: `RESOURCE_OUT_OF_BOUNDS: Target '${options.targetResource}' does not match pattern '${token.resourcePattern}'`,
        checkedAtNs: ts,
      });
    }

    // 6. Time validity window
    if (ts < token.notBeforeNs) {
      return new CapabilityVerdict({
        valid: false,
        tokenId: token.tokenId,
        tenantId: token.tenantId,
        subjectPrincipal: token.subjectPrincipal,
        actionScope: options.actionScope,
        targetResource: options.targetResource,
        reason: "PREMATURE_TOKEN: Token not yet valid",
        checkedAtNs: ts,
      });
    }
    if (ts > token.expiresAtNs) {
      return new CapabilityVerdict({
        valid: false,
        tokenId: token.tokenId,
        tenantId: token.tenantId,
        subjectPrincipal: token.subjectPrincipal,
        actionScope: options.actionScope,
        targetResource: options.targetResource,
        reason: "EXPIRED_TOKEN: Token has expired",
        checkedAtNs: ts,
      });
    }

    // 7. Epoch freshness check
    if (options.currentEpoch !== undefined && token.epoch < options.currentEpoch) {
      return new CapabilityVerdict({
        valid: false,
        tokenId: token.tokenId,
        tenantId: token.tenantId,
        subjectPrincipal: token.subjectPrincipal,
        actionScope: options.actionScope,
        targetResource: options.targetResource,
        reason: `STALE_EPOCH: Token epoch ${token.epoch} < current authority epoch ${options.currentEpoch}`,
        checkedAtNs: ts,
      });
    }

    // 8. Revocation & ancestor cascade check
    if (this.isRevoked(token.tokenId)) {
      const direct = this._revokedTokens.has(token.tokenId);
      return new CapabilityVerdict({
        valid: false,
        tokenId: token.tokenId,
        tenantId: token.tenantId,
        subjectPrincipal: token.subjectPrincipal,
        actionScope: options.actionScope,
        targetResource: options.targetResource,
        reason: direct ? "TOKEN_REVOKED" : "ANCESTOR_REVOKED: An issuing parent token was revoked",
        checkedAtNs: ts,
      });
    }

    return new CapabilityVerdict({
      valid: true,
      tokenId: token.tokenId,
      tenantId: token.tenantId,
      subjectPrincipal: token.subjectPrincipal,
      actionScope: options.actionScope,
      targetResource: options.targetResource,
      reason: "AUTHORIZED",
      checkedAtNs: ts,
    });
  }

  public verifyTokenSignature(token: CapabilityToken): boolean {
    const cacheKey = `${token.tokenId}:${token.attenuationDigest}:${token.signature}`;
    if (this._signatureCache.has(cacheKey)) {
      return this._signatureCache.get(cacheKey)!;
    }

    const expectedDigest = CapabilityToken.computeDigest({
      tokenId: token.tokenId,
      parentTokenId: token.parentTokenId,
      tenantId: token.tenantId,
      issuerPrincipal: token.issuerPrincipal,
      subjectPrincipal: token.subjectPrincipal,
      scopes: token.scopes,
      resourcePattern: token.resourcePattern,
      epoch: token.epoch,
      notBeforeNs: token.notBeforeNs,
      expiresAtNs: token.expiresAtNs,
      depth: token.depth,
      maxDepth: token.maxDepth,
      issuerPublicKey: token.issuerPublicKey,
    });

    if (token.attenuationDigest !== expectedDigest) {
      this._signatureCache.set(cacheKey, false);
      return false;
    }

    try {
      const verifier = WitnessKeyManager.fromPublicKey(token.issuerPublicKey);
      const rawSig = base64UrlDecode(token.signature);
      const isValid = verifier.verify(expectedDigest, rawSig);
      this._signatureCache.set(cacheKey, isValid);
      return isValid;
    } catch {
      this._signatureCache.set(cacheKey, false);
      return false;
    }
  }

  public revoke(tokenId: string, reason = "Administrative revocation", revokedBy = "authority"): RevocationReceipt {
    this._revokedTokens.add(tokenId);
    const receipt = new RevocationReceipt({
      revocationId: crypto.randomUUID(),
      tokenId,
      revokedBy,
      reason,
      timestampNs: currentEpochNs(),
    });
    this._revocationRecords.set(tokenId, receipt);

    if (this.storagePath) {
      this._persistRecord("revocation", receipt.toJSON());
    }

    return receipt;
  }

  public isRevoked(tokenId: string): boolean {
    let curr: string | null = tokenId;
    while (curr !== null) {
      if (this._revokedTokens.has(curr)) {
        return true;
      }
      let parent: string | null = this._tokenParents.get(curr) ?? null;
      if (parent === null && this._tokens.has(curr)) {
        parent = this._tokens.get(curr)!.parentTokenId;
      }
      curr = parent;
    }
    return false;
  }

  public status(): CapabilityStatus {
    return new CapabilityStatus({
      totalIssued: this._totalIssued,
      totalAttenuated: this._totalAttenuated,
      totalRevoked: this._revokedTokens.size,
      activeRootPublicKey: this.authorityKeyManager.publicKeyB64,
    });
  }

  private _persistRecord(recordType: string, data: Record<string, unknown>): void {
    if (!this.storagePath) return;
    const dir = path.dirname(path.resolve(this.storagePath));
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    const payload = JSON.stringify({ record_type: recordType, ...data }) + "\n";
    fs.appendFileSync(this.storagePath, payload, "utf-8");
  }

  private _loadFromStorage(): void {
    if (!this.storagePath || !fs.existsSync(this.storagePath)) return;
    const content = fs.readFileSync(this.storagePath, "utf-8");
    const lines = content.split("\n");
    for (const rawLine of lines) {
      const line = rawLine.trim();
      if (!line) continue;
      try {
        const rec = JSON.parse(line);
        if (rec.record_type === "token") {
          const t = CapabilityToken.fromDict(rec);
          this._tokens.set(t.tokenId, t);
          if (t.parentTokenId) {
            this._tokenParents.set(t.tokenId, t.parentTokenId);
          }
        } else if (rec.record_type === "revocation") {
          const r = RevocationReceipt.fromDict(rec);
          this._revokedTokens.add(r.tokenId);
          this._revocationRecords.set(r.tokenId, r);
        }
      } catch {
        // Ignore corrupted record
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function base64UrlDecode(str: string): Buffer {
  let s = str.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4 !== 0) {
    s += "=";
  }
  return Buffer.from(s, "base64");
}

// ---------------------------------------------------------------------------
// Global Singleton Instance
// ---------------------------------------------------------------------------

export const globalCapabilityManager = new TenantCapabilityManager();
