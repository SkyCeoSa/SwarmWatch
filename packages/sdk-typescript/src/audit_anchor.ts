/**
 * Durable Audit Anchor & Merkle Audit Receipt Engine (Seam 54 / ADR 0065).
 * Enforces append-only root commitment chains, post-crash chain of custody,
 * and verifiable Merkle audit receipts surviving system restarts.
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";

export class AuditIntegrityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AuditIntegrityError";
  }
}

export function sha256Digest(content: string | Buffer): string {
  return crypto.createHash("sha256").update(content).digest("hex");
}

function timingSafeEqualStr(a: string, b: string): boolean {
  const bufA = Buffer.from(a, "utf-8");
  const bufB = Buffer.from(b, "utf-8");
  if (bufA.length !== bufB.length) {
    return false;
  }
  return crypto.timingSafeEqual(bufA, bufB);
}

export interface RootEpochOptions {
  epoch: number;
  rootHash: string;
  prevChainDigest: string;
  chainDigest: string;
  tenantId: string;
  anchoredAt?: string;
  signature?: string;
  metadata?: Record<string, unknown>;
}

export class RootEpoch {
  public readonly epoch: number;
  public readonly rootHash: string;
  public readonly prevChainDigest: string;
  public readonly chainDigest: string;
  public readonly tenantId: string;
  public readonly anchoredAt: string;
  public readonly signature: string;
  public readonly metadata: Record<string, unknown>;

  constructor(opts: RootEpochOptions) {
    this.epoch = opts.epoch;
    this.rootHash = opts.rootHash;
    this.prevChainDigest = opts.prevChainDigest;
    this.chainDigest = opts.chainDigest;
    this.tenantId = opts.tenantId;
    this.anchoredAt = opts.anchoredAt || new Date().toISOString();
    this.signature = opts.signature || "";
    this.metadata = opts.metadata ? { ...opts.metadata } : {};
  }

  public toDict(): Record<string, unknown> {
    return {
      epoch: this.epoch,
      root_hash: this.rootHash,
      prev_chain_digest: this.prevChainDigest,
      chain_digest: this.chainDigest,
      tenant_id: this.tenantId,
      anchored_at: this.anchoredAt,
      signature: this.signature,
      metadata: { ...this.metadata },
    };
  }

  public static fromDict(data: Record<string, any>): RootEpoch {
    return new RootEpoch({
      epoch: Number(data.epoch || 0),
      rootHash: String(data.root_hash || ""),
      prevChainDigest: String(data.prev_chain_digest || ""),
      chainDigest: String(data.chain_digest || ""),
      tenantId: String(data.tenant_id || "system"),
      anchoredAt: String(data.anchored_at || new Date().toISOString()),
      signature: String(data.signature || ""),
      metadata: typeof data.metadata === "object" && data.metadata !== null ? { ...data.metadata } : {},
    });
  }
}

export interface MerkleAuditReceiptOptions {
  receiptId: string;
  epoch: number;
  tenantId: string;
  leafHash: string;
  auditPath: Array<[string, string]>;
  rootHash: string;
  chainDigest: string;
  signature?: string;
  issuedAt?: string;
}

export class MerkleAuditReceipt {
  public readonly receiptId: string;
  public readonly epoch: number;
  public readonly tenantId: string;
  public readonly leafHash: string;
  public readonly auditPath: Array<[string, string]>;
  public readonly rootHash: string;
  public readonly chainDigest: string;
  public readonly signature: string;
  public readonly issuedAt: string;

  constructor(opts: MerkleAuditReceiptOptions) {
    this.receiptId = opts.receiptId;
    this.epoch = opts.epoch;
    this.tenantId = opts.tenantId;
    this.leafHash = opts.leafHash;
    this.auditPath = opts.auditPath ? [...opts.auditPath] : [];
    this.rootHash = opts.rootHash;
    this.chainDigest = opts.chainDigest;
    this.signature = opts.signature || "";
    this.issuedAt = opts.issuedAt || new Date().toISOString();
  }

  public toDict(): Record<string, unknown> {
    return {
      receipt_id: this.receiptId,
      epoch: this.epoch,
      tenant_id: this.tenantId,
      leaf_hash: this.leafHash,
      audit_path: this.auditPath.map((step) => [step[0], step[1]]),
      root_hash: this.rootHash,
      chain_digest: this.chainDigest,
      signature: this.signature,
      issued_at: this.issuedAt,
    };
  }

  public static fromDict(data: Record<string, any>): MerkleAuditReceipt {
    const rawPath = Array.isArray(data.audit_path) ? data.audit_path : [];
    const parsedPath: Array<[string, string]> = [];
    for (const step of rawPath) {
      if (Array.isArray(step) && step.length >= 2) {
        parsedPath.push([String(step[0]), String(step[1])]);
      }
    }

    return new MerkleAuditReceipt({
      receiptId: String(data.receipt_id || ""),
      epoch: Number(data.epoch || 0),
      tenantId: String(data.tenant_id || "default-tenant"),
      leafHash: String(data.leaf_hash || ""),
      auditPath: parsedPath,
      rootHash: String(data.root_hash || ""),
      chainDigest: String(data.chain_digest || ""),
      signature: String(data.signature || ""),
      issuedAt: String(data.issued_at || new Date().toISOString()),
    });
  }
}

export interface VerificationResultOptions {
  valid: boolean;
  epoch: number;
  leafHash: string;
  rootHash: string;
  reason?: string | null;
}

export class VerificationResult {
  public readonly valid: boolean;
  public readonly epoch: number;
  public readonly leafHash: string;
  public readonly rootHash: string;
  public readonly reason: string | null;

  constructor(opts: VerificationResultOptions) {
    this.valid = opts.valid;
    this.epoch = opts.epoch;
    this.leafHash = opts.leafHash;
    this.rootHash = opts.rootHash;
    this.reason = opts.reason || null;
  }

  public toDict(): Record<string, unknown> {
    return {
      valid: this.valid,
      epoch: this.epoch,
      leaf_hash: this.leafHash,
      root_hash: this.rootHash,
      reason: this.reason,
    };
  }
}

export class AuditAnchor {
  public static readonly GENESIS_PAYLOAD = "GENESIS_SWARMWATCH_AUDIT_ANCHOR";

  public readonly storagePath: string;
  private readonly _secretKey: string;
  private _epochs: RootEpoch[] = [];
  private _epochsByNum: Map<number, RootEpoch> = new Map();

  constructor(storagePath?: string, secretKey?: string) {
    this._secretKey =
      secretKey || process.env.SWARMWATCH_AUDIT_SECRET_KEY || "sw_audit_anchor_secret_key_v1";
    this.storagePath =
      storagePath !== undefined
        ? storagePath
        : process.env.SWARMWATCH_AUDIT_STORAGE_PATH || "audit_anchor_epochs.jsonl";

    this._loadOrInitialize();
  }

  public get totalEpochs(): number {
    return this._epochs.length;
  }

  public get latestEpoch(): number {
    return this._epochs.length > 0 ? this._epochs[this._epochs.length - 1].epoch : 0;
  }

  public get latestDigest(): string {
    return this._epochs.length > 0 ? this._epochs[this._epochs.length - 1].chainDigest : "";
  }

  private _computeChainDigest(
    prevDigest: string,
    epoch: number,
    rootHash: string,
    tenantId: string
  ): string {
    const seed = `${prevDigest}:${epoch}:${rootHash}:${tenantId}`;
    return sha256Digest(seed);
  }

  private _computeEpochSignature(epoch: number, rootHash: string, chainDigest: string): string {
    const payload = `${epoch}:${rootHash}:${chainDigest}`;
    return crypto.createHmac("sha256", this._secretKey).update(payload).digest("hex");
  }

  private _computeReceiptSignature(
    receiptId: string,
    epoch: number,
    tenantId: string,
    leafHash: string,
    rootHash: string,
    chainDigest: string
  ): string {
    const payload = `${receiptId}:${epoch}:${tenantId}:${leafHash}:${rootHash}:${chainDigest}`;
    return crypto.createHmac("sha256", this._secretKey).update(payload).digest("hex");
  }

  private _createGenesisEpoch(): RootEpoch {
    const genesisRoot = sha256Digest(AuditAnchor.GENESIS_PAYLOAD);
    const genesisChainDigest = sha256Digest(`0:${genesisRoot}`);
    const genesisSig = this._computeEpochSignature(0, genesisRoot, genesisChainDigest);
    return new RootEpoch({
      epoch: 0,
      rootHash: genesisRoot,
      prevChainDigest: "0".repeat(64),
      chainDigest: genesisChainDigest,
      tenantId: "system",
      anchoredAt: new Date().toISOString(),
      signature: genesisSig,
      metadata: { description: "Genesis Audit Anchor Root" },
    });
  }

  private _loadOrInitialize(): void {
    this._epochs = [];
    this._epochsByNum.clear();

    if (this.storagePath !== ":memory:" && fs.existsSync(this.storagePath)) {
      const content = fs.readFileSync(this.storagePath, "utf-8");
      const lines = content.split("\n");
      const loadedEpochs: RootEpoch[] = [];

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        try {
          const record = JSON.parse(line);
          const ep = RootEpoch.fromDict(record);
          loadedEpochs.push(ep);
        } catch (e: any) {
          throw new AuditIntegrityError(
            `Corrupted record at line ${i + 1} in ${this.storagePath}: ${e.message}`
          );
        }
      }

      if (loadedEpochs.length > 0) {
        this._epochs = loadedEpochs;
        for (const ep of this._epochs) {
          this._epochsByNum.set(ep.epoch, ep);
        }
        this._verifyChainIntegrity();
        return;
      }
    }

    // Initialize Genesis
    const genesis = this._createGenesisEpoch();
    this._epochs.push(genesis);
    this._epochsByNum.set(genesis.epoch, genesis);

    if (this.storagePath !== ":memory:") {
      const dir = path.dirname(this.storagePath);
      if (dir && !fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      fs.writeFileSync(this.storagePath, JSON.stringify(genesis.toDict()) + "\n", "utf-8");
    }
  }

  private _verifyChainIntegrity(): void {
    if (this._epochs.length === 0) {
      throw new AuditIntegrityError("Empty audit epoch history; Genesis is required.");
    }

    // 1. Genesis verification
    const genesis = this._epochs[0];
    if (genesis.epoch !== 0) {
      throw new AuditIntegrityError(`First epoch must be 0 (Genesis), found ${genesis.epoch}.`);
    }
    const expectedGenRoot = sha256Digest(AuditAnchor.GENESIS_PAYLOAD);
    if (!timingSafeEqualStr(genesis.rootHash, expectedGenRoot)) {
      throw new AuditIntegrityError("Genesis root hash does not match canonical Genesis payload.");
    }
    const expectedGenDigest = sha256Digest(`0:${expectedGenRoot}`);
    if (!timingSafeEqualStr(genesis.chainDigest, expectedGenDigest)) {
      throw new AuditIntegrityError("Genesis chain digest does not match expected canonical digest.");
    }
    const expectedGenSig = this._computeEpochSignature(0, expectedGenRoot, expectedGenDigest);
    if (!timingSafeEqualStr(genesis.signature, expectedGenSig)) {
      throw new AuditIntegrityError("Invalid cryptographic HMAC signature on Genesis epoch.");
    }

    // 2. Monotonic succession verification
    for (let i = 1; i < this._epochs.length; i++) {
      const prevEp = this._epochs[i - 1];
      const currEp = this._epochs[i];

      if (currEp.epoch !== prevEp.epoch + 1) {
        throw new AuditIntegrityError(
          `Epoch discontinuity detected: epoch ${currEp.epoch} follows ${prevEp.epoch}.`
        );
      }

      if (!timingSafeEqualStr(currEp.prevChainDigest, prevEp.chainDigest)) {
        throw new AuditIntegrityError(
          `Chain digest link broken between epoch ${prevEp.epoch} and ${currEp.epoch}.`
        );
      }

      const expectedDigest = this._computeChainDigest(
        currEp.prevChainDigest,
        currEp.epoch,
        currEp.rootHash,
        currEp.tenantId
      );
      if (!timingSafeEqualStr(currEp.chainDigest, expectedDigest)) {
        throw new AuditIntegrityError(
          `Chain digest mismatch at epoch ${currEp.epoch}. History rewriting detected!`
        );
      }

      const expectedSig = this._computeEpochSignature(
        currEp.epoch,
        currEp.rootHash,
        currEp.chainDigest
      );
      if (!timingSafeEqualStr(currEp.signature, expectedSig)) {
        throw new AuditIntegrityError(
          `Invalid cryptographic HMAC signature at epoch ${currEp.epoch}.`
        );
      }
    }
  }

  public appendRoot(
    rootHash: string,
    tenantId: string = "default-tenant",
    metadata?: Record<string, unknown>
  ): RootEpoch {
    if (!rootHash) {
      throw new Error("rootHash cannot be empty");
    }

    const nextEpochNum = this._epochs.length;
    const prevDigest = this._epochs[this._epochs.length - 1].chainDigest;

    const newChainDigest = this._computeChainDigest(
      prevDigest,
      nextEpochNum,
      rootHash,
      tenantId
    );
    const sig = this._computeEpochSignature(nextEpochNum, rootHash, newChainDigest);

    const epochRecord = new RootEpoch({
      epoch: nextEpochNum,
      rootHash,
      prevChainDigest: prevDigest,
      chainDigest: newChainDigest,
      tenantId,
      anchoredAt: new Date().toISOString(),
      signature: sig,
      metadata: metadata || {},
    });

    this._epochs.push(epochRecord);
    this._epochsByNum.set(epochRecord.epoch, epochRecord);

    if (this.storagePath !== ":memory:") {
      const dir = path.dirname(this.storagePath);
      if (dir && !fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      fs.appendFileSync(this.storagePath, JSON.stringify(epochRecord.toDict()) + "\n", "utf-8");
    }

    return epochRecord;
  }

  public issueReceipt(
    epoch: number,
    leafHash: string,
    auditPath: Array<[string, string]>,
    tenantId: string = "default-tenant"
  ): MerkleAuditReceipt {
    if (!this._epochsByNum.has(epoch)) {
      throw new Error(`Epoch ${epoch} does not exist in audit anchor ledger.`);
    }

    const anchoredEpoch = this._epochsByNum.get(epoch)!;
    const targetRoot = anchoredEpoch.rootHash;

    // Validate Merkle folding
    let currentHash = leafHash;
    for (const [direction, sibling] of auditPath) {
      let combined: string;
      if (direction === "left") {
        combined = sibling + currentHash;
      } else {
        combined = currentHash + sibling;
      }
      currentHash = sha256Digest(combined);
    }

    if (!timingSafeEqualStr(currentHash, targetRoot)) {
      throw new Error(
        `Supplied Merkle audit path folds to '${currentHash}', which does not match epoch ${epoch} root '${targetRoot}'.`
      );
    }

    const receiptId = `rcpt_${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`;
    const sig = this._computeReceiptSignature(
      receiptId,
      epoch,
      tenantId,
      leafHash,
      targetRoot,
      anchoredEpoch.chainDigest
    );

    return new MerkleAuditReceipt({
      receiptId,
      epoch,
      tenantId,
      leafHash,
      auditPath,
      rootHash: targetRoot,
      chainDigest: anchoredEpoch.chainDigest,
      signature: sig,
      issuedAt: new Date().toISOString(),
    });
  }

  public verifyReceipt(
    receipt: MerkleAuditReceipt | Record<string, any>
  ): VerificationResult {
    const rcpt =
      receipt instanceof MerkleAuditReceipt ? receipt : MerkleAuditReceipt.fromDict(receipt);

    // 1. Fold Merkle path
    let currentHash = rcpt.leafHash;
    for (const [direction, sibling] of rcpt.auditPath) {
      let combined: string;
      if (direction === "left") {
        combined = sibling + currentHash;
      } else {
        combined = currentHash + sibling;
      }
      currentHash = sha256Digest(combined);
    }

    if (!timingSafeEqualStr(currentHash, rcpt.rootHash)) {
      return new VerificationResult({
        valid: false,
        epoch: rcpt.epoch,
        leafHash: rcpt.leafHash,
        rootHash: rcpt.rootHash,
        reason: `Merkle path reconstruction resulted in '${currentHash}', mismatching receipt root '${rcpt.rootHash}'.`,
      });
    }

    // 2. Check epoch existence
    if (!this._epochsByNum.has(rcpt.epoch)) {
      return new VerificationResult({
        valid: false,
        epoch: rcpt.epoch,
        leafHash: rcpt.leafHash,
        rootHash: rcpt.rootHash,
        reason: `Epoch ${rcpt.epoch} not found in durable audit anchor.`,
      });
    }

    const anchoredEpoch = this._epochsByNum.get(rcpt.epoch)!;

    // 3. Check anchored root match
    if (!timingSafeEqualStr(rcpt.rootHash, anchoredEpoch.rootHash)) {
      return new VerificationResult({
        valid: false,
        epoch: rcpt.epoch,
        leafHash: rcpt.leafHash,
        rootHash: rcpt.rootHash,
        reason: `Receipt root_hash does not match anchored root for epoch ${rcpt.epoch}.`,
      });
    }

    // 4. Check chain digest continuity
    if (!timingSafeEqualStr(rcpt.chainDigest, anchoredEpoch.chainDigest)) {
      return new VerificationResult({
        valid: false,
        epoch: rcpt.epoch,
        leafHash: rcpt.leafHash,
        rootHash: rcpt.rootHash,
        reason: `Receipt chain_digest does not match anchored chain_digest for epoch ${rcpt.epoch}.`,
      });
    }

    // 5. Verify HMAC receipt signature
    const expectedSig = this._computeReceiptSignature(
      rcpt.receiptId,
      rcpt.epoch,
      rcpt.tenantId,
      rcpt.leafHash,
      rcpt.rootHash,
      rcpt.chainDigest
    );
    if (!timingSafeEqualStr(rcpt.signature, expectedSig)) {
      return new VerificationResult({
        valid: false,
        epoch: rcpt.epoch,
        leafHash: rcpt.leafHash,
        rootHash: rcpt.rootHash,
        reason: "Invalid cryptographic HMAC receipt signature.",
      });
    }

    return new VerificationResult({
      valid: true,
      epoch: rcpt.epoch,
      leafHash: rcpt.leafHash,
      rootHash: rcpt.rootHash,
      reason: null,
    });
  }

  public auditChain(sinceEpoch: number = 0): Record<string, any> {
    const errors: string[] = [];
    let isValid = true;
    try {
      this._verifyChainIntegrity();
    } catch (err: any) {
      isValid = false;
      errors.push(err.message);
    }

    return {
      valid: isValid,
      total_epochs: this._epochs.length,
      since_epoch: sinceEpoch,
      genesis_hash: this._epochs.length > 0 ? this._epochs[0].rootHash : "",
      latest_epoch: this.latestEpoch,
      latest_digest: this.latestDigest,
      errors,
    };
  }

  public status(tenantId?: string): Record<string, any> {
    const latest = this._epochs.length > 0 ? this._epochs[this._epochs.length - 1] : null;
    return {
      status: "ANCHORED",
      total_epochs: this._epochs.length,
      latest_epoch: latest ? latest.epoch : 0,
      latest_digest: latest ? latest.chainDigest : "",
      latest_root: latest ? latest.rootHash : "",
      genesis_hash: this._epochs.length > 0 ? this._epochs[0].rootHash : "",
      storage_path: this.storagePath,
      tenant_id: tenantId || "all",
    };
  }

  public reset(clearStorage: boolean = false): void {
    this._epochs = [];
    this._epochsByNum.clear();
    if (clearStorage && this.storagePath !== ":memory:" && fs.existsSync(this.storagePath)) {
      try {
        fs.unlinkSync(this.storagePath);
      } catch (e) {}
    }
    this._loadOrInitialize();
  }
}

export const globalAuditAnchor = new AuditAnchor();
