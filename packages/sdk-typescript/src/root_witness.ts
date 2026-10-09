/**
 * Independent Root Witness & Public-Key Chains (Seam 64 / ADR 0075).
 * Implements asymmetric ECDSA (NIST P-256 / ES256) signing, RFC 6962 Certificate
 * Transparency logarithmic consistency proofs, rollback and whole-chain replacement
 * attack detection, NIST SP 800-57 verifiable key rotation logs, and sub-250µs verification.
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

export class WitnessError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WitnessError";
  }
}

export class RollbackAttackError extends WitnessError {
  constructor(message: string) {
    super(message);
    this.name = "RollbackAttackError";
  }
}

export class ConsistencyVerificationError extends WitnessError {
  constructor(message: string) {
    super(message);
    this.name = "ConsistencyVerificationError";
  }
}

export class SplitBrainForkError extends WitnessError {
  constructor(message: string) {
    super(message);
    this.name = "SplitBrainForkError";
  }
}

export class KeyRotationError extends WitnessError {
  constructor(message: string) {
    super(message);
    this.name = "KeyRotationError";
  }
}

// ---------------------------------------------------------------------------
// Base64URL and Hashing Utilities
// ---------------------------------------------------------------------------

export function base64UrlEncode(buffer: Buffer | Uint8Array | string): string {
  const buf = typeof buffer === "string" ? Buffer.from(buffer, "utf-8") : Buffer.from(buffer);
  return buf
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=/g, "");
}

export function base64UrlDecode(str: string): Buffer {
  let s = str.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4 !== 0) {
    s += "=";
  }
  return Buffer.from(s, "base64");
}

export function sha256Digest(content: string | Buffer | Uint8Array): string {
  const hash = crypto.createHash("sha256");
  hash.update(content);
  return hash.digest("hex");
}

// SPKI header for uncompressed NIST P-256 (prime256v1 / secp256r1) EC public key
const P256_SPKI_HEADER = Buffer.from(
  "3059301306072a8648ce3d020106082a8648ce3d030107034200",
  "hex"
);

// ---------------------------------------------------------------------------
// Key Management: WitnessKeyManager
// ---------------------------------------------------------------------------

export class WitnessKeyManager {
  public readonly publicKeyB64: string;
  public readonly privateKeyB64?: string;
  private readonly _privateKeyPem?: string;
  private readonly _privateKeyObj?: crypto.KeyObject;
  private readonly _publicKeyObj: crypto.KeyObject;
  private readonly _rawPublicKey: Buffer;

  constructor(options?: {
    privateKeyB64?: string;
    publicKeyB64?: string;
    privateKeyPem?: string;
  }) {
    if (options?.privateKeyPem) {
      this._privateKeyPem = options.privateKeyPem;
      this._privateKeyObj = crypto.createPrivateKey(this._privateKeyPem);
      if (options.publicKeyB64) {
        this.publicKeyB64 = options.publicKeyB64;
        this._rawPublicKey = base64UrlDecode(options.publicKeyB64);
        const spkiDer = Buffer.concat([P256_SPKI_HEADER, this._rawPublicKey]);
        this._publicKeyObj = crypto.createPublicKey({ key: spkiDer, format: "der", type: "spki" });
      } else {
        const pubKeyObj = crypto.createPublicKey(this._privateKeyPem);
        const spkiDer = pubKeyObj.export({ type: "spki", format: "der" });
        this._rawPublicKey = spkiDer.subarray(spkiDer.length - 65);
        this.publicKeyB64 = base64UrlEncode(this._rawPublicKey);
        this._publicKeyObj = pubKeyObj;
      }
      this.privateKeyB64 = options.privateKeyB64;
    } else if (options?.publicKeyB64) {
      this.publicKeyB64 = options.publicKeyB64;
      this._rawPublicKey = base64UrlDecode(options.publicKeyB64);
      if (this._rawPublicKey.length !== 65 || this._rawPublicKey[0] !== 0x04) {
        throw new Error("Invalid public key: expected 65-byte uncompressed point starting with 0x04");
      }
      const spkiDer = Buffer.concat([P256_SPKI_HEADER, this._rawPublicKey]);
      this._publicKeyObj = crypto.createPublicKey({ key: spkiDer, format: "der", type: "spki" });
      this.privateKeyB64 = options.privateKeyB64;
    } else {
      // Generate new EC prime256v1 (NIST P-256) keypair
      const keyPair = crypto.generateKeyPairSync("ec", {
        namedCurve: "prime256v1",
        publicKeyEncoding: { type: "spki", format: "der" },
        privateKeyEncoding: { type: "pkcs8", format: "pem" },
      });
      this._rawPublicKey = keyPair.publicKey.subarray(keyPair.publicKey.length - 65);
      this.publicKeyB64 = base64UrlEncode(this._rawPublicKey);
      this._privateKeyPem = keyPair.privateKey;
      this.privateKeyB64 = base64UrlEncode(Buffer.from(keyPair.privateKey, "utf-8"));
      this._privateKeyObj = crypto.createPrivateKey(keyPair.privateKey);
      this._publicKeyObj = crypto.createPublicKey({ key: keyPair.publicKey, format: "der", type: "spki" });
    }
  }

  public static generate(): WitnessKeyManager {
    return new WitnessKeyManager();
  }

  public static fromPublicKey(publicKeyB64: string): WitnessKeyManager {
    return new WitnessKeyManager({ publicKeyB64 });
  }

  public getRawPublicKey(): Buffer {
    return this._rawPublicKey;
  }

  public sign(message: Buffer | Uint8Array | string): Buffer {
    if (!this._privateKeyObj) {
      throw new WitnessError("Cannot sign without private key");
    }
    const signer = crypto.createSign("SHA256");
    signer.update(message);
    return signer.sign({
      key: this._privateKeyObj,
      dsaEncoding: "ieee-p1363",
    });
  }

  public signB64(message: Buffer | Uint8Array | string): string {
    return base64UrlEncode(this.sign(message));
  }

  public verify(message: Buffer | Uint8Array | string, signature: Buffer | Uint8Array): boolean {
    try {
      const verifier = crypto.createVerify("SHA256");
      verifier.update(message);
      return verifier.verify(
        {
          key: this._publicKeyObj,
          dsaEncoding: "ieee-p1363",
        },
        signature
      );
    } catch {
      return false;
    }
  }

  public verifyB64(message: Buffer | Uint8Array | string, signatureB64: string): boolean {
    try {
      const sigBuf = base64UrlDecode(signatureB64);
      return this.verify(message, sigBuf);
    } catch {
      return false;
    }
  }
}

// ---------------------------------------------------------------------------
// Merkle Tree & RFC 6962 Consistency Proof Primitives
// ---------------------------------------------------------------------------

export function leafHash(data: string | Buffer | Uint8Array): string {
  const content = typeof data === "string" ? Buffer.from(data, "utf-8") : Buffer.from(data);
  const hash = crypto.createHash("sha256");
  hash.update(Buffer.concat([Buffer.from([0x00]), content]));
  return hash.digest("hex");
}

export function nodeHash(leftHex: string, rightHex: string): string {
  const left = Buffer.from(leftHex, "hex");
  const right = Buffer.from(rightHex, "hex");
  const hash = crypto.createHash("sha256");
  hash.update(Buffer.concat([Buffer.from([0x01]), left, right]));
  return hash.digest("hex");
}

function largestPowerOf2LessThan(n: number): number {
  if (n <= 1) return 0;
  let k = 1;
  while (k < n) {
    k <<= 1;
  }
  return k >> 1;
}

export function mth(leaves: string[]): string {
  const n = leaves.length;
  if (n === 0) return sha256Digest(Buffer.alloc(0));
  if (n === 1) return leaves[0];
  const k = largestPowerOf2LessThan(n);
  return nodeHash(mth(leaves.slice(0, k)), mth(leaves.slice(k)));
}

function subproof(m: number, leaves: string[], b: boolean): string[] {
  const n = leaves.length;
  if (m === n) {
    return b ? [] : [mth(leaves)];
  }
  const k = largestPowerOf2LessThan(n);
  if (m <= k) {
    return [...subproof(m, leaves.slice(0, k), b), mth(leaves.slice(k))];
  } else {
    return [...subproof(m - k, leaves.slice(k), false), mth(leaves.slice(0, k))];
  }
}

export function consistencyProof(m: number, leaves: string[]): string[] {
  const n = leaves.length;
  if (m === n || m === 0) return [];
  const k = largestPowerOf2LessThan(n);
  if (m <= k) {
    return [...subproof(m, leaves.slice(0, k), true), mth(leaves.slice(k))];
  } else {
    return [...subproof(m - k, leaves.slice(k), false), mth(leaves.slice(0, k))];
  }
}

export function verifyConsistencyProof(
  m: number,
  n: number,
  oldRoot: string,
  newRoot: string,
  proof: string[]
): boolean {
  if (m === n) {
    return proof.length === 0 && oldRoot === newRoot;
  }
  if (m === 0 || m > n) return false;
  if (!proof || proof.length === 0) return false;

  let p = [...proof];
  if ((m & (m - 1)) === 0) {
    p = [oldRoot, ...p];
  }

  let fn = m - 1;
  let sn = n - 1;
  while ((fn & 1) === 1) {
    fn >>= 1;
    sn >>= 1;
  }

  if (p.length === 0) return false;
  let fr = p[0];
  let sr = p[0];
  p = p.slice(1);

  for (const c of p) {
    if (sn === 0) return false;
    if ((fn & 1) === 1 || fn === sn) {
      fr = nodeHash(c, fr);
      sr = nodeHash(c, sr);
      while ((fn & 1) === 0 && fn !== 0) {
        fn >>= 1;
        sn >>= 1;
      }
    } else {
      sr = nodeHash(sr, c);
    }
    fn >>= 1;
    sn >>= 1;
  }

  return fr === oldRoot && sr === newRoot && sn === 0;
}

// ---------------------------------------------------------------------------
// Data Models
// ---------------------------------------------------------------------------

export class LeafReceipt {
  public readonly leafIndex: number;
  public readonly leafHash: string;
  public readonly timestampNs: number;
  public readonly metadata: Record<string, unknown>;

  constructor(params: {
    leafIndex: number;
    leafHash: string;
    timestampNs?: number;
    metadata?: Record<string, unknown>;
  }) {
    this.leafIndex = params.leafIndex;
    this.leafHash = params.leafHash;
    this.timestampNs = params.timestampNs ?? Date.now() * 1_000_000;
    this.metadata = params.metadata ?? {};
  }

  public toDict(): Record<string, unknown> {
    return {
      leaf_index: this.leafIndex,
      leaf_hash: this.leafHash,
      timestamp_ns: this.timestampNs,
      metadata: { ...this.metadata },
    };
  }

  public static fromDict(data: Record<string, unknown>): LeafReceipt {
    return new LeafReceipt({
      leafIndex: Number(data.leaf_index ?? 0),
      leafHash: String(data.leaf_hash ?? ""),
      timestampNs: Number(data.timestamp_ns ?? 0),
      metadata: (data.metadata as Record<string, unknown>) ?? {},
    });
  }
}

export class SignedCheckpoint {
  public readonly epoch: number;
  public readonly treeSize: number;
  public readonly rootHash: string;
  public readonly prevCheckpointHash: string;
  public readonly checkpointHash: string;
  public readonly witnessPublicKey: string;
  public readonly signature: string;
  public readonly timestampNs: number;
  public readonly metadata: Record<string, unknown>;

  constructor(params: {
    epoch: number;
    treeSize: number;
    rootHash: string;
    prevCheckpointHash: string;
    checkpointHash: string;
    witnessPublicKey: string;
    signature: string;
    timestampNs?: number;
    metadata?: Record<string, unknown>;
  }) {
    this.epoch = params.epoch;
    this.treeSize = params.treeSize;
    this.rootHash = params.rootHash;
    this.prevCheckpointHash = params.prevCheckpointHash;
    this.checkpointHash = params.checkpointHash;
    this.witnessPublicKey = params.witnessPublicKey;
    this.signature = params.signature;
    this.timestampNs = params.timestampNs ?? Date.now() * 1_000_000;
    this.metadata = params.metadata ?? {};
  }

  public static computeCheckpointHash(
    epoch: number,
    treeSize: number,
    rootHash: string,
    prevCheckpointHash: string,
    timestampNs: number,
    witnessPublicKey: string
  ): string {
    const seed = `sw:cp:v1:${epoch}:${treeSize}:${rootHash}:${prevCheckpointHash}:${timestampNs}:${witnessPublicKey}`;
    return sha256Digest(seed);
  }

  public toDict(): Record<string, unknown> {
    return {
      epoch: this.epoch,
      tree_size: this.treeSize,
      root_hash: this.rootHash,
      prev_checkpoint_hash: this.prevCheckpointHash,
      checkpoint_hash: this.checkpointHash,
      witness_public_key: this.witnessPublicKey,
      signature: this.signature,
      timestamp_ns: this.timestampNs,
      metadata: { ...this.metadata },
    };
  }

  public static fromDict(data: Record<string, unknown>): SignedCheckpoint {
    return new SignedCheckpoint({
      epoch: Number(data.epoch ?? 0),
      treeSize: Number(data.tree_size ?? 0),
      rootHash: String(data.root_hash ?? ""),
      prevCheckpointHash: String(data.prev_checkpoint_hash ?? ""),
      checkpointHash: String(data.checkpoint_hash ?? ""),
      witnessPublicKey: String(data.witness_public_key ?? ""),
      signature: String(data.signature ?? ""),
      timestampNs: Number(data.timestamp_ns ?? 0),
      metadata: (data.metadata as Record<string, unknown>) ?? {},
    });
  }
}

export class ConsistencyProof {
  public readonly oldSize: number;
  public readonly newSize: number;
  public readonly oldRoot: string;
  public readonly newRoot: string;
  public readonly proofHashes: string[];

  constructor(params: {
    oldSize: number;
    newSize: number;
    oldRoot: string;
    newRoot: string;
    proofHashes: string[];
  }) {
    this.oldSize = params.oldSize;
    this.newSize = params.newSize;
    this.oldRoot = params.oldRoot;
    this.newRoot = params.newRoot;
    this.proofHashes = [...params.proofHashes];
  }

  public toDict(): Record<string, unknown> {
    return {
      old_size: this.oldSize,
      new_size: this.newSize,
      old_root: this.oldRoot,
      new_root: this.newRoot,
      proof_hashes: [...this.proofHashes],
    };
  }

  public static fromDict(data: Record<string, unknown>): ConsistencyProof {
    return new ConsistencyProof({
      oldSize: Number(data.old_size ?? 0),
      newSize: Number(data.new_size ?? 0),
      oldRoot: String(data.old_root ?? ""),
      newRoot: String(data.new_root ?? ""),
      proofHashes: Array.isArray(data.proof_hashes)
        ? data.proof_hashes.map(String)
        : [],
    });
  }
}

export class KeyRotationRecord {
  public readonly rotationId: string;
  public readonly oldPublicKey: string;
  public newPublicKey: string;
  public readonly rotationEpoch: number;
  public readonly timestampNs: number;
  public readonly challenge: string;
  public readonly proofOfPossession: string;
  public readonly authorizationSignature: string;

  constructor(params: {
    rotationId: string;
    oldPublicKey: string;
    newPublicKey: string;
    rotationEpoch: number;
    timestampNs: number;
    challenge: string;
    proofOfPossession: string;
    authorizationSignature: string;
  }) {
    this.rotationId = params.rotationId;
    this.oldPublicKey = params.oldPublicKey;
    this.newPublicKey = params.newPublicKey;
    this.rotationEpoch = params.rotationEpoch;
    this.timestampNs = params.timestampNs;
    this.challenge = params.challenge;
    this.proofOfPossession = params.proofOfPossession;
    this.authorizationSignature = params.authorizationSignature;
  }

  public static computeChallenge(
    oldPublicKey: string,
    newPublicKey: string,
    rotationEpoch: number,
    timestampNs: number
  ): string {
    const seed = `sw:rot:v1:${oldPublicKey}:${newPublicKey}:${rotationEpoch}:${timestampNs}`;
    return sha256Digest(seed);
  }

  public toDict(): Record<string, unknown> {
    return {
      rotation_id: this.rotationId,
      old_public_key: this.oldPublicKey,
      new_public_key: this.newPublicKey,
      rotation_epoch: this.rotationEpoch,
      timestamp_ns: this.timestampNs,
      challenge: this.challenge,
      proof_of_possession: this.proofOfPossession,
      authorization_signature: this.authorizationSignature,
    };
  }

  public static fromDict(data: Record<string, unknown>): KeyRotationRecord {
    return new KeyRotationRecord({
      rotationId: String(data.rotation_id ?? ""),
      oldPublicKey: String(data.old_public_key ?? ""),
      newPublicKey: String(data.new_public_key ?? ""),
      rotationEpoch: Number(data.rotation_epoch ?? 0),
      timestampNs: Number(data.timestamp_ns ?? 0),
      challenge: String(data.challenge ?? ""),
      proofOfPossession: String(data.proof_of_possession ?? ""),
      authorizationSignature: String(data.authorization_signature ?? ""),
    });
  }
}

export class WitnessStatus {
  public readonly activePublicKey: string;
  public readonly latestEpoch: number;
  public readonly currentTreeSize: number;
  public readonly latestCheckpointHash: string;
  public readonly totalRotations: number;
  public readonly totalCheckpoints: number;

  constructor(params: {
    activePublicKey: string;
    latestEpoch: number;
    currentTreeSize: number;
    latestCheckpointHash: string;
    totalRotations: number;
    totalCheckpoints: number;
  }) {
    this.activePublicKey = params.activePublicKey;
    this.latestEpoch = params.latestEpoch;
    this.currentTreeSize = params.currentTreeSize;
    this.latestCheckpointHash = params.latestCheckpointHash;
    this.totalRotations = params.totalRotations;
    this.totalCheckpoints = params.totalCheckpoints;
  }

  public toDict(): Record<string, unknown> {
    return {
      active_public_key: this.activePublicKey,
      latest_epoch: this.latestEpoch,
      current_tree_size: this.currentTreeSize,
      latest_checkpoint_hash: this.latestCheckpointHash,
      total_rotations: this.totalRotations,
      total_checkpoints: this.totalCheckpoints,
    };
  }
}

// ---------------------------------------------------------------------------
// Core Deep Module: RootWitness
// ---------------------------------------------------------------------------

export class RootWitness {
  public static readonly GENESIS_PREV_HASH = "0".repeat(64);

  public keyManager: WitnessKeyManager;
  public readonly storagePath?: string;
  private readonly _leaves: string[] = [];
  private readonly _checkpoints: SignedCheckpoint[] = [];
  private readonly _rotationLog: KeyRotationRecord[] = [];

  constructor(options?: {
    keyManager?: WitnessKeyManager;
    storagePath?: string;
  }) {
    this.keyManager = options?.keyManager ?? WitnessKeyManager.generate();
    this.storagePath = options?.storagePath;

    if (this.storagePath && fs.existsSync(this.storagePath)) {
      this._loadFromStorage();
    }
  }

  public get currentTreeSize(): number {
    const storedSize = this._checkpoints.length > 0 ? this._checkpoints[this._checkpoints.length - 1].treeSize : 0;
    return Math.max(this._leaves.length, storedSize);
  }

  public get totalCheckpoints(): number {
    return this._checkpoints.length;
  }

  public get latestEpoch(): number {
    return this._checkpoints.length > 0 ? this._checkpoints[this._checkpoints.length - 1].epoch : -1;
  }

  public submit(
    leafData: string | Buffer | Uint8Array,
    metadata?: Record<string, unknown>
  ): LeafReceipt {
    const lHash = leafHash(leafData);
    const index = this._leaves.length;
    this._leaves.push(lHash);

    const receipt = new LeafReceipt({
      leafIndex: index,
      leafHash: lHash,
      timestampNs: Date.now() * 1_000_000,
      metadata,
    });

    if (this.storagePath) {
      this._persistLeaf(receipt);
    }

    return receipt;
  }

  public checkpoint(metadata?: Record<string, unknown>): SignedCheckpoint {
    const epoch = this._checkpoints.length;
    const treeSize = this._leaves.length;
    const root = mth(this._leaves);

    const prevHash =
      this._checkpoints.length > 0
        ? this._checkpoints[this._checkpoints.length - 1].checkpointHash
        : RootWitness.GENESIS_PREV_HASH;

    const nowNs = Date.now() * 1_000_000;
    const pubKey = this.keyManager.publicKeyB64;

    const cpHash = SignedCheckpoint.computeCheckpointHash(
      epoch,
      treeSize,
      root,
      prevHash,
      nowNs,
      pubKey
    );

    const signature = this.keyManager.signB64(Buffer.from(cpHash, "utf-8"));

    const cp = new SignedCheckpoint({
      epoch,
      treeSize,
      rootHash: root,
      prevCheckpointHash: prevHash,
      checkpointHash: cpHash,
      witnessPublicKey: pubKey,
      signature,
      timestampNs: nowNs,
      metadata,
    });

    this._checkpoints.push(cp);

    if (this.storagePath) {
      this._persistCheckpoint(cp);
    }

    return cp;
  }

  public getLatestCheckpoint(): SignedCheckpoint | null {
    return this._checkpoints.length > 0 ? this._checkpoints[this._checkpoints.length - 1] : null;
  }

  public getCheckpoint(epoch: number = -1): SignedCheckpoint {
    if (this._checkpoints.length === 0) {
      throw new WitnessError("No checkpoints exist in witness");
    }
    if (epoch === -1) {
      return this._checkpoints[this._checkpoints.length - 1];
    }
    if (epoch >= 0 && epoch < this._checkpoints.length) {
      return this._checkpoints[epoch];
    }
    throw new WitnessError(`Checkpoint for epoch ${epoch} not found`);
  }

  public getConsistencyProof(oldSize: number, newSize: number): ConsistencyProof {
    if (oldSize > newSize || newSize > this._leaves.length) {
      throw new WitnessError(
        `Invalid consistency proof bounds: ${oldSize} to ${newSize} (current: ${this._leaves.length})`
      );
    }

    const oldRoot = mth(this._leaves.slice(0, oldSize));
    const newRoot = mth(this._leaves.slice(0, newSize));
    const proofHashes = consistencyProof(oldSize, this._leaves.slice(0, newSize));

    return new ConsistencyProof({
      oldSize,
      newSize,
      oldRoot,
      newRoot,
      proofHashes,
    });
  }

  public ingestExternalCheckpoint(checkpoint: SignedCheckpoint): boolean {
    // 1. Verify asymmetric signature
    if (!RootWitness.verifyCheckpoint(checkpoint, checkpoint.witnessPublicKey)) {
      throw new WitnessError("Checkpoint signature verification failed");
    }

    if (this._checkpoints.length > 0) {
      const latest = this._checkpoints[this._checkpoints.length - 1];
      // 2. Check for rollback attacks
      if (checkpoint.epoch <= latest.epoch) {
        throw new RollbackAttackError(
          `Rollback attack detected: incoming epoch ${checkpoint.epoch} <= active epoch ${latest.epoch}`
        );
      }
      if (checkpoint.treeSize < latest.treeSize) {
        throw new RollbackAttackError(
          `Tree size truncation detected: incoming size ${checkpoint.treeSize} < active size ${latest.treeSize}`
        );
      }

      // 3. Check for split-brain fork
      if (checkpoint.prevCheckpointHash !== latest.checkpointHash) {
        throw new SplitBrainForkError(
          `Split-brain fork detected: incoming prev_hash ${checkpoint.prevCheckpointHash} ` +
            `does not match active head ${latest.checkpointHash}`
        );
      }
    }

    this._checkpoints.push(checkpoint);
    if (this.storagePath) {
      this._persistCheckpoint(checkpoint);
    }
    return true;
  }

  public static verifyCheckpoint(
    checkpoint: SignedCheckpoint,
    publicKey?: string
  ): boolean {
    const targetPub = publicKey || checkpoint.witnessPublicKey;
    if (!targetPub) return false;

    const expectedHash = SignedCheckpoint.computeCheckpointHash(
      checkpoint.epoch,
      checkpoint.treeSize,
      checkpoint.rootHash,
      checkpoint.prevCheckpointHash,
      checkpoint.timestampNs,
      targetPub
    );

    if (checkpoint.checkpointHash !== expectedHash) {
      return false;
    }

    try {
      const verifier = WitnessKeyManager.fromPublicKey(targetPub);
      return verifier.verifyB64(Buffer.from(expectedHash, "utf-8"), checkpoint.signature);
    } catch {
      return false;
    }
  }

  public static verifyConsistency(
    oldCheckpoint: SignedCheckpoint,
    newCheckpoint: SignedCheckpoint,
    proof: ConsistencyProof
  ): boolean {
    if (
      proof.oldSize !== oldCheckpoint.treeSize ||
      proof.newSize !== newCheckpoint.treeSize
    ) {
      throw new ConsistencyVerificationError(
        `Proof size mismatch: (${proof.oldSize}, ${proof.newSize}) != (${oldCheckpoint.treeSize}, ${newCheckpoint.treeSize})`
      );
    }

    if (
      proof.oldRoot !== oldCheckpoint.rootHash ||
      proof.newRoot !== newCheckpoint.rootHash
    ) {
      throw new ConsistencyVerificationError(
        "Root hash mismatch between checkpoints and consistency proof"
      );
    }

    const valid = verifyConsistencyProof(
      proof.oldSize,
      proof.newSize,
      proof.oldRoot,
      proof.newRoot,
      proof.proofHashes
    );

    if (!valid) {
      throw new ConsistencyVerificationError(
        "RFC 6962 consistency proof failed to mathematically fold roots"
      );
    }

    return true;
  }

  public rotateKey(newKeyManager: WitnessKeyManager): KeyRotationRecord {
    const oldPub = this.keyManager.publicKeyB64;
    const newPub = newKeyManager.publicKeyB64;
    const rotationEpoch = this.latestEpoch + 1;
    const nowNs = Date.now() * 1_000_000;

    const challenge = KeyRotationRecord.computeChallenge(
      oldPub,
      newPub,
      rotationEpoch,
      nowNs
    );

    const challengeBytes = Buffer.from(challenge, "utf-8");
    const proofOfPossession = newKeyManager.signB64(challengeBytes);
    const authSig = this.keyManager.signB64(challengeBytes);

    const record = new KeyRotationRecord({
      rotationId: crypto.randomUUID(),
      oldPublicKey: oldPub,
      newPublicKey: newPub,
      rotationEpoch,
      timestampNs: nowNs,
      challenge,
      proofOfPossession,
      authorizationSignature: authSig,
    });

    if (!RootWitness.verifyRotationRecord(record)) {
      throw new KeyRotationError("Generated rotation record failed verification");
    }

    this._rotationLog.push(record);
    this.keyManager = newKeyManager;
    return record;
  }

  public static verifyRotationRecord(record: KeyRotationRecord): boolean {
    const expectedChallenge = KeyRotationRecord.computeChallenge(
      record.oldPublicKey,
      record.newPublicKey,
      record.rotationEpoch,
      record.timestampNs
    );

    if (record.challenge !== expectedChallenge) {
      throw new KeyRotationError("Challenge payload mismatch in key rotation record");
    }

    const challengeBytes = Buffer.from(expectedChallenge, "utf-8");

    // 1. Verify authorization from old key
    const oldVerifier = WitnessKeyManager.fromPublicKey(record.oldPublicKey);
    if (!oldVerifier.verifyB64(challengeBytes, record.authorizationSignature)) {
      throw new KeyRotationError("Authorization signature by outgoing key failed verification");
    }

    // 2. Verify proof of possession by new key
    const newVerifier = WitnessKeyManager.fromPublicKey(record.newPublicKey);
    if (!newVerifier.verifyB64(challengeBytes, record.proofOfPossession)) {
      throw new KeyRotationError("Proof of possession by incoming key failed verification");
    }

    return true;
  }

  public verifyRotationLog(rootPublicKey: string): boolean {
    let currentExpectedKey = rootPublicKey;
    for (const rec of this._rotationLog) {
      if (rec.oldPublicKey !== currentExpectedKey) {
        return false;
      }
      if (!RootWitness.verifyRotationRecord(rec)) {
        return false;
      }
      currentExpectedKey = rec.newPublicKey;
    }
    return currentExpectedKey === this.keyManager.publicKeyB64;
  }

  public status(): WitnessStatus {
    const latestHash =
      this._checkpoints.length > 0
        ? this._checkpoints[this._checkpoints.length - 1].checkpointHash
        : RootWitness.GENESIS_PREV_HASH;

    return new WitnessStatus({
      activePublicKey: this.keyManager.publicKeyB64,
      latestEpoch: this.latestEpoch,
      currentTreeSize: this.currentTreeSize,
      latestCheckpointHash: latestHash,
      totalRotations: this._rotationLog.length,
      totalCheckpoints: this._checkpoints.length,
    });
  }

  private _persistLeaf(receipt: LeafReceipt): void {
    if (!this.storagePath) return;
    fs.mkdirSync(path.dirname(path.resolve(this.storagePath)), { recursive: true });
    const line = JSON.stringify({
      record_type: "leaf",
      ...receipt.toDict(),
    }) + "\n";
    fs.appendFileSync(this.storagePath, line, "utf-8");
  }

  private _persistCheckpoint(checkpoint: SignedCheckpoint): void {
    if (!this.storagePath) return;
    fs.mkdirSync(path.dirname(path.resolve(this.storagePath)), { recursive: true });
    const line = JSON.stringify({
      record_type: "checkpoint",
      ...checkpoint.toDict(),
    }) + "\n";
    fs.appendFileSync(this.storagePath, line, "utf-8");
  }

  private _loadFromStorage(): void {
    if (!this.storagePath || !fs.existsSync(this.storagePath)) return;
    const content = fs.readFileSync(this.storagePath, "utf-8");
    for (const line of content.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const rec = JSON.parse(trimmed);
      if (rec.record_type === "leaf") {
        this._leaves.push(rec.leaf_hash);
      } else if (rec.record_type === "checkpoint" || rec.epoch !== undefined) {
        this._checkpoints.push(SignedCheckpoint.fromDict(rec));
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Global Singleton Instance
// ---------------------------------------------------------------------------

export const globalRootWitness = new RootWitness();
