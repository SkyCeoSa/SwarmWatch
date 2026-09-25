/**
 * Zero-Knowledge Causal Proof Generation & Cryptographic Audit Engine (Seam 49 / ADR 0060).
 * Tamper-evident Merkle-tree causal accumulator, zero-knowledge audit receipts,
 * and mathematical execution verification without disclosing private conversation payloads.
 */

import * as crypto from "node:crypto";

export const ProofCommitmentType = {
  TURN_INCLUSION: "TURN_INCLUSION",
  CIRCUIT_TRIP: "CIRCUIT_TRIP",
  CAUSAL_LINEAGE: "CAUSAL_LINEAGE",
  POLICY_COMPLIANCE: "POLICY_COMPLIANCE",
} as const;

export type ProofCommitmentTypeEnum = typeof ProofCommitmentType[keyof typeof ProofCommitmentType];

export function sha256Digest(content: string | Buffer): string {
  return crypto.createHash("sha256").update(content).digest("hex");
}

export interface SpanCommitmentLeafOptions {
  spanId: string;
  turnIndex: number;
  agentId: string;
  payloadHash: string;
  parentSpanIds?: string[];
  circuitTripped?: boolean;
  tripReasonHash?: string | null;
  timestamp?: string;
  leafHash?: string;
}

export class SpanCommitmentLeaf {
  public readonly spanId: string;
  public readonly turnIndex: number;
  public readonly agentId: string;
  public readonly payloadHash: string;
  public readonly parentSpanIds: string[];
  public readonly circuitTripped: boolean;
  public readonly tripReasonHash: string | null;
  public readonly timestamp: string;
  public leafHash: string;

  constructor(opts: SpanCommitmentLeafOptions) {
    this.spanId = opts.spanId;
    this.turnIndex = opts.turnIndex;
    this.agentId = opts.agentId;
    this.payloadHash = opts.payloadHash;
    this.parentSpanIds = opts.parentSpanIds ? [...opts.parentSpanIds] : [];
    this.circuitTripped = Boolean(opts.circuitTripped);
    this.tripReasonHash = opts.tripReasonHash || null;
    this.timestamp = opts.timestamp || new Date().toISOString();
    this.leafHash = opts.leafHash || this.computeLeafHash();
  }

  public computeLeafHash(): string {
    const parentsStr = [...this.parentSpanIds].sort().join(",");
    const circuitVal = this.circuitTripped ? "1" : "0";
    const reasonVal = this.tripReasonHash || "";
    const seed = `${this.spanId}:${this.turnIndex}:${this.agentId}:${this.payloadHash}:${parentsStr}:${circuitVal}:${reasonVal}`;
    return sha256Digest(seed);
  }

  public toDict(): Record<string, any> {
    return {
      span_id: this.spanId,
      turn_index: this.turnIndex,
      agent_id: this.agentId,
      payload_hash: this.payloadHash,
      parent_span_ids: [...this.parentSpanIds],
      circuit_tripped: this.circuitTripped,
      trip_reason_hash: this.tripReasonHash,
      timestamp: this.timestamp,
      leaf_hash: this.leafHash,
    };
  }

  public static fromDict(data: Record<string, any>): SpanCommitmentLeaf {
    return new SpanCommitmentLeaf({
      spanId: String(data.span_id || ""),
      turnIndex: Number(data.turn_index || 0),
      agentId: String(data.agent_id || "agent-unknown"),
      payloadHash: String(data.payload_hash || sha256Digest("")),
      parentSpanIds: Array.isArray(data.parent_span_ids) ? data.parent_span_ids : [],
      circuitTripped: Boolean(data.circuit_tripped),
      tripReasonHash: data.trip_reason_hash || null,
      timestamp: String(data.timestamp || new Date().toISOString()),
      leafHash: String(data.leaf_hash || ""),
    });
  }
}

export interface MerkleAuditProofOptions {
  leafHash: string;
  leafIndex: number;
  totalLeaves: number;
  siblingHashes: Array<[string, string]>; // ["left" | "right", sibling_hash]
  merkleRoot: string;
}

export class MerkleAuditProof {
  public readonly leafHash: string;
  public readonly leafIndex: number;
  public readonly totalLeaves: number;
  public readonly siblingHashes: Array<[string, string]>;
  public readonly merkleRoot: string;

  constructor(opts: MerkleAuditProofOptions) {
    this.leafHash = opts.leafHash;
    this.leafIndex = opts.leafIndex;
    this.totalLeaves = opts.totalLeaves;
    this.siblingHashes = opts.siblingHashes.map(([dir, hash]) => [String(dir), String(hash)]);
    this.merkleRoot = opts.merkleRoot;
  }

  public toDict(): Record<string, any> {
    return {
      leaf_hash: this.leafHash,
      leaf_index: this.leafIndex,
      total_leaves: this.totalLeaves,
      sibling_hashes: this.siblingHashes.map(([dir, hash]) => [dir, hash]),
      merkle_root: this.merkleRoot,
    };
  }

  public static fromDict(data: Record<string, any>): MerkleAuditProof {
    const rawSiblings = Array.isArray(data.sibling_hashes) ? data.sibling_hashes : [];
    return new MerkleAuditProof({
      leafHash: String(data.leaf_hash || ""),
      leafIndex: Number(data.leaf_index || 0),
      totalLeaves: Number(data.total_leaves || 1),
      siblingHashes: rawSiblings.map((s: any) => [String(s[0]), String(s[1])]),
      merkleRoot: String(data.merkle_root || ""),
    });
  }
}

export interface CausalAuditReceiptOptions {
  receiptId?: string;
  swarmId?: string;
  commitmentType?: ProofCommitmentTypeEnum;
  spanCommitment: SpanCommitmentLeaf;
  merkleProof: MerkleAuditProof;
  sessionMerkleRoot: string;
  generatedAt?: string;
  provenanceSignature?: string;
  verified?: boolean;
}

export class CausalAuditReceipt {
  public readonly receiptId: string;
  public readonly swarmId: string;
  public readonly commitmentType: ProofCommitmentTypeEnum;
  public readonly spanCommitment: SpanCommitmentLeaf;
  public readonly merkleProof: MerkleAuditProof;
  public readonly sessionMerkleRoot: string;
  public readonly generatedAt: string;
  public readonly provenanceSignature: string;
  public readonly verified: boolean;

  constructor(opts: CausalAuditReceiptOptions) {
    this.receiptId = opts.receiptId || `rcpt_${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`;
    this.swarmId = opts.swarmId || "swarm-live-01";
    this.commitmentType = opts.commitmentType || ProofCommitmentType.TURN_INCLUSION;
    this.spanCommitment = opts.spanCommitment;
    this.merkleProof = opts.merkleProof;
    this.sessionMerkleRoot = opts.sessionMerkleRoot;
    this.generatedAt = opts.generatedAt || new Date().toISOString();
    this.provenanceSignature = opts.provenanceSignature || "";
    this.verified = Boolean(opts.verified);
  }

  public toDict(): Record<string, any> {
    return {
      receipt_id: this.receiptId,
      swarm_id: this.swarmId,
      commitment_type: this.commitmentType,
      span_commitment: this.spanCommitment.toDict(),
      merkle_proof: this.merkleProof.toDict(),
      session_merkle_root: this.sessionMerkleRoot,
      generated_at: this.generatedAt,
      provenance_signature: this.provenanceSignature,
      verified: this.verified,
    };
  }

  public static fromDict(data: Record<string, any>): CausalAuditReceipt {
    let ct: ProofCommitmentTypeEnum = ProofCommitmentType.TURN_INCLUSION;
    const rawCt = String(data.commitment_type || "").toUpperCase();
    if (rawCt in ProofCommitmentType) {
      ct = rawCt as ProofCommitmentTypeEnum;
    }

    return new CausalAuditReceipt({
      receiptId: String(data.receipt_id || `rcpt_${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`),
      swarmId: String(data.swarm_id || "swarm-live-01"),
      commitmentType: ct,
      spanCommitment: SpanCommitmentLeaf.fromDict(data.span_commitment || {}),
      merkleProof: MerkleAuditProof.fromDict(data.merkle_proof || {}),
      sessionMerkleRoot: String(data.session_merkle_root || ""),
      generatedAt: String(data.generated_at || new Date().toISOString()),
      provenanceSignature: String(data.provenance_signature || ""),
      verified: Boolean(data.verified),
    });
  }
}

export interface CommitSpanOptions {
  spanId: string;
  turnIndex: number;
  agentId: string;
  payloadContent?: string | Buffer | null;
  payloadHash?: string | null;
  parentSpanIds?: string[];
  circuitTripped?: boolean;
  tripReason?: string | null;
}

export class CausalProofAccumulator {
  public readonly swarmId: string;
  private _leaves: SpanCommitmentLeaf[] = [];
  private _leafIndexBySpanId: Map<string, number> = new Map();
  private _treeLevels: string[][] = [];
  private _merkleRoot: string | null = null;
  private _rebuildNeeded: boolean = true;
  private _totalReceiptsGenerated: number = 0;
  private _totalVerifications: number = 0;

  constructor(swarmId: string = "swarm-live-01") {
    this.swarmId = swarmId;
  }

  public get totalLeaves(): number {
    return this._leaves.length;
  }

  public commitSpan(opts: CommitSpanOptions): SpanCommitmentLeaf {
    let pHash: string;
    if (opts.payloadHash) {
      pHash = opts.payloadHash;
    } else {
      const content = opts.payloadContent !== undefined && opts.payloadContent !== null
        ? opts.payloadContent
        : `span_${opts.spanId}`;
      pHash = sha256Digest(content);
    }

    const reasonHash = opts.tripReason ? sha256Digest(opts.tripReason) : null;

    const leaf = new SpanCommitmentLeaf({
      spanId: opts.spanId,
      turnIndex: opts.turnIndex,
      agentId: opts.agentId,
      payloadHash: pHash,
      parentSpanIds: opts.parentSpanIds || [],
      circuitTripped: Boolean(opts.circuitTripped),
      tripReasonHash: reasonHash,
    });

    const idx = this._leaves.length;
    this._leaves.push(leaf);
    this._leafIndexBySpanId.set(opts.spanId, idx);
    this._rebuildNeeded = true;
    return leaf;
  }

  private _rebuildTree(): void {
    if (this._leaves.length === 0) {
      this._treeLevels = [[]];
      this._merkleRoot = sha256Digest("empty_swarm_session");
      this._rebuildNeeded = false;
      return;
    }

    let currentLevel: string[] = this._leaves.map((l) => l.leafHash);
    const levels: string[][] = [[...currentLevel]];

    while (currentLevel.length > 1) {
      const nextLevel: string[] = [];
      for (let i = 0; i < currentLevel.length; i += 2) {
        const left = currentLevel[i];
        const right = i + 1 < currentLevel.length ? currentLevel[i + 1] : left;
        const combined = sha256Digest(left + right);
        nextLevel.push(combined);
      }
      levels.push(nextLevel);
      currentLevel = nextLevel;
    }

    this._treeLevels = levels;
    this._merkleRoot = currentLevel[0];
    this._rebuildNeeded = false;
  }

  public getMerkleRoot(): string {
    if (this._rebuildNeeded || this._merkleRoot === null) {
      this._rebuildTree();
    }
    return this._merkleRoot!;
  }

  public generateInclusionProof(spanIdOrIndex: string | number): MerkleAuditProof {
    if (this._rebuildNeeded || this._merkleRoot === null) {
      this._rebuildTree();
    }

    let leafIndex: number;
    if (typeof spanIdOrIndex === "string") {
      if (!this._leafIndexBySpanId.has(spanIdOrIndex)) {
        throw new Error(`Span ID '${spanIdOrIndex}' not found in accumulator.`);
      }
      leafIndex = this._leafIndexBySpanId.get(spanIdOrIndex)!;
    } else {
      leafIndex = Number(spanIdOrIndex);
    }

    if (leafIndex < 0 || leafIndex >= this._leaves.length) {
      throw new RangeError(`Leaf index ${leafIndex} out of bounds (total: ${this._leaves.length})`);
    }

    const targetLeaf = this._leaves[leafIndex];
    const siblings: Array<[string, string]> = [];
    let currentIdx = leafIndex;

    for (let levelIdx = 0; levelIdx < this._treeLevels.length - 1; levelIdx++) {
      const level = this._treeLevels[levelIdx];
      if (currentIdx % 2 === 0) {
        // Current is left, sibling is right
        const siblingIdx = currentIdx + 1 < level.length ? currentIdx + 1 : currentIdx;
        siblings.push(["right", level[siblingIdx]]);
      } else {
        // Current is right, sibling is left
        const siblingIdx = currentIdx - 1;
        siblings.push(["left", level[siblingIdx]]);
      }
      currentIdx = Math.floor(currentIdx / 2);
    }

    return new MerkleAuditProof({
      leafHash: targetLeaf.leafHash,
      leafIndex: leafIndex,
      totalLeaves: this._leaves.length,
      siblingHashes: siblings,
      merkleRoot: this._merkleRoot || "",
    });
  }

  public static verifyInclusionProof(proof: MerkleAuditProof, expectedRoot?: string): boolean {
    let currentHash = proof.leafHash;
    for (const [direction, siblingHash] of proof.siblingHashes) {
      let combined: string;
      if (direction === "left") {
        combined = siblingHash + currentHash;
      } else {
        combined = currentHash + siblingHash;
      }
      currentHash = sha256Digest(combined);
    }

    const targetRoot = expectedRoot || proof.merkleRoot;
    return timingSafeEqualStr(currentHash, targetRoot);
  }

  public generateAuditReceipt(
    spanId: string,
    commitmentType: ProofCommitmentTypeEnum = ProofCommitmentType.TURN_INCLUSION,
    secretKey: string = "sw_causal_audit_secret_key"
  ): CausalAuditReceipt {
    this._totalReceiptsGenerated++;
    if (!this._leafIndexBySpanId.has(spanId)) {
      throw new Error(`Span ID '${spanId}' not found in session accumulator.`);
    }

    const idx = this._leafIndexBySpanId.get(spanId)!;
    const leaf = this._leaves[idx];
    const proof = this.generateInclusionProof(idx);
    const root = this.getMerkleRoot();

    const receiptId = `rcpt_${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`;
    const sigPayload = `${receiptId}:${this.swarmId}:${commitmentType}:${leaf.leafHash}:${root}`;
    const signature = crypto.createHmac("sha256", secretKey).update(sigPayload).digest("hex");

    const isValid = CausalProofAccumulator.verifyInclusionProof(proof, root);

    return new CausalAuditReceipt({
      receiptId: receiptId,
      swarmId: this.swarmId,
      commitmentType: commitmentType,
      spanCommitment: leaf,
      merkleProof: proof,
      sessionMerkleRoot: root,
      provenanceSignature: signature,
      verified: isValid,
    });
  }

  public verifyAuditReceipt(
    receipt: CausalAuditReceipt,
    secretKey: string = "sw_causal_audit_secret_key"
  ): { isValid: boolean; error: string | null } {
    this._totalVerifications++;

    // 1. Verify Leaf Hash Integrity
    const computedLeafHash = receipt.spanCommitment.computeLeafHash();
    if (!timingSafeEqualStr(computedLeafHash, receipt.spanCommitment.leafHash)) {
      return { isValid: false, error: "Leaf hash does not match recomputed span commitment attributes." };
    }

    if (!timingSafeEqualStr(computedLeafHash, receipt.merkleProof.leafHash)) {
      return { isValid: false, error: "Merkle proof leaf_hash does not match span commitment leaf_hash." };
    }

    // 2. Verify HMAC Provenance Signature
    const sigPayload = `${receipt.receiptId}:${receipt.swarmId}:${receipt.commitmentType}:${receipt.spanCommitment.leafHash}:${receipt.sessionMerkleRoot}`;
    const expectedSig = crypto.createHmac("sha256", secretKey).update(sigPayload).digest("hex");
    if (!timingSafeEqualStr(receipt.provenanceSignature, expectedSig)) {
      return { isValid: false, error: "Invalid provenance HMAC signature." };
    }

    // 3. Verify Merkle Path Folding to Session Root
    const merkleValid = CausalProofAccumulator.verifyInclusionProof(receipt.merkleProof, receipt.sessionMerkleRoot);
    if (!merkleValid) {
      return { isValid: false, error: "Merkle inclusion proof failed to fold to session Merkle root." };
    }

    return { isValid: true, error: null };
  }

  public getManifest(): Record<string, any> {
    const root = this.getMerkleRoot();
    return {
      swarm_id: this.swarmId,
      session_merkle_root: root,
      total_leaves: this._leaves.length,
      tree_depth: this._treeLevels.length,
      total_receipts_generated: this._totalReceiptsGenerated,
      total_verifications: this._totalVerifications,
      leaves: this._leaves.map((l) => l.toDict()),
    };
  }

  public reset(): void {
    this._leaves = [];
    this._leafIndexBySpanId.clear();
    this._treeLevels = [];
    this._merkleRoot = null;
    this._rebuildNeeded = true;
  }
}

function timingSafeEqualStr(a: string, b: string): boolean {
  if (typeof a !== "string" || typeof b !== "string") {
    return false;
  }
  const bufA = Buffer.from(a, "utf-8");
  const bufB = Buffer.from(b, "utf-8");
  if (bufA.length !== bufB.length) {
    return false;
  }
  return crypto.timingSafeEqual(bufA, bufB);
}

export class ZkProofEngine {
  private _accumulators: Map<string, CausalProofAccumulator> = new Map();

  public getAccumulator(swarmId: string = "swarm-live-01"): CausalProofAccumulator {
    if (!this._accumulators.has(swarmId)) {
      this._accumulators.set(swarmId, new CausalProofAccumulator(swarmId));
    }
    return this._accumulators.get(swarmId)!;
  }

  public commitSpan(
    swarmId: string,
    spanId: string,
    turnIndex: number,
    agentId: string,
    payloadContent?: string | Buffer | null,
    payloadHash?: string | null,
    parentSpanIds?: string[],
    circuitTripped?: boolean,
    tripReason?: string | null
  ): SpanCommitmentLeaf {
    const acc = this.getAccumulator(swarmId);
    return acc.commitSpan({
      spanId,
      turnIndex,
      agentId,
      payloadContent,
      payloadHash,
      parentSpanIds,
      circuitTripped,
      tripReason,
    });
  }

  public getRoot(swarmId: string = "swarm-live-01"): string {
    return this.getAccumulator(swarmId).getMerkleRoot();
  }

  public generateReceipt(
    swarmId: string,
    spanId: string,
    commitmentType: ProofCommitmentTypeEnum = ProofCommitmentType.TURN_INCLUSION,
    secretKey: string = "sw_causal_audit_secret_key"
  ): CausalAuditReceipt {
    return this.getAccumulator(swarmId).generateAuditReceipt(spanId, commitmentType, secretKey);
  }

  public verifyReceipt(
    receipt: CausalAuditReceipt | Record<string, any>,
    secretKey: string = "sw_causal_audit_secret_key"
  ): { isValid: boolean; error: string | null } {
    const parsedReceipt = receipt instanceof CausalAuditReceipt
      ? receipt
      : CausalAuditReceipt.fromDict(receipt);
    return this.getAccumulator(parsedReceipt.swarmId).verifyAuditReceipt(parsedReceipt, secretKey);
  }

  public getManifest(swarmId: string = "swarm-live-01"): Record<string, any> {
    return this.getAccumulator(swarmId).getManifest();
  }

  public reset(swarmId?: string): void {
    if (swarmId) {
      if (this._accumulators.has(swarmId)) {
        this._accumulators.get(swarmId)!.reset();
      }
    } else {
      this._accumulators.clear();
    }
  }
}

export const globalZkProofEngine = new ZkProofEngine();
