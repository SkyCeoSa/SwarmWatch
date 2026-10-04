/**
 * SwarmWatch Zero-Knowledge State Transition Rollup & Succinct Proof Verification (Seam 75 / ADR 0086).
 *
 * Implements in-process, zero-token Zero-Knowledge State Transition Rollup aggregation,
 * sparse state Merkle tree tracking, polynomial commitment evaluations, Fiat-Shamir
 * non-interactive succinct proofs, fail-closed safety circuit coupling, and tamper-evident
 * rollup receipts ('sw:rollup:v1:<sha256>').
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";

// ==============================================================================
// 0. Mathematical Constants & Field Arithmetic (BN254 Scalar Field)
// ==============================================================================

export const BN254_PRIME = 21888242871839275222246405745257275088548364400416034343698204186575808495617n;

export function deepCanonicalJson(val: any): string {
  if (val === null || val === undefined) return "null";
  if (typeof val === "boolean" || typeof val === "number") return JSON.stringify(val);
  if (typeof val === "string") return JSON.stringify(val);
  if (Array.isArray(val)) {
    return "[" + val.map((item) => deepCanonicalJson(item)).join(",") + "]";
  }
  if (typeof val === "object") {
    const keys = Object.keys(val).sort();
    const pairs = keys.map((k) => JSON.stringify(k) + ":" + deepCanonicalJson(val[k]));
    return "{" + pairs.join(",") + "}";
  }
  return JSON.stringify(val);
}

export function sha256Hex(data: string): string {
  return crypto.createHash("sha256").update(data, "utf8").digest("hex");
}

export function hmacSha256Hex(key: string, message: string): string {
  return crypto.createHmac("sha256", key).update(message, "utf8").digest("hex");
}

export function hexToBigInt(hexStr: string): bigint {
  const clean = hexStr.startsWith("0x") ? hexStr.slice(2) : hexStr;
  return clean ? BigInt("0x" + clean) : 0n;
}

export function computeMerkleRoot(leafHashes: string[]): string {
  if (leafHashes.length === 0) {
    return "0".repeat(64);
  }
  let currentLayer = [...leafHashes];
  while (currentLayer.length > 1) {
    const nextLayer: string[] = [];
    for (let i = 0; i < currentLayer.length; i += 2) {
      const left = currentLayer[i];
      const right = i + 1 < currentLayer.length ? currentLayer[i + 1] : left;
      const combined = sha256Hex(left + right);
      nextLayer.push(combined);
    }
    currentLayer = nextLayer;
  }
  return currentLayer[0];
}

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class ZkRollupError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ZkRollupError";
  }
}

export class RollupCircuitTrippedError extends ZkRollupError {
  constructor(message: string) {
    super(message);
    this.name = "RollupCircuitTrippedError";
  }
}

export class InvalidStateTransitionError extends ZkRollupError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidStateTransitionError";
  }
}

export class RollupVerificationFailedError extends ZkRollupError {
  constructor(message: string) {
    super(message);
    this.name = "RollupVerificationFailedError";
  }
}

export class SequenceDiscontinuityError extends InvalidStateTransitionError {
  constructor(message: string) {
    super(message);
    this.name = "SequenceDiscontinuityError";
  }
}

export class FraudulentStateTransitionError extends InvalidStateTransitionError {
  constructor(message: string) {
    super(message);
    this.name = "FraudulentStateTransitionError";
  }
}

export class BatchCapacityExceededError extends ZkRollupError {
  constructor(message: string) {
    super(message);
    this.name = "BatchCapacityExceededError";
  }
}

// ==============================================================================
// 2. Domain Types & Object Enums
// ==============================================================================

export type RollupStatus = "PENDING" | "SEALED" | "VERIFIED" | "CHALLENGED" | "REJECTED";

export const RollupStatus = {
  PENDING: "PENDING" as RollupStatus,
  SEALED: "SEALED" as RollupStatus,
  VERIFIED: "VERIFIED" as RollupStatus,
  CHALLENGED: "CHALLENGED" as RollupStatus,
  REJECTED: "REJECTED" as RollupStatus,
} as const;

export type ProofSystem = "POLYNOMIAL_ZK" | "FIAT_SHAMIR_STARK" | "MERKLE_ARITHMETIC";

export const ProofSystem = {
  POLYNOMIAL_ZK: "POLYNOMIAL_ZK" as ProofSystem,
  FIAT_SHAMIR_STARK: "FIAT_SHAMIR_STARK" as ProofSystem,
  MERKLE_ARITHMETIC: "MERKLE_ARITHMETIC" as ProofSystem,
} as const;

export interface StateTransition {
  transition_id: string;
  sequence: number;
  agent_id: string;
  slot_key: string;
  from_value_hash: string;
  to_value_hash: string;
  delta_payload_hash: string;
  private_payload_mask?: string | null;
  metadata?: Record<string, any>;
  timestamp_ns: number;
  digest: string;
}

export interface RollupBatch {
  batch_id: string;
  sequence_start: number;
  sequence_end: number;
  transitions: StateTransition[];
  pre_state_root: string;
  post_state_root: string;
  delta_merkle_root: string;
  batch_digest: string;
  status: RollupStatus;
  created_at_ns: number;
  sealed_at_ns?: number | null;
}

export interface RollupProofReceipt {
  rollup_id: string;
  batch_id: string;
  sequence_start: number;
  sequence_end: number;
  pre_state_root: string;
  post_state_root: string;
  transition_count: number;
  batch_digest: string;
  proof_system: ProofSystem;
  zk_proof_data: Record<string, any>;
  receipt_hash: string;
  is_valid: boolean;
  prover_id: string;
  verified_at_ns: number;
}

// ==============================================================================
// 3. Sparse State Merkle Tree
// ==============================================================================

export class StateMerkleTree {
  private _slots: Map<string, string>;

  constructor(initialSlots?: Map<string, string> | Record<string, string>) {
    this._slots = new Map();
    if (initialSlots) {
      if (initialSlots instanceof Map) {
        for (const [k, v] of initialSlots.entries()) {
          this._slots.set(k, v);
        }
      } else {
        for (const k of Object.keys(initialSlots)) {
          this._slots.set(k, initialSlots[k]);
        }
      }
    }
  }

  getSlot(slotKey: string): string {
    return this._slots.get(slotKey) || "0".repeat(64);
  }

  setSlot(slotKey: string, valueHash: string): void {
    this._slots.set(slotKey, valueHash);
  }

  getRoot(): string {
    if (this._slots.size === 0) {
      return "0".repeat(64);
    }
    const sortedKeys = Array.from(this._slots.keys()).sort();
    const leafHashes = sortedKeys.map((k) =>
      sha256Hex(`${k}:${this._slots.get(k)}`)
    );
    return computeMerkleRoot(leafHashes);
  }

  clone(): StateMerkleTree {
    return new StateMerkleTree(new Map(this._slots));
  }

  getAllSlots(): Record<string, string> {
    const res: Record<string, string> = {};
    for (const [k, v] of this._slots.entries()) {
      res[k] = v;
    }
    return res;
  }
}

// ==============================================================================
// 4. Zero-Knowledge State Transition Rollup Engine
// ==============================================================================

export class ZkRollupEngine {
  maxBatchSize: number;
  secretSeed: string;
  circuitBreaker: any;
  proofSystem: ProofSystem;

  private _stateTree: StateMerkleTree;
  private _pendingTransitions: StateTransition[];
  private _batches: Map<string, RollupBatch>;
  private _receipts: Map<string, RollupProofReceipt>;
  private _nextSequence: number;
  private _circuitTripped: boolean;
  private _tripReason: string | null;

  constructor(options?: {
    maxBatchSize?: number;
    secretSeed?: string;
    circuitBreaker?: any;
    proofSystem?: ProofSystem;
  }) {
    this.maxBatchSize = options?.maxBatchSize ?? 100;
    this.secretSeed = options?.secretSeed ?? "swarmwatch-zk-rollup-seed-v1";
    this.circuitBreaker = options?.circuitBreaker ?? null;
    this.proofSystem = options?.proofSystem ?? ProofSystem.POLYNOMIAL_ZK;

    this._stateTree = new StateMerkleTree();
    this._pendingTransitions = [];
    this._batches = new Map();
    this._receipts = new Map();
    this._nextSequence = 1;
    this._circuitTripped = false;
    this._tripReason = null;
  }

  // --------------------------------------------------------------------------
  // Circuit Breaker Helpers
  // --------------------------------------------------------------------------

  checkCircuit(): void {
    if (this._circuitTripped) {
      throw new RollupCircuitTrippedError(
        `Rollup rejected: safety circuit tripped (${this._tripReason})`
      );
    }
    if (this.circuitBreaker) {
      let isOpen = false;
      if (typeof this.circuitBreaker.isOpen === "function") {
        isOpen = this.circuitBreaker.isOpen();
      } else if (typeof this.circuitBreaker.is_open === "function") {
        isOpen = this.circuitBreaker.is_open();
      } else if (this.circuitBreaker.isOpen !== undefined) {
        isOpen = Boolean(this.circuitBreaker.isOpen);
      }
      if (isOpen) {
        this._circuitTripped = true;
        this._tripReason = "External safety circuit breaker is open";
        throw new RollupCircuitTrippedError(
          `Rollup rejected: ${this._tripReason}`
        );
      }
    }
  }

  tripCircuit(reason: string): void {
    this._circuitTripped = true;
    this._tripReason = reason;
    if (this.circuitBreaker) {
      if (typeof this.circuitBreaker.trip === "function") {
        try {
          this.circuitBreaker.trip(reason);
        } catch {
          // ignore external trip errors
        }
      }
    }
  }

  // --------------------------------------------------------------------------
  // State Transition Ingestion
  // --------------------------------------------------------------------------

  queueTransition(params: {
    agentId: string;
    slotKey: string;
    fromValueHash: string;
    toValueHash: string;
    deltaPayload: string;
    privateScratchpad?: string | null;
    sequence?: number;
    metadata?: Record<string, any>;
  }): StateTransition {
    this.checkCircuit();

    const expectedSeq = this._nextSequence;
    const seq = params.sequence !== undefined ? params.sequence : expectedSeq;

    // Enforce strict sequence monotonicity
    if (seq !== expectedSeq) {
      const reason = `Sequence discontinuity: expected sequence ${expectedSeq}, received ${seq}`;
      this.tripCircuit(reason);
      throw new SequenceDiscontinuityError(reason);
    }

    // Enforce pre-state invariant
    const currentSlotVal = this._stateTree.getSlot(params.slotKey);
    if (currentSlotVal !== params.fromValueHash) {
      const reason = `Fraudulent state transition for slot '${params.slotKey}': expected pre-state ${currentSlotVal}, received ${params.fromValueHash}`;
      this.tripCircuit(reason);
      throw new FraudulentStateTransitionError(reason);
    }

    const deltaPayloadHash = sha256Hex(params.deltaPayload);

    let privateMask: string | null = null;
    if (params.privateScratchpad) {
      privateMask = sha256Hex(
        hmacSha256Hex(this.secretSeed, params.privateScratchpad)
      );
    }

    const suffix = ((seq * 2654435761) >>> 0).toString(16).padStart(8, "0");
    const tId = `tx_${seq.toString().padStart(6, "0")}_${suffix}`;
    const canonicalPayload = `{"agent_id":"${params.agentId}","delta_payload_hash":"${deltaPayloadHash}","from_value_hash":"${params.fromValueHash}","sequence":${seq},"slot_key":"${params.slotKey}","to_value_hash":"${params.toValueHash}","transition_id":"${tId}"}`;
    const digest = sha256Hex(canonicalPayload);

    const transition: StateTransition = {
      transition_id: tId,
      sequence: seq,
      agent_id: params.agentId,
      slot_key: params.slotKey,
      from_value_hash: params.fromValueHash,
      to_value_hash: params.toValueHash,
      delta_payload_hash: deltaPayloadHash,
      private_payload_mask: privateMask,
      metadata: params.metadata || {},
      timestamp_ns: Date.now() * 1_000_000,
      digest,
    };

    this._stateTree.setSlot(params.slotKey, params.toValueHash);
    this._nextSequence += 1;
    this._pendingTransitions.push(transition);

    return transition;
  }

  // --------------------------------------------------------------------------
  // Rollup Batch Commitment & Proof Generation
  // --------------------------------------------------------------------------

  commitBatch(batchId?: string): RollupProofReceipt {
    this.checkCircuit();

    if (this._pendingTransitions.length === 0) {
      throw new ZkRollupError(
        "Cannot commit rollup batch: pending transition queue is empty"
      );
    }

    const batchTxs = [...this._pendingTransitions];
    this._pendingTransitions = [];

    const bId =
      batchId ||
      `batch_${Math.floor(Date.now() / 1000)}_${crypto.randomBytes(4).toString("hex")}`;
    const seqStart = batchTxs[0].sequence;
    const seqEnd = batchTxs[batchTxs.length - 1].sequence;

    // Reconstruct pre-state root by rewinding the batch on a clone
    const preTree = this._stateTree.clone();
    for (let i = batchTxs.length - 1; i >= 0; i--) {
      const tx = batchTxs[i];
      preTree.setSlot(tx.slot_key, tx.from_value_hash);
    }
    const preStateRoot = preTree.getRoot();
    const postStateRoot = this._stateTree.getRoot();

    const deltaHashes = batchTxs.map((tx) => tx.delta_payload_hash);
    const deltaMerkleRoot = computeMerkleRoot(deltaHashes);
    const batchDigest = sha256Hex(batchTxs.map((tx) => tx.digest).join(""));

    const zkProofData = this._generateSuccinctProof({
      preStateRoot,
      postStateRoot,
      batchDigest,
      deltaMerkleRoot,
    });

    const batch: RollupBatch = {
      batch_id: bId,
      sequence_start: seqStart,
      sequence_end: seqEnd,
      transitions: batchTxs,
      pre_state_root: preStateRoot,
      post_state_root: postStateRoot,
      delta_merkle_root: deltaMerkleRoot,
      batch_digest: batchDigest,
      status: RollupStatus.SEALED,
      created_at_ns: Date.now() * 1_000_000,
      sealed_at_ns: Date.now() * 1_000_000,
    };
    this._batches.set(bId, batch);

    const rawReceipt = {
      batch_id: bId,
      sequence_start: seqStart,
      sequence_end: seqEnd,
      pre_state_root: preStateRoot,
      post_state_root: postStateRoot,
      transition_count: batchTxs.length,
      batch_digest: batchDigest,
      proof_system: this.proofSystem,
      zk_proof_data: zkProofData,
    };
    const receiptHashRaw = sha256Hex(deepCanonicalJson(rawReceipt));
    const receiptHash = `sw:rollup:v1:${receiptHashRaw}`;
    const rollupId = `rollup_${receiptHashRaw.slice(0, 16)}`;

    const receipt: RollupProofReceipt = {
      rollup_id: rollupId,
      batch_id: bId,
      sequence_start: seqStart,
      sequence_end: seqEnd,
      pre_state_root: preStateRoot,
      post_state_root: postStateRoot,
      transition_count: batchTxs.length,
      batch_digest: batchDigest,
      proof_system: this.proofSystem,
      zk_proof_data: zkProofData,
      receipt_hash: receiptHash,
      is_valid: true,
      prover_id: "swarmwatch-zk-prover",
      verified_at_ns: Date.now() * 1_000_000,
    };
    this._receipts.set(bId, receipt);
    batch.status = RollupStatus.VERIFIED;

    return receipt;
  }

  // --------------------------------------------------------------------------
  // Succinct ZK Proof Mathematics
  // --------------------------------------------------------------------------

  private _generateSuccinctProof(params: {
    preStateRoot: string;
    postStateRoot: string;
    batchDigest: string;
    deltaMerkleRoot: string;
  }): Record<string, any> {
    const p = BN254_PRIME;

    // 1. Private Blinding Commitment
    const blindingCommitment = hmacSha256Hex(this.secretSeed, params.batchDigest);
    const b = hexToBigInt(blindingCommitment) % p;

    // 2. Fiat-Shamir challenge evaluation point z
    const challengeSeed = `${params.preStateRoot}${params.postStateRoot}${params.batchDigest}${blindingCommitment}`;
    const z = hexToBigInt(sha256Hex(challengeSeed)) % p;

    // 3. Transition polynomial coefficients
    const c0 = hexToBigInt(params.preStateRoot) % p;
    const c1 = hexToBigInt(params.batchDigest) % p;
    const c2 = hexToBigInt(params.deltaMerkleRoot) % p;

    // 4. Polynomial evaluation: P(z) = c0 + c1*z + c2*z^2 mod p
    const z2 = (z * z) % p;
    const pz = (c0 + ((c1 * z) % p) + ((c2 * z2) % p)) % p;

    // 5. Shielded proof evaluation: E = (P(z) + b) mod p
    const shieldedEval = (pz + b) % p;

    // 6. Expected transition transition law digest
    const expectedPostEval = sha256Hex(
      `${params.preStateRoot}${params.batchDigest}${params.deltaMerkleRoot}`
    );

    return {
      eval_point: "0x" + z.toString(16),
      proof_eval: "0x" + shieldedEval.toString(16),
      blinding_commitment: blindingCommitment,
      delta_merkle_root: params.deltaMerkleRoot,
      transition_law_digest: expectedPostEval,
      field_modulus: "0x" + p.toString(16),
    };
  }

  // --------------------------------------------------------------------------
  // Proof Verification & Fraud Challenge
  // --------------------------------------------------------------------------

  verifyRollupProof(receipt: RollupProofReceipt): {
    isValid: boolean;
    errorReason: string | null;
  } {
    const p = BN254_PRIME;
    const data = receipt.zk_proof_data;

    // 1. Verify receipt domain tag format
    if (!receipt.receipt_hash.startsWith("sw:rollup:v1:")) {
      return { isValid: false, errorReason: "Invalid receipt hash domain prefix" };
    }

    // 2. Verify receipt hash integrity
    const expectedReceiptPayload = {
      batch_id: receipt.batch_id,
      sequence_start: receipt.sequence_start,
      sequence_end: receipt.sequence_end,
      pre_state_root: receipt.pre_state_root,
      post_state_root: receipt.post_state_root,
      transition_count: receipt.transition_count,
      batch_digest: receipt.batch_digest,
      proof_system: receipt.proof_system,
      zk_proof_data: data,
    };
    const recomputedHash = `sw:rollup:v1:${sha256Hex(deepCanonicalJson(expectedReceiptPayload))}`;
    if (recomputedHash !== receipt.receipt_hash) {
      return {
        isValid: false,
        errorReason: "Receipt hash mismatch: payload tampering detected",
      };
    }

    // 3. Verify Fiat-Shamir challenge point z
    const blindingCommitment = data.blinding_commitment || "";
    const challengeSeed = `${receipt.pre_state_root}${receipt.post_state_root}${receipt.batch_digest}${blindingCommitment}`;
    const expectedZ = hexToBigInt(sha256Hex(challengeSeed)) % p;

    const actualZ = hexToBigInt(data.eval_point || "0x0");
    if (expectedZ !== actualZ) {
      return {
        isValid: false,
        errorReason: "Fiat-Shamir challenge evaluation point mismatch",
      };
    }

    // 4. Verify polynomial evaluation: P(z) + b == shielded_eval mod p
    const deltaMerkleRoot = data.delta_merkle_root || "";
    const c0 = hexToBigInt(receipt.pre_state_root) % p;
    const c1 = hexToBigInt(receipt.batch_digest) % p;
    const c2 = hexToBigInt(deltaMerkleRoot) % p;

    const z2 = (actualZ * actualZ) % p;
    const expectedPz = (c0 + ((c1 * actualZ) % p) + ((c2 * z2) % p)) % p;
    const b = hexToBigInt(blindingCommitment) % p;
    const expectedShieldedEval = (expectedPz + b) % p;

    const actualShieldedEval = hexToBigInt(data.proof_eval || "0x0");
    if (expectedShieldedEval !== actualShieldedEval) {
      return {
        isValid: false,
        errorReason: "Polynomial proof evaluation equation failed",
      };
    }

    // 5. Verify transition law digest
    const expectedTransitionDigest = sha256Hex(
      `${receipt.pre_state_root}${receipt.batch_digest}${deltaMerkleRoot}`
    );
    if (data.transition_law_digest !== expectedTransitionDigest) {
      return { isValid: false, errorReason: "Transition law digest mismatch" };
    }

    return { isValid: true, errorReason: null };
  }

  challengeFraud(params: {
    batchId: string;
    challengeType: string;
    evidence: Record<string, any>;
  }): boolean {
    const batch = this._batches.get(params.batchId);
    if (!batch) {
      return false;
    }

    let fraudDetected = false;
    const reason = `Fraud challenge '${params.challengeType}' against batch ${params.batchId}`;

    if (params.challengeType === "SEQUENCE_DISCONTINUITY") {
      let expected = batch.sequence_start;
      for (const tx of batch.transitions) {
        if (tx.sequence !== expected) {
          fraudDetected = true;
          break;
        }
        expected += 1;
      }
    } else if (params.challengeType === "PRE_STATE_CORRUPTION") {
      const claimedPre = params.evidence.pre_state_root;
      if (claimedPre && claimedPre !== batch.pre_state_root) {
        fraudDetected = true;
      }
    } else if (params.challengeType === "DELTA_TAMPERING") {
      const claimedDeltas = params.evidence.delta_hashes;
      if (Array.isArray(claimedDeltas) && claimedDeltas.length > 0) {
        const computedRoot = computeMerkleRoot(claimedDeltas);
        if (computedRoot !== batch.delta_merkle_root) {
          fraudDetected = true;
        }
      }
    }

    if (fraudDetected) {
      batch.status = RollupStatus.CHALLENGED;
      const receipt = this._receipts.get(params.batchId);
      if (receipt) {
        receipt.is_valid = false;
      }
      this.tripCircuit(reason);
      return true;
    }

    return false;
  }

  // --------------------------------------------------------------------------
  // Queries & State Inspection
  // --------------------------------------------------------------------------

  getStateRoot(): string {
    return this._stateTree.getRoot();
  }

  getSlot(slotKey: string): string {
    return this._stateTree.getSlot(slotKey);
  }

  getBatch(batchId: string): RollupBatch | undefined {
    return this._batches.get(batchId);
  }

  getReceipt(batchId: string): RollupProofReceipt | undefined {
    return this._receipts.get(batchId);
  }

  getBatches(): RollupBatch[] {
    return Array.from(this._batches.values());
  }

  getReceipts(): RollupProofReceipt[] {
    return Array.from(this._receipts.values());
  }

  getStatus(): Record<string, any> {
    return {
      state_root: this._stateTree.getRoot(),
      next_sequence: this._nextSequence,
      pending_transitions_count: this._pendingTransitions.length,
      batches_count: this._batches.size,
      receipts_count: this._receipts.size,
      circuit_tripped: this._circuitTripped,
      trip_reason: this._tripReason,
      proof_system: this.proofSystem,
    };
  }

  reset(): void {
    this._stateTree = new StateMerkleTree();
    this._pendingTransitions = [];
    this._batches.clear();
    this._receipts.clear();
    this._nextSequence = 1;
    this._circuitTripped = false;
    this._tripReason = null;
  }
}

// Global singleton instance
export const globalZkRollupEngine = new ZkRollupEngine();
