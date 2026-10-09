/**
 * SwarmWatch Cross-Cluster Byzantine Consensus & Proof-of-Execution Oracle (Seam 74 / ADR 0085).
 *
 * Implements Practical Byzantine Fault Tolerance (PBFT) consensus across heterogeneous agent clusters,
 * threshold multi-node cryptographic attestation, deterministic equivocation detection and slashing,
 * fail-closed safety circuit coupling, and tamper-evident Proof-of-Execution (PoE) oracle certificates.
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class ByzantineOracleError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ByzantineOracleError";
  }
}

export class ByzantineQuorumExceededError extends ByzantineOracleError {
  constructor(message: string) {
    super(message);
    this.name = "ByzantineQuorumExceededError";
  }
}

export class EquivocationDetectedError extends ByzantineOracleError {
  constructor(message: string) {
    super(message);
    this.name = "EquivocationDetectedError";
  }
}

export class InvalidOracleSignatureError extends ByzantineOracleError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidOracleSignatureError";
  }
}

export class OracleCircuitTrippedError extends ByzantineOracleError {
  constructor(message: string) {
    super(message);
    this.name = "OracleCircuitTrippedError";
  }
}

export class InvalidViewSequenceError extends ByzantineOracleError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidViewSequenceError";
  }
}

export class ConsensusTimeoutError extends ByzantineOracleError {
  constructor(message: string) {
    super(message);
    this.name = "ConsensusTimeoutError";
  }
}

// ==============================================================================
// 2. Domain Types & Object Enums
// ==============================================================================

export type BftPhase =
  | "PRE_PREPARE"
  | "PREPARE"
  | "COMMIT"
  | "DECIDED"
  | "VIEW_CHANGE";

export const BftPhase = {
  PRE_PREPARE: "PRE_PREPARE" as BftPhase,
  PREPARE: "PREPARE" as BftPhase,
  COMMIT: "COMMIT" as BftPhase,
  DECIDED: "DECIDED" as BftPhase,
  VIEW_CHANGE: "VIEW_CHANGE" as BftPhase,
} as const;

export type NodeRole = "PRIMARY" | "REPLICA" | "OBSERVER";

export const NodeRole = {
  PRIMARY: "PRIMARY" as NodeRole,
  REPLICA: "REPLICA" as NodeRole,
  OBSERVER: "OBSERVER" as NodeRole,
} as const;

export type NodeStatus = "ACTIVE" | "DEGRADED" | "SLASHED" | "OFFLINE";

export const NodeStatus = {
  ACTIVE: "ACTIVE" as NodeStatus,
  DEGRADED: "DEGRADED" as NodeStatus,
  SLASHED: "SLASHED" as NodeStatus,
  OFFLINE: "OFFLINE" as NodeStatus,
} as const;

// ==============================================================================
// 3. Cryptographic Helper Functions
// ==============================================================================

export function canonicalJsonString(data: Record<string, any>): string {
  const keys = Object.keys(data).sort();
  const sortedObj: Record<string, any> = {};
  for (const k of keys) {
    sortedObj[k] = data[k];
  }
  return JSON.stringify(sortedObj);
}

const ED25519_SPKI_PREFIX = Buffer.from("302a300506032b6570032100", "hex");
const ED25519_PKCS8_PREFIX = Buffer.from("302e020100300506032b657004220420", "hex");

const publicKeyCache = new Map<string, crypto.KeyObject>();
const privateKeyCache = new Map<string, crypto.KeyObject>();

function getOrCreatePublicKey(publicKeyHex: string): crypto.KeyObject {
  let key = publicKeyCache.get(publicKeyHex);
  if (!key) {
    const rawPub = Buffer.from(publicKeyHex, "hex");
    key = crypto.createPublicKey({
      key: Buffer.concat([ED25519_SPKI_PREFIX, rawPub]),
      format: "der",
      type: "spki",
    });
    if (publicKeyCache.size < 1000) {
      publicKeyCache.set(publicKeyHex, key);
    }
  }
  return key;
}

function getOrCreatePrivateKey(privateKeyHex: string): crypto.KeyObject {
  let key = privateKeyCache.get(privateKeyHex);
  if (!key) {
    const rawPriv = Buffer.from(privateKeyHex, "hex");
    key = crypto.createPrivateKey({
      key: Buffer.concat([ED25519_PKCS8_PREFIX, rawPriv]),
      format: "der",
      type: "pkcs8",
    });
    if (privateKeyCache.size < 1000) {
      privateKeyCache.set(privateKeyHex, key);
    }
  }
  return key;
}

export function generateNodeKeypair(): { privateKey: string; publicKeyHex: string } {
  const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
  const rawPub = publicKey.export({ type: "spki", format: "der" });
  const rawPriv = privateKey.export({ type: "pkcs8", format: "der" });
  const privHex = rawPriv.subarray(rawPriv.length - 32).toString("hex");
  const pubHex = rawPub.subarray(rawPub.length - 32).toString("hex");
  publicKeyCache.set(pubHex, publicKey);
  privateKeyCache.set(privHex, privateKey);
  return {
    privateKey: privHex,
    publicKeyHex: pubHex,
  };
}

export function signBftPayload(privateKeyHex: string, message: string): string {
  try {
    if (privateKeyHex.length !== 64) {
      throw new Error(`Invalid private key length: ${privateKeyHex.length} hex chars, expected 64`);
    }
    const privKey = getOrCreatePrivateKey(privateKeyHex);
    return crypto.sign(null, Buffer.from(message, "utf8"), privKey).toString("hex");
  } catch (err: any) {
    throw new ByzantineOracleError(`Failed to sign BFT payload: ${err?.message ?? err}`);
  }
}

const sigVerificationCache = new Map<string, boolean>();
const MAX_SIG_CACHE_SIZE = 8192;

export function verifyBftSignature(
  publicKeyHex: string,
  message: string,
  signatureHex: string,
  privateKey?: string
): boolean {
  try {
    if (publicKeyHex.length !== 64 || signatureHex.length !== 128) {
      return false;
    }
    const cacheKey = `${publicKeyHex}:${signatureHex}:${message}`;
    const cached = sigVerificationCache.get(cacheKey);
    if (cached !== undefined) {
      return cached;
    }
    const sig = Buffer.from(signatureHex, "hex");
    const pubKey = getOrCreatePublicKey(publicKeyHex);
    const valid = crypto.verify(null, Buffer.from(message, "utf8"), pubKey, sig);
    if (sigVerificationCache.size >= MAX_SIG_CACHE_SIZE) {
      const firstKey = sigVerificationCache.keys().next().value;
      if (firstKey !== undefined) sigVerificationCache.delete(firstKey);
    }
    sigVerificationCache.set(cacheKey, valid);
    return valid;
  } catch {
    return false;
  }
}

export function computeCanonicalPoeHash(certDict: Record<string, any>): string {
  const cleanDict: Record<string, any> = {
    action_type: certDict.action_type ?? "",
    certificate_id: certDict.certificate_id ?? "",
    committee_root: certDict.committee_root ?? "",
    digest: certDict.digest ?? "",
    epoch: certDict.epoch ?? 1,
    post_state_hash: certDict.post_state_hash ?? "",
    pre_state_hash: certDict.pre_state_hash ?? "",
    proposal_id: certDict.proposal_id ?? "",
    quorum_required: certDict.quorum_required ?? 0,
    sequence_number: certDict.sequence_number ?? 0,
    signers: Array.isArray(certDict.signers) ? [...certDict.signers].sort() : [],
    view: certDict.view ?? 0,
  };
  const rawDigest = crypto
    .createHash("sha256")
    .update(canonicalJsonString(cleanDict), "utf8")
    .digest("hex");
  return `sw:oracle:poe:v1:${rawDigest}`;
}

// ==============================================================================
// 4. Domain Models
// ==============================================================================

export class ProposalPayload {
  proposal_id: string = "";
  action_type: string = "";
  target_cluster: string = "";
  tenant_id: string = "tenant-default";
  pre_state_hash: string = "";
  post_state_hash: string = "";
  payload_data: Record<string, any> = {};
  timestamp_ns: number = 0;

  constructor(params: {
    proposal_id: string;
    action_type: string;
    target_cluster: string;
    tenant_id?: string;
    pre_state_hash?: string;
    post_state_hash?: string;
    payload_data?: Record<string, any>;
    timestamp_ns?: number;
  }) {
    this.proposal_id = params.proposal_id;
    this.action_type = params.action_type;
    this.target_cluster = params.target_cluster;
    this.tenant_id = params.tenant_id ?? "tenant-default";
    this.pre_state_hash = params.pre_state_hash ?? "";
    this.post_state_hash = params.post_state_hash ?? "";
    this.payload_data = params.payload_data ?? {};
    this.timestamp_ns = params.timestamp_ns ?? Date.now() * 1_000_000;
  }

  computeDigest(): string {
    const d = {
      action_type: this.action_type,
      payload_data: this.payload_data,
      post_state_hash: this.post_state_hash,
      pre_state_hash: this.pre_state_hash,
      proposal_id: this.proposal_id,
      target_cluster: this.target_cluster,
      tenant_id: this.tenant_id,
    };
    return crypto.createHash("sha256").update(canonicalJsonString(d), "utf8").digest("hex");
  }

  toDict(): Record<string, any> {
    return {
      proposal_id: this.proposal_id,
      action_type: this.action_type,
      target_cluster: this.target_cluster,
      tenant_id: this.tenant_id,
      pre_state_hash: this.pre_state_hash,
      post_state_hash: this.post_state_hash,
      payload_data: this.payload_data,
      timestamp_ns: this.timestamp_ns,
    };
  }
}

export class BftMessage {
  msg_id: string = "";
  phase: BftPhase = "PRE_PREPARE";
  view: number = 0;
  sequence_number: number = 0;
  proposal_id: string = "";
  digest: string = "";
  node_id: string = "";
  cluster_id: string = "";
  signature: string = "";
  timestamp_ns: number = 0;

  constructor(params: {
    msg_id: string;
    phase: BftPhase;
    view: number;
    sequence_number: number;
    proposal_id: string;
    digest: string;
    node_id: string;
    cluster_id: string;
    signature?: string;
    timestamp_ns?: number;
  }) {
    this.msg_id = params.msg_id;
    this.phase = params.phase;
    this.view = params.view;
    this.sequence_number = params.sequence_number;
    this.proposal_id = params.proposal_id;
    this.digest = params.digest;
    this.node_id = params.node_id;
    this.cluster_id = params.cluster_id;
    this.signature = params.signature ?? "";
    this.timestamp_ns = params.timestamp_ns ?? Date.now() * 1_000_000;
  }

  signingContent(): string {
    return `${this.phase}:${this.view}:${this.sequence_number}:${this.proposal_id}:${this.digest}:${this.node_id}`;
  }

  toDict(): Record<string, any> {
    return {
      msg_id: this.msg_id,
      phase: this.phase,
      view: this.view,
      sequence_number: this.sequence_number,
      proposal_id: this.proposal_id,
      digest: this.digest,
      node_id: this.node_id,
      cluster_id: this.cluster_id,
      signature: this.signature,
      timestamp_ns: this.timestamp_ns,
    };
  }
}

export class EquivocationEvent {
  event_id: string = "";
  node_id: string = "";
  cluster_id: string = "";
  view: number = 0;
  sequence_number: number = 0;
  digest_1: string = "";
  digest_2: string = "";
  slashed_at_ns: number = 0;

  constructor(params: {
    event_id: string;
    node_id: string;
    cluster_id: string;
    view: number;
    sequence_number: number;
    digest_1: string;
    digest_2: string;
    slashed_at_ns?: number;
  }) {
    this.event_id = params.event_id;
    this.node_id = params.node_id;
    this.cluster_id = params.cluster_id;
    this.view = params.view;
    this.sequence_number = params.sequence_number;
    this.digest_1 = params.digest_1;
    this.digest_2 = params.digest_2;
    this.slashed_at_ns = params.slashed_at_ns ?? Date.now() * 1_000_000;
  }

  toDict(): Record<string, any> {
    return {
      event_id: this.event_id,
      node_id: this.node_id,
      cluster_id: this.cluster_id,
      view: this.view,
      sequence_number: this.sequence_number,
      digest_1: this.digest_1,
      digest_2: this.digest_2,
      slashed_at_ns: this.slashed_at_ns,
    };
  }
}

export class ProofOfExecutionCertificate {
  certificate_id: string = "";
  proposal_id: string = "";
  action_type: string = "";
  view: number = 0;
  sequence_number: number = 0;
  digest: string = "";
  pre_state_hash: string = "";
  post_state_hash: string = "";
  signers: string[] = [];
  signatures: Record<string, string> = {};
  timestamp_ns: number = 0;
  receipt_hash: string = "";
  duration_us: number = 0;
  epoch: number = 1;
  committee_root: string = "";
  quorum_required: number = 0;

  constructor(params: {
    certificate_id: string;
    proposal_id: string;
    action_type: string;
    view: number;
    sequence_number: number;
    digest: string;
    pre_state_hash: string;
    post_state_hash: string;
    signers: string[];
    signatures: Record<string, string>;
    timestamp_ns: number;
    receipt_hash: string;
    duration_us?: number;
    epoch?: number;
    committee_root?: string;
    quorum_required?: number;
  }) {
    this.certificate_id = params.certificate_id;
    this.proposal_id = params.proposal_id;
    this.action_type = params.action_type;
    this.view = params.view;
    this.sequence_number = params.sequence_number;
    this.digest = params.digest;
    this.pre_state_hash = params.pre_state_hash;
    this.post_state_hash = params.post_state_hash;
    this.signers = params.signers;
    this.signatures = params.signatures;
    this.timestamp_ns = params.timestamp_ns;
    this.receipt_hash = params.receipt_hash;
    this.duration_us = params.duration_us ?? 0;
    this.epoch = params.epoch ?? 1;
    this.committee_root = params.committee_root ?? "";
    this.quorum_required = params.quorum_required ?? 0;
  }

  toDict(): Record<string, any> {
    return {
      certificate_id: this.certificate_id,
      proposal_id: this.proposal_id,
      action_type: this.action_type,
      view: this.view,
      sequence_number: this.sequence_number,
      digest: this.digest,
      pre_state_hash: this.pre_state_hash,
      post_state_hash: this.post_state_hash,
      signers: this.signers,
      signatures: this.signatures,
      timestamp_ns: this.timestamp_ns,
      receipt_hash: this.receipt_hash,
      duration_us: this.duration_us,
      epoch: this.epoch,
      committee_root: this.committee_root,
      quorum_required: this.quorum_required,
    };
  }
}

export class ValidatorNode {
  node_id: string = "";
  cluster_id: string = "";
  public_key_hex: string = "";
  role: NodeRole = "REPLICA";
  status: NodeStatus = "ACTIVE";
  fault_score: number = 0;
  registered_at_ns: number = 0;

  constructor(params: {
    node_id: string;
    cluster_id: string;
    public_key_hex: string;
    role?: NodeRole;
    status?: NodeStatus;
    secret_key?: string;
    fault_score?: number;
    registered_at_ns?: number;
  }) {
    this.node_id = params.node_id;
    this.cluster_id = params.cluster_id;
    this.public_key_hex = params.public_key_hex;
    this.role = params.role ?? "REPLICA";
    this.status = params.status ?? "ACTIVE";
    this.fault_score = params.fault_score ?? 0;
    this.registered_at_ns = params.registered_at_ns ?? Date.now() * 1_000_000;
  }

  toDict(): Record<string, any> {
    return {
      node_id: this.node_id,
      cluster_id: this.cluster_id,
      public_key_hex: this.public_key_hex,
      role: this.role,
      status: this.status,
      fault_score: this.fault_score,
      registered_at_ns: this.registered_at_ns,
    };
  }
}

export class NodeSigner {
  node_id: string;
  cluster_id: string;
  private_key_hex: string;
  public_key_hex: string;

  constructor(params: {
    node_id: string;
    cluster_id: string;
    private_key_hex: string;
    public_key_hex?: string;
  }) {
    this.node_id = params.node_id;
    this.cluster_id = params.cluster_id;
    this.private_key_hex = params.private_key_hex;
    if (params.public_key_hex) {
      this.public_key_hex = params.public_key_hex;
    } else {
      const priv = getOrCreatePrivateKey(params.private_key_hex);
      const pub = crypto.createPublicKey(priv);
      const rawPub = pub.export({ type: "spki", format: "der" });
      this.public_key_hex = rawPub.subarray(rawPub.length - 32).toString("hex");
    }
  }

  signPhase(msg: BftMessage): BftMessage {
    msg.node_id = this.node_id;
    msg.cluster_id = this.cluster_id;
    msg.signature = signBftPayload(this.private_key_hex, msg.signingContent());
    return msg;
  }
}

// ==============================================================================
// 5. Core Byzantine Consensus Oracle Engine
// ==============================================================================

export class ByzantineConsensusOracle {
  tenant_id: string = "tenant-default";
  private circuitTripped: boolean = false;
  private view: number = 0;
  private sequenceNumber: number = 0;

  private nodes: Map<string, ValidatorNode> = new Map();
  private proposals: Map<string, ProposalPayload> = new Map();
  private signedDigests: Map<string, string> = new Map();
  private equivocationEvents: EquivocationEvent[] = [];

  private prepareVotes: Map<string, Map<string, string>> = new Map();
  private commitVotes: Map<string, Map<string, string>> = new Map();
  private phaseStates: Map<string, BftPhase> = new Map();
  private certificates: Map<string, ProofOfExecutionCertificate> = new Map();
  private _committeeSize: number = 0;
  private _epoch: number = 1;

  constructor(tenant_id: string = "tenant-default") {
    this.tenant_id = tenant_id;
  }

  // --------------------------------------------------------------------------
  // Circuit Breaker Integration
  // --------------------------------------------------------------------------

  tripCircuit(): void {
    this.circuitTripped = true;
  }

  resetCircuit(): void {
    this.circuitTripped = false;
  }

  isCircuitTripped(): boolean {
    if (this.committeeSize >= 4 && this.activeNodesCount < this.quorumSizeQ) {
      this.circuitTripped = true;
    }
    return this.circuitTripped;
  }

  setNodeStatus(nodeId: string, status: NodeStatus): void {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.status = status;
    }
    if (this.committeeSize >= 4 && this.activeNodesCount < this.quorumSizeQ) {
      this.circuitTripped = true;
    }
  }

  // --------------------------------------------------------------------------
  // Cluster Node Management & PBFT Math
  // --------------------------------------------------------------------------

  registerNode(params: {
    node_id: string;
    cluster_id: string;
    public_key_hex: string;
    secret_key?: string;
    role?: NodeRole;
  }): ValidatorNode {
    const node = new ValidatorNode({
      node_id: params.node_id,
      cluster_id: params.cluster_id,
      public_key_hex: params.public_key_hex,
      role: params.role ?? "REPLICA",
      status: "ACTIVE",
    });
    this.nodes.set(params.node_id, node);
    return node;
  }

  listNodes(): ValidatorNode[] {
    return Array.from(this.nodes.values());
  }

  getNode(nodeId: string): ValidatorNode | undefined {
    return this.nodes.get(nodeId);
  }

  get totalNodesCount(): number {
    return this.nodes.size;
  }

  get activeNodesCount(): number {
    let count = 0;
    for (const n of this.nodes.values()) {
      if (n.status === "ACTIVE") count++;
    }
    return count;
  }

  get epoch(): number {
    return this._epoch;
  }

  get committeeSize(): number {
    if (this._committeeSize > 0) return this._committeeSize;
    return this.nodes.size;
  }

  setCommitteeSize(size: number): void {
    this._committeeSize = size;
  }

  reconfigureCommittee(newSize: number, reason: string = ""): number {
    this._epoch++;
    this._committeeSize = newSize;
    return this._epoch;
  }

  computeCommitteeRoot(): string {
    const activePks = Array.from(this.nodes.values())
      .filter((n) => n.status === "ACTIVE")
      .map((n) => n.public_key_hex)
      .sort();
    return crypto
      .createHash("sha256")
      .update(canonicalJsonString(activePks), "utf8")
      .digest("hex");
  }

  get faultToleranceF(): number {
    const n = this.committeeSize;
    if (n < 1) return 0;
    return Math.floor(Math.max(0, (n - 1) / 3));
  }

  get quorumSizeQ(): number {
    const n = this.committeeSize;
    if (n <= 1) return 1;
    const f = this.faultToleranceF;
    return Math.ceil((n + f + 1) / 2);
  }

  getPrimaryNode(): ValidatorNode | undefined {
    const active = Array.from(this.nodes.values())
      .filter((n) => n.status === "ACTIVE")
      .sort((a, b) => a.node_id.localeCompare(b.node_id));
    if (active.length === 0) return undefined;
    const idx = this.view % active.length;
    return active[idx];
  }

  // --------------------------------------------------------------------------
  // Slashing & Equivocation Defense
  // --------------------------------------------------------------------------

  slashNode(nodeId: string, reason: string = "Equivocation detected"): void {
    const node = this.nodes.get(nodeId);
    if (node) {
      node.status = "SLASHED";
      node.fault_score += 100;
    }

    let slashedCount = 0;
    for (const n of this.nodes.values()) {
      if (n.status === "SLASHED") slashedCount++;
    }
    if (slashedCount > this.faultToleranceF && this.committeeSize >= 4) {
      this.tripCircuit();
    }
    if (this.activeNodesCount < this.quorumSizeQ && this.committeeSize >= 4) {
      this.tripCircuit();
    }
  }

  private checkAndRecordSigning(nodeId: string, view: number, seq: number, digest: string, clusterId: string): void {
    const key = `${nodeId}:${view}:${seq}`;
    const existing = this.signedDigests.get(key);
    if (existing !== undefined && existing !== digest) {
      const evt = new EquivocationEvent({
        event_id: `eqv-${crypto.randomBytes(6).toString("hex")}`,
        node_id: nodeId,
        cluster_id: clusterId,
        view,
        sequence_number: seq,
        digest_1: existing,
        digest_2: digest,
      });
      this.equivocationEvents.push(evt);
      this.slashNode(nodeId, `Equivocation on view=${view}, seq=${seq}`);
      throw new EquivocationDetectedError(
        `Byzantine equivocation detected: node '${nodeId}' signed conflicting digests for view=${view}, seq=${seq}.`
      );
    }
    this.signedDigests.set(key, digest);
  }

  // --------------------------------------------------------------------------
  // PBFT Consensus Protocol Execution (Decoupled Node Signing & Oracle Verification)
  // --------------------------------------------------------------------------

  createPrePrepare(proposal: ProposalPayload, primaryNodeId?: string): BftMessage {
    if (this.circuitTripped) {
      throw new OracleCircuitTrippedError("Cannot propose action: SwarmWatch safety circuit is tripped.");
    }
    const primary = primaryNodeId ? this.nodes.get(primaryNodeId) : this.getPrimaryNode();
    if (!primary || primary.status !== "ACTIVE") {
      throw new ByzantineQuorumExceededError("Cannot propose: Primary validator node unavailable or slashed.");
    }

    this.sequenceNumber++;
    const seq = this.sequenceNumber;
    const view = this.view;
    const digest = proposal.computeDigest();
    this.proposals.set(proposal.proposal_id, proposal);

    return new BftMessage({
      msg_id: `msg-pre-prepare-${crypto.randomBytes(6).toString("hex")}`,
      phase: BftPhase.PRE_PREPARE,
      view,
      sequence_number: seq,
      proposal_id: proposal.proposal_id,
      digest,
      node_id: primary.node_id,
      cluster_id: primary.cluster_id,
    });
  }

  acceptPrePrepare(msg: BftMessage): boolean {
    if (this.circuitTripped) {
      throw new OracleCircuitTrippedError("Cannot accept pre-prepare: SwarmWatch safety circuit is tripped.");
    }
    const primary = this.nodes.get(msg.node_id);
    if (!primary || primary.status !== "ACTIVE") {
      throw new InvalidOracleSignatureError(`Primary node '${msg.node_id}' is invalid or slashed.`);
    }

    if (!verifyBftSignature(primary.public_key_hex, msg.signingContent(), msg.signature)) {
      throw new InvalidOracleSignatureError("Pre-prepare signature verification failed.");
    }

    this.checkAndRecordSigning(msg.node_id, msg.view, msg.sequence_number, msg.digest, primary.cluster_id);
    this.phaseStates.set(`${msg.view}:${msg.sequence_number}`, BftPhase.PRE_PREPARE);

    const voteKey = `${msg.view}:${msg.sequence_number}:${msg.digest}`;
    if (!this.prepareVotes.has(voteKey)) {
      this.prepareVotes.set(voteKey, new Map());
    }
    this.prepareVotes.get(voteKey)!.set(primary.node_id, msg.signature);
    return true;
  }

  propose(
    proposal: ProposalPayload,
    opts?: { privateKey?: string; signature?: string; signer?: NodeSigner } | string
  ): BftMessage {
    const msg = this.createPrePrepare(proposal);
    const privKey = typeof opts === "string" ? opts : opts?.privateKey;
    const signature = typeof opts === "object" ? opts?.signature : undefined;
    const signer = typeof opts === "object" ? opts?.signer : undefined;

    if (signer) {
      signer.signPhase(msg);
    } else if (signature) {
      msg.signature = signature;
    } else if (privKey) {
      msg.signature = signBftPayload(privKey, msg.signingContent());
    } else {
      throw new ByzantineOracleError(
        `Primary node '${msg.node_id}' pre-prepare must be signed externally (provide signer, signature, or privateKey).`
      );
    }
    this.acceptPrePrepare(msg);
    return msg;
  }

  createPrepare(prePrepareMsg: BftMessage, nodeId: string): BftMessage {
    if (this.circuitTripped) {
      throw new OracleCircuitTrippedError("Cannot prepare: SwarmWatch safety circuit is tripped.");
    }
    const node = this.nodes.get(nodeId);
    if (!node || node.status !== "ACTIVE") {
      throw new ByzantineOracleError(`Node '${nodeId}' is not active or authorized.`);
    }

    return new BftMessage({
      msg_id: `msg-prepare-${crypto.randomBytes(6).toString("hex")}`,
      phase: BftPhase.PREPARE,
      view: prePrepareMsg.view,
      sequence_number: prePrepareMsg.sequence_number,
      proposal_id: prePrepareMsg.proposal_id,
      digest: prePrepareMsg.digest,
      node_id: node.node_id,
      cluster_id: node.cluster_id,
    });
  }

  acceptPrepare(prepMsg: BftMessage): boolean {
    if (this.circuitTripped) {
      throw new OracleCircuitTrippedError("Cannot accept prepare: SwarmWatch safety circuit is tripped.");
    }
    const node = this.nodes.get(prepMsg.node_id);
    if (!node || node.status !== "ACTIVE") {
      throw new ByzantineOracleError(`Node '${prepMsg.node_id}' is not active or authorized.`);
    }

    if (prepMsg.view !== this.view) {
      throw new InvalidViewSequenceError(`Message view ${prepMsg.view} does not match active view ${this.view}.`);
    }

    if (!verifyBftSignature(node.public_key_hex, prepMsg.signingContent(), prepMsg.signature)) {
      throw new InvalidOracleSignatureError(`Prepare signature verification failed for node '${prepMsg.node_id}'.`);
    }

    this.checkAndRecordSigning(prepMsg.node_id, prepMsg.view, prepMsg.sequence_number, prepMsg.digest, node.cluster_id);

    const voteKey = `${prepMsg.view}:${prepMsg.sequence_number}:${prepMsg.digest}`;
    if (!this.prepareVotes.has(voteKey)) {
      this.prepareVotes.set(voteKey, new Map());
    }
    this.prepareVotes.get(voteKey)!.set(prepMsg.node_id, prepMsg.signature);

    if (this.prepareVotes.get(voteKey)!.size >= this.quorumSizeQ) {
      this.phaseStates.set(`${prepMsg.view}:${prepMsg.sequence_number}`, BftPhase.PREPARE);
    }
    return true;
  }

  prepare(
    prePrepareMsg: BftMessage,
    nodeId: string,
    opts?: { privateKey?: string; signature?: string; signer?: NodeSigner } | string
  ): BftMessage {
    const prepMsg = this.createPrepare(prePrepareMsg, nodeId);
    const privKey = typeof opts === "string" ? opts : opts?.privateKey;
    const signature = typeof opts === "object" ? opts?.signature : undefined;
    const signer = typeof opts === "object" ? opts?.signer : undefined;

    if (signer) {
      signer.signPhase(prepMsg);
    } else if (signature) {
      prepMsg.signature = signature;
    } else if (privKey) {
      prepMsg.signature = signBftPayload(privKey, prepMsg.signingContent());
    } else {
      throw new ByzantineOracleError(
        `Node '${nodeId}' prepare message must be signed externally (provide signer, signature, or privateKey).`
      );
    }
    this.acceptPrepare(prepMsg);
    return prepMsg;
  }

  createCommit(prepareMsg: BftMessage, nodeId: string): BftMessage {
    if (this.circuitTripped) {
      throw new OracleCircuitTrippedError("Cannot commit: SwarmWatch safety circuit is tripped.");
    }
    const node = this.nodes.get(nodeId);
    if (!node || node.status !== "ACTIVE") {
      throw new ByzantineOracleError(`Node '${nodeId}' is not active or authorized.`);
    }

    const voteKey = `${prepareMsg.view}:${prepareMsg.sequence_number}:${prepareMsg.digest}`;
    const prepares = this.prepareVotes.get(voteKey);
    if (!prepares || prepares.size < this.quorumSizeQ) {
      throw new ByzantineQuorumExceededError(
        `Cannot enter COMMIT phase: Insufficient prepare votes (${prepares?.size ?? 0} < quorum ${this.quorumSizeQ}).`
      );
    }

    return new BftMessage({
      msg_id: `msg-commit-${crypto.randomBytes(6).toString("hex")}`,
      phase: BftPhase.COMMIT,
      view: prepareMsg.view,
      sequence_number: prepareMsg.sequence_number,
      proposal_id: prepareMsg.proposal_id,
      digest: prepareMsg.digest,
      node_id: node.node_id,
      cluster_id: node.cluster_id,
    });
  }

  acceptCommit(commitMsg: BftMessage): boolean {
    if (this.circuitTripped) {
      throw new OracleCircuitTrippedError("Cannot accept commit: SwarmWatch safety circuit is tripped.");
    }
    const node = this.nodes.get(commitMsg.node_id);
    if (!node || node.status !== "ACTIVE") {
      throw new ByzantineOracleError(`Node '${commitMsg.node_id}' is not active or authorized.`);
    }

    if (commitMsg.view !== this.view) {
      throw new InvalidViewSequenceError(`Message view ${commitMsg.view} does not match active view ${this.view}.`);
    }

    if (!verifyBftSignature(node.public_key_hex, commitMsg.signingContent(), commitMsg.signature)) {
      throw new InvalidOracleSignatureError(`Commit signature verification failed for node '${commitMsg.node_id}'.`);
    }

    this.checkAndRecordSigning(commitMsg.node_id, commitMsg.view, commitMsg.sequence_number, commitMsg.digest, node.cluster_id);

    const voteKey = `${commitMsg.view}:${commitMsg.sequence_number}:${commitMsg.digest}`;
    if (!this.commitVotes.has(voteKey)) {
      this.commitVotes.set(voteKey, new Map());
    }
    this.commitVotes.get(voteKey)!.set(commitMsg.node_id, commitMsg.signature);

    if (this.commitVotes.get(voteKey)!.size >= this.quorumSizeQ) {
      this.phaseStates.set(`${commitMsg.view}:${commitMsg.sequence_number}`, BftPhase.COMMIT);
    }
    return true;
  }

  commit(
    prepareMsg: BftMessage,
    nodeId: string,
    opts?: { privateKey?: string; signature?: string; signer?: NodeSigner } | string
  ): BftMessage {
    const commitMsg = this.createCommit(prepareMsg, nodeId);
    const privKey = typeof opts === "string" ? opts : opts?.privateKey;
    const signature = typeof opts === "object" ? opts?.signature : undefined;
    const signer = typeof opts === "object" ? opts?.signer : undefined;

    if (signer) {
      signer.signPhase(commitMsg);
    } else if (signature) {
      commitMsg.signature = signature;
    } else if (privKey) {
      commitMsg.signature = signBftPayload(privKey, commitMsg.signingContent());
    } else {
      throw new ByzantineOracleError(
        `Node '${nodeId}' commit message must be signed externally (provide signer, signature, or privateKey).`
      );
    }
    this.acceptCommit(commitMsg);
    return commitMsg;
  }

  executeAndCertify(proposalId: string, postStateHash?: string): ProofOfExecutionCertificate {
    const t0 = performance.now();
    if (this.circuitTripped) {
      throw new OracleCircuitTrippedError("Cannot certify execution: SwarmWatch safety circuit is tripped.");
    }

    const proposal = this.proposals.get(proposalId);
    if (!proposal) {
      throw new ByzantineOracleError(`Proposal '${proposalId}' not found.`);
    }

    const seq = this.sequenceNumber;
    const view = this.view;
    const digest = proposal.computeDigest();
    const voteKey = `${view}:${seq}:${digest}`;

    const commitMap = this.commitVotes.get(voteKey);
    if (!commitMap || commitMap.size < this.quorumSizeQ) {
      throw new ByzantineQuorumExceededError(
        `Cannot certify execution: Insufficient commit votes (${commitMap?.size ?? 0} < quorum ${this.quorumSizeQ}).`
      );
    }

    const effectivePostHash =
      postStateHash ??
      proposal.post_state_hash ??
      crypto.createHash("sha256").update(`post:${digest}`, "utf8").digest("hex");

    const certId = `poe-cert-${crypto.randomBytes(8).toString("hex")}`;
    const signers = Array.from(commitMap.keys()).sort();
    const signatures: Record<string, string> = {};
    for (const [k, v] of commitMap.entries()) {
      signatures[k] = v;
    }

    const epoch = this._epoch;
    const committeeRoot = this.computeCommitteeRoot();
    const quorumRequired = this.quorumSizeQ;

    const certDict: Record<string, any> = {
      action_type: proposal.action_type,
      certificate_id: certId,
      committee_root: committeeRoot,
      digest,
      epoch,
      post_state_hash: effectivePostHash,
      pre_state_hash: proposal.pre_state_hash,
      proposal_id: proposal.proposal_id,
      quorum_required: quorumRequired,
      sequence_number: seq,
      signers,
      view,
    };
    const receiptHash = computeCanonicalPoeHash(certDict);

    const durationUs = (performance.now() - t0) * 1000.0;
    const cert = new ProofOfExecutionCertificate({
      certificate_id: certId,
      proposal_id: proposal.proposal_id,
      action_type: proposal.action_type,
      view,
      sequence_number: seq,
      digest,
      pre_state_hash: proposal.pre_state_hash,
      post_state_hash: effectivePostHash,
      signers,
      signatures,
      timestamp_ns: Date.now() * 1_000_000,
      receipt_hash: receiptHash,
      duration_us: durationUs,
      epoch,
      committee_root: committeeRoot,
      quorum_required: quorumRequired,
    });

    this.phaseStates.set(`${view}:${seq}`, BftPhase.DECIDED);
    this.certificates.set(certId, cert);
    return cert;
  }

  // --------------------------------------------------------------------------
  // Oracle Certificate Verification
  // --------------------------------------------------------------------------

  verifyPoeCertificate(cert: ProofOfExecutionCertificate | any): boolean {
    const certDict = typeof cert.toDict === "function" ? cert.toDict() : cert;
    const expectedHash = computeCanonicalPoeHash(certDict);
    if (expectedHash !== cert.receipt_hash) {
      return false;
    }

    if (cert.epoch !== undefined && cert.epoch !== this._epoch) {
      return false;
    }

    const expectedCommitteeRoot = this.computeCommitteeRoot();
    if (cert.committee_root && cert.committee_root !== expectedCommitteeRoot) {
      return false;
    }

    const reqQuorum = cert.quorum_required ?? this.quorumSizeQ;
    if (cert.signers.length < reqQuorum) {
      return false;
    }

    if (new Set(cert.signers).size !== cert.signers.length) {
      return false;
    }

    for (const sId of cert.signers) {
      const node = this.nodes.get(sId);
      if (!node || node.status === "SLASHED") {
        return false;
      }
      const sig = cert.signatures[sId];
      if (!sig) {
        return false;
      }
      const expectedContent = `${BftPhase.COMMIT}:${cert.view}:${cert.sequence_number}:${cert.proposal_id}:${cert.digest}:${sId}`;
      const valid = verifyBftSignature(node.public_key_hex, expectedContent, sig);
      if (!valid) {
        return false;
      }
    }

    return true;
  }

  // --------------------------------------------------------------------------
  // View Change
  // --------------------------------------------------------------------------

  triggerViewChange(reason: string = "Primary failure or timeout"): number {
    if (this.circuitTripped) {
      throw new OracleCircuitTrippedError("Cannot trigger view change: safety circuit is tripped.");
    }
    this.view++;
    return this.view;
  }

  // --------------------------------------------------------------------------
  // Receipts & Status
  // --------------------------------------------------------------------------

  getCertificates(limit: number = 100): ProofOfExecutionCertificate[] {
    const certs = Array.from(this.certificates.values());
    return certs.slice(-limit).reverse();
  }

  getCertificate(certificateId: string): ProofOfExecutionCertificate | undefined {
    return this.certificates.get(certificateId);
  }

  getEquivocationEvents(): EquivocationEvent[] {
    return [...this.equivocationEvents];
  }

  status(): Record<string, any> {
    const primary = this.getPrimaryNode();
    return {
      circuit_tripped: this.circuitTripped,
      current_view: this.view,
      sequence_number: this.sequenceNumber,
      total_nodes: this.totalNodesCount,
      active_nodes: this.activeNodesCount,
      fault_tolerance_f: this.faultToleranceF,
      quorum_size_q: this.quorumSizeQ,
      primary_node_id: primary ? primary.node_id : null,
      primary_cluster_id: primary ? primary.cluster_id : null,
      total_proposals: this.proposals.size,
      total_certificates: this.certificates.size,
      equivocation_events_count: this.equivocationEvents.length,
      status_badge: this.circuitTripped ? "BYZANTINE: CIRCUIT TRIPPED" : "BYZANTINE: QUORUM OK",
    };
  }
}

// Global singleton instance
export const globalByzantineOracle = new ByzantineConsensusOracle();
