/**
 * SwarmWatch Autonomous Cross-Cluster State Synchronization & Distributed Hyper-Graph Reconciliation (Seam 77 / ADR 0088).
 *
 * Implements in-process, zero-token cross-cluster state synchronization, distributed hyper-graph CRDT
 * reconciliation, vector clock causal analysis, issuer-bound Proof-of-Execution certificate verification
 * (Invariant I1), fail-closed safety circuit coupling, and tamper-evident sync receipts ('sw:sync:v1:<sha256>').
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";
import {
  computeCanonicalPoeHash,
  signBftPayload,
  verifyBftSignature,
} from "./byzantine_oracle.ts";

// ==============================================================================
// 0. Cryptographic & Canonical Helpers
// ==============================================================================

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

export const canonicalJson = deepCanonicalJson;

export function sha256Hex(data: string): string {
  return crypto.createHash("sha256").update(data, "utf8").digest("hex");
}

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class CrossClusterSyncError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CrossClusterSyncError";
  }
}

export class SyncCircuitTrippedError extends CrossClusterSyncError {
  constructor(message: string) {
    super(message);
    this.name = "SyncCircuitTrippedError";
  }
}

export class UnverifiableCertificateError extends CrossClusterSyncError {
  constructor(message: string) {
    super(message);
    this.name = "UnverifiableCertificateError";
  }
}

export class CausalDiscontinuityError extends CrossClusterSyncError {
  constructor(message: string) {
    super(message);
    this.name = "CausalDiscontinuityError";
  }
}

export class SplitBrainForkError extends CrossClusterSyncError {
  constructor(message: string) {
    super(message);
    this.name = "SplitBrainForkError";
  }
}

export class HyperGraphIntegrityError extends CrossClusterSyncError {
  constructor(message: string) {
    super(message);
    this.name = "HyperGraphIntegrityError";
  }
}

// ==============================================================================
// 2. Domain Enums and Data Models
// ==============================================================================

export const SyncStatus = {
  NOMINAL: "NOMINAL",
  SYNCING: "SYNCING",
  RECONCILED: "RECONCILED",
  DEGRADED: "DEGRADED",
  CIRCUIT_TRIPPED: "CIRCUIT_TRIPPED",
} as const;
export type SyncStatus = (typeof SyncStatus)[keyof typeof SyncStatus];

export const CausalOrderingVerdict = {
  CAUSALLY_AHEAD: "CAUSALLY_AHEAD",
  CONCURRENT: "CONCURRENT",
  CAUSALLY_BEHIND: "CAUSALLY_BEHIND",
  CAUSAL_GAP_DETECTED: "CAUSAL_GAP_DETECTED",
} as const;
export type CausalOrderingVerdict = (typeof CausalOrderingVerdict)[keyof typeof CausalOrderingVerdict];

export const HyperNodeType = {
  AGENT_CONTEXT: "AGENT_CONTEXT",
  RESOURCE_LEASE: "RESOURCE_LEASE",
  SHARED_MEMORY: "SHARED_MEMORY",
  POLICY_STATE: "POLICY_STATE",
  DATA_PIPELINE: "DATA_PIPELINE",
} as const;
export type HyperNodeType = (typeof HyperNodeType)[keyof typeof HyperNodeType];

export const HyperEdgeType = {
  COORDINATION_LEASE: "COORDINATION_LEASE",
  JOINT_CONSENSUS: "JOINT_CONSENSUS",
  CAUSAL_DEPENDENCY: "CAUSAL_DEPENDENCY",
  CROSS_CLUSTER_REPLICATION: "CROSS_CLUSTER_REPLICATION",
} as const;
export type HyperEdgeType = (typeof HyperEdgeType)[keyof typeof HyperEdgeType];

export class HyperNode {
  node_id: string;
  cluster_id: string;
  state_type: HyperNodeType;
  state_payload: Record<string, any>;
  version_vector: Record<string, number>;
  epoch: number;
  updated_at_ns: number;
  digest: string;

  constructor(
    node_id: string,
    cluster_id: string,
    state_type: HyperNodeType,
    state_payload: Record<string, any>,
    version_vector?: Record<string, number>,
    epoch: number = 1,
    updated_at_ns?: number,
    digest: string = ""
  ) {
    this.node_id = node_id;
    this.cluster_id = cluster_id;
    this.state_type = state_type;
    this.state_payload = state_payload;
    this.version_vector = version_vector || {};
    this.epoch = epoch;
    this.updated_at_ns = updated_at_ns ?? Date.now() * 1_000_000;
    this.digest = digest || this.computeDigest();
  }

  computeDigest(): string {
    const sortedVV: Record<string, number> = {};
    for (const k of Object.keys(this.version_vector).sort()) {
      sortedVV[k] = this.version_vector[k];
    }
    const d = {
      cluster_id: this.cluster_id,
      epoch: this.epoch,
      node_id: this.node_id,
      state_payload: this.state_payload,
      state_type: this.state_type,
      version_vector: sortedVV,
    };
    return sha256Hex(deepCanonicalJson(d));
  }

  toDict(): Record<string, any> {
    return {
      node_id: this.node_id,
      cluster_id: this.cluster_id,
      state_type: this.state_type,
      state_payload: this.state_payload,
      version_vector: this.version_vector,
      epoch: this.epoch,
      updated_at_ns: this.updated_at_ns,
      digest: this.digest,
    };
  }

  to_dict(): Record<string, any> {
    return this.toDict();
  }

  static fromDict(data: Record<string, any>): HyperNode {
    return new HyperNode(
      data.node_id,
      data.cluster_id,
      data.state_type as HyperNodeType,
      data.state_payload || {},
      data.version_vector || {},
      data.epoch || 1,
      data.updated_at_ns || 0,
      data.digest || ""
    );
  }

  static from_dict(data: Record<string, any>): HyperNode {
    return HyperNode.fromDict(data);
  }
}

export class HyperEdge {
  edge_id: string;
  node_ids: string[];
  edge_type: HyperEdgeType;
  properties: Record<string, any>;
  weight: number;
  version_vector: Record<string, number>;
  epoch: number;
  updated_at_ns: number;
  digest: string;

  constructor(
    edge_id: string,
    node_ids: string[],
    edge_type: HyperEdgeType,
    properties: Record<string, any> = {},
    weight: number = 1.0,
    version_vector?: Record<string, number>,
    epoch: number = 1,
    updated_at_ns?: number,
    digest: string = ""
  ) {
    const dedupNodes = Array.from(new Set(node_ids)).sort();
    if (dedupNodes.length < 2) {
      throw new HyperGraphIntegrityError(
        `HyperEdge '${edge_id}' must connect at least 2 distinct nodes, found ${dedupNodes.length}`
      );
    }
    this.edge_id = edge_id;
    this.node_ids = dedupNodes;
    this.edge_type = edge_type;
    this.properties = properties;
    this.weight = Math.max(0.0, Math.min(1.0, Number(weight)));
    this.version_vector = version_vector || {};
    this.epoch = epoch;
    this.updated_at_ns = updated_at_ns ?? Date.now() * 1_000_000;
    this.digest = digest || this.computeDigest();
  }

  computeDigest(): string {
    const sortedVV: Record<string, number> = {};
    for (const k of Object.keys(this.version_vector).sort()) {
      sortedVV[k] = this.version_vector[k];
    }
    const d = {
      edge_id: this.edge_id,
      edge_type: this.edge_type,
      epoch: this.epoch,
      node_ids: this.node_ids,
      properties: this.properties,
      version_vector: sortedVV,
      weight: Math.round(this.weight * 10000) / 10000,
    };
    return sha256Hex(deepCanonicalJson(d));
  }

  toDict(): Record<string, any> {
    return {
      edge_id: this.edge_id,
      node_ids: this.node_ids,
      edge_type: this.edge_type,
      properties: this.properties,
      weight: this.weight,
      version_vector: this.version_vector,
      epoch: this.epoch,
      updated_at_ns: this.updated_at_ns,
      digest: this.digest,
    };
  }

  to_dict(): Record<string, any> {
    return this.toDict();
  }

  static fromDict(data: Record<string, any>): HyperEdge {
    return new HyperEdge(
      data.edge_id,
      data.node_ids,
      data.edge_type as HyperEdgeType,
      data.properties || {},
      data.weight !== undefined ? data.weight : 1.0,
      data.version_vector || {},
      data.epoch || 1,
      data.updated_at_ns || 0,
      data.digest || ""
    );
  }

  static from_dict(data: Record<string, any>): HyperEdge {
    return HyperEdge.fromDict(data);
  }
}

export class DistributedHyperGraph {
  nodes: Map<string, HyperNode>;
  edges: Map<string, HyperEdge>;
  cluster_vector: Record<string, number>;

  constructor(cluster_vector?: Record<string, number>) {
    this.nodes = new Map();
    this.edges = new Map();
    this.cluster_vector = cluster_vector || {};
  }

  validateIntegrity(): boolean {
    for (const [edge_id, edge] of this.edges) {
      for (const nid of edge.node_ids) {
        if (!this.nodes.has(nid)) {
          throw new HyperGraphIntegrityError(
            `HyperEdge '${edge_id}' references missing HyperNode '${nid}'`
          );
        }
      }
    }
    return true;
  }

  computeRootDigest(): string {
    const sortedNodeHashes: string[] = [];
    for (const n of this.nodes.values()) {
      sortedNodeHashes.push(n.digest);
    }
    sortedNodeHashes.sort();

    const sortedEdgeHashes: string[] = [];
    for (const e of this.edges.values()) {
      sortedEdgeHashes.push(e.digest);
    }
    sortedEdgeHashes.sort();

    const sortedVector: Record<string, number> = {};
    for (const k of Object.keys(this.cluster_vector).sort()) {
      sortedVector[k] = this.cluster_vector[k];
    }

    const combined = {
      edges_root: sha256Hex(sortedEdgeHashes.join(":")),
      nodes_root: sha256Hex(sortedNodeHashes.join(":")),
      vector: sortedVector,
    };
    return sha256Hex(deepCanonicalJson(combined));
  }

  addNode(node: HyperNode): void {
    this.nodes.set(node.node_id, node);
  }

  addEdge(edge: HyperEdge): void {
    for (const nid of edge.node_ids) {
      if (!this.nodes.has(nid)) {
        throw new HyperGraphIntegrityError(
          `Cannot add HyperEdge '${edge.edge_id}': target node '${nid}' not found`
        );
      }
    }
    this.edges.set(edge.edge_id, edge);
  }

  toDict(): Record<string, any> {
    const nodesObj: Record<string, any> = {};
    for (const [nid, n] of this.nodes) {
      nodesObj[nid] = n.toDict();
    }
    const edgesObj: Record<string, any> = {};
    for (const [eid, e] of this.edges) {
      edgesObj[eid] = e.toDict();
    }
    return {
      nodes: nodesObj,
      edges: edgesObj,
      cluster_vector: { ...this.cluster_vector },
    };
  }

  to_dict(): Record<string, any> {
    return this.toDict();
  }

  static fromDict(data: Record<string, any>): DistributedHyperGraph {
    const g = new DistributedHyperGraph(data.cluster_vector || {});
    for (const [nid, nd] of Object.entries(data.nodes || {})) {
      g.nodes.set(nid, HyperNode.fromDict(nd as Record<string, any>));
    }
    for (const [eid, ed] of Object.entries(data.edges || {})) {
      g.edges.set(eid, HyperEdge.fromDict(ed as Record<string, any>));
    }
    return g;
  }

  static from_dict(data: Record<string, any>): DistributedHyperGraph {
    return DistributedHyperGraph.fromDict(data);
  }
}

export class SyncPacket {
  packet_id: string;
  source_cluster: string;
  target_cluster: string;
  epoch: number;
  committee_root: string;
  quorum_required: number;
  vector_clock: Record<string, number>;
  nodes_delta: HyperNode[];
  edges_delta: HyperEdge[];
  certificate: Record<string, any> | null;
  timestamp_ns: number;
  packet_hash: string;

  constructor(
    packet_id: string,
    source_cluster: string,
    target_cluster: string,
    epoch: number,
    committee_root: string,
    quorum_required: number,
    vector_clock: Record<string, number>,
    nodes_delta: HyperNode[],
    edges_delta: HyperEdge[],
    certificate: Record<string, any> | null = null,
    timestamp_ns?: number,
    packet_hash: string = ""
  ) {
    this.packet_id = packet_id;
    this.source_cluster = source_cluster;
    this.target_cluster = target_cluster;
    this.epoch = epoch;
    this.committee_root = committee_root;
    this.quorum_required = quorum_required;
    this.vector_clock = vector_clock;
    this.nodes_delta = nodes_delta;
    this.edges_delta = edges_delta;
    this.certificate = certificate;
    this.timestamp_ns = timestamp_ns ?? Date.now() * 1_000_000;
    this.packet_hash = packet_hash || this.computePacketHash();
  }

  computePacketHash(): string {
    const sortedVC: Record<string, number> = {};
    for (const k of Object.keys(this.vector_clock).sort()) {
      sortedVC[k] = this.vector_clock[k];
    }
    const clean = {
      certificate_hash: this.certificate ? this.certificate.certificate_id || "" : "",
      committee_root: this.committee_root,
      edges: this.edges_delta.map((e) => e.digest).sort(),
      epoch: this.epoch,
      nodes: this.nodes_delta.map((n) => n.digest).sort(),
      packet_id: this.packet_id,
      quorum_required: this.quorum_required,
      source_cluster: this.source_cluster,
      target_cluster: this.target_cluster,
      vector_clock: sortedVC,
    };
    return sha256Hex(deepCanonicalJson(clean));
  }

  toDict(): Record<string, any> {
    return {
      packet_id: this.packet_id,
      source_cluster: this.source_cluster,
      target_cluster: this.target_cluster,
      epoch: this.epoch,
      committee_root: this.committee_root,
      quorum_required: this.quorum_required,
      vector_clock: this.vector_clock,
      nodes_delta: this.nodes_delta.map((n) => n.toDict()),
      edges_delta: this.edges_delta.map((e) => e.toDict()),
      certificate: this.certificate,
      timestamp_ns: this.timestamp_ns,
      packet_hash: this.packet_hash,
    };
  }

  static fromDict(data: Record<string, any>): SyncPacket {
    return new SyncPacket(
      data.packet_id,
      data.source_cluster,
      data.target_cluster,
      data.epoch || 1,
      data.committee_root || "",
      data.quorum_required || 0,
      data.vector_clock || {},
      (data.nodes_delta || []).map((n: any) => HyperNode.fromDict(n)),
      (data.edges_delta || []).map((e: any) => HyperEdge.fromDict(e)),
      data.certificate || null,
      data.timestamp_ns || 0,
      data.packet_hash || ""
    );
  }
}

export class SyncReconciliationReceipt {
  receipt_id: string;
  source_cluster: string;
  target_cluster: string;
  nodes_reconciled: number;
  edges_reconciled: number;
  merged_vector_clock: Record<string, number>;
  composite_root_digest: string;
  causal_verdict: CausalOrderingVerdict;
  timestamp_ns: number;
  receipt_hash: string;

  constructor(
    receipt_id: string,
    source_cluster: string,
    target_cluster: string,
    nodes_reconciled: number,
    edges_reconciled: number,
    merged_vector_clock: Record<string, number>,
    composite_root_digest: string,
    causal_verdict: CausalOrderingVerdict,
    timestamp_ns?: number,
    receipt_hash: string = ""
  ) {
    this.receipt_id = receipt_id;
    this.source_cluster = source_cluster;
    this.target_cluster = target_cluster;
    this.nodes_reconciled = nodes_reconciled;
    this.edges_reconciled = edges_reconciled;
    this.merged_vector_clock = merged_vector_clock;
    this.composite_root_digest = composite_root_digest;
    this.causal_verdict = causal_verdict;
    this.timestamp_ns = timestamp_ns ?? Date.now() * 1_000_000;
    this.receipt_hash = receipt_hash || this.computeReceiptHash();
  }

  computeReceiptHash(): string {
    const sortedVC: Record<string, number> = {};
    for (const k of Object.keys(this.merged_vector_clock).sort()) {
      sortedVC[k] = this.merged_vector_clock[k];
    }
    const clean = {
      causal_verdict: String(this.causal_verdict),
      composite_root_digest: this.composite_root_digest,
      edges_reconciled: this.edges_reconciled,
      merged_vector_clock: sortedVC,
      nodes_reconciled: this.nodes_reconciled,
      receipt_id: this.receipt_id,
      source_cluster: this.source_cluster,
      target_cluster: this.target_cluster,
    };
    const digest = sha256Hex(deepCanonicalJson(clean));
    return `sw:sync:v1:${digest}`;
  }

  toDict(): Record<string, any> {
    return {
      receipt_id: this.receipt_id,
      source_cluster: this.source_cluster,
      target_cluster: this.target_cluster,
      nodes_reconciled: this.nodes_reconciled,
      edges_reconciled: this.edges_reconciled,
      merged_vector_clock: this.merged_vector_clock,
      composite_root_digest: this.composite_root_digest,
      causal_verdict: this.causal_verdict,
      timestamp_ns: this.timestamp_ns,
      receipt_hash: this.receipt_hash,
    };
  }
}

export class AntiEntropyDigest {
  cluster_id: string;
  epoch: number;
  vector_clock: Record<string, number>;
  hypergraph_root_digest: string;
  node_count: number;
  edge_count: number;
  timestamp_ns: number;

  constructor(
    cluster_id: string,
    epoch: number,
    vector_clock: Record<string, number>,
    hypergraph_root_digest: string,
    node_count: number,
    edge_count: number,
    timestamp_ns?: number
  ) {
    this.cluster_id = cluster_id;
    this.epoch = epoch;
    this.vector_clock = vector_clock;
    this.hypergraph_root_digest = hypergraph_root_digest;
    this.node_count = node_count;
    this.edge_count = edge_count;
    this.timestamp_ns = timestamp_ns ?? Date.now() * 1_000_000;
  }

  toDict(): Record<string, any> {
    return {
      cluster_id: this.cluster_id,
      epoch: this.epoch,
      vector_clock: this.vector_clock,
      hypergraph_root_digest: this.hypergraph_root_digest,
      node_count: this.node_count,
      edge_count: this.edge_count,
      timestamp_ns: this.timestamp_ns,
    };
  }
}

// ==============================================================================
// 3. Vector Clock and Invariant I1 Algorithms
// ==============================================================================

export function compareVectorClocks(
  localVC: Record<string, number>,
  remoteVC: Record<string, number>,
  deltaCount: number = 1
): CausalOrderingVerdict {
  const allKeys = new Set([...Object.keys(localVC), ...Object.keys(remoteVC)]);
  let hasRemoteGreater = false;
  let hasLocalGreater = false;
  let hasGap = false;
  const allowedStep = Math.max(1, deltaCount);

  for (const k of allKeys) {
    const lVal = localVC[k] ?? 0;
    const rVal = remoteVC[k] ?? 0;

    if (rVal > lVal) {
      hasRemoteGreater = true;
      if (lVal > 0 && rVal > lVal + allowedStep) {
        hasGap = true;
      } else if (lVal === 0 && rVal > allowedStep) {
        hasGap = true;
      }
    } else if (lVal > rVal) {
      hasLocalGreater = true;
    }
  }

  if (hasGap) {
    return CausalOrderingVerdict.CAUSAL_GAP_DETECTED;
  }
  if (hasRemoteGreater && !hasLocalGreater) {
    return CausalOrderingVerdict.CAUSALLY_AHEAD;
  }
  if (hasLocalGreater && !hasRemoteGreater) {
    return CausalOrderingVerdict.CAUSALLY_BEHIND;
  }
  if (!hasRemoteGreater && !hasLocalGreater) {
    return CausalOrderingVerdict.CAUSALLY_BEHIND;
  }

  return CausalOrderingVerdict.CONCURRENT;
}

export function computeCanonicalPoeSignedMessage(certDict: Record<string, any>): string {
  const cid = String(certDict.certificate_id ?? "");
  const pid = String(certDict.proposal_id ?? "");
  const act = String(certDict.action_type ?? "");
  const epoch = String(certDict.epoch ?? 0);
  const croot = String(certDict.committee_root ?? "");
  const pre = String(certDict.pre_state_hash ?? "");
  const post = String(certDict.post_state_hash ?? "");
  const digest = String(certDict.digest ?? "");
  const pkt = String(certDict.packet_hash ?? "");
  return `poe:cert:v1:${cid}:${pid}:${act}:${epoch}:${croot}:${pre}:${post}:${digest}:${pkt}`;
}

export function computeCanonicalPoeDigest(certDict: Record<string, any>): string {
  return sha256Hex(computeCanonicalPoeSignedMessage(certDict));
}

export function signCrossClusterPoe(certDict: Record<string, any>, privateKeyHex: string): string {
  const msg = computeCanonicalPoeSignedMessage(certDict);
  return signBftPayload(privateKeyHex, msg);
}

export function verifyCrossClusterPoe(
  certDict: Record<string, any> | null,
  knownCommitteeRoot: string,
  expectedEpoch: number,
  knownSignersKeys?: Record<string, string>,
  expectedQuorum?: number
): boolean {
  if (!certDict || typeof certDict !== "object") return false;

  const certEpoch = certDict.epoch ?? 0;
  const certCommitteeRoot = certDict.committee_root ?? "";
  const signers = certDict.signers || [];

  if (certEpoch !== expectedEpoch) return false;
  if (certCommitteeRoot !== knownCommitteeRoot || !knownCommitteeRoot) return false;

  // Mandatory deduplication: reject repeated signers
  if (!Array.isArray(signers) || signers.length === 0) return false;
  if (new Set(signers).size !== signers.length) return false;

  // Mandatory pinned validator keys (never trust unpinned keys or cert's self-asserted keys)
  if (!knownSignersKeys || typeof knownSignersKeys !== "object" || Object.keys(knownSignersKeys).length === 0) {
    return false;
  }

  // Quorum threshold: compute from known committee keys or explicit expected_quorum
  // Strict ceiling formula: (n + f + 2) // 2, safe at all N including N=5 and N=8
  let quorumReq: number;
  if (expectedQuorum !== undefined) {
    quorumReq = expectedQuorum;
  } else {
    const n = Object.keys(knownSignersKeys).length;
    const f = Math.floor((n - 1) / 3);
    quorumReq = Math.max(1, Math.floor((n + f + 2) / 2));
  }

  if (signers.length < quorumReq || quorumReq <= 0) return false;

  // Check canonical PoE receipt hash integrity (MANDATORY, timing-safe)
  const receiptHash = certDict.receipt_hash ?? "";
  if (!receiptHash || typeof receiptHash !== "string") return false;
  const computedPoeHash = computeCanonicalPoeHash(certDict);
  const bufA = Buffer.from(receiptHash, "utf-8");
  const bufB = Buffer.from(computedPoeHash, "utf-8");
  if (bufA.length !== bufB.length || !crypto.timingSafeEqual(bufA, bufB)) {
    return false;
  }

  const signatures = certDict.signatures;
  if (!signatures || typeof signatures !== "object" || Object.keys(signatures).length === 0) {
    return false;
  }

  const canonicalMsg = computeCanonicalPoeSignedMessage(certDict);
  const canonicalDigest = computeCanonicalPoeDigest(certDict);

  let verifiedCount = 0;
  for (const signerId of signers) {
    if (!knownSignersKeys[signerId]) return false;
    const pubHex = knownSignersKeys[signerId];
    const sigHex = signatures[signerId];
    if (!sigHex) return false;
    if (
      verifyBftSignature(pubHex, canonicalMsg, sigHex) ||
      verifyBftSignature(pubHex, canonicalDigest, sigHex)
    ) {
      verifiedCount++;
    } else {
      return false;
    }
  }
  if (verifiedCount < quorumReq) return false;

  return true;
}

// ==============================================================================
// 4. CrossClusterSyncEngine
// ==============================================================================

export class CrossClusterSyncEngine {
  cluster_id: string;
  epoch: number;
  committee_root: string;
  known_foreign_committees: Map<string, Map<number, string>>;
  known_foreign_validators: Map<string, Map<string, string>>;
  hypergraph: DistributedHyperGraph;
  receipts: SyncReconciliationReceipt[];
  dangling_edge_buffer: HyperEdge[];
  circuit_tripped: boolean;
  trip_reason: string | null;
  require_foreign_certificate: boolean;

  constructor(
    cluster_id: string = "cluster-local",
    epoch: number = 1,
    committee_root: string = "0000000000000000000000000000000000000000000000000000000000000000",
    require_foreign_certificate: boolean = true
  ) {
    this.cluster_id = cluster_id;
    this.epoch = epoch;
    this.committee_root = committee_root;
    this.require_foreign_certificate = require_foreign_certificate;
    this.known_foreign_committees = new Map();
    this.known_foreign_validators = new Map();
    this.hypergraph = new DistributedHyperGraph({ [cluster_id]: 0 });
    this.receipts = [];
    this.dangling_edge_buffer = [];
    this.circuit_tripped = false;
    this.trip_reason = null;
  }

  registerForeignCommittee(
    cluster_id: string,
    epoch: number,
    committee_root: string,
    validator_keys?: Record<string, string>
  ): void {
    if (!this.known_foreign_committees.has(cluster_id)) {
      this.known_foreign_committees.set(cluster_id, new Map());
    }
    this.known_foreign_committees.get(cluster_id)!.set(epoch, committee_root);

    if (validator_keys) {
      if (!this.known_foreign_validators.has(cluster_id)) {
        this.known_foreign_validators.set(cluster_id, new Map());
      }
      const valMap = this.known_foreign_validators.get(cluster_id)!;
      for (const [k, v] of Object.entries(validator_keys)) {
        valMap.set(k, v);
      }
    }
  }

  tripCircuit(reason: string): void {
    this.circuit_tripped = true;
    this.trip_reason = reason;
  }

  rearmCircuit(): void {
    this.circuit_tripped = false;
    this.trip_reason = null;
  }

  upsertLocalNode(
    node_id: string,
    state_type: HyperNodeType,
    state_payload: Record<string, any>
  ): HyperNode {
    if (this.circuit_tripped) {
      throw new SyncCircuitTrippedError(`Circuit tripped: ${this.trip_reason}`);
    }

    const curSeq = (this.hypergraph.cluster_vector[this.cluster_id] || 0) + 1;
    this.hypergraph.cluster_vector[this.cluster_id] = curSeq;

    const node = new HyperNode(
      node_id,
      this.cluster_id,
      state_type,
      state_payload,
      { ...this.hypergraph.cluster_vector },
      this.epoch,
      Date.now() * 1_000_000
    );
    this.hypergraph.addNode(node);
    return node;
  }

  upsertLocalEdge(
    edge_id: string,
    node_ids: string[],
    edge_type: HyperEdgeType,
    properties: Record<string, any> = {},
    weight: number = 1.0
  ): HyperEdge {
    if (this.circuit_tripped) {
      throw new SyncCircuitTrippedError(`Circuit tripped: ${this.trip_reason}`);
    }

    const curSeq = (this.hypergraph.cluster_vector[this.cluster_id] || 0) + 1;
    this.hypergraph.cluster_vector[this.cluster_id] = curSeq;

    const edge = new HyperEdge(
      edge_id,
      node_ids,
      edge_type,
      properties,
      weight,
      { ...this.hypergraph.cluster_vector },
      this.epoch,
      Date.now() * 1_000_000
    );
    this.hypergraph.addEdge(edge);
    return edge;
  }

  createSyncPacket(
    target_cluster: string,
    nodes?: HyperNode[],
    edges?: HyperEdge[],
    certificate: Record<string, any> | null = null
  ): SyncPacket {
    if (this.circuit_tripped) {
      throw new SyncCircuitTrippedError(`Circuit tripped: ${this.trip_reason}`);
    }

    const nodesDelta = nodes !== undefined ? nodes : Array.from(this.hypergraph.nodes.values());
    const edgesDelta = edges !== undefined ? edges : Array.from(this.hypergraph.edges.values());

    return new SyncPacket(
      `sync-pkt-${crypto.randomBytes(6).toString("hex")}`,
      this.cluster_id,
      target_cluster,
      this.epoch,
      this.committee_root,
      certificate ? certificate.quorum_required || 0 : 0,
      { ...this.hypergraph.cluster_vector },
      nodesDelta,
      edgesDelta,
      certificate
    );
  }

  ingestSyncPacket(packet: SyncPacket): SyncReconciliationReceipt {
    if (this.circuit_tripped) {
      throw new SyncCircuitTrippedError(`Circuit tripped: ${this.trip_reason}`);
    }

    // Invariant I1 Verification
    if (packet.source_cluster === this.cluster_id) {
      this.tripCircuit(`Identity spoof detected: foreign packet claiming local cluster ID '${packet.source_cluster}'`);
      throw new UnverifiableCertificateError(
        `Invariant I1 failure: external sync packet claims local cluster identity '${packet.source_cluster}' (PoC-F spoof rejected)`
      );
    }

    if (!this.known_foreign_committees.has(packet.source_cluster)) {
      this.tripCircuit(`Unregistered foreign source cluster '${packet.source_cluster}'`);
      throw new UnverifiableCertificateError(
        `Invariant I1 failure: cluster '${packet.source_cluster}' is not a registered foreign cluster`
      );
    }

    if (this.require_foreign_certificate || packet.certificate) {
      if (!packet.certificate) {
        this.tripCircuit(`Missing mandatory Proof-of-Execution certificate for packet ${packet.packet_id}`);
        throw new UnverifiableCertificateError(
          `Invariant I1 failure: foreign packet '${packet.packet_id}' from '${packet.source_cluster}' requires a Proof-of-Execution certificate`
        );
      }

      // Packet hash binding verification (PoC-E)
      const certPktHash = packet.certificate.packet_hash;
      const expectedPktHash = packet.packet_hash || packet.computePacketHash();
      if (certPktHash && certPktHash !== expectedPktHash) {
        this.tripCircuit(`Certificate packet_hash mismatch for packet ${packet.packet_id}`);
        throw new UnverifiableCertificateError(
          `Invariant I1 failure: Proof-of-Execution certificate packet_hash '${certPktHash}' does not match packet hash '${expectedPktHash}' (PoC-E rejected)`
        );
      }

      const knownRoot = this.known_foreign_committees.get(packet.source_cluster)?.get(packet.epoch);
      if (!knownRoot) {
        this.tripCircuit(`Unknown committee root for cluster ${packet.source_cluster} epoch ${packet.epoch}`);
        throw new UnverifiableCertificateError(
          `Invariant I1 failure: cluster '${packet.source_cluster}' epoch ${packet.epoch} committee root not registered`
        );
      }

      let validatorKeys: Record<string, string> | undefined;
      const vMap = this.known_foreign_validators.get(packet.source_cluster);
      if (vMap) {
        validatorKeys = Object.fromEntries(vMap.entries());
      }

      if (!validatorKeys || Object.keys(validatorKeys).length === 0) {
        this.tripCircuit(`Missing pinned validator keys for cluster ${packet.source_cluster}`);
        throw new UnverifiableCertificateError(
          `Invariant I1 failure: cluster '${packet.source_cluster}' has no pinned validator keys`
        );
      }

      if (!verifyCrossClusterPoe(packet.certificate, knownRoot, packet.epoch, validatorKeys)) {
        this.tripCircuit(`Cryptographic certificate verification failed for packet ${packet.packet_id}`);
        throw new UnverifiableCertificateError(
          `Invariant I1 failure: Proof-of-Execution certificate for '${packet.source_cluster}' is invalid`
        );
      }
    }

    // Causal Classification
    const deltaCount = packet.nodes_delta.length + packet.edges_delta.length;
    const causalVerdict = compareVectorClocks(
      this.hypergraph.cluster_vector,
      packet.vector_clock,
      deltaCount
    );

    if (causalVerdict === CausalOrderingVerdict.CAUSAL_GAP_DETECTED) {
      this.tripCircuit(`Causal discontinuity detected from ${packet.source_cluster}`);
      throw new CausalDiscontinuityError(
        `Causal gap detected: incoming vector clock has sequence gap against local`
      );
    }

    if (causalVerdict === CausalOrderingVerdict.CAUSALLY_BEHIND) {
      const receipt = new SyncReconciliationReceipt(
        `sync-rec-${crypto.randomBytes(6).toString("hex")}`,
        packet.source_cluster,
        this.cluster_id,
        0,
        0,
        { ...this.hypergraph.cluster_vector },
        this.hypergraph.computeRootDigest(),
        causalVerdict
      );
      this.receipts.push(receipt);
      return receipt;
    }

    // CRDT Reconciliation
    let nodesMerged = 0;
    let edgesMerged = 0;

    for (const rNode of packet.nodes_delta) {
      if (!this.hypergraph.nodes.has(rNode.node_id)) {
        this.hypergraph.addNode(rNode);
        nodesMerged++;
      } else {
        const lNode = this.hypergraph.nodes.get(rNode.node_id)!;
        const lKey = [lNode.epoch, lNode.updated_at_ns, lNode.cluster_id];
        const rKey = [rNode.epoch, rNode.updated_at_ns, rNode.cluster_id];
        let remoteWins = false;
        if (rKey[0] > lKey[0]) remoteWins = true;
        else if (rKey[0] === lKey[0] && rKey[1] > lKey[1]) remoteWins = true;
        else if (rKey[0] === lKey[0] && rKey[1] === lKey[1] && rKey[2] > lKey[2]) remoteWins = true;

        if (remoteWins) {
          this.hypergraph.addNode(rNode);
          nodesMerged++;
        }
      }
    }

    for (const rEdge of packet.edges_delta) {
      const missingNode = rEdge.node_ids.some((nid) => !this.hypergraph.nodes.has(nid));
      if (missingNode) {
        this.dangling_edge_buffer.push(rEdge);
        continue;
      }

      if (!this.hypergraph.edges.has(rEdge.edge_id)) {
        this.hypergraph.addEdge(rEdge);
        edgesMerged++;
      } else {
        const lEdge = this.hypergraph.edges.get(rEdge.edge_id)!;
        const mergedNodes = Array.from(new Set([...lEdge.node_ids, ...rEdge.node_ids])).sort();
        const mergedWeight = Math.max(lEdge.weight, rEdge.weight);

        // Commutative deterministic property merge
        const allKeys = Array.from(
          new Set([...Object.keys(lEdge.properties), ...Object.keys(rEdge.properties)])
        ).sort();
        const mergedProps: Record<string, any> = {};
        for (const pk of allKeys) {
          const inL = pk in lEdge.properties;
          const inR = pk in rEdge.properties;
          if (inL && inR) {
            const lVal = lEdge.properties[pk];
            const rVal = rEdge.properties[pk];
            if (lVal === rVal) {
              mergedProps[pk] = lVal;
            } else {
              const lKey = [lEdge.epoch, lEdge.updated_at_ns];
              const rKey = [rEdge.epoch, rEdge.updated_at_ns];
              if (rKey[0] > lKey[0] || (rKey[0] === lKey[0] && rKey[1] > lKey[1])) {
                mergedProps[pk] = rVal;
              } else if (lKey[0] > rKey[0] || (lKey[0] === rKey[0] && lKey[1] > rKey[1])) {
                mergedProps[pk] = lVal;
              } else {
                mergedProps[pk] = String(lVal) > String(rVal) ? lVal : rVal;
              }
            }
          } else if (inL) {
            mergedProps[pk] = lEdge.properties[pk];
          } else {
            mergedProps[pk] = rEdge.properties[pk];
          }
        }

        const mergedVC: Record<string, number> = { ...lEdge.version_vector };
        for (const [k, v] of Object.entries(rEdge.version_vector)) {
          mergedVC[k] = Math.max(mergedVC[k] || 0, v);
        }

        const winnerEpoch = Math.max(lEdge.epoch, rEdge.epoch);
        const winnerUpdated = Math.max(lEdge.updated_at_ns, rEdge.updated_at_ns);
        const winnerEdgeType =
          rEdge.epoch > lEdge.epoch ||
          (rEdge.epoch === lEdge.epoch && rEdge.updated_at_ns > lEdge.updated_at_ns)
            ? rEdge.edge_type
            : lEdge.edge_type;

        const mergedEdge = new HyperEdge(
          rEdge.edge_id,
          mergedNodes,
          winnerEdgeType,
          mergedProps,
          mergedWeight,
          mergedVC,
          winnerEpoch,
          winnerUpdated
        );
        this.hypergraph.addEdge(mergedEdge);
        edgesMerged++;
      }
    }

    // Drain any resolvable dangling edges
    if (this.dangling_edge_buffer.length > 0) {
      const remaining: HyperEdge[] = [];
      for (const bEdge of this.dangling_edge_buffer) {
        if (bEdge.node_ids.every((nid) => this.hypergraph.nodes.has(nid))) {
          if (!this.hypergraph.edges.has(bEdge.edge_id)) {
            this.hypergraph.addEdge(bEdge);
            edgesMerged++;
          }
        } else {
          remaining.push(bEdge);
        }
      }
      this.dangling_edge_buffer = remaining;
    }

    for (const [k, v] of Object.entries(packet.vector_clock)) {
      this.hypergraph.cluster_vector[k] = Math.max(
        this.hypergraph.cluster_vector[k] || 0,
        v
      );
    }
    this.hypergraph.cluster_vector[this.cluster_id] =
      (this.hypergraph.cluster_vector[this.cluster_id] || 0) + 1;

    this.hypergraph.validateIntegrity();

    const receipt = new SyncReconciliationReceipt(
      `sync-rec-${crypto.randomBytes(6).toString("hex")}`,
      packet.source_cluster,
      this.cluster_id,
      nodesMerged,
      edgesMerged,
      { ...this.hypergraph.cluster_vector },
      this.hypergraph.computeRootDigest(),
      causalVerdict
    );
    this.receipts.push(receipt);
    return receipt;
  }

  generateAntiEntropyDigest(): AntiEntropyDigest {
    return new AntiEntropyDigest(
      this.cluster_id,
      this.epoch,
      { ...this.hypergraph.cluster_vector },
      this.hypergraph.computeRootDigest(),
      this.hypergraph.nodes.size,
      this.hypergraph.edges.size
    );
  }

  verifyReconciliationReceipt(receipt: SyncReconciliationReceipt): boolean {
    const computed = receipt.computeReceiptHash();
    return computed === receipt.receipt_hash;
  }

  getStatus(): Record<string, any> {
    const status = this.circuit_tripped ? SyncStatus.CIRCUIT_TRIPPED : SyncStatus.NOMINAL;
    const knownComms: Record<string, number[]> = {};
    for (const [c, epochMap] of this.known_foreign_committees) {
      knownComms[c] = Array.from(epochMap.keys());
    }
    return {
      cluster_id: this.cluster_id,
      epoch: this.epoch,
      committee_root: this.committee_root,
      status,
      circuit_tripped: this.circuit_tripped,
      trip_reason: this.trip_reason,
      nodes_count: this.hypergraph.nodes.size,
      edges_count: this.hypergraph.edges.size,
      vector_clock: { ...this.hypergraph.cluster_vector },
      hypergraph_root_digest: this.hypergraph.computeRootDigest(),
      receipts_count: this.receipts.length,
      known_committees: knownComms,
    };
  }

  reset(): void {
    this.hypergraph = new DistributedHyperGraph({ [this.cluster_id]: 0 });
    this.receipts = [];
    this.circuit_tripped = false;
    this.trip_reason = null;
    this.known_foreign_committees.clear();
    this.known_foreign_validators.clear();
  }
}

export const globalCrossClusterSyncEngine = new CrossClusterSyncEngine();
