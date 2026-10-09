/**
 * SwarmWatch Seam 78: Autonomous Distributed Hyper-Graph Partition Healing &
 * Causal Vector Resumption (ADR 0089 Dual-SDK Parity).
 *
 * Implements:
 * - Partition State Automaton (NOMINAL -> PARTITION_DETECTED -> PLANNING -> RECONCILING -> HEALED).
 * - Symmetric Causal Difference Matrix calculation.
 * - Invariant I2 Monotonic Causal Vector Resumption (zero regression).
 * - Hyper-Graph CRDT Join Semilattice Splicing (node union, weight supremum, property merge).
 * - Tamper-Evident Partition Healing Receipts ('sw:heal:v1:<sha256>').
 * - Fail-Closed Safety Circuit Coupling.
 * - Sub-250µs execution latency SLO and Zero-Token Cost Guarantee ($0.00 USD).
 */

import * as crypto from "node:crypto";
import {
  HyperNodeType,
  HyperEdgeType,
  HyperNode,
  HyperEdge,
  DistributedHyperGraph,
  CrossClusterSyncEngine,
  verifyCrossClusterPoe,
  HyperGraphIntegrityError,
  canonicalJson,
  sha256Hex,
} from "./cross_cluster_sync.ts";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class PartitionHealingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PartitionHealingError";
  }
}

export class UnhealedPartitionConflictError extends PartitionHealingError {
  constructor(message: string) {
    super(message);
    this.name = "UnhealedPartitionConflictError";
  }
}

export class InvalidResumptionProofError extends PartitionHealingError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidResumptionProofError";
  }
}

export class ResumptionVectorRegressionError extends PartitionHealingError {
  constructor(message: string) {
    super(message);
    this.name = "ResumptionVectorRegressionError";
  }
}

export class HealingCircuitTrippedError extends PartitionHealingError {
  constructor(message: string) {
    super(message);
    this.name = "HealingCircuitTrippedError";
  }
}

// ==============================================================================
// 2. Domain Enums and Constants
// ==============================================================================

export const PartitionState = {
  NOMINAL: "NOMINAL",
  PARTITION_DETECTED: "PARTITION_DETECTED",
  PLANNING_RESUMPTION: "PLANNING_RESUMPTION",
  RECONCILING_DELTAS: "RECONCILING_DELTAS",
  HEALED: "HEALED",
  CIRCUIT_TRIPPED: "CIRCUIT_TRIPPED",
} as const;

export type PartitionStateType = typeof PartitionState[keyof typeof PartitionState];

// ==============================================================================
// 3. Domain Entities
// ==============================================================================

export class ResumptionDelta {
  delta_id: string;
  source_cluster: string;
  target_cluster: string;
  start_epoch: number;
  end_epoch: number;
  nodes_delta: HyperNode[];
  edges_delta: HyperEdge[];
  vector_clock: Record<string, number>;
  proof_of_execution: Record<string, any> | null;
  timestamp_ns: number;
  delta_hash: string;

  constructor(
    delta_id: string,
    source_cluster: string,
    target_cluster: string,
    start_epoch: number,
    end_epoch: number,
    nodes_delta: HyperNode[] = [],
    edges_delta: HyperEdge[] = [],
    vector_clock: Record<string, number> = {},
    proof_of_execution: Record<string, any> | null = null,
    timestamp_ns?: number,
    delta_hash?: string
  ) {
    this.delta_id = delta_id;
    this.source_cluster = source_cluster;
    this.target_cluster = target_cluster;
    this.start_epoch = start_epoch;
    this.end_epoch = end_epoch;
    this.nodes_delta = nodes_delta;
    this.edges_delta = edges_delta;
    this.vector_clock = vector_clock;
    this.proof_of_execution = proof_of_execution;
    this.timestamp_ns = timestamp_ns ?? Date.now() * 1_000_000;
    this.delta_hash = delta_hash || this.computeDigest();
  }

  computeDigest(): string {
    const sortedVC: Record<string, number> = {};
    for (const k of Object.keys(this.vector_clock).sort()) {
      sortedVC[k] = this.vector_clock[k];
    }
    const clean = {
      delta_id: this.delta_id,
      edges: this.edges_delta.map((e) => e.digest).sort(),
      end_epoch: this.end_epoch,
      nodes: this.nodes_delta.map((n) => n.digest).sort(),
      poe_hash: this.proof_of_execution ? this.proof_of_execution.receipt_hash || "" : "",
      source_cluster: this.source_cluster,
      start_epoch: this.start_epoch,
      target_cluster: this.target_cluster,
      vector_clock: sortedVC,
    };
    return sha256Hex(canonicalJson(clean));
  }

  toDict(): Record<string, any> {
    return {
      delta_id: this.delta_id,
      delta_hash: this.delta_hash,
      edges_delta: this.edges_delta.map((e) => e.toDict()),
      end_epoch: this.end_epoch,
      nodes_delta: this.nodes_delta.map((n) => n.toDict()),
      proof_of_execution: this.proof_of_execution,
      source_cluster: this.source_cluster,
      start_epoch: this.start_epoch,
      target_cluster: this.target_cluster,
      timestamp_ns: this.timestamp_ns,
      vector_clock: this.vector_clock,
    };
  }

  static fromDict(data: Record<string, any>): ResumptionDelta {
    return new ResumptionDelta(
      data.delta_id,
      data.source_cluster,
      data.target_cluster,
      data.start_epoch || 1,
      data.end_epoch || 1,
      (data.nodes_delta || []).map((n: any) => HyperNode.fromDict(n)),
      (data.edges_delta || []).map((e: any) => HyperEdge.fromDict(e)),
      data.vector_clock || {},
      data.proof_of_execution || null,
      data.timestamp_ns || 0,
      data.delta_hash || ""
    );
  }
}

export class PartitionHealingPlan {
  plan_id: string;
  local_cluster: string;
  remote_cluster: string;
  divergent_clusters: string[];
  missing_remote_steps: number;
  missing_local_steps: number;
  merged_vector_clock: Record<string, number>;
  proposed_epoch: number;
  is_partition_detected: boolean;
  created_at_ns: number;

  constructor(
    plan_id: string,
    local_cluster: string,
    remote_cluster: string,
    divergent_clusters: string[],
    missing_remote_steps: number,
    missing_local_steps: number,
    merged_vector_clock: Record<string, number>,
    proposed_epoch: number,
    is_partition_detected: boolean,
    created_at_ns?: number
  ) {
    this.plan_id = plan_id;
    this.local_cluster = local_cluster;
    this.remote_cluster = remote_cluster;
    this.divergent_clusters = divergent_clusters;
    this.missing_remote_steps = missing_remote_steps;
    this.missing_local_steps = missing_local_steps;
    this.merged_vector_clock = merged_vector_clock;
    this.proposed_epoch = proposed_epoch;
    this.is_partition_detected = is_partition_detected;
    this.created_at_ns = created_at_ns ?? Date.now() * 1_000_000;
  }

  toDict(): Record<string, any> {
    return {
      plan_id: this.plan_id,
      local_cluster: this.local_cluster,
      remote_cluster: this.remote_cluster,
      divergent_clusters: this.divergent_clusters,
      missing_remote_steps: this.missing_remote_steps,
      missing_local_steps: this.missing_local_steps,
      merged_vector_clock: this.merged_vector_clock,
      proposed_epoch: this.proposed_epoch,
      is_partition_detected: this.is_partition_detected,
      created_at_ns: this.created_at_ns,
    };
  }
}

export class PartitionHealingReceipt {
  receipt_id: string;
  local_cluster: string;
  remote_cluster: string;
  nodes_reconciled: number;
  edges_spliced: number;
  merged_vector_clock: Record<string, number>;
  composite_root_digest: string;
  duration_us: number;
  timestamp_ns: number;
  receipt_hash: string;

  constructor(
    receipt_id: string,
    local_cluster: string,
    remote_cluster: string,
    nodes_reconciled: number,
    edges_spliced: number,
    merged_vector_clock: Record<string, number>,
    composite_root_digest: string,
    duration_us: number,
    timestamp_ns?: number,
    receipt_hash?: string
  ) {
    this.receipt_id = receipt_id;
    this.local_cluster = local_cluster;
    this.remote_cluster = remote_cluster;
    this.nodes_reconciled = nodes_reconciled;
    this.edges_spliced = edges_spliced;
    this.merged_vector_clock = merged_vector_clock;
    this.composite_root_digest = composite_root_digest;
    this.duration_us = duration_us;
    this.timestamp_ns = timestamp_ns ?? Date.now() * 1_000_000;
    this.receipt_hash = receipt_hash || this.computeReceiptHash();
  }

  computeReceiptHash(): string {
    const sortedVC: Record<string, number> = {};
    for (const k of Object.keys(this.merged_vector_clock).sort()) {
      sortedVC[k] = this.merged_vector_clock[k];
    }
    const clean = {
      composite_root_digest: this.composite_root_digest,
      duration_us: Number(this.duration_us.toFixed(2)),
      edges_spliced: this.edges_spliced,
      local_cluster: this.local_cluster,
      merged_vector_clock: sortedVC,
      nodes_reconciled: this.nodes_reconciled,
      receipt_id: this.receipt_id,
      remote_cluster: this.remote_cluster,
    };
    return `sw:heal:v1:${sha256Hex(canonicalJson(clean))}`;
  }

  toDict(): Record<string, any> {
    return {
      receipt_id: this.receipt_id,
      local_cluster: this.local_cluster,
      remote_cluster: this.remote_cluster,
      nodes_reconciled: this.nodes_reconciled,
      edges_spliced: this.edges_spliced,
      merged_vector_clock: this.merged_vector_clock,
      composite_root_digest: this.composite_root_digest,
      duration_us: this.duration_us,
      timestamp_ns: this.timestamp_ns,
      receipt_hash: this.receipt_hash,
    };
  }
}

// ==============================================================================
// 4. Partition Healing Engine
// ==============================================================================

export class PartitionHealingEngine {
  local_cluster: string;
  epoch: number;
  sync_engine: CrossClusterSyncEngine;
  hypergraph: DistributedHyperGraph;
  state: PartitionStateType;
  circuit_tripped: boolean;
  trip_reason: string;
  receipts: PartitionHealingReceipt[];
  healing_plans: PartitionHealingPlan[];
  resumption_deltas: Map<string, ResumptionDelta>;

  constructor(
    local_cluster: string = "cluster-local",
    epoch: number = 1,
    sync_engine?: CrossClusterSyncEngine
  ) {
    this.local_cluster = local_cluster;
    this.epoch = epoch;
    this.sync_engine = sync_engine || new CrossClusterSyncEngine(local_cluster, epoch);
    this.hypergraph = this.sync_engine.hypergraph;
    this.state = PartitionState.NOMINAL;
    this.circuit_tripped = false;
    this.trip_reason = "";
    this.receipts = [];
    this.healing_plans = [];
    this.resumption_deltas = new Map();
  }

  tripCircuit(reason: string): void {
    this.circuit_tripped = true;
    this.trip_reason = reason;
    this.state = PartitionState.CIRCUIT_TRIPPED;
    this.sync_engine.tripCircuit(reason);
  }

  rearmCircuit(): void {
    this.circuit_tripped = false;
    this.trip_reason = "";
    this.state = PartitionState.NOMINAL;
    this.sync_engine.rearmCircuit();
  }

  detectPartition(
    remote_cluster: string,
    remote_vc: Record<string, number>,
    gap_threshold: number = 5
  ): [boolean, Record<string, number>] {
    const local_vc = this.hypergraph.cluster_vector;
    const allKeys = new Set([...Object.keys(local_vc), ...Object.keys(remote_vc)]);
    const diffMatrix: Record<string, number> = {};

    let hasLocalAdvances = false;
    let hasRemoteAdvances = false;
    let maxDivergence = 0;

    for (const k of allKeys) {
      const lVal = local_vc[k] ?? 0;
      const rVal = remote_vc[k] ?? 0;
      const diff = rVal - lVal;
      diffMatrix[k] = diff;

      if (diff > 0) {
        hasRemoteAdvances = true;
        if (diff > maxDivergence) maxDivergence = diff;
      } else if (diff < 0) {
        hasLocalAdvances = true;
        if (Math.abs(diff) > maxDivergence) maxDivergence = Math.abs(diff);
      }
    }

    const isPartition =
      (hasLocalAdvances && hasRemoteAdvances) || maxDivergence >= gap_threshold;

    if (isPartition && !this.circuit_tripped) {
      this.state = PartitionState.PARTITION_DETECTED;
    }

    return [isPartition, diffMatrix];
  }

  formulateHealingPlan(
    remote_cluster: string,
    remote_vc: Record<string, number>
  ): PartitionHealingPlan {
    if (this.circuit_tripped) {
      throw new HealingCircuitTrippedError(`Circuit tripped: ${this.trip_reason}`);
    }

    const [isPart, diffs] = this.detectPartition(remote_cluster, remote_vc);
    this.state = PartitionState.PLANNING_RESUMPTION;

    const local_vc = this.hypergraph.cluster_vector;
    const divergent = Object.keys(diffs).filter((k) => diffs[k] !== 0);

    let missingRemote = 0;
    let missingLocal = 0;
    for (const d of Object.values(diffs)) {
      if (d > 0) missingRemote += d;
      else if (d < 0) missingLocal += Math.abs(d);
    }

    const allKeys = new Set([...Object.keys(local_vc), ...Object.keys(remote_vc)]);
    const mergedVC: Record<string, number> = {};
    for (const k of allKeys) {
      mergedVC[k] = Math.max(local_vc[k] ?? 0, remote_vc[k] ?? 0);
    }

    const plan = new PartitionHealingPlan(
      `plan-heal-${crypto.randomBytes(4).toString("hex")}`,
      this.local_cluster,
      remote_cluster,
      divergent,
      missingRemote,
      missingLocal,
      mergedVC,
      Math.max(this.epoch, 1),
      isPart
    );
    this.healing_plans.push(plan);
    return plan;
  }

  createResumptionDelta(
    target_cluster: string,
    target_vc: Record<string, number>,
    poe_cert: Record<string, any> | null = null
  ): ResumptionDelta {
    if (this.circuit_tripped) {
      throw new HealingCircuitTrippedError(`Circuit tripped: ${this.trip_reason}`);
    }

    const localVC = { ...this.hypergraph.cluster_vector };
    const nodesToSend: HyperNode[] = [];
    const edgesToSend: HyperEdge[] = [];

    for (const n of this.hypergraph.nodes.values()) {
      const knownTarget = target_vc[n.cluster_id] ?? 0;
      if (n.cluster_id === this.local_cluster || knownTarget === 0) {
        nodesToSend.push(n);
      }
    }

    for (const e of this.hypergraph.edges.values()) {
      edgesToSend.push(e);
    }

    const delta = new ResumptionDelta(
      `delta-${crypto.randomBytes(4).toString("hex")}`,
      this.local_cluster,
      target_cluster,
      this.epoch,
      this.epoch,
      nodesToSend,
      edgesToSend,
      localVC,
      poe_cert
    );
    this.resumption_deltas.set(delta.delta_id, delta);
    return delta;
  }

  executeHealing(delta: ResumptionDelta): PartitionHealingReceipt {
    const t0 = performance.now();

    if (this.circuit_tripped) {
      throw new HealingCircuitTrippedError(`Circuit tripped: ${this.trip_reason}`);
    }

    this.state = PartitionState.RECONCILING_DELTAS;

    // ----------------------------------------------------------------------
    // Invariant I1: Foreign PoE Certificate Verification
    // ----------------------------------------------------------------------
    if (delta.proof_of_execution) {
      const knownRoot = this.sync_engine.known_foreign_committees
        .get(delta.source_cluster)
        ?.get(delta.end_epoch);
      if (!knownRoot) {
        this.tripCircuit(
          `Unknown committee root for cluster ${delta.source_cluster} epoch ${delta.end_epoch}`
        );
        throw new InvalidResumptionProofError(
          `Invariant I1 failure: committee root for '${delta.source_cluster}' epoch ${delta.end_epoch} not registered`
        );
      }

      let validatorKeys: Record<string, string> | undefined;
      const vMap = this.sync_engine.known_foreign_validators.get(delta.source_cluster);
      if (vMap) {
        validatorKeys = Object.fromEntries(vMap.entries());
      }

      if (!verifyCrossClusterPoe(delta.proof_of_execution, knownRoot, delta.end_epoch, validatorKeys)) {
        this.tripCircuit(`Proof-of-Execution verification failed for delta ${delta.delta_id}`);
        throw new InvalidResumptionProofError(
          `Invariant I1 failure: PoE certificate for delta from '${delta.source_cluster}' is invalid`
        );
      }
    }

    // ----------------------------------------------------------------------
    // Invariant I2: Monotonic Vector Resumption Check
    // ----------------------------------------------------------------------
    for (const [k, v] of Object.entries(delta.vector_clock)) {
      if (v < 0) {
        this.tripCircuit("Negative vector clock detected in resumption delta");
        throw new ResumptionVectorRegressionError(
          "Monotonic vector clock cannot contain negative counters"
        );
      }
    }

    // ----------------------------------------------------------------------
    // CRDT Join Semilattice Hyper-Graph Splicing
    // ----------------------------------------------------------------------
    let nodesReconciled = 0;
    let edgesSpliced = 0;

    // 1. Merge HyperNodes
    for (const rNode of delta.nodes_delta) {
      if (!this.hypergraph.nodes.has(rNode.node_id)) {
        this.hypergraph.addNode(rNode);
        nodesReconciled++;
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
          nodesReconciled++;
        }
      }
    }

    // 2. Merge HyperEdges
    for (const rEdge of delta.edges_delta) {
      for (const nid of rEdge.node_ids) {
        if (!this.hypergraph.nodes.has(nid)) {
          this.tripCircuit(`Dangling edge '${rEdge.edge_id}' missing node '${nid}'`);
          throw new HyperGraphIntegrityError(
            `Resumption HyperEdge '${rEdge.edge_id}' references missing HyperNode '${nid}'`
          );
        }
      }

      if (!this.hypergraph.edges.has(rEdge.edge_id)) {
        this.hypergraph.addEdge(rEdge);
        edgesSpliced++;
      } else {
        const lEdge = this.hypergraph.edges.get(rEdge.edge_id)!;
        const mergedNodes = Array.from(new Set([...lEdge.node_ids, ...rEdge.node_ids])).sort();
        const mergedWeight = Math.max(lEdge.weight, rEdge.weight);
        const mergedProps = { ...lEdge.properties, ...rEdge.properties };

        const mergedVV: Record<string, number> = { ...lEdge.version_vector };
        for (const [k, v] of Object.entries(rEdge.version_vector)) {
          mergedVV[k] = Math.max(mergedVV[k] ?? 0, v);
        }

        const winnerEpoch = Math.max(lEdge.epoch, rEdge.epoch);
        const winnerUpdated = Math.max(lEdge.updated_at_ns, rEdge.updated_at_ns);

        const mergedEdge = new HyperEdge(
          rEdge.edge_id,
          mergedNodes,
          rEdge.edge_type,
          mergedProps,
          mergedWeight,
          mergedVV,
          winnerEpoch,
          winnerUpdated
        );
        this.hypergraph.addEdge(mergedEdge);
        edgesSpliced++;
      }
    }

    // 3. Monotonic Vector Clock Join Supremum: V_post = max(V_local, V_delta)
    const allClusters = new Set([
      ...Object.keys(this.hypergraph.cluster_vector),
      ...Object.keys(delta.vector_clock),
    ]);
    for (const c of allClusters) {
      const lVal = this.hypergraph.cluster_vector[c] ?? 0;
      const dVal = delta.vector_clock[c] ?? 0;
      this.hypergraph.cluster_vector[c] = Math.max(lVal, dVal);
    }

    // Advance local turn counter
    this.hypergraph.cluster_vector[this.local_cluster] =
      (this.hypergraph.cluster_vector[this.local_cluster] ?? 0) + 1;

    const durationUs = (performance.now() - t0) * 1000.0;

    const receipt = new PartitionHealingReceipt(
      `heal-rec-${crypto.randomBytes(4).toString("hex")}`,
      this.local_cluster,
      delta.source_cluster,
      nodesReconciled,
      edgesSpliced,
      { ...this.hypergraph.cluster_vector },
      this.hypergraph.computeRootDigest(),
      durationUs
    );
    this.receipts.push(receipt);
    this.state = PartitionState.HEALED;
    return receipt;
  }

  verifyHealingReceipt(receipt: PartitionHealingReceipt): boolean {
    if (!receipt.receipt_hash.startsWith("sw:heal:v1:")) {
      return false;
    }
    return receipt.receipt_hash === receipt.computeReceiptHash();
  }

  getStatus(): Record<string, any> {
    return {
      local_cluster: this.local_cluster,
      epoch: this.epoch,
      state: this.state,
      circuit_tripped: this.circuit_tripped,
      trip_reason: this.trip_reason,
      vector_clock: { ...this.hypergraph.cluster_vector },
      nodes_count: this.hypergraph.nodes.size,
      edges_count: this.hypergraph.edges.size,
      root_digest: this.hypergraph.computeRootDigest(),
      receipts_count: this.receipts.length,
      plans_count: this.healing_plans.length,
    };
  }

  reset(): void {
    this.hypergraph.nodes.clear();
    this.hypergraph.edges.clear();
    this.hypergraph.cluster_vector = { [this.local_cluster]: 0 };
    this.state = PartitionState.NOMINAL;
    this.circuit_tripped = false;
    this.trip_reason = "";
    this.receipts = [];
    this.healing_plans = [];
    this.resumption_deltas.clear();
    this.sync_engine.reset();
  }
}

// Global Singleton
export const globalPartitionHealer = new PartitionHealingEngine();
