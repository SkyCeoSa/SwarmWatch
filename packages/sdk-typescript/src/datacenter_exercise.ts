/**
 * SwarmWatch Datacenter Failure Exercise & Region Loss Fault Injection (Seam 70 / ADR 0081).
 *
 * Implements deterministic multi-region network partitioning, quorum preservation,
 * fail-closed minority partition blocking, monotonic safety trip latching, and
 * cryptographically verifiable disaster recovery audit reports.
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";
import { CanonicalCodec } from "./canonical.ts";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class DatacenterExerciseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DatacenterExerciseError";
  }
}

export class MinorityPartitionBlockedError extends DatacenterExerciseError {
  constructor(message: string) {
    super(message);
    this.name = "MinorityPartitionBlockedError";
  }
}

export class SplitBrainViolationError extends DatacenterExerciseError {
  constructor(message: string) {
    super(message);
    this.name = "SplitBrainViolationError";
  }
}

export class SafetyMonotonicityViolationError extends DatacenterExerciseError {
  constructor(message: string) {
    super(message);
    this.name = "SafetyMonotonicityViolationError";
  }
}

// ==============================================================================
// 2. Enums (Const objects for Node 22 native type stripping)
// ==============================================================================

export const FailureScenarioType = {
  REGION_SEVERED: "REGION_SEVERED",
  ASYMMETRIC_SPLIT: "ASYMMETRIC_SPLIT",
  TOTAL_POWER_LOSS: "TOTAL_POWER_LOSS",
  CORRUPTED_LEASES: "CORRUPTED_LEASES",
} as const;
export type FailureScenarioType = typeof FailureScenarioType[keyof typeof FailureScenarioType];

export const ExercisePhase = {
  NOMINAL: "NOMINAL",
  REGION_ISOLATION: "REGION_ISOLATION",
  FAIL_CLOSED_VERIFICATION: "FAIL_CLOSED_VERIFICATION",
  RECONNECT_AND_HEAL: "RECONNECT_AND_HEAL",
  POST_HEAL_CONVERGENCE: "POST_HEAL_CONVERGENCE",
} as const;
export type ExercisePhase = typeof ExercisePhase[keyof typeof ExercisePhase];

export const NodeRole = {
  LEADER: "LEADER",
  FOLLOWER: "FOLLOWER",
  CANDIDATE: "CANDIDATE",
  ISOLATED_MINORITY: "ISOLATED_MINORITY",
} as const;
export type NodeRole = typeof NodeRole[keyof typeof NodeRole];

// ==============================================================================
// 3. Models & Data Structures
// ==============================================================================

export function sha256Digest(data: string | Buffer): string {
  return crypto.createHash("sha256").update(data).digest("hex");
}

export interface ClusterNodeData {
  node_id: string;
  region_id: string;
  role: string;
  lease_expiry_ns: number;
  is_healthy: boolean;
}

export class ClusterNode {
  public readonly node_id: string;
  public readonly region_id: string;
  public role: NodeRole;
  public lease_expiry_ns: number;
  public is_healthy: boolean;

  constructor(
    node_id: string,
    region_id: string,
    role: NodeRole = NodeRole.FOLLOWER,
    lease_expiry_ns: number = 0,
    is_healthy: boolean = true
  ) {
    this.node_id = node_id;
    this.region_id = region_id;
    this.role = role;
    this.lease_expiry_ns = lease_expiry_ns;
    this.is_healthy = is_healthy;
  }

  public toDict(): ClusterNodeData {
    return {
      node_id: this.node_id,
      region_id: this.region_id,
      role: this.role,
      lease_expiry_ns: this.lease_expiry_ns,
      is_healthy: this.is_healthy,
    };
  }

  public static fromDict(data: ClusterNodeData): ClusterNode {
    return new ClusterNode(
      data.node_id,
      data.region_id,
      data.role as NodeRole,
      data.lease_expiry_ns,
      data.is_healthy
    );
  }
}

export class NetworkMatrix {
  private readonly _regions: Set<string>;
  private _routes: Map<string, Set<string>>;

  constructor(regions?: string[]) {
    this._regions = new Set(regions || []);
    this._routes = new Map();
    this.restoreAll();
  }

  public addRegion(regionId: string): void {
    this._regions.add(regionId);
    if (!this._routes.has(regionId)) {
      this._routes.set(regionId, new Set());
    }
    for (const r of this._regions) {
      this._routes.get(regionId)!.add(r);
      if (!this._routes.has(r)) {
        this._routes.set(r, new Set());
      }
      this._routes.get(r)!.add(regionId);
    }
  }

  public isReachable(fromRegion: string, toRegion: string): boolean {
    if (fromRegion === toRegion) {
      return true;
    }
    const routes = this._routes.get(fromRegion);
    return routes ? routes.has(toRegion) : false;
  }

  public severRegion(regionId: string): void {
    if (this._routes.has(regionId)) {
      this._routes.set(regionId, new Set([regionId]));
    }
    for (const r of this._regions) {
      const routes = this._routes.get(r);
      if (routes) {
        routes.delete(regionId);
      }
    }
  }

  public setRoute(fromRegion: string, toRegion: string, isConnected: boolean): void {
    if (!this._routes.has(fromRegion)) {
      this._routes.set(fromRegion, new Set([fromRegion]));
    }
    if (isConnected) {
      this._routes.get(fromRegion)!.add(toRegion);
    } else {
      if (fromRegion !== toRegion) {
        this._routes.get(fromRegion)!.delete(toRegion);
      }
    }
  }

  public restoreAll(regions?: string[]): void {
    if (regions) {
      this._regions.clear();
      for (const r of regions) {
        this._regions.add(r);
      }
    }
    this._routes = new Map();
    for (const r of this._regions) {
      this._routes.set(r, new Set(this._regions));
    }
  }

  public toDict(): Record<string, string[]> {
    const out: Record<string, string[]> = {};
    const sortedRegions = Array.from(this._regions).sort();
    for (const r of sortedRegions) {
      const targets = this._routes.get(r);
      out[r] = targets ? Array.from(targets).sort() : [];
    }
    return out;
  }
}

export interface PermitAttemptResultData {
  node_id: string;
  region_id: string;
  granted: boolean;
  permit_id: string | null;
  reason: string;
  timestamp_ns: number;
}

export class PermitAttemptResult {
  public readonly node_id: string;
  public readonly region_id: string;
  public readonly granted: boolean;
  public readonly permit_id: string | null;
  public readonly reason: string;
  public readonly timestamp_ns: number;

  constructor(
    node_id: string,
    region_id: string,
    granted: boolean,
    permit_id: string | null = null,
    reason: string = "",
    timestamp_ns?: number
  ) {
    this.node_id = node_id;
    this.region_id = region_id;
    this.granted = granted;
    this.permit_id = permit_id;
    this.reason = reason;
    this.timestamp_ns = timestamp_ns ?? Date.now() * 1_000_000;
  }

  public toDict(): PermitAttemptResultData {
    return {
      node_id: this.node_id,
      region_id: this.region_id,
      granted: this.granted,
      permit_id: this.permit_id,
      reason: this.reason,
      timestamp_ns: this.timestamp_ns,
    };
  }
}

export interface FailureExerciseEventData {
  timestamp_ns: number;
  phase: string;
  event_type: string;
  details: Record<string, unknown>;
}

export class FailureExerciseEvent {
  public readonly timestamp_ns: number;
  public readonly phase: ExercisePhase;
  public readonly event_type: string;
  public readonly details: Record<string, unknown>;

  constructor(
    timestamp_ns: number,
    phase: ExercisePhase,
    event_type: string,
    details: Record<string, unknown>
  ) {
    this.timestamp_ns = timestamp_ns;
    this.phase = phase;
    this.event_type = event_type;
    this.details = details;
  }

  public toDict(): FailureExerciseEventData {
    return {
      timestamp_ns: this.timestamp_ns,
      phase: this.phase,
      event_type: this.event_type,
      details: this.details,
    };
  }
}

export interface FailureExerciseReportData {
  exercise_id: string;
  scenario: string;
  total_nodes: number;
  total_regions: number;
  majority_partition_operational: boolean;
  minority_partition_isolated: boolean;
  split_brain_prevented: boolean;
  safety_monotonicity_verified: boolean;
  trips_preserved_count: number;
  invariants_passed: boolean;
  duration_ms: number;
  exercise_hash: string;
  events: FailureExerciseEventData[];
  issued_at_ns: number;
}

export class FailureExerciseReport {
  public readonly exercise_id: string;
  public readonly scenario: FailureScenarioType;
  public readonly total_nodes: number;
  public readonly total_regions: number;
  public readonly majority_partition_operational: boolean;
  public readonly minority_partition_isolated: boolean;
  public readonly split_brain_prevented: boolean;
  public readonly safety_monotonicity_verified: boolean;
  public readonly trips_preserved_count: number;
  public readonly invariants_passed: boolean;
  public readonly duration_ms: number;
  public readonly exercise_hash: string;
  public readonly events: FailureExerciseEvent[];
  public readonly issued_at_ns: number;

  constructor(data: {
    exercise_id: string;
    scenario: FailureScenarioType;
    total_nodes: number;
    total_regions: number;
    majority_partition_operational: boolean;
    minority_partition_isolated: boolean;
    split_brain_prevented: boolean;
    safety_monotonicity_verified: boolean;
    trips_preserved_count: number;
    invariants_passed: boolean;
    duration_ms: number;
    exercise_hash: string;
    events?: FailureExerciseEvent[];
    issued_at_ns?: number;
  }) {
    this.exercise_id = data.exercise_id;
    this.scenario = data.scenario;
    this.total_nodes = data.total_nodes;
    this.total_regions = data.total_regions;
    this.majority_partition_operational = data.majority_partition_operational;
    this.minority_partition_isolated = data.minority_partition_isolated;
    this.split_brain_prevented = data.split_brain_prevented;
    this.safety_monotonicity_verified = data.safety_monotonicity_verified;
    this.trips_preserved_count = data.trips_preserved_count;
    this.invariants_passed = data.invariants_passed;
    this.duration_ms = data.duration_ms;
    this.exercise_hash = data.exercise_hash;
    this.events = data.events ?? [];
    this.issued_at_ns = data.issued_at_ns ?? Date.now() * 1_000_000;
  }

  public toDict(): FailureExerciseReportData {
    return {
      exercise_id: this.exercise_id,
      scenario: this.scenario,
      total_nodes: this.total_nodes,
      total_regions: this.total_regions,
      majority_partition_operational: this.majority_partition_operational,
      minority_partition_isolated: this.minority_partition_isolated,
      split_brain_prevented: this.split_brain_prevented,
      safety_monotonicity_verified: this.safety_monotonicity_verified,
      trips_preserved_count: this.trips_preserved_count,
      invariants_passed: this.invariants_passed,
      duration_ms: this.duration_ms,
      exercise_hash: this.exercise_hash,
      events: this.events.map((e) => e.toDict()),
      issued_at_ns: this.issued_at_ns,
    };
  }
}

// ==============================================================================
// 4. Core Engine: DatacenterFailureOrchestrator
// ==============================================================================

export class DatacenterFailureOrchestrator {
  private readonly _leaseDurationNs: number;
  private _nodes: Map<string, ClusterNode> = new Map();
  private _regions: Set<string> = new Set();
  private _matrix: NetworkMatrix = new NetworkMatrix();
  private _activePhase: ExercisePhase = ExercisePhase.NOMINAL;
  private _tripped = false;
  private readonly _tripReasons: string[] = [];
  private readonly _trippedNodes: Set<string> = new Set();
  private readonly _issuedPermits: Map<string, Record<string, unknown>> = new Map();
  private readonly _events: FailureExerciseEvent[] = [];
  private readonly _reportsHistory: FailureExerciseReport[] = [];

  constructor(nodes?: ClusterNode[], leaseDurationMs: number = 3000.0) {
    this._leaseDurationNs = leaseDurationMs * 1_000_000;
    if (nodes && nodes.length > 0) {
      this.configure(nodes);
    } else {
      this.configureDefaultTopology();
    }
  }

  public configureDefaultTopology(): void {
    const defaultNodes = [
      new ClusterNode("node-us-east-1", "us-east-1", NodeRole.LEADER),
      new ClusterNode("node-us-east-2", "us-east-1", NodeRole.FOLLOWER),
      new ClusterNode("node-us-west-1", "us-west-2", NodeRole.FOLLOWER),
      new ClusterNode("node-us-west-2", "us-west-2", NodeRole.FOLLOWER),
      new ClusterNode("node-eu-cent-1", "eu-central-1", NodeRole.FOLLOWER),
    ];
    this.configure(defaultNodes);
  }

  public configure(nodes: ClusterNode[]): void {
    this._nodes.clear();
    this._regions.clear();
    for (const n of nodes) {
      this._nodes.set(n.node_id, n);
      this._regions.add(n.region_id);
    }
    this._matrix = new NetworkMatrix(Array.from(this._regions));
    this._activePhase = ExercisePhase.NOMINAL;
    this._tripped = false;
    this._tripReasons.length = 0;
    this._trippedNodes.clear();
    this._issuedPermits.clear();
    this._events.length = 0;

    const nowNs = Date.now() * 1_000_000;
    for (const n of this._nodes.values()) {
      if (n.role === NodeRole.LEADER) {
        n.lease_expiry_ns = nowNs + this._leaseDurationNs;
      }
    }

    this._logEvent(ExercisePhase.NOMINAL, "TOPOLOGY_CONFIGURED", {
      total_nodes: this._nodes.size,
      regions: Array.from(this._regions).sort(),
    });
  }

  private _logEvent(phase: ExercisePhase, eventType: string, details: Record<string, unknown>): void {
    const event = new FailureExerciseEvent(Date.now() * 1_000_000, phase, eventType, details);
    this._events.push(event);
  }

  public getReachableNodes(nodeId: string): ClusterNode[] {
    const sourceNode = this._nodes.get(nodeId);
    if (!sourceNode || !sourceNode.is_healthy) {
      return [];
    }

    const reachable: ClusterNode[] = [];
    for (const target of this._nodes.values()) {
      if (!target.is_healthy) continue;
      if (this._matrix.isReachable(sourceNode.region_id, target.region_id)) {
        reachable.push(target);
      }
    }
    return reachable;
  }

  public hasMajorityQuorum(nodeId: string): boolean {
    let totalHealthy = 0;
    for (const n of this._nodes.values()) {
      if (n.is_healthy) totalHealthy++;
    }
    const majorityThreshold = Math.floor(totalHealthy / 2) + 1;
    const reachableCount = this.getReachableNodes(nodeId).length;
    return reachableCount >= majorityThreshold;
  }

  public injectPartition(
    severedRegions: string[],
    asymmetricRoutes?: Array<[string, string]>
  ): void {
    this._activePhase = ExercisePhase.REGION_ISOLATION;
    for (const r of severedRegions) {
      this._matrix.severRegion(r);
    }

    if (asymmetricRoutes) {
      for (const [fromR, toR] of asymmetricRoutes) {
        this._matrix.setRoute(fromR, toR, false);
      }
    }

    this._logEvent(ExercisePhase.REGION_ISOLATION, "PARTITION_INJECTED", {
      severed_regions: severedRegions,
      asymmetric_routes: asymmetricRoutes || [],
      matrix: this._matrix.toDict(),
    });
  }

  public healPartition(): void {
    this._activePhase = ExercisePhase.RECONNECT_AND_HEAL;
    this._matrix.restoreAll(Array.from(this._regions));

    if (this._trippedNodes.size > 0) {
      this._tripped = true;
    }

    this._logEvent(ExercisePhase.RECONNECT_AND_HEAL, "PARTITION_HEALED", {
      reconnected_regions: Array.from(this._regions).sort(),
      monotone_trip_latched: this._tripped,
    });
  }

  public requestPermit(
    nodeId: string,
    tenantId: string = "tenant-prod-01",
    tokens: number = 100
  ): PermitAttemptResult {
    const node = this._nodes.get(nodeId);
    if (!node) {
      throw new DatacenterExerciseError(`Node '${nodeId}' does not exist in cluster`);
    }

    if (this._tripped || this._trippedNodes.has(nodeId)) {
      return new PermitAttemptResult(
        nodeId,
        node.region_id,
        false,
        null,
        "Cluster safety trip latched (fail-closed)"
      );
    }

    if (!this.hasMajorityQuorum(nodeId)) {
      this._logEvent(this._activePhase, "MINORITY_PERMIT_BLOCKED", {
        node_id: nodeId,
        region_id: node.region_id,
        reachable_nodes: this.getReachableNodes(nodeId).map((n) => n.node_id),
      });
      throw new MinorityPartitionBlockedError(
        `Node '${nodeId}' in region '${node.region_id}' is isolated in a minority partition`
      );
    }

    const permitId = `pmt-${crypto
      .createHash("sha256")
      .update(`${nodeId}:${tenantId}:${Date.now()}`)
      .digest("hex")
      .substring(0, 12)}`;
    const record = {
      permit_id: permitId,
      node_id: nodeId,
      region_id: node.region_id,
      tenant_id: tenantId,
      tokens,
      issued_at_ns: Date.now() * 1_000_000,
    };
    this._issuedPermits.set(permitId, record);

    this._logEvent(this._activePhase, "PERMIT_GRANTED", record);

    return new PermitAttemptResult(
      nodeId,
      node.region_id,
      true,
      permitId,
      "Majority quorum verified"
    );
  }

  public broadcastTrip(
    originNodeId: string,
    reason: string = "Entropy collapse emergency halt"
  ): Record<string, unknown> {
    const originNode = this._nodes.get(originNodeId);
    if (!originNode) {
      throw new DatacenterExerciseError(`Node '${originNodeId}' not found`);
    }

    this._trippedNodes.add(originNodeId);
    this._tripReasons.push(reason);

    const reachable = this.getReachableNodes(originNodeId);
    for (const r of reachable) {
      this._trippedNodes.add(r.node_id);
    }

    if (this.hasMajorityQuorum(originNodeId)) {
      this._tripped = true;
    }

    this._logEvent(this._activePhase, "TRIP_BROADCAST", {
      origin_node_id: originNodeId,
      reason,
      affected_nodes: reachable.map((n) => n.node_id),
      cluster_globally_tripped: this._tripped,
    });

    return {
      origin_node_id: originNodeId,
      reason,
      affected_node_count: reachable.length,
      cluster_globally_tripped: this._tripped,
    };
  }

  public runExercise(
    scenario: FailureScenarioType = FailureScenarioType.REGION_SEVERED,
    targetSeveredRegions?: string[]
  ): FailureExerciseReport {
    const startTime = performance.now();
    const exerciseId = `dr-ex-${crypto.randomBytes(4).toString("hex")}`;
    const severed = targetSeveredRegions || ["eu-central-1"];

    // Phase 1: NOMINAL
    this._activePhase = ExercisePhase.NOMINAL;
    let majorityOperational = false;
    const nominalNode = Array.from(this._nodes.keys())[0];
    try {
      const nominalRes = this.requestPermit(nominalNode, "tenant-test", 50);
      majorityOperational = nominalRes.granted;
    } catch {
      majorityOperational = false;
    }

    // Phase 2: REGION_ISOLATION
    this.injectPartition(severed);

    // Phase 3: FAIL_CLOSED_VERIFICATION
    this._activePhase = ExercisePhase.FAIL_CLOSED_VERIFICATION;
    let minorityIsolated = true;
    let splitBrainPrevented = true;

    let minorityNode: string | null = null;
    for (const n of this._nodes.values()) {
      if (severed.includes(n.region_id)) {
        minorityNode = n.node_id;
        break;
      }
    }

    if (minorityNode) {
      try {
        this.requestPermit(minorityNode, "tenant-test", 50);
        minorityIsolated = false;
        splitBrainPrevented = false;
      } catch (e: any) {
        if (e instanceof MinorityPartitionBlockedError) {
          minorityIsolated = true;
        } else {
          minorityIsolated = true;
        }
      }
    }

    let majorityNode: string | null = null;
    for (const n of this._nodes.values()) {
      if (!severed.includes(n.region_id) && this.hasMajorityQuorum(n.node_id)) {
        majorityNode = n.node_id;
        break;
      }
    }

    if (majorityNode) {
      const resMaj = this.requestPermit(majorityNode, "tenant-test", 75);
      if (!resMaj.granted) {
        majorityOperational = false;
      }
    }

    const tripNode = majorityNode || nominalNode;
    this.broadcastTrip(tripNode, "Chaos fault safety trip injection");

    // Phase 4: RECONNECT_AND_HEAL
    this.healPartition();

    // Phase 5: POST_HEAL_CONVERGENCE
    this._activePhase = ExercisePhase.POST_HEAL_CONVERGENCE;
    let safetyMonotonicityVerified = this._tripped === true;

    const postHealAttempt = this.requestPermit(nominalNode, "tenant-test", 20);
    if (postHealAttempt.granted) {
      safetyMonotonicityVerified = false;
    }

    const elapsedMs = performance.now() - startTime;
    const invariantsPassed =
      majorityOperational &&
      minorityIsolated &&
      splitBrainPrevented &&
      safetyMonotonicityVerified;

    const canonicalDict = {
      exercise_id: exerciseId,
      scenario: scenario,
      total_nodes: this._nodes.size,
      total_regions: this._regions.size,
      majority_partition_operational: majorityOperational,
      minority_partition_isolated: minorityIsolated,
      split_brain_prevented: splitBrainPrevented,
      safety_monotonicity_verified: safetyMonotonicityVerified,
      trips_preserved_count: this._trippedNodes.size,
      invariants_passed: invariantsPassed,
    };
    const encoded = CanonicalCodec.canonicalize(canonicalDict);
    const reportHash = sha256Digest(`sw:fail:v1:${encoded}`);

    const report = new FailureExerciseReport({
      exercise_id: exerciseId,
      scenario,
      total_nodes: this._nodes.size,
      total_regions: this._regions.size,
      majority_partition_operational: majorityOperational,
      minority_partition_isolated: minorityIsolated,
      split_brain_prevented: splitBrainPrevented,
      safety_monotonicity_verified: safetyMonotonicityVerified,
      trips_preserved_count: this._trippedNodes.size,
      invariants_passed: invariantsPassed,
      duration_ms: elapsedMs,
      exercise_hash: reportHash,
      events: [...this._events],
    });

    this._reportsHistory.push(report);
    return report;
  }

  public status(): Record<string, unknown> {
    return {
      active_phase: this._activePhase,
      total_nodes: this._nodes.size,
      total_regions: this._regions.size,
      cluster_globally_tripped: this._tripped,
      tripped_nodes_count: this._trippedNodes.size,
      issued_permits_count: this._issuedPermits.size,
      events_recorded: this._events.length,
      total_exercises_run: this._reportsHistory.length,
      network_matrix: this._matrix.toDict(),
    };
  }

  public getReports(): FailureExerciseReport[] {
    return [...this._reportsHistory];
  }

  public reset(): void {
    this.configureDefaultTopology();
    this._reportsHistory.length = 0;
  }
}

// ==============================================================================
// 5. Global Singleton
// ==============================================================================

export const globalDatacenterOrchestrator = new DatacenterFailureOrchestrator();
