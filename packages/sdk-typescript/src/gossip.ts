/**
 * SwarmWatch Decentralized Epidemic Gossip Telemetry Mesh Sync (Seam 45 / ADR 0056).
 * Provides in-process, peer-to-peer epidemic gossip protocols, Vector Clock causality
 * tracking, and anti-entropy synchronization across distributed agent nodes with
 * zero central brokers and zero LLM token consumption.
 */

import { randomUUID } from "node:crypto";

export const MeshNodeStatus = {
  ACTIVE: "active",
  SUSPECT: "suspect",
  DEAD: "dead",
} as const;

export type MeshNodeStatusType = (typeof MeshNodeStatus)[keyof typeof MeshNodeStatus];

export const MeshConvergenceHealth = {
  CONVERGED: "converged",
  SYNCING: "syncing",
  PARTITIONED: "partitioned",
} as const;

export type MeshConvergenceHealthType = (typeof MeshConvergenceHealth)[keyof typeof MeshConvergenceHealth];

export interface MeshNode {
  nodeId: string;
  endpoint: string;
  status: MeshNodeStatusType;
  lastHeartbeat: number;
  generation: number;
  metadata: Record<string, unknown>;
}

export class VectorClock {
  public clock: Record<string, number>;

  constructor(clock: Record<string, number> = {}) {
    this.clock = { ...clock };
  }

  public get(nodeId: string): number {
    return this.clock[nodeId] ?? 0;
  }

  public set(nodeId: string, counter: number): void {
    this.clock[nodeId] = Math.max(0, counter);
  }

  public increment(nodeId: string): number {
    const current = (this.clock[nodeId] ?? 0) + 1;
    this.clock[nodeId] = current;
    return current;
  }

  public merge(other: VectorClock): VectorClock {
    const merged: Record<string, number> = { ...this.clock };
    for (const [k, v] of Object.entries(other.clock)) {
      merged[k] = Math.max(merged[k] ?? 0, v);
    }
    return new VectorClock(merged);
  }

  public dominates(other: VectorClock): boolean {
    const allKeys = new Set([...Object.keys(this.clock), ...Object.keys(other.clock)]);
    let greaterFound = false;
    for (const k of allKeys) {
      const vSelf = this.clock[k] ?? 0;
      const vOther = other.clock[k] ?? 0;
      if (vSelf < vOther) return false;
      if (vSelf > vOther) greaterFound = true;
    }
    return greaterFound;
  }

  public isConcurrentWith(other: VectorClock): boolean {
    const keysA = Object.keys(this.clock);
    const keysB = Object.keys(other.clock);
    if (keysA.length === keysB.length && keysA.every((k) => this.clock[k] === other.clock[k])) {
      return false;
    }
    return !this.dominates(other) && !other.dominates(this);
  }

  public toJSON(): Record<string, number> {
    return { ...this.clock };
  }

  public static fromJSON(data: Record<string, number>): VectorClock {
    return new VectorClock(data);
  }
}

export interface GossipDigest {
  senderId: string;
  vectorClock: Record<string, number>;
  knownSpanIds: string[];
  knownTripIds: string[];
  knownWatermarkIds: string[];
  timestamp: number;
}

export interface GossipDelta {
  senderId: string;
  targetId: string;
  vectorClock: Record<string, number>;
  spans: Record<string, unknown>[];
  circuitTrips: Record<string, unknown>[];
  watermarks: Record<string, unknown>[];
  timestamp: number;
}

export interface MeshStatusReport {
  localNodeId: string;
  localEndpoint: string;
  activePeersCount: number;
  suspectPeersCount: number;
  deadPeersCount: number;
  totalSyncedSpans: number;
  totalSyncedTrips: number;
  totalSyncedWatermarks: number;
  gossipRoundsCompleted: number;
  convergenceHealth: MeshConvergenceHealthType;
  vectorClock: Record<string, number>;
  peers: MeshNode[];
}

export class GossipMeshManager {
  public nodeId: string;
  public endpoint: string;
  public fanout: number;
  public heartbeatTimeout: number;

  public vectorClock: VectorClock;
  public peers: Map<string, MeshNode> = new Map();
  public spans: Map<string, Record<string, unknown>> = new Map();
  public circuitTrips: Map<string, Record<string, unknown>> = new Map();
  public watermarks: Map<string, Record<string, unknown>> = new Map();
  public gossipRoundsCompleted: number = 0;

  constructor(options?: {
    nodeId?: string;
    endpoint?: string;
    fanout?: number;
    heartbeatTimeout?: number;
  }) {
    this.nodeId = options?.nodeId ?? `node-${randomUUID().slice(0, 8)}`;
    this.endpoint = options?.endpoint ?? "http://localhost:8765";
    this.fanout = Math.max(1, options?.fanout ?? 3);
    this.heartbeatTimeout = options?.heartbeatTimeout ?? 30.0;

    this.vectorClock = new VectorClock({ [this.nodeId]: 0 });
  }

  public registerPeer(
    nodeId: string,
    endpoint: string,
    metadata: Record<string, unknown> = {}
  ): MeshNode {
    if (nodeId === this.nodeId) {
      this.endpoint = endpoint;
      return {
        nodeId: this.nodeId,
        endpoint: this.endpoint,
        status: MeshNodeStatus.ACTIVE,
        lastHeartbeat: Date.now() / 1000,
        generation: 1,
        metadata: {},
      };
    }

    const existing = this.peers.get(nodeId);
    if (existing) {
      existing.endpoint = endpoint;
      existing.status = MeshNodeStatus.ACTIVE;
      existing.lastHeartbeat = Date.now() / 1000;
      Object.assign(existing.metadata, metadata);
      return existing;
    }

    const newNode: MeshNode = {
      nodeId,
      endpoint,
      status: MeshNodeStatus.ACTIVE,
      lastHeartbeat: Date.now() / 1000,
      generation: 1,
      metadata: { ...metadata },
    };
    this.peers.set(nodeId, newNode);
    if (this.vectorClock.get(nodeId) === 0) {
      this.vectorClock.set(nodeId, 0);
    }
    return newNode;
  }

  public deregisterPeer(nodeId: string): boolean {
    return this.peers.delete(nodeId);
  }

  public markPeerSuspect(nodeId: string): void {
    const peer = this.peers.get(nodeId);
    if (peer) {
      peer.status = MeshNodeStatus.SUSPECT;
    }
  }

  public markPeerDead(nodeId: string): void {
    const peer = this.peers.get(nodeId);
    if (peer) {
      peer.status = MeshNodeStatus.DEAD;
    }
  }

  public listPeers(includeDead: boolean = false): MeshNode[] {
    const now = Date.now() / 1000;
    const result: MeshNode[] = [];
    for (const p of this.peers.values()) {
      if (p.status === MeshNodeStatus.ACTIVE && now - p.lastHeartbeat > this.heartbeatTimeout) {
        p.status = MeshNodeStatus.SUSPECT;
      }
      if (includeDead || p.status !== MeshNodeStatus.DEAD) {
        result.push(p);
      }
    }
    return result;
  }

  public recordLocalSpan(span: Record<string, unknown>): string {
    const spanId = String(span.span_id || span.spanId || `span-${randomUUID().slice(0, 8)}`);
    const record = {
      ...span,
      span_id: spanId,
      _origin_node: this.nodeId,
      _recorded_at: Date.now() / 1000,
    };
    this.vectorClock.increment(this.nodeId);
    this.spans.set(spanId, record);
    return spanId;
  }

  public recordLocalCircuitTrip(trip: Record<string, unknown>): string {
    const tripId = String(trip.trip_id || trip.tripId || `trip-${randomUUID().slice(0, 8)}`);
    const record = {
      ...trip,
      trip_id: tripId,
      _origin_node: this.nodeId,
      _recorded_at: Date.now() / 1000,
    };
    this.vectorClock.increment(this.nodeId);
    this.circuitTrips.set(tripId, record);
    return tripId;
  }

  public recordLocalWatermark(watermark: Record<string, unknown>): string {
    const wmId = String(watermark.artifact_id || watermark.artifactId || `wm-${randomUUID().slice(0, 8)}`);
    const record = {
      ...watermark,
      artifact_id: wmId,
      _origin_node: this.nodeId,
    };
    this.watermarks.set(wmId, record);
    return wmId;
  }

  public generateDigest(): GossipDigest {
    return {
      senderId: this.nodeId,
      vectorClock: this.vectorClock.toJSON(),
      knownSpanIds: Array.from(this.spans.keys()),
      knownTripIds: Array.from(this.circuitTrips.keys()),
      knownWatermarkIds: Array.from(this.watermarks.keys()),
      timestamp: Date.now() / 1000,
    };
  }

  public computeDeltaForDigest(
    peerDigest: GossipDigest,
    targetPeerId?: string
  ): GossipDelta {
    const peerSpans = new Set(peerDigest.knownSpanIds);
    const peerTrips = new Set(peerDigest.knownTripIds);
    const peerWms = new Set(peerDigest.knownWatermarkIds);

    const missingSpans: Record<string, unknown>[] = [];
    for (const [id, span] of this.spans.entries()) {
      if (!peerSpans.has(id)) {
        missingSpans.push(span);
      }
    }

    const missingTrips: Record<string, unknown>[] = [];
    for (const [id, trip] of this.circuitTrips.entries()) {
      if (!peerTrips.has(id)) {
        missingTrips.push(trip);
      }
    }

    const missingWms: Record<string, unknown>[] = [];
    for (const [id, wm] of this.watermarks.entries()) {
      if (!peerWms.has(id)) {
        missingWms.push(wm);
      }
    }

    return {
      senderId: this.nodeId,
      targetId: targetPeerId || peerDigest.senderId,
      vectorClock: this.vectorClock.toJSON(),
      spans: missingSpans,
      circuitTrips: missingTrips,
      watermarks: missingWms,
      timestamp: Date.now() / 1000,
    };
  }

  public applyDelta(delta: GossipDelta): {
    spansAdded: number;
    tripsAdded: number;
    watermarksAdded: number;
  } {
    let spansAdded = 0;
    let tripsAdded = 0;
    let watermarksAdded = 0;

    for (const s of delta.spans) {
      const id = String(s.span_id || s.spanId || "");
      if (id && !this.spans.has(id)) {
        this.spans.set(id, { ...s });
        spansAdded++;
      }
    }

    for (const t of delta.circuitTrips) {
      const id = String(t.trip_id || t.tripId || "");
      if (id && !this.circuitTrips.has(id)) {
        this.circuitTrips.set(id, { ...t });
        tripsAdded++;
      }
    }

    for (const w of delta.watermarks) {
      const id = String(w.artifact_id || w.artifactId || "");
      if (id && !this.watermarks.has(id)) {
        this.watermarks.set(id, { ...w });
        watermarksAdded++;
      }
    }

    const peerClock = VectorClock.fromJSON(delta.vectorClock);
    this.vectorClock = this.vectorClock.merge(peerClock);

    const peer = this.peers.get(delta.senderId);
    if (peer) {
      peer.lastHeartbeat = Date.now() / 1000;
      peer.status = MeshNodeStatus.ACTIVE;
    }

    return { spansAdded, tripsAdded, watermarksAdded };
  }

  public selectGossipTargets(k?: number): MeshNode[] {
    const fanout = k ?? this.fanout;
    const activePeers = Array.from(this.peers.values()).filter(
      (p) => p.status === MeshNodeStatus.ACTIVE
    );
    if (activePeers.length === 0) return [];
    const shuffled = [...activePeers].sort(() => Math.random() - 0.5);
    return shuffled.slice(0, Math.min(activePeers.length, fanout));
  }

  public runGossipRound(peerManagers?: GossipMeshManager[]): {
    round: number;
    peersSynced: number;
    spansExchanged: number;
    tripsExchanged: number;
    watermarksExchanged: number;
    convergenceHealth: MeshConvergenceHealthType;
  } {
    this.gossipRoundsCompleted++;
    let peersSynced = 0;
    let spansExchanged = 0;
    let tripsExchanged = 0;
    let watermarksExchanged = 0;

    if (peerManagers) {
      const peerMap = new Map(
        peerManagers
          .filter((m) => m.nodeId !== this.nodeId)
          .map((m) => [m.nodeId, m])
      );
      const targets = this.selectGossipTargets();
      for (const target of targets) {
        const targetMgr = peerMap.get(target.nodeId);
        if (targetMgr) {
          // 1. Target generates digest, self computes delta -> target applies delta
          const targetDigest = targetMgr.generateDigest();
          const outboundDelta = this.computeDeltaForDigest(targetDigest, target.nodeId);
          const resOut = targetMgr.applyDelta(outboundDelta);

          // 2. Self generates digest, target computes delta -> self applies delta
          const localDigest = this.generateDigest();
          const inboundDelta = targetMgr.computeDeltaForDigest(localDigest, this.nodeId);
          const resIn = this.applyDelta(inboundDelta);

          spansExchanged += resOut.spansAdded + resIn.spansAdded;
          tripsExchanged += resOut.tripsAdded + resIn.tripsAdded;
          watermarksExchanged += resOut.watermarksAdded + resIn.watermarksAdded;
          peersSynced++;
        }
      }
    }

    const convergenceHealth = this.evaluateConvergence();
    return {
      round: this.gossipRoundsCompleted,
      peersSynced,
      spansExchanged,
      tripsExchanged,
      watermarksExchanged,
      convergenceHealth,
    };
  }

  public evaluateConvergence(): MeshConvergenceHealthType {
    if (this.peers.size === 0) return MeshConvergenceHealth.CONVERGED;

    const peersArr = Array.from(this.peers.values());
    const active = peersArr.filter((p) => p.status === MeshNodeStatus.ACTIVE);
    const suspect = peersArr.filter((p) => p.status === MeshNodeStatus.SUSPECT);
    const dead = peersArr.filter((p) => p.status === MeshNodeStatus.DEAD);

    if (dead.length > active.length) {
      return MeshConvergenceHealth.PARTITIONED;
    }
    if (suspect.length > 0) {
      return MeshConvergenceHealth.SYNCING;
    }
    return MeshConvergenceHealth.CONVERGED;
  }

  public getStatus(): MeshStatusReport {
    let active = 0;
    let suspect = 0;
    let dead = 0;
    const peersList = this.listPeers(true);
    for (const p of peersList) {
      if (p.status === MeshNodeStatus.ACTIVE) active++;
      else if (p.status === MeshNodeStatus.SUSPECT) suspect++;
      else dead++;
    }

    return {
      localNodeId: this.nodeId,
      localEndpoint: this.endpoint,
      activePeersCount: active,
      suspectPeersCount: suspect,
      deadPeersCount: dead,
      totalSyncedSpans: this.spans.size,
      totalSyncedTrips: this.circuitTrips.size,
      totalSyncedWatermarks: this.watermarks.size,
      gossipRoundsCompleted: this.gossipRoundsCompleted,
      convergenceHealth: this.evaluateConvergence(),
      vectorClock: this.vectorClock.toJSON(),
      peers: peersList,
    };
  }

  public reset(): void {
    this.vectorClock = new VectorClock({ [this.nodeId]: 0 });
    this.peers.clear();
    this.spans.clear();
    this.circuitTrips.clear();
    this.watermarks.clear();
    this.gossipRoundsCompleted = 0;
  }
}

export const globalGossipManager = new GossipMeshManager();
