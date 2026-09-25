/**
 * SwarmWatch Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer (Seam 46 / ADR 0057).
 * Provides deterministic 32-bit consistent hashing with dynamic virtual node allocation,
 * real-time node capacity scoring, causal session affinity, and automatic spillover routing
 * in <25 microseconds per decision with zero external LLM API calls.
 */

import { createHash } from "node:crypto";

export const NodeHealthTier = {
  OPTIMAL: "optimal",       // Capacity >= 0.70
  NOMINAL: "nominal",       // 0.40 <= Capacity < 0.70
  CONGESTED: "congested",   // 0.15 <= Capacity < 0.40
  OVERLOADED: "overloaded", // Capacity < 0.15
} as const;

export type NodeHealthTierType = (typeof NodeHealthTier)[keyof typeof NodeHealthTier];

export interface NodeComputeMetrics {
  cpuUtilization: number;     // 0.0 to 1.0
  memoryUtilization: number;  // 0.0 to 1.0
  queueDepth: number;         // Current awaiting tasks
  maxQueueDepth?: number;     // Target queue saturation limit (default: 1000)
  avgLatencyMs?: number;      // Rolling execution latency in ms
  reportedAt?: number;
}

export function computeCapacityScore(
  metrics: NodeComputeMetrics,
  wCpu = 0.40,
  wMem = 0.35,
  wQueue = 0.25
): number {
  const uCpu = Math.max(0, Math.min(1, metrics.cpuUtilization ?? 0));
  const uMem = Math.max(0, Math.min(1, metrics.memoryUtilization ?? 0));
  const maxQ = Math.max(1, metrics.maxQueueDepth ?? 1000);
  const qRatio = Math.max(0, Math.min(1, (metrics.queueDepth ?? 0) / maxQ));

  const rawScore = (wCpu * (1.0 - uCpu)) + (wMem * (1.0 - uMem)) + (wQueue * (1.0 - qRatio));
  return Number(Math.max(0, Math.min(1, rawScore)).toFixed(4));
}

export function getHealthTier(capacityScore: number): NodeHealthTierType {
  if (capacityScore >= 0.70) return NodeHealthTier.OPTIMAL;
  if (capacityScore >= 0.40) return NodeHealthTier.NOMINAL;
  if (capacityScore >= 0.15) return NodeHealthTier.CONGESTED;
  return NodeHealthTier.OVERLOADED;
}

export function hashToken32(key: string): number {
  return createHash("md5").update(key).digest().readUInt32BE(0);
}

export interface VirtualNode {
  token: number;
  nodeId: string;
  vnodeIdx: number;
}

export interface ShardRoutingKey {
  tenantId: string;
  projectId: string;
  swarmId: string;
  sessionAffinityKey?: string;
}

export function formatShardRoutingKey(key: ShardRoutingKey | string): string {
  if (typeof key === "string") return key;
  if (key.sessionAffinityKey) {
    return `${key.tenantId}:${key.projectId}:${key.swarmId}:${key.sessionAffinityKey}`;
  }
  return `${key.tenantId}:${key.projectId}:${key.swarmId}`;
}

export function parseShardRoutingKey(raw: string): ShardRoutingKey {
  const parts = raw.split(":");
  if (parts.length >= 4) {
    return { tenantId: parts[0], projectId: parts[1], swarmId: parts[2], sessionAffinityKey: parts[3] };
  } else if (parts.length === 3) {
    return { tenantId: parts[0], projectId: parts[1], swarmId: parts[2] };
  } else if (parts.length === 2) {
    return { tenantId: "default", projectId: parts[0], swarmId: parts[1] };
  }
  return { tenantId: "default", projectId: "default", swarmId: raw };
}

export interface ShardRouteResult {
  routingKey: string;
  primaryNodeId: string;
  endpoint: string;
  spilloverNodeId?: string;
  spilloverEndpoint?: string;
  routeMode: "primary" | "spillover" | "fallback";
  capacityScore: number;
  healthTier: NodeHealthTierType;
  lookupLatencyNs: number;
}

export interface ShardingStatusReport {
  totalPhysicalNodes: number;
  totalVirtualNodes: number;
  nodes: Record<string, {
    endpoint: string;
    metrics: Record<string, unknown>;
    capacityScore: number;
    healthTier: string;
    vnodes: number;
  }>;
  rebalanceCount: number;
  lastRebalanceAt: number;
}

export class ConsistentHashRing {
  public baseVnodes: number;
  private ring: Map<number, VirtualNode> = new Map();
  private sortedTokens: number[] = [];
  private nodeWeights: Map<string, number> = new Map();

  constructor(baseVnodes = 64) {
    this.baseVnodes = Math.max(1, baseVnodes);
  }

  public addNode(nodeId: string, capacityScore: number): number {
    this.removeNode(nodeId);
    const score = Math.max(0, Math.min(1, capacityScore));
    this.nodeWeights.set(nodeId, score);
    const vnodeCount = Math.max(1, Math.floor(this.baseVnodes * score));

    for (let i = 0; i < vnodeCount; i++) {
      const vnodeKey = `${nodeId}#vnode${i}`;
      const token = hashToken32(vnodeKey);
      this.ring.set(token, { token, nodeId, vnodeIdx: i });
    }

    this.sortedTokens = Array.from(this.ring.keys()).sort((a, b) => a - b);
    return vnodeCount;
  }

  public removeNode(nodeId: string): void {
    for (const [token, vnode] of this.ring.entries()) {
      if (vnode.nodeId === nodeId) {
        this.ring.delete(token);
      }
    }
    this.nodeWeights.delete(nodeId);
    this.sortedTokens = Array.from(this.ring.keys()).sort((a, b) => a - b);
  }

  public lookup(key: string): [string | null, string | null] {
    if (this.sortedTokens.length === 0) return [null, null];

    const token = hashToken32(key);
    let low = 0;
    let high = this.sortedTokens.length;

    while (low < high) {
      const mid = (low + high) >>> 1;
      if (this.sortedTokens[mid] < token) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    const idx = low === this.sortedTokens.length ? 0 : low;
    const primaryToken = this.sortedTokens[idx];
    const primaryNode = this.ring.get(primaryToken)!.nodeId;

    let spilloverNode: string | null = null;
    const totalTokens = this.sortedTokens.length;
    for (let step = 1; step < totalTokens; step++) {
      const nextIdx = (idx + step) % totalTokens;
      const candNode = this.ring.get(this.sortedTokens[nextIdx])!.nodeId;
      if (candNode !== primaryNode) {
        spilloverNode = candNode;
        break;
      }
    }

    return [primaryNode, spilloverNode];
  }

  public getSuccessor(nodeId: string): string | null {
    if (this.sortedTokens.length === 0) return null;
    for (let idx = 0; idx < this.sortedTokens.length; idx++) {
      if (this.ring.get(this.sortedTokens[idx])!.nodeId === nodeId) {
        const total = this.sortedTokens.length;
        for (let step = 1; step < total; step++) {
          const cand = this.ring.get(this.sortedTokens[(idx + step) % total])!.nodeId;
          if (cand !== nodeId) return cand;
        }
      }
    }
    const firstTok = this.sortedTokens[0];
    const cand = this.ring.get(firstTok)!.nodeId;
    return cand !== nodeId ? cand : null;
  }

  public get totalTokens(): number {
    return this.sortedTokens.length;
  }

  public get nodeCount(): number {
    return this.nodeWeights.size;
  }

  public getTokens(): Array<{ token: number; nodeId: string; vnodeIdx: number }> {
    return this.sortedTokens.map((tok) => {
      const vn = this.ring.get(tok)!;
      return { token: tok, nodeId: vn.nodeId, vnodeIdx: vn.vnodeIdx };
    });
  }
}

export class ComputeAwareBalancer {
  public ring: ConsistentHashRing;
  public spilloverThreshold: number;
  private endpoints: Map<string, string> = new Map();
  private metrics: Map<string, NodeComputeMetrics> = new Map();
  private sessionAffinity: Map<string, string> = new Map();
  private rebalanceCount = 0;
  private lastRebalanceAt = Date.now();

  constructor(baseVnodes = 64, spilloverThreshold = 0.15) {
    this.ring = new ConsistentHashRing(baseVnodes);
    this.spilloverThreshold = spilloverThreshold;
  }

  public registerNode(
    nodeId: string,
    endpoint: string,
    metrics?: NodeComputeMetrics
  ): void {
    this.endpoints.set(nodeId, endpoint);
    const m: NodeComputeMetrics = metrics ?? {
      cpuUtilization: 0,
      memoryUtilization: 0,
      queueDepth: 0,
      maxQueueDepth: 1000,
      avgLatencyMs: 0,
      reportedAt: Date.now(),
    };
    this.metrics.set(nodeId, m);
    const score = computeCapacityScore(m);
    this.ring.addNode(nodeId, score);
  }

  public deregisterNode(nodeId: string): void {
    this.ring.removeNode(nodeId);
    this.endpoints.delete(nodeId);
    this.metrics.delete(nodeId);
    for (const [k, v] of this.sessionAffinity.entries()) {
      if (v === nodeId) {
        this.sessionAffinity.delete(k);
      }
    }
  }

  public clearSessionAffinity(): void {
    this.sessionAffinity.clear();
  }

  public updateNodeMetrics(nodeId: string, metrics: NodeComputeMetrics): void {
    if (!this.endpoints.has(nodeId)) {
      this.endpoints.set(nodeId, `http://${nodeId}:8765`);
    }
    this.metrics.set(nodeId, metrics);
    const score = computeCapacityScore(metrics);
    this.ring.addNode(nodeId, score);
  }

  public route(routingKey: ShardRoutingKey | string): ShardRouteResult {
    const startNs = process.hrtime.bigint();
    const keyStr = formatShardRoutingKey(routingKey);

    let primaryId: string | null = null;
    let spilloverId: string | null = null;

    if (this.sessionAffinity.has(keyStr) && this.endpoints.has(this.sessionAffinity.get(keyStr)!)) {
      primaryId = this.sessionAffinity.get(keyStr)!;
      spilloverId = this.ring.getSuccessor(primaryId);
    } else {
      const [p, s] = this.ring.lookup(keyStr);
      primaryId = p;
      spilloverId = s;
      if (primaryId) {
        this.sessionAffinity.set(keyStr, primaryId);
      }
    }

    if (!primaryId) {
      const lookupLatencyNs = Number(process.hrtime.bigint() - startNs);
      return {
        routingKey: keyStr,
        primaryNodeId: "local-fallback",
        endpoint: "http://127.0.0.1:8765",
        routeMode: "fallback",
        capacityScore: 1.0,
        healthTier: NodeHealthTier.OPTIMAL,
        lookupLatencyNs,
      };
    }

    const primaryMetrics = this.metrics.get(primaryId) ?? {
      cpuUtilization: 0,
      memoryUtilization: 0,
      queueDepth: 0,
    };
    const primaryScore = computeCapacityScore(primaryMetrics);
    const primaryTier = getHealthTier(primaryScore);
    const primaryEndpoint = this.endpoints.get(primaryId) ?? `http://${primaryId}:8765`;
    const spilloverEndpoint = spilloverId ? (this.endpoints.get(spilloverId) ?? `http://${spilloverId}:8765`) : undefined;

    if (primaryScore < this.spilloverThreshold && spilloverId) {
      const lookupLatencyNs = Number(process.hrtime.bigint() - startNs);
      const spilloverMetrics = this.metrics.get(spilloverId) ?? {
        cpuUtilization: 0,
        memoryUtilization: 0,
        queueDepth: 0,
      };
      const spilloverScore = computeCapacityScore(spilloverMetrics);
      return {
        routingKey: keyStr,
        primaryNodeId: spilloverId,
        endpoint: spilloverEndpoint ?? primaryEndpoint,
        spilloverNodeId: primaryId,
        spilloverEndpoint: primaryEndpoint,
        routeMode: "spillover",
        capacityScore: spilloverScore,
        healthTier: getHealthTier(spilloverScore),
        lookupLatencyNs,
      };
    }

    const lookupLatencyNs = Number(process.hrtime.bigint() - startNs);
    return {
      routingKey: keyStr,
      primaryNodeId: primaryId,
      endpoint: primaryEndpoint,
      spilloverNodeId: spilloverId ?? undefined,
      spilloverEndpoint,
      routeMode: "primary",
      capacityScore: primaryScore,
      healthTier: primaryTier,
      lookupLatencyNs,
    };
  }

  public rebalance(): ShardingStatusReport {
    for (const [nodeId, metrics] of this.metrics.entries()) {
      const score = computeCapacityScore(metrics);
      this.ring.addNode(nodeId, score);
    }
    this.rebalanceCount++;
    this.lastRebalanceAt = Date.now();
    return this.getStatus();
  }

  public getStatus(): ShardingStatusReport {
    const nodesInfo: ShardingStatusReport["nodes"] = {};
    for (const [nodeId, endpoint] of this.endpoints.entries()) {
      const m = this.metrics.get(nodeId) ?? {
        cpuUtilization: 0,
        memoryUtilization: 0,
        queueDepth: 0,
      };
      const score = computeCapacityScore(m);
      const tier = getHealthTier(score);
      const vnodeCount = this.ring.getTokens().filter((t) => t.nodeId === nodeId).length;

      nodesInfo[nodeId] = {
        endpoint,
        metrics: {
          cpuUtilization: m.cpuUtilization,
          memoryUtilization: m.memoryUtilization,
          queueDepth: m.queueDepth,
          maxQueueDepth: m.maxQueueDepth ?? 1000,
          avgLatencyMs: m.avgLatencyMs ?? 0,
          reportedAt: m.reportedAt ?? Date.now(),
        },
        capacityScore: score,
        healthTier: tier,
        vnodes: vnodeCount,
      };
    }

    return {
      totalPhysicalNodes: this.endpoints.size,
      totalVirtualNodes: this.ring.totalTokens,
      nodes: nodesInfo,
      rebalanceCount: this.rebalanceCount,
      lastRebalanceAt: this.lastRebalanceAt,
    };
  }

  public getRingTokens(): Array<{ token: number; nodeId: string; vnodeIdx: number }> {
    return this.ring.getTokens();
  }
}

// Global process singleton
export const globalComputeBalancer = new ComputeAwareBalancer();
globalComputeBalancer.registerNode("node-alpha", "http://127.0.0.1:8765", {
  cpuUtilization: 0.15,
  memoryUtilization: 0.25,
  queueDepth: 4,
  maxQueueDepth: 1000,
  avgLatencyMs: 1.2,
  reportedAt: Date.now(),
});
