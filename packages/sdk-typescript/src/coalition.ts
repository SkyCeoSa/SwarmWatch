/**
 * SwarmWatch Real-Time Topological Graph Clustering & Dynamic Agent Coalitions (TypeScript Parity).
 * Analyzes directed causal interaction graphs, calculates degree and bottleneck transit
 * centrality, computes Newman-Girvan modularity, and dynamically forms autonomous agent
 * coalitions with designated Coalition Leaders (ADR 0047).
 *
 * Conforms strictly to Node 22 type-stripping rules: no TS enums, no parameter properties.
 */

import { randomUUID } from "node:crypto";

export interface NodeCentrality {
  agentId: string;
  inDegree: number;
  outDegree: number;
  totalDegree: number;
  bottleneckScore: number;
  isBottleneck: boolean;
}

export interface AgentCoalition {
  coalitionId: string;
  memberAgents: string[];
  leaderAgent: string;
  interactionDensity: number;
  isDeadlocked: boolean;
  totalTokens: number;
  createdAt: string;
}

export interface TopologicalClusterResult {
  swarmId: string;
  timestamp: string;
  totalAgents: number;
  totalInteractions: number;
  modularityScore: number;
  clusters: string[][];
  coalitions: AgentCoalition[];
  nodeCentralities: Record<string, NodeCentrality>;
  bottleneckAgents: string[];
}

export class TopologicalCoalitionEngine {
  public bottleneckThreshold: number;
  public densityThreshold: number;

  // swarmId -> source -> target -> weight
  private adjacency: Map<string, Map<string, Map<string, number>>>;
  // swarmId -> source -> target -> tokens
  private edgeTokens: Map<string, Map<string, Map<string, number>>>;
  // swarmId -> Set of agents
  private agents: Map<string, Set<string>>;
  // swarmId -> count
  private interactionCounts: Map<string, number>;
  // swarmId -> latest result
  private latestResults: Map<string, TopologicalClusterResult>;

  constructor(bottleneckThreshold: number = 0.15, densityThreshold: number = 0.40) {
    this.bottleneckThreshold = bottleneckThreshold;
    this.densityThreshold = densityThreshold;
    this.adjacency = new Map();
    this.edgeTokens = new Map();
    this.agents = new Map();
    this.interactionCounts = new Map();
    this.latestResults = new Map();
  }

  public recordInteraction(
    swarmId: string,
    sourceAgent: string,
    targetAgent: string,
    tokens: number = 0
  ): void {
    if (!sourceAgent || !targetAgent || sourceAgent === targetAgent) {
      return;
    }

    let swarmAdj = this.adjacency.get(swarmId);
    if (!swarmAdj) {
      swarmAdj = new Map();
      this.adjacency.set(swarmId, swarmAdj);
    }
    let srcAdj = swarmAdj.get(sourceAgent);
    if (!srcAdj) {
      srcAdj = new Map();
      swarmAdj.set(sourceAgent, srcAdj);
    }

    let swarmTokens = this.edgeTokens.get(swarmId);
    if (!swarmTokens) {
      swarmTokens = new Map();
      this.edgeTokens.set(swarmId, swarmTokens);
    }
    let srcTokens = swarmTokens.get(sourceAgent);
    if (!srcTokens) {
      srcTokens = new Map();
      swarmTokens.set(sourceAgent, srcTokens);
    }

    let swarmAgents = this.agents.get(swarmId);
    if (!swarmAgents) {
      swarmAgents = new Set();
      this.agents.set(swarmId, swarmAgents);
    }

    const weightDelta = 1.0 + Math.log1p(Math.max(0, tokens));
    srcAdj.set(targetAgent, (srcAdj.get(targetAgent) || 0) + weightDelta);
    srcTokens.set(targetAgent, (srcTokens.get(targetAgent) || 0) + tokens);

    swarmAgents.add(sourceAgent);
    swarmAgents.add(targetAgent);

    this.interactionCounts.set(
      swarmId,
      (this.interactionCounts.get(swarmId) || 0) + 1
    );
  }

  public computeCentrality(swarmId: string): Record<string, NodeCentrality> {
    const agentSet = this.agents.get(swarmId);
    if (!agentSet || agentSet.size === 0) {
      return {};
    }

    const agentList = Array.from(agentSet);
    const swarmAdj = this.adjacency.get(swarmId) || new Map();

    const inWeights = new Map<string, number>();
    const outWeights = new Map<string, number>();
    let totalGraphWeight = 0;

    for (const [u, targets] of swarmAdj.entries()) {
      for (const [v, w] of targets.entries()) {
        outWeights.set(u, (outWeights.get(u) || 0) + w);
        inWeights.set(v, (inWeights.get(v) || 0) + w);
        totalGraphWeight += w;
      }
    }

    totalGraphWeight = Math.max(1.0, totalGraphWeight);
    const centralities: Record<string, NodeCentrality> = {};

    for (const a of agentList) {
      const inW = inWeights.get(a) || 0;
      const outW = outWeights.get(a) || 0;
      const cin = inW / totalGraphWeight;
      const cout = outW / totalGraphWeight;
      const deg = inW + outW;
      const bScore = Math.sqrt(cin * cout) * (1.0 + Math.log1p(deg));
      const isBtn = bScore >= this.bottleneckThreshold;

      centralities[a] = {
        agentId: a,
        inDegree: Math.round(cin * 10000) / 10000,
        outDegree: Math.round(cout * 10000) / 10000,
        totalDegree: Math.round(deg * 10000) / 10000,
        bottleneckScore: Math.round(bScore * 10000) / 10000,
        isBottleneck: isBtn,
      };
    }

    return centralities;
  }

  public computeClusters(swarmId: string): TopologicalClusterResult {
    const agentSet = this.agents.get(swarmId);
    if (!agentSet || agentSet.size === 0) {
      const emptyResult: TopologicalClusterResult = {
        swarmId,
        timestamp: new Date().toISOString(),
        totalAgents: 0,
        totalInteractions: 0,
        modularityScore: 0.0,
        clusters: [],
        coalitions: [],
        nodeCentralities: {},
        bottleneckAgents: [],
      };
      this.latestResults.set(swarmId, emptyResult);
      return emptyResult;
    }

    const agents = Array.from(agentSet).sort();
    const swarmAdj = this.adjacency.get(swarmId) || new Map();
    const swarmTokens = this.edgeTokens.get(swarmId) || new Map();
    const numInteractions = this.interactionCounts.get(swarmId) || 0;
    const centralities = this.computeCentrality(swarmId);

    // Symmetrized affinity matrix
    const affinity = new Map<string, Map<string, number>>();
    const degree = new Map<string, number>();
    let totalM = 0;

    for (const u of agents) {
      for (const v of agents) {
        if (u === v) continue;
        const w_uv = swarmAdj.get(u)?.get(v) || 0;
        const w_vu = swarmAdj.get(v)?.get(u) || 0;
        const symW = (w_uv + w_vu) / 2.0;
        if (symW > 0) {
          if (!affinity.has(u)) affinity.set(u, new Map());
          affinity.get(u)!.set(v, symW);
          degree.set(u, (degree.get(u) || 0) + symW);
          totalM += symW;
        }
      }
    }

    totalM = totalM / 2.0;

    // Fast label propagation
    const labels = new Map<string, string>();
    for (const a of agents) labels.set(a, a);

    for (let iter = 0; iter < 5; iter++) {
      let changed = false;
      for (const u of agents) {
        const neighbors = affinity.get(u);
        if (!neighbors) continue;
        const labelWeights = new Map<string, number>();
        for (const [v, w] of neighbors.entries()) {
          const l = labels.get(v)!;
          labelWeights.set(l, (labelWeights.get(l) || 0) + w);
        }

        let bestLabel = labels.get(u)!;
        let maxWeight = -1;
        for (const [lbl, wt] of labelWeights.entries()) {
          if (wt > maxWeight) {
            maxWeight = wt;
            bestLabel = lbl;
          }
        }

        if (bestLabel !== labels.get(u)) {
          labels.set(u, bestLabel);
          changed = true;
        }
      }
      if (!changed) break;
    }

    // Group nodes by label
    const clusterMap = new Map<string, string[]>();
    for (const [a, lbl] of labels.entries()) {
      if (!clusterMap.has(lbl)) clusterMap.set(lbl, []);
      clusterMap.get(lbl)!.push(a);
    }

    const clusters = Array.from(clusterMap.values())
      .filter((nodes) => nodes.length > 0)
      .map((nodes) => nodes.sort());

    // Newman-Girvan Modularity Q
    let modularity = 0;
    if (totalM > 0) {
      for (const cluster of clusters) {
        for (const u of cluster) {
          for (const v of cluster) {
            const a_uv = affinity.get(u)?.get(v) || 0;
            const exp = ((degree.get(u) || 0) * (degree.get(v) || 0)) / (2.0 * totalM);
            modularity += a_uv - exp;
          }
        }
      }
      modularity = modularity / (2.0 * totalM);
      modularity = Math.max(-0.5, Math.min(1.0, modularity));
    }

    // Form Dynamic Agent Coalitions
    const coalitions: AgentCoalition[] = [];
    for (const cluster of clusters) {
      if (cluster.length < 2) continue;

      const k = cluster.length;
      const maxPossibleEdges = k * (k - 1);
      let actualInternalWeight = 0;
      let internalEdgeCount = 0;
      let maxInternalEdge = 1.0;
      let coalitionTokens = 0;

      const nodeInternalDegree = new Map<string, number>();
      for (const u of cluster) {
        for (const v of cluster) {
          if (u !== v) {
            const w = swarmAdj.get(u)?.get(v) || 0;
            if (w > 0) {
              actualInternalWeight += w;
              internalEdgeCount += 1;
              maxInternalEdge = Math.max(maxInternalEdge, w);
              nodeInternalDegree.set(u, (nodeInternalDegree.get(u) || 0) + w);
              nodeInternalDegree.set(v, (nodeInternalDegree.get(v) || 0) + w);
            }
            coalitionTokens += swarmTokens.get(u)?.get(v) || 0;
          }
        }
      }

      const density =
        maxPossibleEdges > 0
          ? actualInternalWeight / (maxPossibleEdges * maxInternalEdge)
          : 1.0;

      // Select Leader
      let leader = cluster[0];
      let bestScore = -1;
      for (const a of cluster) {
        const score = (nodeInternalDegree.get(a) || 0) + (centralities[a]?.totalDegree || 0);
        if (score > bestScore) {
          bestScore = score;
          leader = a;
        }
      }

      // Deadlock check
      let externalEdges = 0;
      for (const u of cluster) {
        const uTargets = swarmAdj.get(u);
        if (uTargets) {
          for (const v of uTargets.keys()) {
            if (!cluster.includes(v)) {
              externalEdges += 1;
            }
          }
        }
      }

      const isDeadlocked =
        internalEdgeCount >= 2 && externalEdges === 0 && density >= 0.70;

      if (density >= this.densityThreshold || cluster.length >= 3) {
        coalitions.push({
          coalitionId: `coalition-${randomUUID().slice(0, 8)}`,
          memberAgents: cluster,
          leaderAgent: leader,
          interactionDensity: Math.round(density * 10000) / 10000,
          isDeadlocked,
          totalTokens: coalitionTokens,
          createdAt: new Date().toISOString(),
        });
      }
    }

    const bottleneckAgents = Object.values(centralities)
      .filter((c) => c.isBottleneck)
      .map((c) => c.agentId);

    const result: TopologicalClusterResult = {
      swarmId,
      timestamp: new Date().toISOString(),
      totalAgents: agents.length,
      totalInteractions: numInteractions,
      modularityScore: Math.round(modularity * 10000) / 10000,
      clusters,
      coalitions,
      nodeCentralities: centralities,
      bottleneckAgents,
    };

    this.latestResults.set(swarmId, result);
    return result;
  }

  public getCoalitions(swarmId: string): AgentCoalition[] {
    const latest = this.latestResults.get(swarmId);
    if (latest) return latest.coalitions;
    return this.computeClusters(swarmId).coalitions;
  }

  public reset(swarmId: string): void {
    this.adjacency.delete(swarmId);
    this.edgeTokens.delete(swarmId);
    this.agents.delete(swarmId);
    this.interactionCounts.delete(swarmId);
    this.latestResults.delete(swarmId);
  }
}

export const globalCoalitionEngine = new TopologicalCoalitionEngine();
