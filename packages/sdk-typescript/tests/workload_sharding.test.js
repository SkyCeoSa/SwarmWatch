import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  NodeHealthTier,
  computeCapacityScore,
  getHealthTier,
  hashToken32,
  ConsistentHashRing,
  ComputeAwareBalancer,
  globalComputeBalancer,
} from "../src/sharding.ts";

describe("Seam 46: Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer", () => {
  test("computeCapacityScore formulas and health tier mapping", () => {
    // Idle node -> 1.0 (Optimal)
    const idleScore = computeCapacityScore({ cpuUtilization: 0, memoryUtilization: 0, queueDepth: 0 });
    assert.equal(idleScore, 1.0);
    assert.equal(getHealthTier(idleScore), NodeHealthTier.OPTIMAL);

    // Overloaded node -> 0.0 (Overloaded)
    const fullScore = computeCapacityScore({
      cpuUtilization: 1.0,
      memoryUtilization: 1.0,
      queueDepth: 1000,
      maxQueueDepth: 1000,
    });
    assert.equal(fullScore, 0.0);
    assert.equal(getHealthTier(fullScore), NodeHealthTier.OVERLOADED);

    // Mid load
    const midScore = computeCapacityScore({
      cpuUtilization: 0.4,
      memoryUtilization: 0.5,
      queueDepth: 200,
      maxQueueDepth: 1000,
    });
    assert.ok(midScore >= 0.60 && midScore <= 0.63);
    assert.equal(getHealthTier(midScore), NodeHealthTier.NOMINAL);

    // Congested
    assert.equal(getHealthTier(0.25), NodeHealthTier.CONGESTED);
  });

  test("ConsistentHashRing deterministic 32-bit token generation and dynamic vnodes", () => {
    const ring = new ConsistentHashRing(64);

    // Deterministic 32-bit integer token
    const tok1 = hashToken32("node-1#vnode0");
    const tok2 = hashToken32("node-1#vnode0");
    assert.equal(tok1, tok2);
    assert.ok(tok1 >= 0 && tok1 <= 0xffffffff);

    // Node with capacity 1.0 receives base vnodes (64)
    const v1 = ring.addNode("node-1", 1.0);
    assert.equal(v1, 64);
    assert.equal(ring.totalTokens, 64);
    assert.equal(ring.nodeCount, 1);

    // Node with capacity 0.5 receives 32 vnodes
    const v2 = ring.addNode("node-2", 0.5);
    assert.equal(v2, 32);
    assert.equal(ring.totalTokens, 96);
    assert.equal(ring.nodeCount, 2);

    // Remove node-1
    ring.removeNode("node-1");
    assert.equal(ring.totalTokens, 32);
    assert.equal(ring.nodeCount, 1);
  });

  test("ConsistentHashRing lookup and circular wrap", () => {
    const ring = new ConsistentHashRing(32);
    ring.addNode("node-alpha", 1.0);
    ring.addNode("node-beta", 1.0);

    const [p1, s1] = ring.lookup("swarm-session-12345");
    assert.ok(p1 === "node-alpha" || p1 === "node-beta");
    assert.ok(s1 === "node-alpha" || s1 === "node-beta");
    assert.notEqual(p1, s1);

    // Consistent hashing consistency: same key hits same primary
    const [p2, s2] = ring.lookup("swarm-session-12345");
    assert.equal(p1, p2);
    assert.equal(s1, s2);
  });

  test("ComputeAwareBalancer routing, causal session affinity, and nanosecond latency", () => {
    const balancer = new ComputeAwareBalancer(32);
    balancer.registerNode("node-a", "http://10.0.0.1:8765", { cpuUtilization: 0.2, memoryUtilization: 0.3, queueDepth: 10 });
    balancer.registerNode("node-b", "http://10.0.0.2:8765", { cpuUtilization: 0.25, memoryUtilization: 0.35, queueDepth: 20 });
    balancer.registerNode("node-c", "http://10.0.0.3:8765", { cpuUtilization: 0.1, memoryUtilization: 0.2, queueDepth: 5 });

    const key = { tenantId: "tenant-acme", projectId: "proj-finance", swarmId: "swarm-007" };
    const res1 = balancer.route(key);

    assert.equal(res1.routeMode, "primary");
    assert.ok(["node-a", "node-b", "node-c"].includes(res1.primaryNodeId));
    assert.ok(res1.endpoint.startsWith("http://"));
    assert.ok(res1.lookupLatencyNs > 0);

    // Session affinity
    const res2 = balancer.route(key);
    assert.equal(res2.primaryNodeId, res1.primaryNodeId);
    assert.equal(res2.endpoint, res1.endpoint);
  });

  test("ComputeAwareBalancer overload spillover routing and recovery", () => {
    const balancer = new ComputeAwareBalancer(32, 0.15);
    balancer.registerNode("node-1", "http://10.0.0.1:8765", { cpuUtilization: 0.1, memoryUtilization: 0.1, queueDepth: 10 });
    balancer.registerNode("node-2", "http://10.0.0.2:8765", { cpuUtilization: 0.1, memoryUtilization: 0.1, queueDepth: 10 });

    const key = "swarm-heavy-task-99";
    const initialRes = balancer.route(key);
    assert.equal(initialRes.routeMode, "primary");
    const primaryId = initialRes.primaryNodeId;

    // Overload primary node (score < 0.15)
    balancer.updateNodeMetrics(primaryId, {
      cpuUtilization: 0.95,
      memoryUtilization: 0.95,
      queueDepth: 980,
      maxQueueDepth: 1000,
    });

    const spilloverRes = balancer.route(key);
    assert.equal(spilloverRes.routeMode, "spillover");
    assert.notEqual(spilloverRes.primaryNodeId, primaryId);
    assert.equal(spilloverRes.spilloverNodeId, primaryId);

    // Recover primary node
    balancer.updateNodeMetrics(primaryId, {
      cpuUtilization: 0.1,
      memoryUtilization: 0.1,
      queueDepth: 10,
      maxQueueDepth: 1000,
    });

    const restoredRes = balancer.route(key);
    assert.equal(restoredRes.routeMode, "primary");
    assert.equal(restoredRes.primaryNodeId, primaryId);
  });

  test("globalComputeBalancer default initialization and status reporting", () => {
    const status = globalComputeBalancer.getStatus();
    assert.ok(status.totalPhysicalNodes >= 1);
    assert.ok(status.totalVirtualNodes >= 1);
    assert.ok(status.nodes["node-alpha"]);

    const tokens = globalComputeBalancer.getRingTokens();
    assert.ok(tokens.length >= 1);

    const rebalanceReport = globalComputeBalancer.rebalance();
    assert.ok(rebalanceReport.rebalanceCount >= 1);
  });
});
