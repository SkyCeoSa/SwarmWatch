import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  MeshNodeStatus,
  MeshConvergenceHealth,
  VectorClock,
  GossipMeshManager,
  globalGossipManager,
} from "../src/gossip.ts";

describe("Seam 45: Decentralized Epidemic Gossip Telemetry Mesh Sync", () => {
  test("VectorClock causality algebra: increment, dominance, concurrency, and merge", () => {
    const c1 = new VectorClock({ "node-A": 1, "node-B": 0 });
    const c2 = new VectorClock({ "node-A": 1, "node-B": 1 });

    // c2 dominates c1
    assert.equal(c2.dominates(c1), true);
    assert.equal(c1.dominates(c2), false);
    assert.equal(c1.isConcurrentWith(c2), false);

    // Concurrency
    const c3 = new VectorClock({ "node-A": 2, "node-B": 0 });
    assert.equal(c3.isConcurrentWith(c2), true);
    assert.equal(c2.isConcurrentWith(c3), true);

    // Merge
    const merged = c2.merge(c3);
    assert.equal(merged.get("node-A"), 2);
    assert.equal(merged.get("node-B"), 1);
    assert.equal(merged.dominates(c2), true);
    assert.equal(merged.dominates(c3), true);

    // Increment
    merged.increment("node-C");
    assert.equal(merged.get("node-C"), 1);
  });

  test("Mesh node registration, listing, and lifecycle transitions", () => {
    const mgr = new GossipMeshManager({ nodeId: "local-node", endpoint: "http://127.0.0.1:8765" });
    assert.equal(mgr.nodeId, "local-node");

    const peer = mgr.registerPeer("remote-node-1", "http://10.0.0.1:8765", { region: "us-west" });
    assert.equal(peer.nodeId, "remote-node-1");
    assert.equal(peer.status, MeshNodeStatus.ACTIVE);
    assert.equal(peer.metadata.region, "us-west");

    const peers = mgr.listPeers();
    assert.equal(peers.length, 1);
    assert.equal(peers[0].nodeId, "remote-node-1");

    mgr.markPeerSuspect("remote-node-1");
    assert.equal(mgr.peers.get("remote-node-1")?.status, MeshNodeStatus.SUSPECT);
    assert.equal(mgr.evaluateConvergence(), MeshConvergenceHealth.SYNCING);

    assert.equal(mgr.deregisterPeer("remote-node-1"), true);
    assert.equal(mgr.listPeers().length, 0);
  });

  test("Local telemetry events advance VectorClock and register records", () => {
    const mgr = new GossipMeshManager({ nodeId: "node-1" });

    const sid = mgr.recordLocalSpan({ span_id: "s-01", tokens: 250 });
    assert.equal(sid, "s-01");
    assert.equal(mgr.vectorClock.get("node-1"), 1);
    assert.equal(mgr.spans.has("s-01"), true);

    const tid = mgr.recordLocalCircuitTrip({ trip_id: "t-01", reason: "budget_overflow" });
    assert.equal(tid, "t-01");
    assert.equal(mgr.vectorClock.get("node-1"), 2);
    assert.equal(mgr.circuitTrips.has("t-01"), true);

    const wid = mgr.recordLocalWatermark({ artifact_id: "w-01", hash: "sig123" });
    assert.equal(wid, "w-01");
    assert.equal(mgr.watermarks.has("w-01"), true);
  });

  test("Pairwise Anti-Entropy digest exchange and delta reconciliation", () => {
    const mgrA = new GossipMeshManager({ nodeId: "node-A" });
    const mgrB = new GossipMeshManager({ nodeId: "node-B" });

    mgrA.registerPeer("node-B", "http://127.0.0.1:8766");
    mgrB.registerPeer("node-A", "http://127.0.0.1:8765");

    mgrA.recordLocalSpan({ span_id: "span-A1", prompt: "Hello" });
    mgrA.recordLocalCircuitTrip({ trip_id: "trip-A1", reason: "loop_detected" });

    const digestB = mgrB.generateDigest();
    assert.equal(digestB.knownSpanIds.length, 0);

    const deltaAToB = mgrA.computeDeltaForDigest(digestB);
    assert.equal(deltaAToB.spans.length, 1);
    assert.equal(deltaAToB.circuitTrips.length, 1);

    const res = mgrB.applyDelta(deltaAToB);
    assert.equal(res.spansAdded, 1);
    assert.equal(res.tripsAdded, 1);
    assert.equal(mgrB.spans.has("span-A1"), true);
    assert.equal(mgrB.circuitTrips.has("trip-A1"), true);
    assert.ok(mgrB.vectorClock.get("node-A") >= 2);
  });

  test("Multi-node epidemic mesh reaches complete eventual consistency", () => {
    const n1 = new GossipMeshManager({ nodeId: "n1", fanout: 2 });
    const n2 = new GossipMeshManager({ nodeId: "n2", fanout: 2 });
    const n3 = new GossipMeshManager({ nodeId: "n3", fanout: 2 });

    const cluster = [n1, n2, n3];
    for (const src of cluster) {
      for (const dst of cluster) {
        if (src.nodeId !== dst.nodeId) {
          src.registerPeer(dst.nodeId, `http://${dst.nodeId}:8765`);
        }
      }
    }

    n1.recordLocalSpan({ span_id: "s1" });
    n1.recordLocalCircuitTrip({ trip_id: "t1" });

    n2.recordLocalSpan({ span_id: "s2" });
    n2.recordLocalWatermark({ artifact_id: "w2" });

    n3.recordLocalSpan({ span_id: "s3" });

    for (let round = 0; round < 2; round++) {
      for (const node of cluster) {
        node.runGossipRound(cluster);
      }
    }

    for (const node of cluster) {
      assert.equal(node.spans.size, 3, `Node ${node.nodeId} must have all 3 spans`);
      assert.equal(node.circuitTrips.size, 1, `Node ${node.nodeId} must have the circuit trip`);
      assert.equal(node.watermarks.size, 1, `Node ${node.nodeId} must have the watermark`);
      assert.equal(node.evaluateConvergence(), MeshConvergenceHealth.CONVERGED);
    }
  });

  test("Zero-token and microsecond performance benchmark", () => {
    const mgr = new GossipMeshManager({ nodeId: "perf-node" });
    for (let i = 0; i < 10; i++) {
      mgr.registerPeer(`peer-${i}`, `http://peer-${i}:8765`);
      mgr.recordLocalSpan({ span_id: `s-${i}` });
    }

    const t0 = performance.now();
    const otherClock = new VectorClock(Object.fromEntries(Array.from({ length: 10 }, (_, i) => [`peer-${i}`, i])));
    for (let i = 0; i < 2000; i++) {
      mgr.vectorClock = mgr.vectorClock.merge(otherClock);
      mgr.generateDigest();
    }
    const elapsed = performance.now() - t0;
    assert.ok(elapsed < 100, `Execution took ${elapsed.toFixed(2)}ms, expected < 100ms`);
  });
});
