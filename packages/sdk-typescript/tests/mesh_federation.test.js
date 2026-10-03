/**
 * Unit and integration test suite for Dynamic Agent Federation & Swarm Mesh Topology Routing (Seam 72 / ADR 0083).
 * Plain ES module for Node.js native execution with --experimental-strip-types.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  MeshFederationError,
  MeshPeerAuthenticationError,
  MeshRoutingLoopError,
  MeshHopLimitExceededError,
  MeshDestinationUnreachableError,
  MeshCapabilityFencingError,
  MeshCircuitTrippedError,
  MeshNodeStatus,
  MeshNodeRole,
  MeshRoutingStatus,
  PeerNode,
  MeshLink,
  MeshPacket,
  MeshRoutingReceipt,
  MeshFederationRouter,
  globalMeshRouter,
  generatePeerKeypair,
  signPeerData,
} from "../src/mesh_federation.ts";

test("Peer registration and signature verification", () => {
  const router = new MeshFederationRouter();
  const { keyManager: km1, publicKeyHex: pubHex } = generatePeerKeypair();

  // 1. Registration without signature succeeds
  const peer1 = router.registerPeer({
    node_id: "node-01",
    cluster_id: "us-east-1",
    tenant_id: "tenant-alpha",
    endpoint_url: "https://node01.east.internal",
    public_key_hex: pubHex,
    role: MeshNodeRole.GATEWAY,
  });
  assert.equal(peer1.node_id, "node-01");
  assert.equal(peer1.status, MeshNodeStatus.ONLINE);

  // 2. Registration with valid signature succeeds
  const { keyManager: km2, publicKeyHex: pub2Hex } = generatePeerKeypair();
  const data = Buffer.from("node-02:us-west-2:tenant-alpha:https://node02.west.internal", "utf-8");
  const sig = signPeerData(km2, data);

  const peer2 = router.registerPeer({
    node_id: "node-02",
    cluster_id: "us-west-2",
    tenant_id: "tenant-alpha",
    endpoint_url: "https://node02.west.internal",
    public_key_hex: pub2Hex,
    signature: sig,
  });
  assert.equal(peer2.node_id, "node-02");

  // 3. Registration with invalid signature throws MeshPeerAuthenticationError
  assert.throws(
    () => {
      router.registerPeer({
        node_id: "node-fake",
        cluster_id: "us-east-1",
        tenant_id: "tenant-alpha",
        endpoint_url: "https://fake.internal",
        public_key_hex: pubHex,
        signature: "deadbeef".repeat(8),
      });
    },
    (err) => err instanceof MeshPeerAuthenticationError
  );
});

test("Link establishment and topology graph", () => {
  const router = new MeshFederationRouter();
  router.registerPeer({ node_id: "n1", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://n1", public_key_hex: "pub1" });
  router.registerPeer({ node_id: "n2", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://n2", public_key_hex: "pub2" });
  router.registerPeer({ node_id: "n3", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://n3", public_key_hex: "pub3" });

  // Establish links
  assert.equal(router.establishLink("n1", "n2", 1.5, 1.0, true), true);
  assert.equal(router.establishLink("n2", "n3", 2.0, 1.0, false), true);

  const links = router.listLinks();
  assert.equal(links.length, 3); // n1->n2, n2->n1, n2->n3

  // Link to unknown node throws MeshDestinationUnreachableError
  assert.throws(
    () => {
      router.establishLink("n1", "unknown-node");
    },
    (err) => err instanceof MeshDestinationUnreachableError
  );
});

test("Dijkstra shortest path routing", () => {
  const router = new MeshFederationRouter();
  // Build diamond graph: A -> B (1) -> D (1); A -> C (5) -> D (1)
  router.registerPeer({ node_id: "A", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://a", public_key_hex: "pub_a" });
  router.registerPeer({ node_id: "B", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://b", public_key_hex: "pub_b" });
  router.registerPeer({ node_id: "C", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://c", public_key_hex: "pub_c" });
  router.registerPeer({ node_id: "D", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://d", public_key_hex: "pub_d" });

  router.establishLink("A", "B", 1.0);
  router.establishLink("B", "D", 1.0);
  router.establishLink("A", "C", 5.0);
  router.establishLink("C", "D", 1.0);

  // Shortest path from A to D must go through B (total weight 2.0 vs 6.0)
  const path = router.findOptimalPath("A", "D");
  assert.deepEqual(path, ["A", "B", "D"]);

  // Deactivate B -> D link, path dynamically reroutes through C
  router.setLinkStatus("B", "D", false);
  const newPath = router.findOptimalPath("A", "D");
  assert.deepEqual(newPath, ["A", "C", "D"]);
});

test("Stale peer heartbeat and eviction", () => {
  const router = new MeshFederationRouter(5.0);
  router.registerPeer({ node_id: "p1", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://p1", public_key_hex: "pub1" });
  router.registerPeer({ node_id: "p2", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://p2", public_key_hex: "pub2" });
  router.establishLink("p1", "p2");

  const tNow = Date.now() / 1000;
  // At tNow, no eviction
  const evicted0 = router.evictStalePeers(tNow);
  assert.equal(evicted0.length, 0);

  // At tNow + 10s, both are stale
  const evicted1 = router.evictStalePeers(tNow + 10.0);
  assert.equal(evicted1.length, 2);
  assert.equal(router.getPeer("p1")?.status, MeshNodeStatus.EVICTED);

  // Routing to evicted peer throws MeshDestinationUnreachableError
  assert.throws(
    () => {
      router.findOptimalPath("p1", "p2");
    },
    (err) => err instanceof MeshDestinationUnreachableError
  );

  // Heartbeat revives p1
  router.heartbeat("p1");
  assert.equal(router.getPeer("p1")?.status, MeshNodeStatus.ONLINE);
});

test("Mutual cryptographic handshake", () => {
  const router = new MeshFederationRouter();
  const { keyManager: kmA, publicKeyHex: pubA } = generatePeerKeypair();
  const { keyManager: kmB, publicKeyHex: pubB } = generatePeerKeypair();

  router.registerPeer({ node_id: "node-a", cluster_id: "cluster-east", tenant_id: "tenant-1", endpoint_url: "http://a", public_key_hex: pubA });
  router.registerPeer({ node_id: "node-b", cluster_id: "cluster-west", tenant_id: "tenant-1", endpoint_url: "http://b", public_key_hex: pubB });

  const { request: req, response: res } = router.performHandshake(
    "node-a",
    "node-b",
    kmA,
    kmB
  );

  assert.equal(req.source_node_id, "node-a");
  assert.equal(req.target_node_id, "node-b");
  assert.equal(res.status, "ACCEPTED");
  assert.ok(res.session_id.startsWith("mesh-sess-"));
  assert.equal(router.status()["active_sessions_count"], 1);
});

test("Packet routing and receipt generation", () => {
  const router = new MeshFederationRouter();
  router.registerPeer({ node_id: "node-src", cluster_id: "c1", tenant_id: "tenant-alpha", endpoint_url: "http://src", public_key_hex: "pub_src" });
  router.registerPeer({ node_id: "node-relay", cluster_id: "c1", tenant_id: "tenant-alpha", endpoint_url: "http://relay", public_key_hex: "pub_relay" });
  router.registerPeer({ node_id: "node-dst", cluster_id: "c1", tenant_id: "tenant-alpha", endpoint_url: "http://dst", public_key_hex: "pub_dst" });

  router.establishLink("node-src", "node-relay");
  router.establishLink("node-relay", "node-dst");

  const packet = new MeshPacket({
    packet_id: "pkt-100",
    source_node_id: "node-src",
    dest_node_id: "node-dst",
    tenant_id: "tenant-alpha",
    payload: { task: "federated_eval", tokens: 42 },
  });

  const receipt = router.routePacket(packet);
  assert.equal(receipt.status, MeshRoutingStatus.ROUTED_RELAY);
  assert.deepEqual(receipt.path_traversed, ["node-src", "node-relay", "node-dst"]);
  assert.equal(receipt.total_hops, 2);
  assert.ok(receipt.receipt_hash.startsWith("sw:mesh:v1:"));
  assert.ok(receipt.duration_us > 0);

  // Look up receipt
  const found = router.getReceipt(receipt.receipt_id);
  assert.ok(found !== undefined);
  assert.equal(found?.packet_id, "pkt-100");
});

test("Hop limit (TTL) exceeded", () => {
  const router = new MeshFederationRouter();
  router.registerPeer({ node_id: "src", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://s", public_key_hex: "p1" });
  router.registerPeer({ node_id: "dst", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://d", public_key_hex: "p2" });
  router.establishLink("src", "dst");

  const packet = new MeshPacket({
    packet_id: "pkt-ttl",
    source_node_id: "src",
    dest_node_id: "dst",
    tenant_id: "t1",
    payload: {},
    hop_count: 16,
    max_hops: 16, // Already at limit
  });

  assert.throws(
    () => {
      router.routePacket(packet);
    },
    (err) => err instanceof MeshHopLimitExceededError
  );

  const receipts = router.getReceipts("t1");
  assert.equal(receipts.length, 1);
  assert.equal(receipts[0].status, MeshRoutingStatus.DROPPED_TTL);
});

test("Loop detection and prevention", () => {
  const router = new MeshFederationRouter();
  router.registerPeer({ node_id: "node-x", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://x", public_key_hex: "px" });
  router.registerPeer({ node_id: "node-y", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://y", public_key_hex: "py" });
  router.establishLink("node-x", "node-y");

  // Packet claims node-x is already in path (loop)
  const packet = new MeshPacket({
    packet_id: "pkt-loop",
    source_node_id: "node-x",
    dest_node_id: "node-y",
    tenant_id: "t1",
    payload: {},
    path_traversed: ["node-x", "node-z"],
  });

  assert.throws(
    () => {
      router.routePacket(packet);
    },
    (err) => err instanceof MeshRoutingLoopError
  );
});

test("Cross-tenant capability fencing", () => {
  const router = new MeshFederationRouter();
  router.registerPeer({ node_id: "node-t1", cluster_id: "c1", tenant_id: "tenant-corp", endpoint_url: "http://t1", public_key_hex: "p1" });
  router.registerPeer({ node_id: "node-t2", cluster_id: "c1", tenant_id: "tenant-finance", endpoint_url: "http://t2", public_key_hex: "p2" });
  router.establishLink("node-t1", "node-t2");

  // 1. Cross-tenant packet without capability token fails
  const packetUnauth = new MeshPacket({
    packet_id: "pkt-cross-1",
    source_node_id: "node-t1",
    dest_node_id: "node-t2",
    tenant_id: "tenant-corp",
    payload: {},
  });
  assert.throws(
    () => {
      router.routePacket(packetUnauth);
    },
    (err) => err instanceof MeshCapabilityFencingError
  );

  // 2. Cross-tenant packet with valid token succeeds
  const packetAuth = new MeshPacket({
    packet_id: "pkt-cross-2",
    source_node_id: "node-t1",
    dest_node_id: "node-t2",
    tenant_id: "tenant-corp",
    payload: { msg: "authorized_cross_audit" },
    capability_token: "valid-cross-tenant-token-alpha",
  });
  const receipt = router.routePacket(packetAuth);
  assert.equal(receipt.status, MeshRoutingStatus.DELIVERED);
});

test("Safety circuit tripped blocks mesh fail-closed", () => {
  const router = new MeshFederationRouter();
  router.registerPeer({ node_id: "s1", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://s1", public_key_hex: "p1" });
  router.registerPeer({ node_id: "s2", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://s2", public_key_hex: "p2" });
  router.establishLink("s1", "s2");

  router.setCircuitTripped(true);

  const packet = new MeshPacket({
    packet_id: "pkt-blocked",
    source_node_id: "s1",
    dest_node_id: "s2",
    tenant_id: "t1",
    payload: {},
  });

  assert.throws(
    () => {
      router.routePacket(packet);
    },
    (err) => err instanceof MeshCircuitTrippedError
  );

  const receipts = router.getReceipts("t1");
  assert.equal(receipts[receipts.length - 1].status, MeshRoutingStatus.BLOCKED_CIRCUIT);
});

test("Tamper-evident receipt canonical hash", () => {
  const router = new MeshFederationRouter();
  router.registerPeer({ node_id: "n-a", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://a", public_key_hex: "pa" });
  router.registerPeer({ node_id: "n-b", cluster_id: "c1", tenant_id: "t1", endpoint_url: "http://b", public_key_hex: "pb" });
  router.establishLink("n-a", "n-b");

  const pkt = new MeshPacket({
    packet_id: "pkt-hash",
    source_node_id: "n-a",
    dest_node_id: "n-b",
    tenant_id: "t1",
    payload: { v: 1 },
  });
  const receipt = router.routePacket(pkt);

  assert.ok(receipt.receipt_hash.startsWith("sw:mesh:v1:"));
  const d = receipt.toDict();
  const reconstructed = MeshRoutingReceipt.fromDict(d);
  assert.equal(reconstructed.receipt_hash, receipt.receipt_hash);
  assert.equal(reconstructed.duration_us, receipt.duration_us);
});

test("Sub-250us mesh routing SLO", () => {
  const router = new MeshFederationRouter();
  // Build 5-node chain
  for (let i = 0; i < 5; i++) {
    router.registerPeer({
      node_id: `bench-node-${i}`,
      cluster_id: "c1",
      tenant_id: "t-bench",
      endpoint_url: `http://b${i}`,
      public_key_hex: `pub${i}`,
    });
    if (i > 0) {
      router.establishLink(`bench-node-${i - 1}`, `bench-node-${i}`);
    }
  }

  // Warmup
  for (let i = 0; i < 10; i++) {
    const pkt = new MeshPacket({
      packet_id: "pkt-warm",
      source_node_id: "bench-node-0",
      dest_node_id: "bench-node-4",
      tenant_id: "t-bench",
      payload: {},
    });
    router.routePacket(pkt);
  }

  // Benchmark: 500 complete packet route calculations across 4 hops
  const iterations = 500;
  const t0 = performance.now();
  for (let j = 0; j < iterations; j++) {
    const pkt = new MeshPacket({
      packet_id: `pkt-bench-${j}`,
      source_node_id: "bench-node-0",
      dest_node_id: "bench-node-4",
      tenant_id: "t-bench",
      payload: {},
    });
    router.routePacket(pkt);
  }
  const t1 = performance.now();

  const avgUs = ((t1 - t0) / iterations) * 1000;
  assert.ok(avgUs < 250.0, `Mesh routing roundtrip took ${avgUs.toFixed(2)}µs, exceeding 250µs SLO`);
});

test("Global mesh router singleton status and reset", () => {
  const st = globalMeshRouter.status();
  assert.ok("online_peers_count" in st);
  assert.ok("active_links_count" in st);

  globalMeshRouter.registerPeer({
    node_id: "g-peer",
    cluster_id: "g-cluster",
    tenant_id: "g-tenant",
    endpoint_url: "http://g",
    public_key_hex: "pub_g",
  });
  assert.ok(Number(globalMeshRouter.status()["total_peers_count"]) >= 1);

  globalMeshRouter.reset();
  assert.equal(globalMeshRouter.status()["total_peers_count"], 0);
  assert.equal(globalMeshRouter.status()["circuit_tripped"], false);
});
