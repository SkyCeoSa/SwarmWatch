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
  signPeerRegistration,
  computePeerRegistrationPayload,
} from "../src/mesh_federation.ts";

function makeRegData(
  node_id,
  cluster_id = "c1",
  tenant_id = "t1",
  endpoint_url = "http://node",
  role = MeshNodeRole.WORKER,
  customKm = null,
  customPub = null
) {
  let km = customKm;
  let pub = customPub;
  if (!km || !pub) {
    const kp = generatePeerKeypair();
    km = kp.keyManager;
    pub = kp.publicKeyHex;
  }
  const sig = signPeerRegistration(km, node_id, cluster_id, tenant_id, endpoint_url, pub, role);
  return {
    node_id,
    cluster_id,
    tenant_id,
    endpoint_url,
    public_key_hex: pub,
    role,
    signature: sig,
  };
}

test("Peer registration and signature verification", () => {
  const router = new MeshFederationRouter();
  const { keyManager: km1, publicKeyHex: pubHex } = generatePeerKeypair();

  // 1. Registration without signature is rejected (PoC-H)
  assert.throws(
    () => {
      router.registerPeer({
        node_id: "node-01",
        cluster_id: "us-east-1",
        tenant_id: "tenant-alpha",
        endpoint_url: "https://node01.east.internal",
        public_key_hex: pubHex,
        role: MeshNodeRole.GATEWAY,
      });
    },
    (err) => err instanceof MeshPeerAuthenticationError && /requires valid cryptographic signature/.test(err.message)
  );

  // 1b. Registration with malformed key is rejected (PoC-H squatting)
  assert.throws(
    () => {
      router.registerPeer({
        node_id: "node-victim",
        cluster_id: "us-east-1",
        tenant_id: "tenant-alpha",
        endpoint_url: "https://evil.internal",
        public_key_hex: "zz-not-hex",
        signature: "any-sig",
      });
    },
    (err) => err instanceof MeshPeerAuthenticationError && /Malformed public key/.test(err.message)
  );

  // 2. Registration with valid signature succeeds
  const sig1 = signPeerRegistration(
    km1,
    "node-01",
    "us-east-1",
    "tenant-alpha",
    "https://node01.east.internal",
    pubHex,
    MeshNodeRole.GATEWAY
  );
  const peer1 = router.registerPeer({
    node_id: "node-01",
    cluster_id: "us-east-1",
    tenant_id: "tenant-alpha",
    endpoint_url: "https://node01.east.internal",
    public_key_hex: pubHex,
    role: MeshNodeRole.GATEWAY,
    signature: sig1,
  });
  assert.equal(peer1.node_id, "node-01");
  assert.equal(peer1.status, MeshNodeStatus.ONLINE);

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
    (err) => err instanceof MeshPeerAuthenticationError && /Cryptographic signature verification failed/.test(err.message)
  );

  // 4. Attempting to overwrite existing node without signature throws MeshPeerAuthenticationError
  assert.throws(
    () => {
      router.registerPeer({
        node_id: "node-01",
        cluster_id: "us-east-1",
        tenant_id: "tenant-alpha",
        endpoint_url: "https://hijack.evil.internal",
        public_key_hex: pubHex,
      });
    },
    (err) => err instanceof MeshPeerAuthenticationError && /Cannot overwrite existing peer node/.test(err.message)
  );

  // 5. Attempting to overwrite existing node with Mallory's key throws MeshPeerAuthenticationError (PoC-G)
  const { keyManager: kmMallory, publicKeyHex: pubMallory } = generatePeerKeypair();
  const mallorySig = signPeerRegistration(
    kmMallory,
    "node-01",
    "us-east-1",
    "tenant-alpha",
    "https://hijack.evil.internal",
    pubMallory,
    MeshNodeRole.GATEWAY
  );
  assert.throws(
    () => {
      router.registerPeer({
        node_id: "node-01",
        cluster_id: "us-east-1",
        tenant_id: "tenant-alpha",
        endpoint_url: "https://hijack.evil.internal",
        public_key_hex: pubMallory,
        signature: mallorySig,
      });
    },
    (err) => err instanceof MeshPeerAuthenticationError && /Node hijacking attempt detected/.test(err.message)
  );

  // 5b. Signature replay attack with attacker key is rejected (PoC-G replay)
  assert.throws(
    () => {
      router.registerPeer({
        node_id: "node-01",
        cluster_id: "us-east-1",
        tenant_id: "tenant-alpha",
        endpoint_url: "https://hijack.evil.internal",
        public_key_hex: pubMallory,
        signature: sig1,
      });
    },
    (err) => err instanceof MeshPeerAuthenticationError && /Node hijacking attempt detected/.test(err.message)
  );

  // 6. Legitimate owner re-registration signed by km1 succeeds
  const validUpdateSig = signPeerRegistration(
    km1,
    "node-01",
    "us-east-1",
    "tenant-alpha",
    "https://node01-updated.east.internal",
    pubHex,
    MeshNodeRole.GATEWAY
  );
  const updatedPeer = router.registerPeer({
    node_id: "node-01",
    cluster_id: "us-east-1",
    tenant_id: "tenant-alpha",
    endpoint_url: "https://node01-updated.east.internal",
    public_key_hex: pubHex,
    role: MeshNodeRole.GATEWAY,
    signature: validUpdateSig,
  });
  assert.equal(updatedPeer.endpoint_url, "https://node01-updated.east.internal");
});

test("Link establishment and topology graph", () => {
  const router = new MeshFederationRouter();
  router.registerPeer(makeRegData("n1", "c1", "t1", "http://n1"));
  router.registerPeer(makeRegData("n2", "c1", "t1", "http://n2"));
  router.registerPeer(makeRegData("n3", "c1", "t1", "http://n3"));

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
  router.registerPeer(makeRegData("A", "c1", "t1", "http://a"));
  router.registerPeer(makeRegData("B", "c1", "t1", "http://b"));
  router.registerPeer(makeRegData("C", "c1", "t1", "http://c"));
  router.registerPeer(makeRegData("D", "c1", "t1", "http://d"));

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
  router.registerPeer(makeRegData("p1", "c1", "t1", "http://p1"));
  router.registerPeer(makeRegData("p2", "c1", "t1", "http://p2"));
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

  router.registerPeer(makeRegData("node-a", "cluster-east", "tenant-1", "http://a", MeshNodeRole.WORKER, kmA, pubA));
  router.registerPeer(makeRegData("node-b", "cluster-west", "tenant-1", "http://b", MeshNodeRole.WORKER, kmB, pubB));

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
  router.registerPeer(makeRegData("node-src", "c1", "tenant-alpha", "http://src"));
  router.registerPeer(makeRegData("node-relay", "c1", "tenant-alpha", "http://relay"));
  router.registerPeer(makeRegData("node-dst", "c1", "tenant-alpha", "http://dst"));

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
  router.registerPeer(makeRegData("src", "c1", "t1", "http://s"));
  router.registerPeer(makeRegData("dst", "c1", "t1", "http://d"));
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
  router.registerPeer(makeRegData("node-x", "c1", "t1", "http://x"));
  router.registerPeer(makeRegData("node-y", "c1", "t1", "http://y"));
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
  router.registerPeer(makeRegData("node-t1", "c1", "tenant-corp", "http://t1"));
  router.registerPeer(makeRegData("node-t2", "c1", "tenant-finance", "http://t2"));
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
  router.registerPeer(makeRegData("s1", "c1", "t1", "http://s1"));
  router.registerPeer(makeRegData("s2", "c1", "t1", "http://s2"));
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
  router.registerPeer(makeRegData("n-a", "c1", "t1", "http://a"));
  router.registerPeer(makeRegData("n-b", "c1", "t1", "http://b"));
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
    router.registerPeer(makeRegData(`bench-node-${i}`, "c1", "t-bench", `http://b${i}`));
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

  globalMeshRouter.registerPeer(makeRegData("g-peer", "g-cluster", "g-tenant", "http://g"));
  assert.ok(Number(globalMeshRouter.status()["total_peers_count"]) >= 1);

  globalMeshRouter.reset();
  assert.equal(globalMeshRouter.status()["total_peers_count"], 0);
  assert.equal(globalMeshRouter.status()["circuit_tripped"], false);
});
