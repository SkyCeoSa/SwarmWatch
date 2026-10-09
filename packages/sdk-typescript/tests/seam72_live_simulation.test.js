/**
 * Automated Test Harness for Seam 72 Live Mesh Federation Simulation (Node.js Native ESM).
 * Validates dual-SDK parity for all 8 operational phases.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  MeshFederationRouter,
  MeshNodeRole,
  MeshNodeStatus,
  MeshRoutingStatus,
  PeerNode,
  MeshLink,
  MeshPacket,
  MeshRoutingReceipt,
  generatePeerKeypair,
  signPeerData,
  signPeerRegistration,
  MeshPeerAuthenticationError,
  MeshRoutingLoopError,
  MeshHopLimitExceededError,
  MeshDestinationUnreachableError,
  MeshCapabilityFencingError,
  MeshCircuitTrippedError,
} from "../src/mesh_federation.ts";

test("Seam 72 Live Mesh Federation Full Lifecycle Simulation", () => {
  const router = new MeshFederationRouter(30.0);
  const keypairs = new Map();

  // Phase 1: Peer Provisioning
  const nodes = [
    { id: "gw-us-east-1", cluster: "us-east", tenant: "tenant-quantum", url: "https://east.quantum.internal", role: MeshNodeRole.GATEWAY },
    { id: "router-us-central", cluster: "us-central", tenant: "tenant-quantum", url: "https://central.quantum.internal", role: MeshNodeRole.ROUTER },
    { id: "worker-us-west-2", cluster: "us-west", tenant: "tenant-quantum", url: "https://west.quantum.internal", role: MeshNodeRole.WORKER },
    { id: "edge-eu-west-1", cluster: "eu-west", tenant: "tenant-quantum", url: "https://eu.quantum.internal", role: MeshNodeRole.EDGE },
    { id: "gw-foreign-corp", cluster: "ap-south", tenant: "tenant-external", url: "https://corp.external.internal", role: MeshNodeRole.GATEWAY },
  ];

  for (const n of nodes) {
    const { keyManager: km, publicKeyHex: pub } = generatePeerKeypair();
    keypairs.set(n.id, { km, pub });
    const sig = signPeerRegistration(km, n.id, n.cluster, n.tenant, n.url, pub, n.role);
    const peer = router.registerPeer({
      node_id: n.id,
      cluster_id: n.cluster,
      tenant_id: n.tenant,
      endpoint_url: n.url,
      public_key_hex: pub,
      role: n.role,
      signature: sig,
    });
    assert.equal(peer.status, MeshNodeStatus.ONLINE);
  }
  assert.equal(router.listPeers().length, 5);

  // Phase 2: Mutual Zero-Trust Handshakes
  const kmEast = keypairs.get("gw-us-east-1").km;
  const kmCentral = keypairs.get("router-us-central").km;

  const { request: req, response: res } = router.performHandshake(
    "gw-us-east-1",
    "router-us-central",
    kmEast,
    kmCentral
  );
  assert.equal(res.status, "ACCEPTED");
  assert.ok(res.session_id.startsWith("mesh-sess-"));

  // Phase 3: Weighted Link Topology
  router.establishLink("gw-us-east-1", "router-us-central", 1.2, 15.0, true);
  router.establishLink("router-us-central", "worker-us-west-2", 2.0, 25.0, true);
  router.establishLink("gw-us-east-1", "worker-us-west-2", 6.5, 70.0, true);
  router.establishLink("router-us-central", "edge-eu-west-1", 4.0, 55.0, true);
  router.establishLink("gw-us-east-1", "gw-foreign-corp", 10.0, 120.0, true);

  assert.equal(router.listLinks().length, 10);

  // Phase 4: Dijkstra & Dynamic Rerouting
  const path1 = router.findOptimalPath("gw-us-east-1", "worker-us-west-2");
  assert.deepEqual(path1, ["gw-us-east-1", "router-us-central", "worker-us-west-2"]);

  // Sever Central <-> West
  router.setLinkStatus("router-us-central", "worker-us-west-2", false);
  router.setLinkStatus("worker-us-west-2", "router-us-central", false);

  const path2 = router.findOptimalPath("gw-us-east-1", "worker-us-west-2");
  assert.deepEqual(path2, ["gw-us-east-1", "worker-us-west-2"]);

  // Restore
  router.setLinkStatus("router-us-central", "worker-us-west-2", true);
  router.setLinkStatus("worker-us-west-2", "router-us-central", true);

  // Phase 5: TTL & Loop Prevention
  const pktTtl = new MeshPacket({
    packet_id: "pkt-sim-ttl",
    source_node_id: "gw-us-east-1",
    dest_node_id: "worker-us-west-2",
    tenant_id: "tenant-quantum",
    hop_count: 16,
    max_hops: 16,
  });
  assert.throws(() => router.routePacket(pktTtl), (err) => err instanceof MeshHopLimitExceededError);

  const pktLoop = new MeshPacket({
    packet_id: "pkt-sim-loop",
    source_node_id: "gw-us-east-1",
    dest_node_id: "worker-us-west-2",
    tenant_id: "tenant-quantum",
    path_traversed: ["gw-us-east-1", "router-us-central"],
  });
  assert.throws(() => router.routePacket(pktLoop), (err) => err instanceof MeshRoutingLoopError);

  // Phase 6: Cross-Tenant Fencing
  const pktCrossUnauth = new MeshPacket({
    packet_id: "pkt-cross-unauth",
    source_node_id: "gw-us-east-1",
    dest_node_id: "gw-foreign-corp",
    tenant_id: "tenant-quantum",
  });
  assert.throws(() => router.routePacket(pktCrossUnauth), (err) => err instanceof MeshCapabilityFencingError);

  const pktCrossAuth = new MeshPacket({
    packet_id: "pkt-cross-auth",
    source_node_id: "gw-us-east-1",
    dest_node_id: "gw-foreign-corp",
    tenant_id: "tenant-quantum",
    capability_token: "valid-cross-tenant-token-audit-01",
  });
  const authReceipt = router.routePacket(pktCrossAuth);
  assert.equal(authReceipt.status, MeshRoutingStatus.DELIVERED);

  // Phase 7: Fail-Closed Safety Circuit
  router.setCircuitTripped(true);
  const pktTripped = new MeshPacket({
    packet_id: "pkt-tripped",
    source_node_id: "gw-us-east-1",
    dest_node_id: "worker-us-west-2",
    tenant_id: "tenant-quantum",
  });
  assert.throws(() => router.routePacket(pktTripped), (err) => err instanceof MeshCircuitTrippedError);
  router.setCircuitTripped(false);

  // Phase 8: 1,000 Packets Benchmark
  const iterations = 1000;
  const t0 = performance.now();
  for (let j = 0; j < iterations; j++) {
    const pkt = new MeshPacket({
      packet_id: `pkt-bench-ts-${j}`,
      source_node_id: "gw-us-east-1",
      dest_node_id: "worker-us-west-2",
      tenant_id: "tenant-quantum",
    });
    const r = router.routePacket(pkt);
    assert.ok(r.receipt_hash.startsWith("sw:mesh:v1:"));
  }
  const t1 = performance.now();
  const avgUs = ((t1 - t0) / iterations) * 1000;
  assert.ok(avgUs < 250.0, `TS Benchmark exceeded 250µs SLO: ${avgUs.toFixed(2)}µs`);

  const st = router.status();
  assert.equal(st["online_peers_count"], 5);
  assert.equal(st["active_links_count"], 10);
  assert.equal(st["active_sessions_count"], 1);
  assert.equal(st["circuit_tripped"], false);
});
