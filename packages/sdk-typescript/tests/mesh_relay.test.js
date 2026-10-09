import test from "node:test";
import assert from "node:assert/strict";

import {
  MeshRelayGateway,
  MeshIngestionAdmissionError,
  MeshRelayCapacityExceededError,
  MeshRouteNotFoundError,
  MeshRelayCircuitTrippedError,
  computeMeshPacketId,
  computePacketSignature,
  verifyPacketSignature,
} from "../src/mesh_relay.ts";
import { SafetyCircuitBus } from "../src/circuit_bus.ts";

function createSamplePacket(options = {}) {
  const tenantId = options.tenantId || "tenant-alpha";
  const secret = options.secret || "secret-key-12345";
  const sourceRegion = options.sourceRegion || "us-east-1";
  const hlcTimestamp = options.hlcTimestamp || "1720000000000000000:0:node-1";
  const payloadType = options.payloadType || "span";
  const payload = options.payload || { agent: "analyst-01", turns: 3 };
  const payloadDigest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  const packetId = computeMeshPacketId(tenantId, sourceRegion, hlcTimestamp, payloadDigest);
  const signature = computePacketSignature(secret, tenantId, sourceRegion, hlcTimestamp, payloadDigest);

  return {
    packet_id: packetId,
    tenant_id: tenantId,
    source_region: sourceRegion,
    hlc_timestamp: hlcTimestamp,
    payload_type: payloadType,
    payload_digest: payloadDigest,
    payload,
    signature,
  };
}

test("Seam 91: Packet signature verification and ID derivation (TypeScript)", () => {
  const packet = createSamplePacket();
  assert.equal(verifyPacketSignature("secret-key-12345", packet), true);
  assert.equal(verifyPacketSignature("wrong-secret", packet), false);
});

test("Seam 91: Unauthenticated or forged packet rejected MR1 (TypeScript)", () => {
  const gateway = new MeshRelayGateway("us-east-1");
  gateway.registerNode({
    node_id: "node-us-1",
    region: "us-east-1",
    endpoint: "https://us.relay:8765",
    latency_ms: 1.0,
    is_healthy: true,
    queue_depth: 0,
    max_capacity: 1000,
  });

  const validPacket = createSamplePacket();

  // Invalid secret
  assert.throws(
    () => gateway.ingest(validPacket, "wrong-secret"),
    MeshIngestionAdmissionError
  );

  // Forged packet ID
  const forgedPacket = createSamplePacket();
  forgedPacket.packet_id = "0000000000000000000000000000000000000000000000000000000000000000";
  assert.throws(
    () => gateway.ingest(forgedPacket, "secret-key-12345"),
    MeshIngestionAdmissionError
  );
});

test("Seam 91: Anycast topological routing MR2 (TypeScript)", () => {
  const gateway = new MeshRelayGateway("us-east-1");
  gateway.registerNode({
    node_id: "node-us-fast",
    region: "us-east-1",
    endpoint: "https://fast:8765",
    latency_ms: 2.0,
    is_healthy: true,
    queue_depth: 0,
    max_capacity: 1000,
  });
  gateway.registerNode({
    node_id: "node-us-slow",
    region: "us-east-1",
    endpoint: "https://slow:8765",
    latency_ms: 20.0,
    is_healthy: true,
    queue_depth: 0,
    max_capacity: 1000,
  });

  const packet = createSamplePacket({ sourceRegion: "us-east-1" });
  const receipt = gateway.ingest(packet, "secret-key-12345");

  assert.equal(receipt.target_node_id, "node-us-fast");
  assert.equal(receipt.routed_region, "us-east-1");
  assert.equal(receipt.duplicate, false);
  assert.equal(receipt.queue_position, 0);
});

test("Seam 91: Content-addressed deduplication MR3 (TypeScript)", () => {
  const gateway = new MeshRelayGateway("us-east-1");
  gateway.registerNode({
    node_id: "node-1",
    region: "us-east-1",
    endpoint: "https://node1:8765",
    latency_ms: 1.0,
    is_healthy: true,
    queue_depth: 0,
    max_capacity: 1000,
  });

  const packet = createSamplePacket();
  const receipt1 = gateway.ingest(packet, "secret-key-12345");
  assert.equal(receipt1.duplicate, false);
  assert.equal(gateway.getRegionQueueDepth("us-east-1"), 1);

  // Ingest identical packet
  const receipt2 = gateway.ingest(packet, "secret-key-12345");
  assert.equal(receipt2.duplicate, true);
  assert.equal(receipt2.packet_id, receipt1.packet_id);
  assert.equal(receipt2.receipt_id, receipt1.receipt_id);
  assert.equal(gateway.getRegionQueueDepth("us-east-1"), 1);
});

test("Seam 91: Bounded capacity and circuit bus overflow MR4 (TypeScript)", () => {
  const circuitBus = new SafetyCircuitBus();
  const gateway = new MeshRelayGateway("us-east-1", 2, circuitBus);
  gateway.registerNode({
    node_id: "node-tiny",
    region: "us-east-1",
    endpoint: "https://tiny:8765",
    latency_ms: 1.0,
    is_healthy: true,
    queue_depth: 0,
    max_capacity: 2,
  });

  const p1 = createSamplePacket({ hlcTimestamp: "100:0:n1" });
  const p2 = createSamplePacket({ hlcTimestamp: "101:0:n1" });
  gateway.ingest(p1, "secret-key-12345");
  gateway.ingest(p2, "secret-key-12345");

  assert.equal(gateway.getRegionQueueDepth("us-east-1"), 2);
  assert.equal(circuitBus.isTripped(), false);

  // 3rd packet overflows capacity
  const p3 = createSamplePacket({ hlcTimestamp: "102:0:n1" });
  assert.throws(
    () => gateway.ingest(p3, "secret-key-12345"),
    MeshRelayCapacityExceededError
  );

  assert.equal(circuitBus.isTripped(), true);
  assert.equal(circuitBus.getStatus().last_trip_event.reason, "MESH_RELAY_OVERFLOW");

  // Subsequent packet rejected by tripped circuit
  const p4 = createSamplePacket({ hlcTimestamp: "103:0:n1" });
  assert.throws(
    () => gateway.ingest(p4, "secret-key-12345"),
    MeshRelayCircuitTrippedError
  );
});

test("Seam 91: Drain batch and queue decrement (TypeScript)", () => {
  const gateway = new MeshRelayGateway("us-east-1");
  gateway.registerNode({
    node_id: "node-1",
    region: "us-east-1",
    endpoint: "https://node1:8765",
    latency_ms: 1.0,
    is_healthy: true,
    queue_depth: 0,
    max_capacity: 1000,
  });

  const p1 = createSamplePacket({ hlcTimestamp: "100:0:n1" });
  const p2 = createSamplePacket({ hlcTimestamp: "101:0:n1" });
  gateway.ingest(p1, "secret-key-12345");
  gateway.ingest(p2, "secret-key-12345");

  assert.equal(gateway.getRegionQueueDepth("us-east-1"), 2);

  const drained1 = gateway.drainBatch("us-east-1", 1);
  assert.equal(drained1.length, 1);
  assert.equal(drained1[0].packet_id, p1.packet_id);
  assert.equal(gateway.getRegionQueueDepth("us-east-1"), 1);

  const drained2 = gateway.drainBatch("us-east-1", 5);
  assert.equal(drained2.length, 1);
  assert.equal(drained2[0].packet_id, p2.packet_id);
  assert.equal(gateway.getRegionQueueDepth("us-east-1"), 0);
});

test("Seam 91: Route not found trips circuit (TypeScript)", () => {
  const circuitBus = new SafetyCircuitBus();
  const gateway = new MeshRelayGateway("us-east-1", 1000, circuitBus);

  const p = createSamplePacket();
  assert.throws(
    () => gateway.ingest(p, "secret-key-12345"),
    MeshRouteNotFoundError
  );

  assert.equal(circuitBus.isTripped(), true);
  assert.equal(circuitBus.getStatus().last_trip_event.reason, "MESH_ROUTE_EXHAUSTION");
});

test("Seam 91: Sub-250 microsecond execution latency SLO (TypeScript)", () => {
  const gateway = new MeshRelayGateway("us-east-1");
  gateway.registerNode({
    node_id: "node-bench",
    region: "us-east-1",
    endpoint: "https://bench:8765",
    latency_ms: 1.0,
    is_healthy: true,
    queue_depth: 0,
    max_capacity: 10000,
  });

  const packets = [];
  for (let i = 0; i < 100; i++) {
    packets.push(createSamplePacket({ hlcTimestamp: `${1000 + i}:0:n1` }));
  }

  // Warmup
  gateway.ingest(packets[0], "secret-key-12345");

  const start = performance.now();
  for (let i = 1; i < 100; i++) {
    gateway.ingest(packets[i], "secret-key-12345");
  }
  const end = performance.now();

  const meanMs = (end - start) / 99;
  const meanUs = meanMs * 1000;
  assert.ok(meanUs < 250, `Mean latency ${meanUs.toFixed(2)}µs exceeded 250µs SLO`);
});
