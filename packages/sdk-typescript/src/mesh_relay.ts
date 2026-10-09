/**
 * SwarmWatch Global Distributed Mesh Relay & Anycast Ingestion Gateway (Seam 91 / ADR 0102).
 *
 * Implements low-latency edge admission with cryptographic tenant authorization (MR1),
 * topological anycast latency and queue-aware routing (MR2), causal content-addressed
 * deduplication (MR3), and bounded ring buffer queuing coupled with fail-closed safety
 * circuit latching (MR4).
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";
import { SafetyCircuitBus } from "./circuit_bus.ts";
import { HybridLogicalClock } from "./hlc.ts";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class MeshRelayError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MeshRelayError";
  }
}

export class MeshIngestionAdmissionError extends MeshRelayError {
  constructor(message: string) {
    super(message);
    this.name = "MeshIngestionAdmissionError";
  }
}

export class MeshRelayCapacityExceededError extends MeshRelayError {
  constructor(message: string) {
    super(message);
    this.name = "MeshRelayCapacityExceededError";
  }
}

export class MeshRouteNotFoundError extends MeshRelayError {
  constructor(message: string) {
    super(message);
    this.name = "MeshRouteNotFoundError";
  }
}

export class MeshRelayCircuitTrippedError extends MeshRelayError {
  constructor(message: string) {
    super(message);
    this.name = "MeshRelayCircuitTrippedError";
  }
}

// ==============================================================================
// 2. Domain Models & Canonical Helpers
// ==============================================================================

export interface MeshRelayPacket {
  packet_id: string;
  tenant_id: string;
  source_region: string;
  hlc_timestamp: string;
  payload_type: string;
  payload_digest: string;
  payload: Record<string, unknown>;
  signature: string;
}

export type MeshPacket = MeshRelayPacket;

export interface RelayNode {
  node_id: string;
  region: string;
  endpoint: string;
  latency_ms: number;
  is_healthy: boolean;
  queue_depth: number;
  max_capacity: number;
}

export interface IngestionReceipt {
  receipt_id: string;
  packet_id: string;
  accepted_at_hlc: string;
  routed_region: string;
  target_node_id: string;
  queue_position: number;
  duplicate: boolean;
}

export function computeMeshPacketId(
  tenantId: string,
  sourceRegion: string,
  hlcTimestamp: string,
  payloadDigest: string
): string {
  const raw = `${tenantId.trim()}:${sourceRegion.trim()}:${hlcTimestamp.trim()}:${payloadDigest.trim()}`;
  return crypto.createHash("sha256").update(raw, "utf8").digest("hex");
}

export function computePacketSignature(
  secret: string,
  tenantId: string,
  sourceRegion: string,
  hlcTimestamp: string,
  payloadDigest: string
): string {
  const raw = `sw:relay:auth:v1:${tenantId.trim()}:${sourceRegion.trim()}:${hlcTimestamp.trim()}:${payloadDigest.trim()}`;
  return crypto.createHmac("sha256", secret).update(raw, "utf8").digest("hex");
}

export function verifyPacketSignature(secret: string, packet: MeshPacket): boolean {
  const expected = computePacketSignature(
    secret,
    packet.tenant_id,
    packet.source_region,
    packet.hlc_timestamp,
    packet.payload_digest
  );
  if (expected.length !== packet.signature.length) {
    return false;
  }
  return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(packet.signature));
}

// ==============================================================================
// 3. Mesh Relay Gateway Deep Module
// ==============================================================================

export class MeshRelayGateway {
  public local_region: string;
  public default_capacity: number;
  public circuit_bus?: SafetyCircuitBus;

  private _hlc: HybridLogicalClock;
  private _nodes: Map<string, RelayNode> = new Map();
  private _region_queues: Map<string, MeshPacket[]> = new Map();
  private _dedup_cache: Map<string, IngestionReceipt> = new Map();

  constructor(
    localRegion: string = "us-east-1",
    defaultCapacity: number = 10000,
    circuitBus?: SafetyCircuitBus
  ) {
    this.local_region = localRegion;
    this.default_capacity = defaultCapacity;
    this.circuit_bus = circuitBus;
    this._hlc = new HybridLogicalClock(`relay-${localRegion}`);
  }

  bindCircuitBus(circuitBus: SafetyCircuitBus): void {
    this.circuit_bus = circuitBus;
  }

  registerNode(node: RelayNode): void {
    this._nodes.set(node.node_id, { ...node });
    if (!this._region_queues.has(node.region)) {
      this._region_queues.set(node.region, []);
    }
  }

  updateNodeHealth(nodeId: string, isHealthy: boolean, latencyMs?: number): void {
    const node = this._nodes.get(nodeId);
    if (node) {
      node.is_healthy = isHealthy;
      if (latencyMs !== undefined && latencyMs >= 0) {
        node.latency_ms = latencyMs;
      }
    }
  }

  getRegionQueueDepth(region: string): number {
    const q = this._region_queues.get(region);
    return q ? q.length : 0;
  }

  getTopology(): Record<string, unknown> {
    const nodesInfo = Array.from(this._nodes.values()).map((n) => ({
      node_id: n.node_id,
      region: n.region,
      endpoint: n.endpoint,
      latency_ms: n.latency_ms,
      is_healthy: n.is_healthy,
      queue_depth: n.queue_depth,
      max_capacity: n.max_capacity,
    }));

    const regionsInfo: Record<string, number> = {};
    for (const [r, q] of this._region_queues.entries()) {
      regionsInfo[r] = q.length;
    }

    return {
      local_region: this.local_region,
      node_count: this._nodes.size,
      nodes: nodesInfo,
      region_queues: regionsInfo,
      dedup_cached_count: this._dedup_cache.size,
    };
  }

  private _selectAnycastNode(preferredRegion?: string): RelayNode {
    const healthyNodes = Array.from(this._nodes.values()).filter((n) => n.is_healthy);
    if (healthyNodes.length === 0) {
      throw new MeshRouteNotFoundError("No healthy relay node available in topology.");
    }

    let candidates: RelayNode[] = [];
    if (preferredRegion) {
      candidates = healthyNodes.filter(
        (n) => n.region === preferredRegion && n.queue_depth < n.max_capacity
      );
    }

    if (candidates.length === 0) {
      candidates = healthyNodes.filter((n) => n.queue_depth < n.max_capacity);
    }

    if (candidates.length === 0) {
      throw new MeshRelayCapacityExceededError("All healthy relay nodes have reached maximum capacity.");
    }

    // Cost S = latency_ms * (1.0 + queue_depth / max_capacity)
    let bestNode = candidates[0];
    let minScore = Infinity;

    for (const node of candidates) {
      const loadFactor = node.max_capacity > 0 ? node.queue_depth / node.max_capacity : 1.0;
      const score = node.latency_ms * (1.0 + loadFactor);
      if (score < minScore) {
        minScore = score;
        bestNode = node;
      }
    }

    return bestNode;
  }

  ingest(packet: MeshPacket, tenantSecret: string): IngestionReceipt {
    // 1. Circuit Bus Health Check
    if (this.circuit_bus && this.circuit_bus.isTripped()) {
      throw new MeshRelayCircuitTrippedError("Ingestion rejected: Safety circuit bus is currently TRIPPED.");
    }

    // 2. Cryptographic Admission & Invariant Verification (MR1)
    if (!packet.tenant_id || !packet.signature) {
      throw new MeshIngestionAdmissionError("Missing tenant identifier or authentication signature.");
    }

    if (!verifyPacketSignature(tenantSecret, packet)) {
      throw new MeshIngestionAdmissionError("Authentication signature mismatch: invalid tenant secret.");
    }

    const expectedId = computeMeshPacketId(
      packet.tenant_id,
      packet.source_region,
      packet.hlc_timestamp,
      packet.payload_digest
    );
    if (packet.packet_id !== expectedId) {
      throw new MeshIngestionAdmissionError(
        `Packet ID mismatch: expected '${expectedId}', received '${packet.packet_id}'.`
      );
    }

    // 3. Deduplication Check (MR3)
    if (this._dedup_cache.has(packet.packet_id)) {
      const cached = this._dedup_cache.get(packet.packet_id)!;
      return {
        receipt_id: cached.receipt_id,
        packet_id: cached.packet_id,
        accepted_at_hlc: cached.accepted_at_hlc,
        routed_region: cached.routed_region,
        target_node_id: cached.target_node_id,
        queue_position: cached.queue_position,
        duplicate: true,
      };
    }

    // 4. Anycast Route Determination (MR2)
    let targetNode: RelayNode;
    try {
      targetNode = this._selectAnycastNode(packet.source_region);
    } catch (err) {
      if (err instanceof MeshRelayCapacityExceededError) {
        if (this.circuit_bus) {
          this.circuit_bus.trip(
            "MESH_RELAY_OVERFLOW",
            `relay:${packet.source_region}`
          );
        }
        throw err;
      }
      if (err instanceof MeshRouteNotFoundError) {
        if (this.circuit_bus) {
          this.circuit_bus.trip(
            "MESH_ROUTE_EXHAUSTION",
            `relay:${packet.source_region}`
          );
        }
        throw err;
      }
      throw err;
    }

    // 5. Bounded Capacity & Queue Check (MR4)
    const region = targetNode.region;
    if (!this._region_queues.has(region)) {
      this._region_queues.set(region, []);
    }

    const regionQueue = this._region_queues.get(region)!;
    if (regionQueue.length >= this.default_capacity || targetNode.queue_depth >= targetNode.max_capacity) {
      if (this.circuit_bus) {
        this.circuit_bus.trip(
          "MESH_RELAY_OVERFLOW",
          `relay:${region}`
        );
      }
      throw new MeshRelayCapacityExceededError(
        `Relay capacity overflow in region '${region}' (queue depth: ${regionQueue.length}).`
      );
    }

    // 6. Admission & Receipt Generation
    const acceptedHlc = this._hlc.now().toString();
    const position = regionQueue.length;
    regionQueue.push(packet);
    targetNode.queue_depth += 1;

    const receiptDigest = crypto
      .createHash("sha256")
      .update(`${packet.packet_id}:${acceptedHlc}:${region}:${targetNode.node_id}:${position}`, "utf8")
      .digest("hex");
    const receiptId = `sw:relay:rcpt:v1:${receiptDigest}`;

    const receipt: IngestionReceipt = {
      receipt_id: receiptId,
      packet_id: packet.packet_id,
      accepted_at_hlc: acceptedHlc,
      routed_region: region,
      target_node_id: targetNode.node_id,
      queue_position: position,
      duplicate: false,
    };

    this._dedup_cache.set(packet.packet_id, receipt);
    return receipt;
  }

  drainBatch(region: string, maxCount: number = 100): MeshPacket[] {
    const queue = this._region_queues.get(region);
    if (!queue || queue.length === 0) {
      return [];
    }

    const drained: MeshPacket[] = [];
    while (queue.length > 0 && drained.length < maxCount) {
      drained.push(queue.shift()!);
    }

    // Adjust node queue depths
    const regionNodes = Array.from(this._nodes.values()).filter((n) => n.region === region);
    let countToDecrement = drained.length;
    for (const node of regionNodes) {
      if (countToDecrement <= 0) break;
      const decrement = Math.min(node.queue_depth, countToDecrement);
      node.queue_depth -= decrement;
      countToDecrement -= decrement;
    }

    return drained;
  }
}

export const globalMeshRelay: MeshRelayGateway = new MeshRelayGateway();
