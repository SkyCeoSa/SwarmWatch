# -*- coding: utf-8 -*-
"""
Tests for SwarmWatch Global Distributed Mesh Relay & Anycast Ingestion Gateway (Seam 91 / ADR 0102).
"""

import time
import pytest

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.mesh_relay import (
    MeshRelayError,
    MeshIngestionAdmissionError,
    MeshRelayCapacityExceededError,
    MeshRouteNotFoundError,
    MeshRelayCircuitTrippedError,
    MeshPacket,
    RelayNode,
    IngestionReceipt,
    compute_mesh_packet_id,
    compute_packet_signature,
    verify_packet_signature,
    MeshRelayGateway,
)


def _create_sample_packet(
    tenant_id: str = "tenant-alpha",
    secret: str = "secret-key-12345",
    source_region: str = "us-east-1",
    hlc_timestamp: str = "1720000000000000000:0:node-1",
    payload_type: str = "span",
    payload: dict = None,
) -> MeshPacket:
    if payload is None:
        payload = {"agent": "analyst-01", "turns": 3}
    payload_digest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    packet_id = compute_mesh_packet_id(
        tenant_id=tenant_id,
        source_region=source_region,
        hlc_timestamp=hlc_timestamp,
        payload_digest=payload_digest,
    )
    sig = compute_packet_signature(
        secret=secret,
        tenant_id=tenant_id,
        source_region=source_region,
        hlc_timestamp=hlc_timestamp,
        payload_digest=payload_digest,
    )
    return MeshPacket(
        packet_id=packet_id,
        tenant_id=tenant_id,
        source_region=source_region,
        hlc_timestamp=hlc_timestamp,
        payload_type=payload_type,
        payload_digest=payload_digest,
        payload=payload,
        signature=sig,
    )


def test_mesh_packet_signature_and_id_derivation():
    packet = _create_sample_packet()
    assert verify_packet_signature("secret-key-12345", packet) is True
    assert verify_packet_signature("wrong-secret", packet) is False


def test_unauthenticated_or_forged_packet_rejected_mr1():
    gateway = MeshRelayGateway(local_region="us-east-1")
    gateway.register_node(RelayNode(node_id="node-us-1", region="us-east-1", endpoint="https://us.relay:8765"))

    valid_packet = _create_sample_packet()

    # 1. Invalid Secret
    with pytest.raises(MeshIngestionAdmissionError, match="Authentication signature mismatch"):
        gateway.ingest(valid_packet, tenant_secret="wrong-secret")

    # 2. Forged Packet ID
    forged_packet = _create_sample_packet()
    forged_packet.packet_id = "0000000000000000000000000000000000000000000000000000000000000000"
    with pytest.raises(MeshIngestionAdmissionError):
        gateway.ingest(forged_packet, tenant_secret="secret-key-12345")


def test_anycast_topological_routing_mr2():
    gateway = MeshRelayGateway(local_region="us-east-1")
    # Register two nodes in us-east-1 with different latencies
    gateway.register_node(
        RelayNode(node_id="node-us-fast", region="us-east-1", endpoint="https://fast:8765", latency_ms=2.0)
    )
    gateway.register_node(
        RelayNode(node_id="node-us-slow", region="us-east-1", endpoint="https://slow:8765", latency_ms=15.0)
    )

    packet = _create_sample_packet(source_region="us-east-1")
    receipt = gateway.ingest(packet, tenant_secret="secret-key-12345")

    # Should select the faster node
    assert receipt.target_node_id == "node-us-fast"
    assert receipt.routed_region == "us-east-1"
    assert receipt.duplicate is False
    assert receipt.queue_position == 0


def test_content_addressed_deduplication_mr3():
    gateway = MeshRelayGateway(local_region="us-east-1")
    gateway.register_node(RelayNode(node_id="node-1", region="us-east-1", endpoint="https://node1:8765"))

    packet = _create_sample_packet()
    receipt1 = gateway.ingest(packet, tenant_secret="secret-key-12345")
    assert receipt1.duplicate is False
    assert gateway.get_region_queue_depth("us-east-1") == 1

    # Ingest identical packet again
    receipt2 = gateway.ingest(packet, tenant_secret="secret-key-12345")
    assert receipt2.duplicate is True
    assert receipt2.packet_id == receipt1.packet_id
    assert receipt2.receipt_id == receipt1.receipt_id
    # Queue depth should NOT increase on duplicate
    assert gateway.get_region_queue_depth("us-east-1") == 1


def test_bounded_capacity_and_circuit_bus_overflow_mr4():
    circuit_bus = SafetyCircuitBus()
    gateway = MeshRelayGateway(local_region="us-east-1", default_capacity=2, circuit_bus=circuit_bus)
    gateway.register_node(
        RelayNode(node_id="node-tiny", region="us-east-1", endpoint="https://tiny:8765", max_capacity=2)
    )

    # Ingest 2 packets (at capacity)
    p1 = _create_sample_packet(hlc_timestamp="100:0:n1")
    p2 = _create_sample_packet(hlc_timestamp="101:0:n1")
    gateway.ingest(p1, tenant_secret="secret-key-12345")
    gateway.ingest(p2, tenant_secret="secret-key-12345")

    assert gateway.get_region_queue_depth("us-east-1") == 2
    assert circuit_bus.is_tripped() is False

    # Ingest 3rd packet (exceeds capacity)
    p3 = _create_sample_packet(hlc_timestamp="102:0:n1")
    with pytest.raises(MeshRelayCapacityExceededError):
        gateway.ingest(p3, tenant_secret="secret-key-12345")

    # Safety circuit bus must be tripped fail-closed
    assert circuit_bus.is_tripped() is True
    assert circuit_bus.get_status().last_trip_event.reason == "MESH_RELAY_OVERFLOW"

    # Subsequent packet rejected due to tripped circuit
    p4 = _create_sample_packet(hlc_timestamp="103:0:n1")
    with pytest.raises(MeshRelayCircuitTrippedError):
        gateway.ingest(p4, tenant_secret="secret-key-12345")


def test_drain_batch_and_queue_decrement():
    gateway = MeshRelayGateway(local_region="us-east-1")
    gateway.register_node(RelayNode(node_id="node-1", region="us-east-1", endpoint="https://node1:8765"))

    p1 = _create_sample_packet(hlc_timestamp="100:0:n1")
    p2 = _create_sample_packet(hlc_timestamp="101:0:n1")
    gateway.ingest(p1, tenant_secret="secret-key-12345")
    gateway.ingest(p2, tenant_secret="secret-key-12345")

    assert gateway.get_region_queue_depth("us-east-1") == 2

    drained = gateway.drain_batch(region="us-east-1", max_count=1)
    assert len(drained) == 1
    assert drained[0].packet_id == p1.packet_id
    assert gateway.get_region_queue_depth("us-east-1") == 1

    drained2 = gateway.drain_batch(region="us-east-1", max_count=5)
    assert len(drained2) == 1
    assert drained2[0].packet_id == p2.packet_id
    assert gateway.get_region_queue_depth("us-east-1") == 0


def test_mesh_route_not_found_trips_circuit():
    circuit_bus = SafetyCircuitBus()
    gateway = MeshRelayGateway(circuit_bus=circuit_bus)
    # No nodes registered
    p = _create_sample_packet()
    with pytest.raises(MeshRouteNotFoundError):
        gateway.ingest(p, tenant_secret="secret-key-12345")

    assert circuit_bus.is_tripped() is True
    assert circuit_bus.get_status().last_trip_event.reason == "MESH_ROUTE_EXHAUSTION"


def test_sub_250_microsecond_latency_slo():
    gateway = MeshRelayGateway(local_region="us-east-1")
    gateway.register_node(RelayNode(node_id="node-bench", region="us-east-1", endpoint="https://bench:8765"))

    packets = [_create_sample_packet(hlc_timestamp=f"{1000 + i}:0:n1") for i in range(100)]

    # Warmup
    gateway.ingest(packets[0], tenant_secret="secret-key-12345")

    t0 = time.perf_counter_ns()
    for p in packets[1:]:
        gateway.ingest(p, tenant_secret="secret-key-12345")
    t1 = time.perf_counter_ns()

    mean_ns = (t1 - t0) / 99
    mean_us = mean_ns / 1000.0
    # SLO: < 250µs
    assert mean_us < 250.0, f"Mean latency {mean_us:.2f}µs exceeded 250µs SLO"
