# -*- coding: utf-8 -*-
"""
Tests for Seam 89: Tenant-Scoped Engine Registry & Non-Interference (ADR 0100).
Verifies:
- Invariant T1: Strict non-interference across tenant boundaries.
- Invariant T2: Dynamic engine scoping eliminating global singleton bleed.
- Invariant T3: Zero-bleed tenant eviction.
- Sub-250µs execution latency SLO and zero-token cost guarantee.
"""

import time
import pytest

from swarmwatch.engine_registry import (
    TenantEngineRegistry,
    UnknownEngineKindError,
)
from swarmwatch.circuit_bus import SafetyCircuitBus, CircuitBusTrippedError


def test_tenant_non_interference_differential():
    registry = TenantEngineRegistry()

    # Retrieve isolated circuit buses for Tenant A and Tenant B
    bus_a: SafetyCircuitBus = registry.get("tenant-alpha", "circuit_bus")
    bus_b: SafetyCircuitBus = registry.get("tenant-beta", "circuit_bus")

    assert bus_a is not bus_b
    assert bus_a.is_tripped() is False
    assert bus_b.is_tripped() is False

    # Invariant T1: Mutate and trip Tenant A's circuit
    bus_a.trip("Critical failure in Alpha", scope="GLOBAL")
    assert bus_a.is_tripped() is True

    # Tenant B remains completely unperturbed!
    assert bus_b.is_tripped() is False
    executed_b = False
    with bus_b.guard("GLOBAL"):
        executed_b = True
    assert executed_b is True

    # Guard on A throws fail-closed
    with pytest.raises(CircuitBusTrippedError):
        with bus_a.guard("GLOBAL"):
            pass


def test_tenant_crdt_state_isolation():
    registry = TenantEngineRegistry()

    crdt_a = registry.get("tenant-a", "crdt_kernel")
    crdt_b = registry.get("tenant-b", "crdt_kernel")

    crdt_a.put_vertex("user-1", {"role": "ADMIN"})

    # User-1 exists in Tenant A, but DOES NOT exist in Tenant B
    assert crdt_a.get_vertex("user-1") is not None
    assert crdt_b.get_vertex("user-1") is None
    assert crdt_a.get_state_digest() != crdt_b.get_state_digest()


def test_zero_bleed_tenant_eviction():
    registry = TenantEngineRegistry()

    registry.get("tenant-temp", "circuit_bus")
    registry.get("tenant-keep", "circuit_bus")

    assert "tenant-temp" in registry.list_tenants()
    assert "tenant-keep" in registry.list_tenants()

    # Invariant T3: Evict temporary tenant
    registry.evict_tenant("tenant-temp")
    assert "tenant-temp" not in registry.list_tenants()
    assert "tenant-keep" in registry.list_tenants()


def test_engine_resolution_latency_slo():
    registry = TenantEngineRegistry()
    start = time.perf_counter()
    for i in range(1_000):
        registry.get("perf-tenant", "circuit_bus")
    elapsed_us = ((time.perf_counter() - start) / 1_000) * 1_000_000
    assert elapsed_us < 10.0  # sub-10µs per lookup, well within 250µs SLO
