# -*- coding: utf-8 -*-
"""
SwarmWatch Tenant-Scoped Engine Registry & Non-Interference (Seam 89 / ADR 0100).

Implements strict multi-tenant non-interference (T1), dynamic engine isolation (T2),
and zero-bleed tenant lifecycle eviction (T3), eliminating un-scoped global singletons.

Invariants:
- T1: For tenants A != B, no sequence of mutations under A can observe or affect state of B.
- T2: All engines (circuit, crdt, barrier, stream) resolve through tenant-scoped registries.
- T3: Evicting tenant A purges all associated state without affecting tenant B.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import Any, Callable, Dict, List, Optional, Set

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.crdt_kernel import DeltaCrdtKernel
from swarmwatch.barrier_lease import LeasedBarrierCoordinator
from swarmwatch.durable_stream import DurableCausalStream
from swarmwatch.optimistic_attestation import OptimisticAttestationEngine


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class EngineRegistryError(Exception):
    """Base exception for tenant engine registry operations."""


class UnknownEngineKindError(EngineRegistryError):
    """Raised when an unregistered engine type is requested without a factory."""


# ==============================================================================
# 2. Tenant Engine Registry Deep Module
# ==============================================================================

class TenantEngineRegistry:
    """
    Central registry managing isolated, tenant-scoped subsystem engines.
    Prevents cross-tenant state leakage and ensures strict non-interference.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # tenant_id -> kind -> engine_instance
        self._registry: Dict[str, Dict[str, Any]] = {}
        # kind -> factory_func(tenant_id)
        self._factories: Dict[str, Callable[[str], Any]] = {
            "circuit_bus": lambda tid: SafetyCircuitBus(),
            "crdt_kernel": lambda tid: DeltaCrdtKernel(node_id=tid),
            "leased_barrier": lambda tid: LeasedBarrierCoordinator(),
            "durable_stream": lambda tid: DurableCausalStream(stream_id=tid),
            "attestation": lambda tid: OptimisticAttestationEngine(),
        }

    def register_factory(self, kind: str, factory: Callable[[str], Any]) -> None:
        """Registers a constructor factory for a specific engine kind."""
        with self._lock:
            self._factories[kind.lower()] = factory

    def get(
        self,
        tenant_id: str,
        kind: str,
        factory: Optional[Callable[[str], Any]] = None,
    ) -> Any:
        """
        Retrieves or lazily instantiates an isolated engine instance for the tenant (Invariant T2).
        """
        tid = str(tenant_id).strip()
        k = kind.lower().strip()

        with self._lock:
            if tid not in self._registry:
                self._registry[tid] = {}

            if k not in self._registry[tid]:
                fact = factory or self._factories.get(k)
                if not fact:
                    raise UnknownEngineKindError(f"No factory registered for engine kind '{k}'")
                self._registry[tid][k] = fact(tid)

            return self._registry[tid][k]

    def has_engine(self, tenant_id: str, kind: str) -> bool:
        """Checks if an engine is actively initialized for the tenant."""
        tid = str(tenant_id).strip()
        k = kind.lower().strip()
        with self._lock:
            return tid in self._registry and k in self._registry[tid]

    def evict_tenant(self, tenant_id: str) -> None:
        """
        Evicts all engines and active in-memory state for a tenant (Invariant T3).
        Guarantees zero-bleed cleanup without affecting other tenants.
        """
        tid = str(tenant_id).strip()
        with self._lock:
            if tid in self._registry:
                del self._registry[tid]

    def list_tenants(self) -> List[str]:
        """Returns sorted list of currently active tenant identifiers."""
        with self._lock:
            return sorted(list(self._registry.keys()))

    def clear_for_test(self) -> None:
        """Clears all tenants for testing isolation."""
        with self._lock:
            self._registry.clear()


# Authoritative Global Registry Singleton
global_engine_registry: TenantEngineRegistry = TenantEngineRegistry()
