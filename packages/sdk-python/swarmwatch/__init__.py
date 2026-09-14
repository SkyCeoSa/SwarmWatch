# -*- coding: utf-8 -*-
"""
SwarmWatch: Universal Observability, Entropy Circuit Breaker & Time-Machine Debugging for Multi-Agent Systems.
"""

from swarmwatch.schema import SwarmSpan, StateSnapshot, CircuitEvent, CounterfactualSavings
from swarmwatch.storage import StorageAdapter, SQLiteStorageAdapter, ClickHouseStorageAdapter
from swarmwatch.transport import TelemetryTransport, TransportConfig
from swarmwatch.webhooks import WebhookConfig, WebhookNotifier
from swarmwatch.core import (
    SwarmClient,
    SpanContext,
    init,
    get_client,
    agent,
    span,
    flush,
    shutdown,
    sync_state,
    record_circuit_event,
)
from swarmwatch.serializer import (
    SafeStateSerializer,
    TimeMachine,
)

from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPolicy,
    CircuitPreset,
    CircuitDecision,
    SwarmCircuitBreakException,
    compute_shannon_entropy,
    compute_lexical_similarity,
)

__all__ = [
    "SwarmSpan",
    "StateSnapshot",
    "CircuitEvent",
    "CounterfactualSavings",
    "StorageAdapter",
    "SQLiteStorageAdapter",
    "ClickHouseStorageAdapter",
    "TelemetryTransport",
    "TransportConfig",
    "WebhookConfig",
    "WebhookNotifier",
    "SwarmClient",
    "SpanContext",
    "EntropyCircuitBreaker",
    "CircuitPolicy",
    "CircuitPreset",
    "CircuitDecision",
    "SwarmCircuitBreakException",
    "compute_shannon_entropy",
    "compute_lexical_similarity",
    "SafeStateSerializer",
    "TimeMachine",
    "sync_state",
    "init",
    "get_client",
    "agent",
    "span",
    "flush",
    "shutdown",
    "record_circuit_event",
]
