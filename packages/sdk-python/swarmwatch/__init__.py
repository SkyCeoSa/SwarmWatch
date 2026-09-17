# -*- coding: utf-8 -*-
"""
SwarmWatch: Universal Observability, Entropy Circuit Breaker & Time-Machine Debugging for Multi-Agent Systems.
"""

from swarmwatch.schema import SwarmSpan, StateSnapshot, CircuitEvent, CounterfactualSavings
from swarmwatch.storage import StorageAdapter, SQLiteStorageAdapter, ClickHouseStorageAdapter
from swarmwatch.transport import TelemetryTransport, TransportConfig
from swarmwatch.webhooks import WebhookConfig, WebhookNotifier
from swarmwatch.otel import to_otel_genai_span, OtelGenAiExporter
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

from swarmwatch.tenant import (
    Organization,
    Project,
    ApiKey,
    TenantContext,
    ApiKeyManager,
)
from swarmwatch.fleet import (
    FleetCircuitCoordinator,
    FleetMetrics,
    FleetEvent,
)
from swarmwatch.metrics import (
    PrometheusMetricsRegistry,
    Counter,
    Gauge,
    Histogram,
    record_span_ingested,
    record_circuit_trip,
    record_state_snapshot,
    record_branch_fork,
    set_active_swarms,
    set_active_websockets,
    set_telemetry_queue_depth,
)
from swarmwatch.adapters.llamaindex import SwarmWatchLlamaIndexHandler
from swarmwatch.adapters.autogen import SwarmWatchAutoGenListener
from swarmwatch.adapters.dspy import SwarmWatchDSPyTracker
from swarmwatch.adapters.semantic_kernel import (
    SwarmWatchSemanticKernelFilter,
    FunctionInvocationContext,
)
from swarmwatch.topology import (
    AgentNode,
    CausalEdge,
    CausalTopology,
    build_topology_from_spans,
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
    "to_otel_genai_span",
    "OtelGenAiExporter",
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
    "Organization",
    "Project",
    "ApiKey",
    "TenantContext",
    "ApiKeyManager",
    "FleetCircuitCoordinator",
    "FleetMetrics",
    "FleetEvent",
    "PrometheusMetricsRegistry",
    "Counter",
    "Gauge",
    "Histogram",
    "record_span_ingested",
    "record_circuit_trip",
    "record_state_snapshot",
    "record_branch_fork",
    "set_active_swarms",
    "set_active_websockets",
    "set_telemetry_queue_depth",
    "SwarmWatchLlamaIndexHandler",
    "SwarmWatchAutoGenListener",
    "SwarmWatchDSPyTracker",
    "SwarmWatchSemanticKernelFilter",
    "FunctionInvocationContext",
    "AgentNode",
    "CausalEdge",
    "CausalTopology",
    "build_topology_from_spans",
]



