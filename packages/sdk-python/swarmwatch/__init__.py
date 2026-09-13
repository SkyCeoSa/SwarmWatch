# -*- coding: utf-8 -*-
"""
SwarmWatch: Universal Observability, Entropy Circuit Breaker & Time-Machine Debugging for Multi-Agent Systems.
"""

from swarmwatch.schema import SwarmSpan, StateSnapshot, CircuitEvent
from swarmwatch.storage import StorageAdapter, SQLiteStorageAdapter
from swarmwatch.transport import TelemetryTransport, TransportConfig
from swarmwatch.core import (
    SwarmClient,
    SpanContext,
    init,
    get_client,
    agent,
    span,
    flush,
    shutdown,
)

__all__ = [
    "SwarmSpan",
    "StateSnapshot",
    "CircuitEvent",
    "StorageAdapter",
    "SQLiteStorageAdapter",
    "TelemetryTransport",
    "TransportConfig",
    "SwarmClient",
    "SpanContext",
    "init",
    "get_client",
    "agent",
    "span",
    "flush",
    "shutdown",
]
