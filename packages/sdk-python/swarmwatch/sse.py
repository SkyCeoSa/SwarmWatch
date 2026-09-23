# -*- coding: utf-8 -*-
"""
SwarmWatch Server-Sent Events (SSE) Real-Time Notification Engine (Seam 28 / ADR 0039).

Provides a lightweight, unidirectional, firewall-friendly W3C text/event-stream broadcast
channel for multi-agent observability, circuit trips, warnings, and fleet halts.
Includes automatic keep-alive pings, client reconnect recovery via Last-Event-ID ring-buffer replay,
and granular filtering across swarms, tenant projects, and severity levels.
"""

import asyncio
from collections import deque
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional, Set, Union
import uuid

from pydantic import BaseModel, Field

from swarmwatch.schema import CircuitEvent
from swarmwatch.metrics import Gauge, Counter

logger = logging.getLogger("swarmwatch.sse")

# Prometheus Metrics for SSE
sse_active_clients_gauge = Gauge(
    "swarmwatch_active_sse_clients",
    "Current number of active Server-Sent Events (SSE) subscriber connections",
)
sse_events_sent_counter = Counter(
    "swarmwatch_sse_events_sent_total",
    "Total count of Server-Sent Events dispatched to subscribers",
    label_names=["event_type"],
)


class SseEvent(BaseModel):
    """A structured Server-Sent Event conforming to the W3C specification."""
    id: str = Field(default_factory=lambda: f"evt-{uuid.uuid4().hex[:12]}")
    event: str = Field(default="message", description="Event name/type (e.g. circuit_trip, circuit_warning, fleet_trip, ping)")
    data: Dict[str, Any] = Field(default_factory=dict, description="Structured event payload")
    retry: int = Field(default=3000, description="Client reconnection time in milliseconds")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def format_sse(self) -> str:
        """
        Formats the event into the standard W3C text/event-stream wire format:
        id: <id>\\n
        event: <event>\\n
        retry: <retry>\\n
        data: <json>\\n\\n
        """
        json_data = json.dumps(self.data, separators=(",", ":"), default=str)
        return f"id: {self.id}\nevent: {self.event}\nretry: {self.retry}\ndata: {json_data}\n\n"


class SseSubscriber:
    """Represents an active client stream connected to an SSE endpoint."""

    def __init__(
        self,
        queue: Optional[asyncio.Queue] = None,
        swarm_id: Optional[str] = None,
        project_id: Optional[str] = None,
        min_level: str = "warning",
        topics: Optional[Set[str]] = None,
        max_queue_size: int = 500,
    ):
        self.id: str = f"sub-{uuid.uuid4().hex[:8]}"
        self.queue: asyncio.Queue = queue if queue is not None else asyncio.Queue(maxsize=max_queue_size)
        self.swarm_id: Optional[str] = swarm_id
        self.project_id: Optional[str] = project_id
        self.min_level: str = min_level.lower()
        self.topics: Optional[Set[str]] = topics
        self.created_at: datetime = datetime.now(timezone.utc)

    def matches(self, event: SseEvent) -> bool:
        """Checks if the given SseEvent passes this subscriber's filtering rules."""
        # Always allow pings/heartbeats
        if event.event == "ping":
            return True

        # Topic filter
        if self.topics and event.event not in self.topics:
            return False

        # Min level filter: if 'trip', exclude warning events
        if self.min_level == "trip":
            if event.event in ("circuit_warning", "warning"):
                return False
            if event.data.get("event_type", "").lower() == "warning":
                return False

        # Swarm ID filter
        if self.swarm_id:
            event_swarm = event.data.get("swarm_id")
            if event_swarm and event_swarm != self.swarm_id:
                return False

        # Project ID filter
        if self.project_id and self.project_id != "default-project":
            event_proj = event.data.get("project_id")
            if event_proj and event_proj != self.project_id:
                return False

        return True


class SseEventManager:
    """
    Thread-safe and asyncio-native manager for Server-Sent Events subscribers.
    Maintains active client queues, bounded ring buffer for Last-Event-ID replay,
    and automatic keep-alive ping broadcasting.
    """

    def __init__(self, ring_buffer_size: int = 200):
        self._subscribers: Set[SseSubscriber] = set()
        self._recent_events: deque = deque(maxlen=ring_buffer_size)
        self._lock = asyncio.Lock()

    @property
    def active_subscriber_count(self) -> int:
        return len(self._subscribers)

    async def register(self, subscriber: SseSubscriber) -> None:
        """Registers a new active SSE subscriber stream."""
        async with self._lock:
            self._subscribers.add(subscriber)
            sse_active_clients_gauge.set(len(self._subscribers))
            logger.debug("Registered SSE subscriber %s (total: %d)", subscriber.id, len(self._subscribers))

    async def unregister(self, subscriber: SseSubscriber) -> None:
        """Unregisters an active SSE subscriber stream."""
        async with self._lock:
            self._subscribers.discard(subscriber)
            sse_active_clients_gauge.set(len(self._subscribers))
            logger.debug("Unregistered SSE subscriber %s (remaining: %d)", subscriber.id, len(self._subscribers))

    def get_replay_events(self, last_event_id: str, subscriber: SseSubscriber) -> List[SseEvent]:
        """
        Retrieves missed events from the ring buffer occurring strictly after last_event_id
        that match the subscriber's filtering criteria.
        """
        if not last_event_id:
            return []

        replay: List[SseEvent] = []
        found_target = False

        for evt in self._recent_events:
            if found_target:
                if subscriber.matches(evt):
                    replay.append(evt)
            elif evt.id == last_event_id:
                found_target = True

        # If last_event_id wasn't found (expired past ring buffer capacity), return all matching recent
        if not found_target:
            replay = [evt for evt in self._recent_events if subscriber.matches(evt)]

        return replay

    async def broadcast(self, event_or_circuit: Union[SseEvent, CircuitEvent, Dict[str, Any]]) -> int:
        """
        Dispatches an event to all matching active subscribers and stores it in the ring buffer.
        Returns the number of subscribers the event was queued for.
        """
        if isinstance(event_or_circuit, SseEvent):
            event = event_or_circuit
        elif isinstance(event_or_circuit, CircuitEvent):
            is_trip = event_or_circuit.event_type.lower() == "trip"
            event_name = "circuit_trip" if is_trip else "circuit_warning"
            data = {
                "id": event_or_circuit.id,
                "swarm_id": event_or_circuit.swarm_id,
                "span_id": event_or_circuit.span_id,
                "event_type": event_or_circuit.event_type,
                "metric_val": float(event_or_circuit.metric_val),
                "reason": event_or_circuit.reason,
                "created_at": event_or_circuit.created_at.isoformat(),
            }
            if event_or_circuit.counterfactual_savings:
                data["counterfactual_savings"] = event_or_circuit.counterfactual_savings.model_dump()
            event = SseEvent(
                id=event_or_circuit.id,
                event=event_name,
                data=data,
            )
        elif isinstance(event_or_circuit, dict):
            event = SseEvent(
                id=event_or_circuit.get("id", f"evt-{uuid.uuid4().hex[:12]}"),
                event=event_or_circuit.get("event", "message"),
                data=event_or_circuit.get("data", event_or_circuit),
            )
        else:
            raise ValueError(f"Unsupported event type: {type(event_or_circuit)}")

        # Store in ring buffer (except pings)
        if event.event != "ping":
            self._recent_events.append(event)
            sse_events_sent_counter.inc(labels={"event_type": event.event})

        dispatched_count = 0
        async with self._lock:
            active = list(self._subscribers)

        for sub in active:
            if sub.matches(event):
                try:
                    sub.queue.put_nowait(event)
                    dispatched_count += 1
                except asyncio.QueueFull:
                    logger.warning("SSE subscriber %s queue full; dropping event %s", sub.id, event.id)

        return dispatched_count

    @staticmethod
    def create_ping_event() -> SseEvent:
        """Creates a standard keep-alive ping event."""
        return SseEvent(
            event="ping",
            data={"timestamp": datetime.now(timezone.utc).isoformat()},
            retry=3000,
        )


# Global singleton instance for server-wide SSE event dispatch
global_sse_manager: SseEventManager = SseEventManager()
