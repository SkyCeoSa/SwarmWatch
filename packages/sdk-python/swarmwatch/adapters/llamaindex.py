# -*- coding: utf-8 -*-
"""
SwarmWatch Hybrid Adapter for LlamaIndex (Seam 5).

Provides non-invasive BaseCallbackHandler integration, tracing query planning,
retrieval, synthesis, and multi-agent workflow steps with parent-child causal DAGs
and zero-token Shannon entropy loop protection.
"""

from datetime import datetime, timezone
import logging
import time
from typing import Any, Dict, List, Optional
import uuid

import swarmwatch
from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPreset,
    SwarmCircuitBreakException,
)
from swarmwatch.schema import SwarmSpan

logger = logging.getLogger("swarmwatch.adapters.llamaindex")

try:
    from llama_index.core.callbacks.base_handler import BaseCallbackHandler
    LLAMAINDEX_AVAILABLE = True
except ImportError:
    try:
        from llama_index.callbacks.base_handler import BaseCallbackHandler
        LLAMAINDEX_AVAILABLE = True
    except ImportError:
        BaseCallbackHandler = object
        LLAMAINDEX_AVAILABLE = False


class SwarmWatchLlamaIndexHandler(BaseCallbackHandler):
    """
    LlamaIndex compatible CallbackHandler.
    Captures query lifecycle, LLM synthesis, function calls, and agent steps as SwarmSpans.
    Halts runaway query/agent loops when entropy plunges or repetitive queries occur.
    """

    def __init__(
        self,
        swarm_id: Optional[str] = None,
        circuit_breaker: Optional[EntropyCircuitBreaker] = None,
        branch_id: str = "main",
        event_starts_to_ignore: Optional[List[str]] = None,
        event_ends_to_ignore: Optional[List[str]] = None,
    ):
        if LLAMAINDEX_AVAILABLE and hasattr(super(), "__init__"):
            super().__init__(
                event_starts_to_ignore=event_starts_to_ignore or [],
                event_ends_to_ignore=event_ends_to_ignore or [],
            )
        self.swarm_id = swarm_id or (swarmwatch.get_client().swarm_id if swarmwatch.get_client() else "llamaindex-swarm")
        self.circuit_breaker = circuit_breaker or EntropyCircuitBreaker(preset=CircuitPreset.STRICT)
        self.branch_id = branch_id

        # Internal tracking for in-flight events: event_id -> metadata
        self._active_events: Dict[str, Dict[str, Any]] = {}
        # Mapping event_id to generated span_id for parent-child lineage
        self._event_span_map: Dict[str, str] = {}

    def on_event_start(
        self,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
        event_id: str = "",
        parent_id: str = "",
        **kwargs: Any,
    ) -> str:
        """Invoked when a LlamaIndex event begins (e.g. query, retrieve, llm, agent_step)."""
        event_id = event_id or str(uuid.uuid4())
        start_time = datetime.now(timezone.utc)
        payload = payload or {}

        # 1. Proactive Circuit Breaker Evaluation on prompt / thought / query
        eval_text = ""
        for key in ("prompt", "thought", "query_str", "query", "input"):
            if key in payload and payload[key]:
                eval_text += f" {payload[key]}"
        eval_text = eval_text.strip()

        if eval_text:
            self.circuit_breaker.evaluate(
                eval_text,
                agent_id=f"llamaindex:{event_type}",
                swarm_id=self.swarm_id,
            )

        # 2. Store event context & pre-assign span_id
        span_id = str(uuid.uuid4())
        self._event_span_map[event_id] = span_id
        self._active_events[event_id] = {
            "event_type": str(event_type),
            "payload": payload,
            "parent_id": parent_id,
            "span_id": span_id,
            "start_time": start_time,
            "t0": time.perf_counter(),
        }
        return event_id

    def on_event_end(
        self,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
        event_id: str = "",
        **kwargs: Any,
    ) -> None:
        """Invoked when a LlamaIndex event completes."""
        event_info = self._active_events.pop(event_id, None)
        start_payload = event_info["payload"] if event_info else {}
        start_time = event_info["start_time"] if event_info else datetime.now(timezone.utc)
        parent_event_id = event_info.get("parent_id", "") if event_info else ""
        span_id = event_info.get("span_id", str(uuid.uuid4())) if event_info else str(uuid.uuid4())
        end_time = datetime.now(timezone.utc)
        payload = payload or {}

        # 1. Evaluate Output with Circuit Breaker
        eval_text = ""
        for key in ("response", "result", "output"):
            if key in payload and payload[key]:
                eval_text += f" {payload[key]}"
        eval_text = eval_text.strip()

        if eval_text:
            self.circuit_breaker.evaluate(
                eval_text,
                agent_id=f"llamaindex:{event_type}",
                swarm_id=self.swarm_id,
            )

        # 2. Resolve parent span ID from parent event ID
        resolved_parent_id = self._event_span_map.get(parent_event_id)

        # 3. Create SwarmSpan
        span = SwarmSpan(
            id=span_id,
            swarm_id=self.swarm_id,
            branch_id=self.branch_id,
            parent_span_id=resolved_parent_id,
            span_type=str(event_type),
            source_agent=f"llamaindex:{event_type}",
            target_agent="llamaindex_engine",
            input_payload=start_payload,
            output_payload=payload,
            start_time=start_time,
            end_time=end_time,
        )


        client = swarmwatch.get_client()
        if client:
            client.transport.emit_span(span)

    def start_trace(self, trace_id: Optional[str] = None) -> None:
        pass

    def end_trace(
        self,
        trace_id: Optional[str] = None,
        trace_map: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        pass
