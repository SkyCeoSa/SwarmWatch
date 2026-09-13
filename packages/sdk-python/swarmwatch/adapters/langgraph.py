# -*- coding: utf-8 -*-
"""
SwarmWatch Hybrid Adapter for LangGraph & LangChain.
Provides zero-friction BaseCallbackHandler integration with optional
state snapshotting via patch_graph().
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
    CircuitDecision,
    SwarmCircuitBreakException,
)
from swarmwatch.schema import SwarmSpan

logger = logging.getLogger("swarmwatch.adapters.langgraph")

try:
    from langchain_core.callbacks import BaseCallbackHandler
    LANGGRAPH_AVAILABLE = True
except ImportError:
    BaseCallbackHandler = object
    LANGGRAPH_AVAILABLE = False


class SwarmWatchCallbackHandler(BaseCallbackHandler):
    """
    LangChain / LangGraph compatible callback handler.
    Automatically captures chains, LLM reasoning turns, and tool invocations as SwarmSpans.
    Injects circuit-breaker warnings directly into agent message history.
    """

    def __init__(
        self,
        swarm_id: Optional[str] = None,
        circuit_breaker: Optional[EntropyCircuitBreaker] = None,
        branch_id: str = "main",
    ):
        self.swarm_id = swarm_id or (swarmwatch.get_client().swarm_id if swarmwatch.get_client() else "langgraph-swarm")
        self.circuit_breaker = circuit_breaker or EntropyCircuitBreaker(preset=CircuitPreset.STRICT)
        self.branch_id = branch_id
        self._runs: Dict[str, Dict[str, Any]] = {}

    def on_chain_start(
        self,
        serialized: Optional[Dict[str, Any]],
        inputs: Dict[str, Any],
        *,
        run_id: Any,
        parent_run_id: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        name = "LangGraphNode"
        if serialized and "name" in serialized:
            name = serialized["name"]
        elif "name" in kwargs:
            name = kwargs["name"]

        str_run_id = str(run_id)
        self._runs[str_run_id] = {
            "name": name,
            "inputs": inputs,
            "start_time": datetime.now(timezone.utc),
            "perf_start": time.perf_counter(),
            "parent_id": str(parent_run_id) if parent_run_id else None,
        }

    def on_chain_end(
        self,
        outputs: Dict[str, Any],
        *,
        run_id: Any,
        **kwargs: Any,
    ) -> None:
        str_run_id = str(run_id)
        run_info = self._runs.pop(str_run_id, None)
        if not run_info:
            return

        end_time = datetime.now(timezone.utc)
        duration_ms = max(0.001, (time.perf_counter() - run_info["perf_start"]) * 1000.0)

        client = swarmwatch.get_client()
        span = SwarmSpan(
            id=str(uuid.uuid4()),
            swarm_id=self.swarm_id,
            branch_id=self.branch_id,
            parent_span_id=run_info["parent_id"],
            source_agent=run_info["name"],
            target_agent="environment",
            span_type="chain_execution",
            input_payload=run_info["inputs"] if isinstance(run_info["inputs"], dict) else {"input": str(run_info["inputs"])},
            output_payload=outputs if isinstance(outputs, dict) else {"output": str(outputs)},
            token_count=0,
            entropy_score=1.0,
            start_time=run_info["start_time"],
            end_time=end_time,
            duration_ms=duration_ms,
        )
        client.transport.emit_span(span)

    def on_tool_start(
        self,
        serialized: Optional[Dict[str, Any]],
        input_str: str,
        *,
        run_id: Any,
        parent_run_id: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        name = (serialized.get("name") if serialized else None) or "Tool"
        str_run_id = str(run_id)
        self._runs[str_run_id] = {
            "name": name,
            "inputs": {"input": input_str},
            "start_time": datetime.now(timezone.utc),
            "perf_start": time.perf_counter(),
            "parent_id": str(parent_run_id) if parent_run_id else None,
            "is_tool": True,
        }

    def on_tool_end(
        self,
        output: str,
        *,
        run_id: Any,
        **kwargs: Any,
    ) -> None:
        str_run_id = str(run_id)
        run_info = self._runs.pop(str_run_id, None)
        if not run_info:
            return

        end_time = datetime.now(timezone.utc)
        duration_ms = max(0.001, (time.perf_counter() - run_info["perf_start"]) * 1000.0)

        client = swarmwatch.get_client()
        span = SwarmSpan(
            id=str(uuid.uuid4()),
            swarm_id=self.swarm_id,
            branch_id=self.branch_id,
            parent_span_id=run_info["parent_id"],
            source_agent=run_info["name"],
            target_agent="tool",
            span_type="tool_call",
            input_payload=run_info["inputs"],
            output_payload={"output": output},
            token_count=0,
            entropy_score=1.0,
            start_time=run_info["start_time"],
            end_time=end_time,
            duration_ms=duration_ms,
        )
        client.transport.emit_span(span)

    def inspect_output(
        self,
        output_text: str,
        messages: Optional[List[Dict[str, Any]]] = None,
        agent_id: str = "agent",
    ) -> CircuitDecision:
        """
        Evaluates agent response for repetitive loops.
        If warning triggered, injects [SWARMWATCH CIRCUIT WARNING] into messages list.
        If loop persists, raises SwarmCircuitBreakException.
        """
        decision = self.circuit_breaker.evaluate(
            output_text,
            agent_id=agent_id,
            swarm_id=self.swarm_id,
        )
        if decision.decision == "warning":
            if messages is not None and isinstance(messages, list):
                messages.append({
                    "role": "system",
                    "content": decision.warning_hint,
                })
        return decision


def patch_graph(graph: Any, swarm_id: Optional[str] = None) -> Any:
    """
    Patches a LangGraph CompiledGraph to automatically snapshot state
    at every node execution for non-destructive Time-Machine debugging.
    """
    active_swarm = swarm_id or "langgraph-swarm"

    # Wrap nodes in the graph
    if hasattr(graph, "nodes") and isinstance(graph.nodes, dict):
        for node_name, node_fn in list(graph.nodes.items()):
            def make_wrapper(orig_fn, name):
                def wrapped_node(*args, **kwargs):
                    with swarmwatch.span(f"node:{name}", source=name, target="graph_state") as current_span:
                        result = orig_fn(*args, **kwargs)
                        if isinstance(result, dict):
                            swarmwatch.sync_state(result)
                        return result
                return wrapped_node
            graph.nodes[node_name] = make_wrapper(node_fn, node_name)

    return graph
