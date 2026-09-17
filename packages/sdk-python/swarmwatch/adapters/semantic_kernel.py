# -*- coding: utf-8 -*-
"""
SwarmWatch Adapter for Microsoft Semantic Kernel (Seam 5 & ADR 0018).

Provides non-invasive function invocation filter integration, tracing tool calls
and multi-agent plan steps, and tripping Shannon entropy circuit breakers on
recursive or cyclic tool loops.
"""

from datetime import datetime, timezone
import logging
import time
from typing import Any, Awaitable, Callable, Dict, Optional
import uuid

import swarmwatch
from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPreset,
    SwarmCircuitBreakException,
)
from swarmwatch.schema import SwarmSpan

logger = logging.getLogger("swarmwatch.adapters.semantic_kernel")


class FunctionInvocationContext:
    """
    Standard or mock invocation context for Microsoft Semantic Kernel function filters.
    Encapsulates plugin name, function name, arguments, and function result.
    """

    def __init__(
        self,
        plugin_name: str,
        function_name: str,
        arguments: Optional[Dict[str, Any]] = None,
        result: Any = None,
    ):
        self.plugin_name = plugin_name
        self.function_name = function_name
        self.arguments = arguments or {}
        self.result = result
        # Compatibility with semantic_kernel.functions.kernel_function_metadata
        self.function = self

    @property
    def name(self) -> str:
        return self.function_name


class SwarmWatchSemanticKernelFilter:
    """
    Microsoft Semantic Kernel function invocation filter.
    Intercepts function/tool executions in Semantic Kernel swarms,
    proactively evaluates argument entropy to halt runaway tool recursion,
    and records SwarmSpan telemetry.
    """

    def __init__(
        self,
        swarm_id: Optional[str] = None,
        circuit_breaker: Optional[EntropyCircuitBreaker] = None,
        branch_id: str = "main",
    ):
        self.swarm_id = swarm_id or (
            swarmwatch.get_client().swarm_id
            if swarmwatch.get_client()
            else "semantic-kernel-swarm"
        )
        self.circuit_breaker = circuit_breaker or EntropyCircuitBreaker(
            preset=CircuitPreset.STRICT
        )
        self.branch_id = branch_id

    def on_function_invocation(
        self,
        context: Any,
        next_step: Callable[[Any], None],
    ) -> None:
        """
        Synchronous function invocation filter hook for Semantic Kernel.
        """
        plugin_name = getattr(context, "plugin_name", "SemanticKernelPlugin")
        function_name = getattr(context, "function_name", getattr(getattr(context, "function", None), "name", "tool"))
        args = getattr(context, "arguments", {}) or {}

        # 1. Proactive circuit breaker check on tool call arguments
        eval_text = f"{plugin_name}:{function_name} " + " ".join(str(v) for v in args.values() if v)
        if eval_text.strip():
            self.circuit_breaker.evaluate(
                eval_text,
                agent_id=str(plugin_name),
                swarm_id=self.swarm_id,
            )

        # 2. Execute inner function
        t0 = time.perf_counter()
        next_step(context)
        duration_ms = (time.perf_counter() - t0) * 1000.0

        # 3. Emit SwarmSpan
        self._emit_span(context, plugin_name, function_name, args, duration_ms)

    async def on_function_invocation_async(
        self,
        context: Any,
        next_step: Callable[[Any], Awaitable[None]],
    ) -> None:
        """
        Asynchronous function invocation filter hook for Semantic Kernel.
        """
        plugin_name = getattr(context, "plugin_name", "SemanticKernelPlugin")
        function_name = getattr(context, "function_name", getattr(getattr(context, "function", None), "name", "tool"))
        args = getattr(context, "arguments", {}) or {}

        # 1. Proactive circuit breaker check on tool call arguments
        eval_text = f"{plugin_name}:{function_name} " + " ".join(str(v) for v in args.values() if v)
        if eval_text.strip():
            self.circuit_breaker.evaluate(
                eval_text,
                agent_id=str(plugin_name),
                swarm_id=self.swarm_id,
            )

        # 2. Execute inner async function
        t0 = time.perf_counter()
        await next_step(context)
        duration_ms = (time.perf_counter() - t0) * 1000.0

        # 3. Emit SwarmSpan
        self._emit_span(context, plugin_name, function_name, args, duration_ms)

    def _emit_span(
        self,
        context: Any,
        plugin_name: str,
        function_name: str,
        args: Dict[str, Any],
        duration_ms: float,
    ) -> None:
        client = swarmwatch.get_client()
        if not client:
            return

        res = getattr(context, "result", None)
        output_payload = res if isinstance(res, dict) else {"result": str(res)}
        raw_text = str(args) + str(output_payload)
        token_count = max(1, len(raw_text) // 4)

        span_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        span = SwarmSpan(
            span_id=span_id,
            swarm_id=self.swarm_id,
            branch_id=self.branch_id,
            span_type="function_filter_invocation",
            source_agent=str(plugin_name),
            target_agent=str(function_name),
            input_payload=args,
            output_payload=output_payload,
            token_count=token_count,
            duration_ms=int(duration_ms),
            start_time=now,
            end_time=now,
        )
        client.transport.emit_span(span)
