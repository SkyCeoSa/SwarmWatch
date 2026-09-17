# -*- coding: utf-8 -*-
"""
SwarmWatch Adapter for Stanford DSPy (Seam 5 & ADR 0018).

Provides non-invasive programmatic tracking, context-manager wrappers,
teleprompter candidate generation monitoring, and Shannon entropy circuit
breaking against runaway optimization loops.
"""

from datetime import datetime, timezone
import logging
import time
from typing import Any, Callable, Dict, Optional
import uuid

import swarmwatch
from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPreset,
    SwarmCircuitBreakException,
)
from swarmwatch.schema import SwarmSpan

logger = logging.getLogger("swarmwatch.adapters.dspy")


class SwarmWatchDSPyTracker:
    """
    Context manager and programmatic tracer for Stanford DSPy programs and teleprompters.
    Captures module forward executions and teleprompter optimization rounds as SwarmSpans,
    and trips the EntropyCircuitBreaker when degenerate prompt loops or repetition occurs.
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
            else "dspy-swarm"
        )
        self.circuit_breaker = circuit_breaker or EntropyCircuitBreaker(
            preset=CircuitPreset.STRICT
        )
        self.branch_id = branch_id

    def __enter__(self) -> "SwarmWatchDSPyTracker":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def track_forward(
        self,
        module_name: str,
        input_kwargs: Dict[str, Any],
        output: Any,
        duration_ms: Optional[float] = None,
    ) -> SwarmSpan:
        """
        Records a DSPy module forward execution (e.g. Predict, ChainOfThought).
        Evaluates input/output entropy and records a SwarmSpan.
        """
        # 1. Evaluate input and output content for repetition loops
        eval_text = " ".join(str(v) for v in input_kwargs.values() if v)
        if isinstance(output, str):
            eval_text += f" {output}"
        elif isinstance(output, dict):
            eval_text += " " + " ".join(str(v) for v in output.values() if v)

        if eval_text.strip():
            self.circuit_breaker.evaluate(
                eval_text,
                agent_id=f"dspy:{module_name}",
                swarm_id=self.swarm_id,
            )

        # 2. Approximate tokens (1 token per 4 chars)
        token_count = max(1, len(eval_text) // 4)
        span_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        span = SwarmSpan(
            span_id=span_id,
            swarm_id=self.swarm_id,
            branch_id=self.branch_id,
            span_type="dspy_module",
            source_agent=f"dspy:{module_name}",
            target_agent="dspy_program",
            input_payload=input_kwargs,
            output_payload={"result": str(output)} if not isinstance(output, dict) else output,
            token_count=token_count,
            duration_ms=int(duration_ms) if duration_ms is not None else 350,
            start_time=now,
            end_time=now,
        )

        client = swarmwatch.get_client()
        if client:
            client.transport.emit_span(span)

        return span

    def track_optimization_step(
        self,
        teleprompter: str,
        candidate_id: str,
        prompt: str,
        metric_score: Optional[float] = None,
        duration_ms: Optional[float] = None,
    ) -> SwarmSpan:
        """
        Records an automated prompt compiler / teleprompter optimization iteration
        (e.g. BootstrapFewShot, MIPRO, SignatureOptimizer).
        Halts the teleprompter loop if candidate prompt variations collapse in entropy.
        """
        # 1. Proactive circuit breaker evaluation on candidate prompt
        if prompt.strip():
            self.circuit_breaker.evaluate(
                prompt,
                agent_id=f"dspy:{teleprompter}",
                swarm_id=self.swarm_id,
            )

        token_count = max(1, len(prompt) // 4)
        span_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        span = SwarmSpan(
            span_id=span_id,
            swarm_id=self.swarm_id,
            branch_id=self.branch_id,
            span_type="teleprompter_iteration",
            source_agent=f"dspy:{teleprompter}",
            target_agent="dspy_compiler",
            input_payload={"candidate_id": candidate_id, "prompt": prompt},
            output_payload={"metric_score": metric_score} if metric_score is not None else {},
            token_count=token_count,
            duration_ms=int(duration_ms) if duration_ms is not None else 500,
            start_time=now,
            end_time=now,
        )

        client = swarmwatch.get_client()
        if client:
            client.transport.emit_span(span)

        return span

    def wrap(self, func: Callable[..., Any], module_name: Optional[str] = None) -> Callable[..., Any]:
        """Wraps a callable or forward method to track execution automatically."""
        name = module_name or getattr(func, "__name__", "Module")

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            t0 = time.perf_counter()
            result = func(*args, **kwargs)
            duration_ms = (time.perf_counter() - t0) * 1000.0
            self.track_forward(
                module_name=name,
                input_kwargs=kwargs or {"args": [str(a) for a in args]},
                output=result,
                duration_ms=duration_ms,
            )
            return result

        return wrapper
