# -*- coding: utf-8 -*-
"""
SwarmWatch Listener Adapter for CrewAI.
Unifies step_callback and task_callback to trace multi-agent execution,
prevent infinite agent thought loops, and snapshot state into the Time Machine.
"""

from datetime import datetime, timezone
import logging
from typing import Any, Optional

import swarmwatch
from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPreset,
    SwarmCircuitBreakException,
)

logger = logging.getLogger("swarmwatch.adapters.crewai")


class SwarmWatchCrewListener:
    """
    Unified listener for CrewAI Crew executions.
    Pass as step_callback=listener.on_step and task_callback=listener.on_task.
    """

    def __init__(
        self,
        swarm_id: Optional[str] = None,
        circuit_breaker: Optional[EntropyCircuitBreaker] = None,
        branch_id: str = "main",
    ):
        self.swarm_id = swarm_id or (swarmwatch.get_client().swarm_id if swarmwatch.get_client() else "crewai-swarm")
        self.circuit_breaker = circuit_breaker or EntropyCircuitBreaker(preset=CircuitPreset.STRICT)
        self.branch_id = branch_id

    def on_step(self, step_output: Any) -> None:
        """
        Invoked on each internal agent reasoning/tool step.
        """
        agent_name = getattr(step_output, "agent", "CrewAgent")
        thought = str(getattr(step_output, "thought", ""))
        tool = str(getattr(step_output, "tool", "environment"))
        tool_input = getattr(step_output, "tool_input", {})
        result = str(getattr(step_output, "result", ""))

        # 1. Circuit Breaker Loop Detection on Thought + Result
        eval_text = f"{thought} {result}".strip()
        if eval_text:
            self.circuit_breaker.evaluate(
                eval_text,
                agent_id=str(agent_name),
                swarm_id=self.swarm_id,
            )

        # 2. Emit Agent Step Span using client.span
        client = swarmwatch.get_client()
        with client.span(
            name=f"step:{tool}",
            source=str(agent_name),
            target=tool,
            span_type="agent_step",
            branch_id=self.branch_id,
            input_payload={"thought": thought, "tool_input": tool_input},
        ) as step_span:
            step_span.output_payload["result"] = result

    def on_task(self, task_output: Any) -> None:
        """
        Invoked on overall task completion.
        """
        agent_name = getattr(task_output, "agent", "CrewAgent")
        description = str(getattr(task_output, "description", "task"))
        raw_output = str(getattr(task_output, "raw", ""))

        client = swarmwatch.get_client()
        with client.span(
            name=f"task:{description}",
            source=str(agent_name),
            target="crew_manager",
            span_type="task_completed",
            branch_id=self.branch_id,
            input_payload={"description": description},
        ) as task_span:
            task_span.output_payload["raw"] = raw_output

            # Snapshot task state into Time Machine bound to this span
            swarmwatch.sync_state({
                "task_description": description,
                "task_result": raw_output,
                "completed_by": str(agent_name),
            })
