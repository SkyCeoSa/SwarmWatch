# -*- coding: utf-8 -*-
"""
SwarmWatch Listener Adapter for Microsoft AutoGen (Seam 5).

Integrates via register_hook or conversation turn processing. Traces multi-agent
dialogues, measures token metrics, and halts conversational ping-pong deadlocks.
"""

from datetime import datetime, timezone
import logging
import time
from typing import Any, Dict, Optional, Union
import uuid

import swarmwatch
from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPreset,
    SwarmCircuitBreakException,
)
from swarmwatch.schema import SwarmSpan

logger = logging.getLogger("swarmwatch.adapters.autogen")


class SwarmWatchAutoGenListener:
    """
    Unified listener for Microsoft AutoGen ConversableAgent swarms.
    Attaches to agents via register_hook(hookable_method="process_message_before_send")
    or direct message interception.
    """

    def __init__(
        self,
        swarm_id: Optional[str] = None,
        circuit_breaker: Optional[EntropyCircuitBreaker] = None,
        branch_id: str = "main",
    ):
        self.swarm_id = swarm_id or (swarmwatch.get_client().swarm_id if swarmwatch.get_client() else "autogen-swarm")
        self.circuit_breaker = circuit_breaker or EntropyCircuitBreaker(preset=CircuitPreset.STRICT)
        self.branch_id = branch_id

    def register(self, agent: Any) -> None:
        """Registers listener onto an AutoGen ConversableAgent using official register_hook."""
        if hasattr(agent, "register_hook"):
            agent.register_hook(
                hookable_method="process_message_before_send",
                hook=self.process_message_before_send,
            )
        else:
            logger.warning("Agent does not support register_hook; conversational tracing must be called manually.")

    def process_message_before_send(
        self,
        sender: Any,
        message: Union[str, Dict[str, Any]],
        recipient: Any,
        silent: bool = False,
    ) -> Union[str, Dict[str, Any]]:
        """
        Hook executed by AutoGen immediately before a message is transmitted.
        Evaluates dialogue entropy to break conversational ping-pong loops and records a SwarmSpan.
        """
        sender_name = getattr(sender, "name", str(sender))
        recipient_name = getattr(recipient, "name", str(recipient))

        content_text = ""
        if isinstance(message, dict):
            content_text = str(message.get("content", ""))
        else:
            content_text = str(message)

        # 1. Evaluate conversational turn with circuit breaker
        if content_text.strip():
            self.circuit_breaker.evaluate(
                content_text,
                agent_id=str(sender_name),
                swarm_id=self.swarm_id,
            )

        # 2. Emit SwarmSpan for this ConversationalTurn
        client = swarmwatch.get_client()
        if client:
            span_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            span = SwarmSpan(
                span_id=span_id,
                swarm_id=self.swarm_id,
                branch_id=self.branch_id,
                span_type="conversational_turn",
                source_agent=str(sender_name),
                target_agent=str(recipient_name),
                input_payload={
                    "sender": str(sender_name),
                    "recipient": str(recipient_name),
                    "content": content_text,
                    "raw": message,
                },
                output_payload={"status": "sent"},
                start_time=now,
                end_time=now,
            )
            client.transport.emit_span(span)

        # Return original message unchanged
        return message
