# -*- coding: utf-8 -*-
"""
TDD Tests for SwarmWatch Microsoft AutoGen Adapter (Seam 5).
Verifies ConversationalTurn tracking, register_hook integration,
and circuit breaker intervention on conversational ping-pong loops.
"""

from unittest.mock import MagicMock
import pytest

import swarmwatch
from swarmwatch.adapters.autogen import SwarmWatchAutoGenListener
from swarmwatch.circuit import SwarmCircuitBreakException, EntropyCircuitBreaker, CircuitPreset


def test_autogen_listener_message_interception(tmp_path):
    db_path = str(tmp_path / "autogen_intercept.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-autogen-intercept")
    listener = SwarmWatchAutoGenListener(swarm_id="swarm-autogen-intercept")

    mock_sender = MagicMock()
    mock_sender.name = "UserProxy"
    mock_recipient = MagicMock()
    mock_recipient.name = "CodeReviewer"

    message = {"content": "Review the pull request for memory leaks.", "role": "user"}

    # Process message before send
    returned = listener.process_message_before_send(
        sender=mock_sender,
        message=message,
        recipient=mock_recipient,
    )
    assert returned == message

    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-autogen-intercept")
    assert len(spans) == 1
    span = spans[0]
    assert span.source_agent == "UserProxy"
    assert span.span_type == "conversational_turn"
    assert span.input_payload["recipient"] == "CodeReviewer"
    assert "Review the pull request" in span.input_payload["content"]


def test_autogen_register_hook_integration():
    listener = SwarmWatchAutoGenListener(swarm_id="swarm-hook-test")
    mock_agent = MagicMock()

    listener.register(mock_agent)

    mock_agent.register_hook.assert_called_once()
    args, kwargs = mock_agent.register_hook.call_args
    assert kwargs.get("hookable_method") == "process_message_before_send"


def test_autogen_conversational_ping_pong_circuit_breaker(tmp_path):
    db_path = str(tmp_path / "autogen_pingpong.db")
    swarmwatch.init(db_path=db_path, swarm_id="swarm-autogen-pingpong")

    circuit = EntropyCircuitBreaker(preset=CircuitPreset.STRICT)
    listener = SwarmWatchAutoGenListener(
        swarm_id="swarm-autogen-pingpong",
        circuit_breaker=circuit,
    )

    agent_a = MagicMock()
    agent_a.name = "AgentA"
    agent_b = MagicMock()
    agent_b.name = "AgentB"

    # Conversational dead loop: repeated identical messages
    deadlock_msg = "Thank you very much for your cooperation. Have a great day!"

    # Turn 1: Pass
    listener.process_message_before_send(sender=agent_a, message=deadlock_msg, recipient=agent_b)

    # Turn 2: Warning hint issued (consecutive warning 1 of 1)
    listener.process_message_before_send(sender=agent_b, message=deadlock_msg, recipient=agent_a)

    # Turn 3: Hard trip on consecutive identical repetition
    with pytest.raises(SwarmCircuitBreakException):
        listener.process_message_before_send(sender=agent_a, message=deadlock_msg, recipient=agent_b)

