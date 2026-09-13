"""
TDD Tests for SwarmWatch CrewAI Listener Adapter (Step + Task Hook)
"""
import pytest
from unittest.mock import MagicMock
from swarmwatch.adapters.crewai import SwarmWatchCrewListener
from swarmwatch.circuit import SwarmCircuitBreakException
import swarmwatch


def test_crewai_listener_step_and_task_callbacks(tmp_path):
    db_path = str(tmp_path / "crewai_adapter.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-crewai-test")

    listener = SwarmWatchCrewListener(swarm_id="swarm-crewai-test")

    # 1. Simulate agent step callback
    mock_step_output = MagicMock()
    mock_step_output.agent = "ResearcherAgent"
    mock_step_output.thought = "Searching market databases..."
    mock_step_output.tool = "WebSearch"
    mock_step_output.tool_input = {"query": "AI Agents"}
    mock_step_output.result = "Found 15 high-ranking papers"

    listener.on_step(mock_step_output)

    # 2. Simulate task completion callback
    mock_task_output = MagicMock()
    mock_task_output.agent = "ResearcherAgent"
    mock_task_output.description = "Gather AI market signals"
    mock_task_output.raw = "Final market signal summary"

    listener.on_task(mock_task_output)

    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-crewai-test")
    assert len(spans) == 2
    assert spans[0].span_type == "agent_step"
    assert spans[0].source_agent == "ResearcherAgent"
    assert spans[1].span_type == "task_completed"
    assert spans[1].output_payload["raw"] == "Final market signal summary"


def test_crewai_circuit_breaker_halts_loop(tmp_path):
    db_path = str(tmp_path / "crewai_circuit.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-crewai-loop")

    listener = SwarmWatchCrewListener(swarm_id="swarm-crewai-loop")

    mock_stuck_output = MagicMock()
    mock_stuck_output.agent = "StuckCoder"
    mock_stuck_output.thought = "Cannot fix indentation error on line 42. Retrying..."
    mock_stuck_output.tool = "PythonCodeRunner"
    mock_stuck_output.tool_input = {"file": "app.py"}
    mock_stuck_output.result = "SyntaxError: indentation"

    # Turn 1: Pass
    listener.on_step(mock_stuck_output)

    # Turn 2: Warning
    listener.on_step(mock_stuck_output)

    # Turn 3: Trip & halt
    with pytest.raises(SwarmCircuitBreakException):
        listener.on_step(mock_stuck_output)
