"""
TDD Tests for Ticket 03: Public Developer Surface (Decorators & Context Managers)
"""
import asyncio
import os
import pytest
from unittest.mock import MagicMock

import swarmwatch
from swarmwatch.schema import SwarmSpan
from swarmwatch.storage import SQLiteStorageAdapter


@pytest.fixture(autouse=True)
def reset_swarmwatch_client():
    # Teardown before and after each test
    yield
    swarmwatch.shutdown()


def test_init_and_sync_agent_decorator(tmp_path):
    db_path = str(tmp_path / "sync_agent.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-sync-test")

    @swarmwatch.agent(name="analyst_bob", role="analyst")
    def analyze_data(query: str, limit: int = 5):
        return {"result": f"Analyzed {query}", "count": limit}

    res = analyze_data("Q3 Revenue", limit=10)
    assert res == {"result": "Analyzed Q3 Revenue", "count": 10}

    swarmwatch.flush()

    storage = client.storage
    spans = storage.query_spans(swarm_id="swarm-sync-test")
    assert len(spans) == 1
    span = spans[0]
    assert span.source_agent == "analyst_bob"
    assert span.input_payload["query"] == "Q3 Revenue"
    assert span.input_payload["limit"] == 10
    assert span.output_payload["result"] == "Analyzed Q3 Revenue"
    assert span.duration_ms > 0


@pytest.mark.asyncio
async def test_async_agent_decorator(tmp_path):
    db_path = str(tmp_path / "async_agent.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-async-test")

    @swarmwatch.agent(name="async_researcher", role="researcher")
    async def fetch_insights(topic: str):
        await asyncio.sleep(0.01)
        return {"topic": topic, "status": "complete"}

    res = await fetch_insights("Quantum Computing")
    assert res == {"topic": "Quantum Computing", "status": "complete"}

    swarmwatch.flush()

    storage = client.storage
    spans = storage.query_spans(swarm_id="swarm-async-test")
    assert len(spans) == 1
    span = spans[0]
    assert span.source_agent == "async_researcher"
    assert span.input_payload["topic"] == "Quantum Computing"
    assert span.output_payload["status"] == "complete"


def test_agent_decorator_exception_handling(tmp_path):
    db_path = str(tmp_path / "error_agent.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-error-test")

    @swarmwatch.agent(name="buggy_agent", role="debugger")
    def faulty_action():
        raise ValueError("Simulated catastrophic agent failure")

    with pytest.raises(ValueError, match="Simulated catastrophic agent failure"):
        faulty_action()

    swarmwatch.flush()

    storage = client.storage
    spans = storage.query_spans(swarm_id="swarm-error-test")
    assert len(spans) == 1
    span = spans[0]
    assert span.source_agent == "buggy_agent"
    assert span.output_payload["error"] == "ValueError"
    assert "Simulated catastrophic agent failure" in span.output_payload["message"]


def test_nested_span_context_managers(tmp_path):
    db_path = str(tmp_path / "nested_spans.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-nested-test")

    with swarmwatch.span("planning_phase", source="coordinator", target="planner") as outer_span:
        assert outer_span.parent_span_id is None

        with swarmwatch.span("subtask_execution", source="planner", target="worker") as inner_span:
            assert inner_span.parent_span_id == outer_span.id

    swarmwatch.flush()

    storage = client.storage
    spans = storage.query_spans(swarm_id="swarm-nested-test")
    assert len(spans) == 2

    # Verify parent-child DAG linkage
    inner = [s for s in spans if s.target_agent == "worker"][0]
    outer = [s for s in spans if s.target_agent == "planner"][0]
    assert inner.parent_span_id == outer.id
