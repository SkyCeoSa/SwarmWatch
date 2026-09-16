# -*- coding: utf-8 -*-
"""
TDD Tests for SwarmWatch LlamaIndex Adapter (Seam 5).
Verifies BaseCallbackHandler event mapping, parent-child causal DAG tracking,
and proactive Shannon entropy circuit breaker halts on query loops.
"""

from unittest.mock import MagicMock
import pytest

import swarmwatch
from swarmwatch.adapters.llamaindex import SwarmWatchLlamaIndexHandler
from swarmwatch.circuit import SwarmCircuitBreakException, EntropyCircuitBreaker, CircuitPreset


def test_llamaindex_handler_event_lifecycle(tmp_path):
    db_path = str(tmp_path / "llamaindex_lifecycle.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-llamaindex-lifecycle")
    handler = SwarmWatchLlamaIndexHandler(swarm_id="swarm-llamaindex-lifecycle")

    # Start and end LLM event
    handler.on_event_start(
        event_type="llm",
        payload={"prompt": "Analyze market indicators for Q4"},
        event_id="evt-llm-1",
    )
    handler.on_event_end(
        event_type="llm",
        payload={"response": "Q4 shows robust bullish momentum across tech equities."},
        event_id="evt-llm-1",
    )

    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-llamaindex-lifecycle")
    assert len(spans) == 1
    span = spans[0]
    assert span.span_type == "llm"
    assert span.input_payload["prompt"] == "Analyze market indicators for Q4"
    assert span.output_payload["response"] == "Q4 shows robust bullish momentum across tech equities."


def test_llamaindex_parent_child_causal_dag(tmp_path):
    db_path = str(tmp_path / "llamaindex_causal.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-llamaindex-causal")
    handler = SwarmWatchLlamaIndexHandler(swarm_id="swarm-llamaindex-causal")

    # Parent event: query
    handler.on_event_start(
        event_type="query",
        payload={"query_str": "Find relevant financial disclosure"},
        event_id="evt-root",
    )

    # Child event: retrieve with parent_id
    handler.on_event_start(
        event_type="retrieve",
        payload={"nodes": ["doc_1", "doc_2"]},
        event_id="evt-child",
        parent_id="evt-root",
    )
    handler.on_event_end(
        event_type="retrieve",
        payload={"nodes_count": 2},
        event_id="evt-child",
    )

    handler.on_event_end(
        event_type="query",
        payload={"status": "completed"},
        event_id="evt-root",
    )

    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-llamaindex-causal")
    assert len(spans) == 2

    child_span = next(s for s in spans if s.span_type == "retrieve")
    root_span = next(s for s in spans if s.span_type == "query")
    assert child_span.parent_id == root_span.span_id


def test_llamaindex_circuit_breaker_halts_loop(tmp_path):
    db_path = str(tmp_path / "llamaindex_loop.db")
    swarmwatch.init(db_path=db_path, swarm_id="swarm-llamaindex-loop")

    circuit = EntropyCircuitBreaker(preset=CircuitPreset.STRICT)
    handler = SwarmWatchLlamaIndexHandler(
        swarm_id="swarm-llamaindex-loop",
        circuit_breaker=circuit,
    )

    loop_text = "Retrying query index because vector distance threshold not met."

    # Turn 1: passes
    handler.on_event_start(
        event_type="agent_step",
        payload={"thought": loop_text},
        event_id="evt-loop-1",
    )
    handler.on_event_end(
        event_type="agent_step",
        payload={"result": loop_text},
        event_id="evt-loop-1",
    )

    # Turn 2: warning or circuit trips on identical repetition
    with pytest.raises(SwarmCircuitBreakException):
        handler.on_event_start(
            event_type="agent_step",
            payload={"thought": loop_text},
            event_id="evt-loop-2",
        )
        handler.on_event_end(
            event_type="agent_step",
            payload={"result": loop_text},
            event_id="evt-loop-2",
        )
