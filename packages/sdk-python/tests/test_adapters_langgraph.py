"""
TDD Tests for SwarmWatch LangGraph Hybrid Adapter (Callback + State Patcher)
"""
import pytest
from unittest.mock import MagicMock
from swarmwatch.adapters.langgraph import (
    SwarmWatchCallbackHandler,
    patch_graph,
    LANGGRAPH_AVAILABLE,
)
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.circuit import SwarmCircuitBreakException
import swarmwatch


def test_langgraph_callback_handler_emits_spans(tmp_path):
    db_path = str(tmp_path / "langgraph_adapter.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-langgraph-test")

    handler = SwarmWatchCallbackHandler(swarm_id="swarm-langgraph-test")

    # Simulate LangChain / LangGraph chain start and end
    run_id = "test-run-123"
    handler.on_chain_start(
        serialized={"name": "ResearchChain"},
        inputs={"topic": "Quantum Computing"},
        run_id=run_id,
    )
    handler.on_chain_end(
        outputs={"summary": "Quantum Computing advances in 2026"},
        run_id=run_id,
    )

    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-langgraph-test")
    assert len(spans) == 1
    assert spans[0].source_agent == "ResearchChain"
    assert spans[0].input_payload["topic"] == "Quantum Computing"
    assert spans[0].output_payload["summary"] == "Quantum Computing advances in 2026"
    assert spans[0].duration_ms >= 0


def test_langgraph_circuit_breaker_prompt_injection(tmp_path):
    db_path = str(tmp_path / "langgraph_circuit.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-langgraph-circuit")

    handler = SwarmWatchCallbackHandler(swarm_id="swarm-langgraph-circuit")

    loop_text = "Failed to access file. Retrying identical request..."
    messages = [{"role": "user", "content": "Fetch data"}]

    # Step 1: Warning injected into messages
    decision = handler.inspect_output(loop_text, messages=messages)
    assert decision.decision == "pass"

    # Step 2: Loop triggered -> Warning hint injected as system message
    decision2 = handler.inspect_output(loop_text, messages=messages)
    assert decision2.decision == "warning"
    assert any("[SWARMWATCH CIRCUIT WARNING]" in m.get("content", "") for m in messages)

    # Step 3: Repeated again -> Halts execution
    with pytest.raises(SwarmCircuitBreakException):
        handler.inspect_output(loop_text, messages=messages)


def test_patch_graph_state_snapshotting(tmp_path):
    db_path = str(tmp_path / "langgraph_patch.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-patch-test")

    # Mock a LangGraph CompiledGraph
    class DummyGraph:
        def __init__(self):
            self.nodes = {}

        def invoke(self, state, config=None):
            # Execute node
            for name, node_fn in self.nodes.items():
                state = node_fn(state)
            return state

    graph = DummyGraph()
    def sample_node(state):
        return {**state, "step_1": "executed"}
    graph.nodes["node_1"] = sample_node

    patched = patch_graph(graph, swarm_id="swarm-patch-test")
    final_state = patched.invoke({"initial": "data"})

    assert final_state["step_1"] == "executed"
    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-patch-test")
    assert len(spans) >= 1
