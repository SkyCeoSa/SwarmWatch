"""
TDD Tests for Ticket 05: Safe State Serializer & Time-Machine Branching
"""
import pytest
from swarmwatch.serializer import SafeStateSerializer, TimeMachine
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.schema import SwarmSpan, StateSnapshot
import swarmwatch


class CustomObject:
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return f"<CustomObject value={self.value}>"


def test_safe_serializer_handles_non_serializable_and_redacts_keys():
    serializer = SafeStateSerializer()

    raw_state = {
        "user_id": 42,
        "api_key": "sk-proj-abc1234567890abcdef1234567890",
        "nested": {
            "token": "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            "db_pass": "super_secret_password_123",
            "connection": CustomObject("live_socket"),
        },
        "circular": None
    }
    # Create circular reference
    raw_state["circular"] = raw_state

    sanitized = serializer.sanitize(raw_state)

    # 1. Non-serializable handled safely without crashing
    assert "<CustomObject value=live_socket>" in str(sanitized["nested"]["connection"])

    # 2. Circular reference replaced safely
    assert sanitized["circular"] == "[Circular Reference]"

    # 3. Secret redaction
    assert "sk-proj-abc1234567890" not in str(sanitized)
    assert "[REDACTED]" in sanitized["api_key"]
    assert "[REDACTED]" in sanitized["nested"]["token"]


@pytest.mark.asyncio
async def test_time_machine_step_back_and_branch_forking(tmp_path):
    db_path = str(tmp_path / "time_machine.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    time_machine = TimeMachine(storage=storage)

    # 1. Setup an initial span and state snapshot on main branch
    span_1 = SwarmSpan(
        id="span-tm-01",
        swarm_id="swarm-tm",
        branch_id="main",
        source_agent="planner",
        target_agent="executor",
        span_type="agent_call",
        input_payload={"goal": "calculate square"},
        output_payload={"state": "ready"},
    )
    await storage.save_spans([span_1])

    snapshot_data = {
        "step": 1,
        "variables": {"x": 10, "status": "initialized"},
        "history": ["start"]
    }
    snap = await time_machine.snapshot(
        span_id="span-tm-01",
        state_data=snapshot_data,
        branch_id="main"
    )
    assert snap.state_hash is not None

    # 2. Add second span on main branch with mutated state
    span_2 = SwarmSpan(
        id="span-tm-02",
        swarm_id="swarm-tm",
        branch_id="main",
        parent_span_id="span-tm-01",
        source_agent="executor",
        target_agent="evaluator",
        span_type="agent_call",
        input_payload={"action": "buggy_step"},
        output_payload={"error": "corrupted"},
    )
    await storage.save_spans([span_2])

    # 3. Time Machine: Step back to span-tm-01 and fork execution!
    forked_branch_id = "fork-01"
    restored_state = await time_machine.step_back(
        target_span_id="span-tm-01",
        new_branch_id=forked_branch_id
    )

    # 4. Verify restored state matches snapshot
    assert restored_state["variables"]["x"] == 10
    assert restored_state["step"] == 1

    # 5. Verify isolated branch creation
    # Emit a new span on the forked branch
    span_forked = SwarmSpan(
        id="span-tm-fork-01",
        swarm_id="swarm-tm",
        branch_id=forked_branch_id,
        parent_span_id="span-tm-01",
        source_agent="executor",
        target_agent="evaluator",
        span_type="agent_call",
        input_payload={"action": "corrected_step"},
        output_payload={"result": 100},
    )
    await storage.save_spans([span_forked])

    # Query main vs forked
    main_spans = await storage.get_swarm_spans(swarm_id="swarm-tm", branch_id="main")
    fork_spans = await storage.get_swarm_spans(swarm_id="swarm-tm", branch_id=forked_branch_id)

    assert len(main_spans) == 2
    assert len(fork_spans) == 1
    assert fork_spans[0].output_payload["result"] == 100
    assert fork_spans[0].branch_id == "fork-01"

    branches = await storage.list_branches(swarm_id="swarm-tm")
    assert "main" in branches
    assert "fork-01" in branches

    await storage.close()


def test_swarmwatch_sync_state_integration(tmp_path):
    db_path = str(tmp_path / "sync_state.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-sync-state")

    with swarmwatch.span("stateful_agent_step", source="agent_a", target="agent_b") as current_span:
        my_agent_state = {"memory": ["item1", "item2"], "confidence": 0.95}
        snap = swarmwatch.sync_state(my_agent_state)
        assert snap.span_id == current_span.id
        assert snap.state_data["confidence"] == 0.95

    swarmwatch.flush()
    swarmwatch.shutdown()
