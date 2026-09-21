"""
TDD Tests for Ticket 05: Safe State Serializer & Time-Machine Branching
"""
import pytest
from swarmwatch.serializer import (
    SafeStateSerializer,
    TimeMachine,
    compute_state_diff,
    DiffEntry,
    DiffSummary,
    StateDiffReport,
)
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


def test_compute_state_diff_empty_and_identical():
    # Empty states
    report = compute_state_diff({}, {})
    assert isinstance(report, StateDiffReport)
    assert report.entries == []
    assert report.summary.total_changes == 0
    assert report.summary.unchanged == 0

    # Identical non-empty states
    state = {"role": "agent", "budget": 100, "meta": {"retries": 3}}
    report2 = compute_state_diff(state, state)
    assert report2.summary.total_changes == 0
    assert report2.summary.unchanged == 3
    assert report2.summary.added == 0
    assert report2.summary.modified == 0
    assert report2.summary.removed == 0


def test_compute_state_diff_mutations_and_summary():
    state_a = {
        "status": "pending",
        "counter": 1,
        "config": {"model": "gemini", "max_tokens": 1000},
        "deprecated_key": "legacy_val",
        "same": 42,
    }
    state_b = {
        "status": "active",
        "counter": 2,
        "config": {"model": "gemini-pro", "max_tokens": 1000},
        "new_key": "fresh_val",
        "same": 42,
    }

    report = compute_state_diff(state_a, state_b, span_id_a="span-01", span_id_b="span-02")
    assert report.span_id_a == "span-01"
    assert report.span_id_b == "span-02"

    entries_by_path = {e.path: e for e in report.entries}
    assert "new_key" in entries_by_path
    assert entries_by_path["new_key"].kind == "added"
    assert entries_by_path["new_key"].new_value == "fresh_val"

    assert "deprecated_key" in entries_by_path
    assert entries_by_path["deprecated_key"].kind == "removed"
    assert entries_by_path["deprecated_key"].old_value == "legacy_val"

    assert "status" in entries_by_path
    assert entries_by_path["status"].kind == "modified"
    assert entries_by_path["status"].old_value == "pending"
    assert entries_by_path["status"].new_value == "active"

    assert "config.model" in entries_by_path
    assert entries_by_path["config.model"].kind == "modified"

    assert "config.max_tokens" in entries_by_path
    assert entries_by_path["config.max_tokens"].kind == "unchanged"

    assert "same" in entries_by_path
    assert entries_by_path["same"].kind == "unchanged"

    # Verify Summary counts
    assert report.summary.added == 1
    assert report.summary.removed == 1
    assert report.summary.modified == 3  # status, counter, config.model
    assert report.summary.unchanged == 2  # config.max_tokens, same
    assert report.summary.total_changes == 5


def test_compute_state_diff_secret_redaction_detection():
    state_a = {
        "api_key": "[REDACTED]",
        "db_secret": "[REDACTED]",
        "version": 1,
    }
    state_b = {
        "api_key": "[REDACTED]",
        "db_secret": "[REDACTED]",
        "version": 2,
    }

    report = compute_state_diff(state_a, state_b)
    assert report.summary.redacted_properties == 2

    entries_by_path = {e.path: e for e in report.entries}
    assert entries_by_path["api_key"].is_redacted is True
    assert entries_by_path["api_key"].kind == "unchanged"
    assert entries_by_path["db_secret"].is_redacted is True
    assert entries_by_path["version"].is_redacted is False
    assert entries_by_path["version"].kind == "modified"


@pytest.mark.asyncio
async def test_time_machine_diff_snapshots_integration(tmp_path):
    db_file = str(tmp_path / "tm_diff.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    await storage.initialize()

    tm = TimeMachine(storage=storage)

    snap1 = await tm.snapshot(
        span_id="span-tm-diff-1",
        state_data={"stage": "analysis", "confidence": 0.5, "secret": "sk-proj-supersecret1234567890"},
        branch_id="main",
    )
    snap2 = await tm.snapshot(
        span_id="span-tm-diff-2",
        state_data={"stage": "execution", "confidence": 0.95, "secret": "sk-proj-supersecret1234567890", "added_flag": True},
        branch_id="main",
    )

    diff_report = await tm.diff_snapshots(
        swarm_id="swarm-diff",
        span_id_a="span-tm-diff-1",
        span_id_b="span-tm-diff-2",
    )

    assert diff_report.hash_a == snap1.state_hash
    assert diff_report.hash_b == snap2.state_hash
    assert diff_report.summary.added == 1
    assert diff_report.summary.modified == 2  # stage, confidence
    assert diff_report.summary.redacted_properties == 1  # secret is sanitized to [REDACTED]
    assert diff_report.summary.total_changes == 3

    await storage.close()

