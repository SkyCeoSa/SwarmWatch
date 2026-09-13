# -*- coding: utf-8 -*-
import asyncio
import os
import tempfile
import pytest
from datetime import datetime, timezone

from swarmwatch.schema import SwarmSpan, StateSnapshot
from swarmwatch.storage import SQLiteStorageAdapter


@pytest.mark.asyncio
async def test_sqlite_storage_adapter_lifecycle_and_branch_isolation():
    """
    HIGHEST TEST SEAM: Verifies end-to-end multi-agent span persistence,
    causal DAG linkage, WAL pragma activation, state snapshots, and branch isolation.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test_swarmwatch.db")
        adapter = SQLiteStorageAdapter(db_path=db_path)
        await adapter.initialize()

        # 1. Verify WAL Mode is active
        journal_mode = await adapter.get_journal_mode()
        assert journal_mode.upper() == "WAL", f"Expected WAL mode, got {journal_mode}"

        swarm_id = "swarm-alpha-1"
        now = datetime.now(timezone.utc)

        # 2. Construct a causal chain of 3 spans in the 'main' branch:
        # Span 1: Planner -> Coder
        span_1 = SwarmSpan(
            id="span-001",
            swarm_id=swarm_id,
            branch_id="main",
            parent_span_id=None,
            source_agent="planner",
            target_agent="coder",
            span_type="agent_call",
            input_payload={"task": "Write a binary search algorithm"},
            output_payload={"status": "delegated"},
            token_count=150,
            entropy_score=1.0,
            start_time=now,
            end_time=now,
            duration_ms=45.2
        )

        # Span 2: Coder -> Python Tool
        span_2 = SwarmSpan(
            id="span-002",
            swarm_id=swarm_id,
            branch_id="main",
            parent_span_id="span-001",
            source_agent="coder",
            target_agent="tool_python",
            span_type="tool_call",
            input_payload={"code": "def binary_search: pass"},
            output_payload={"stdout": "Tests passed"},
            token_count=320,
            entropy_score=0.95,
            start_time=now,
            end_time=now,
            duration_ms=120.0
        )

        # Span 3: Coder -> Planner (Final Answer)
        span_3 = SwarmSpan(
            id="span-003",
            swarm_id=swarm_id,
            branch_id="main",
            parent_span_id="span-002",
            source_agent="coder",
            target_agent="planner",
            span_type="agent_call",
            input_payload={"result": "Completed"},
            output_payload={"answer": "Here is the algorithm."},
            token_count=210,
            entropy_score=0.92,
            start_time=now,
            end_time=now,
            duration_ms=60.5
        )

        # 3. Batch Save Spans
        await adapter.save_spans([span_1, span_2, span_3])

        # 4. Query Spans for main branch
        main_spans = await adapter.get_swarm_spans(swarm_id, branch_id="main")
        assert len(main_spans) == 3, f"Expected 3 spans, got {len(main_spans)}"
        assert [s.id for s in main_spans] == ["span-001", "span-002", "span-003"]
        assert main_spans[1].parent_span_id == "span-001"
        assert main_spans[2].parent_span_id == "span-002"
        assert main_spans[1].source_agent == "coder"
        assert main_spans[1].target_agent == "tool_python"

        # 5. Snapshot Storage & Retrieval
        snapshot = StateSnapshot(
            id="snap-001",
            span_id="span-002",
            branch_id="main",
            state_hash="hash-abc-123",
            state_data={"variables": {"arr": [1, 2, 3], "target": 2}},
            created_at=now
        )
        await adapter.save_snapshot(snapshot)

        retrieved_snap = await adapter.get_snapshot(span_id="span-002")
        assert retrieved_snap is not None
        assert retrieved_snap.state_hash == "hash-abc-123"
        assert retrieved_snap.state_data["variables"]["target"] == 2

        # 6. Branch Isolation Test
        fork_spans = await adapter.get_swarm_spans(swarm_id, branch_id="fork-1")
        assert len(fork_spans) == 0, "Fork branch must be isolated and empty initially"

        branches = await adapter.list_branches(swarm_id)
        assert "main" in branches

        await adapter.close()
