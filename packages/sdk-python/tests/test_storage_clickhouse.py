# -*- coding: utf-8 -*-
"""
TDD Tests for ClickHouseStorageAdapter (Seam 4: Storage Adapter Interface).
Validates columnar batch ingestion, snapshot compression roundtrip, and branch isolation.
"""
from datetime import datetime, timezone
import pytest

from swarmwatch.schema import SwarmSpan, StateSnapshot, CircuitEvent
from swarmwatch.storage import ClickHouseStorageAdapter, StorageAdapter


@pytest.mark.asyncio
async def test_clickhouse_storage_adapter_interface_compliance():
    """Verify that ClickHouseStorageAdapter strictly adheres to StorageAdapter abstract interface."""
    adapter = ClickHouseStorageAdapter(use_mock=True)
    assert isinstance(adapter, StorageAdapter)
    await adapter.initialize()
    await adapter.close()


@pytest.mark.asyncio
async def test_clickhouse_storage_adapter_span_batch_lifecycle():
    """
    Verifies that ClickHouseStorageAdapter ingests batches of SwarmSpan objects,
    preserves causal linkage, filters by swarm_id and branch_id, and lists branches.
    """
    adapter = ClickHouseStorageAdapter(use_mock=True)
    await adapter.initialize()

    swarm_id = "swarm-ch-001"
    now = datetime.now(timezone.utc)

    # 1. Create spans in 'main' branch
    span_1 = SwarmSpan(
        id="ch-span-1",
        swarm_id=swarm_id,
        branch_id="main",
        parent_span_id=None,
        source_agent="orchestrator",
        target_agent="worker_1",
        span_type="agent_call",
        input_payload={"prompt": "Analyze dataset"},
        output_payload={"status": "working"},
        token_count=120,
        entropy_score=0.98,
        start_time=now,
        end_time=now,
        duration_ms=25.0,
    )

    span_2 = SwarmSpan(
        id="ch-span-2",
        swarm_id=swarm_id,
        branch_id="main",
        parent_span_id="ch-span-1",
        source_agent="worker_1",
        target_agent="database",
        span_type="tool_call",
        input_payload={"sql": "SELECT count() FROM logs"},
        output_payload={"count": 5000000},
        token_count=85,
        entropy_score=0.91,
        start_time=now,
        end_time=now,
        duration_ms=110.0,
    )

    # 2. Create span in 'experimental' branch
    span_exp = SwarmSpan(
        id="ch-span-exp",
        swarm_id=swarm_id,
        branch_id="experimental",
        parent_span_id="ch-span-1",
        source_agent="worker_1",
        target_agent="cache",
        span_type="tool_call",
        input_payload={"key": "cached_count"},
        output_payload={"count": 5000000},
        token_count=40,
        entropy_score=0.88,
        start_time=now,
        end_time=now,
        duration_ms=5.0,
    )

    # Batch save
    await adapter.save_spans([span_1, span_2, span_exp])

    # Query main branch
    main_spans = await adapter.get_swarm_spans(swarm_id=swarm_id, branch_id="main")
    assert len(main_spans) == 2
    assert main_spans[0].id == "ch-span-1"
    assert main_spans[1].id == "ch-span-2"
    assert main_spans[1].parent_span_id == "ch-span-1"
    assert main_spans[1].output_payload["count"] == 5000000

    # Query experimental branch
    exp_spans = await adapter.get_swarm_spans(swarm_id=swarm_id, branch_id="experimental")
    assert len(exp_spans) == 1
    assert exp_spans[0].id == "ch-span-exp"
    assert exp_spans[0].branch_id == "experimental"

    # Branch list
    branches = await adapter.list_branches(swarm_id=swarm_id)
    assert "main" in branches
    assert "experimental" in branches

    await adapter.close()


@pytest.mark.asyncio
async def test_clickhouse_storage_adapter_snapshot_and_circuit_event():
    """Verifies snapshot compression roundtrip and circuit event recording in ClickHouse adapter."""
    adapter = ClickHouseStorageAdapter(use_mock=True)
    await adapter.initialize()

    now = datetime.now(timezone.utc)
    snapshot = StateSnapshot(
        id="snap-ch-01",
        span_id="ch-span-2",
        branch_id="main",
        state_hash="sha256-hash-test",
        state_data={"agent_state": {"step": 4, "memory": ["item1", "item2"]}},
        created_at=now,
    )

    await adapter.save_snapshot(snapshot)

    retrieved = await adapter.get_snapshot(span_id="ch-span-2")
    assert retrieved is not None
    assert retrieved.id == "snap-ch-01"
    assert retrieved.state_hash == "sha256-hash-test"
    assert retrieved.state_data["agent_state"]["step"] == 4

    # Non-existent snapshot returns None
    missing = await adapter.get_snapshot(span_id="non-existent")
    assert missing is None

    # Circuit event
    event = CircuitEvent(
        id="ce-001",
        swarm_id="swarm-ch-001",
        span_id="ch-span-2",
        event_type="warning",
        reason="High lexical repetition",
        metric_val=0.88,
        created_at=now,
    )
    await adapter.record_circuit_event(event)

    await adapter.close()
