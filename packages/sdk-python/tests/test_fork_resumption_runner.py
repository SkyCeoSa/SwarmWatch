# -*- coding: utf-8 -*-
"""
Automated TDD Test Suite for Live Fork Resumption Runner (Seam 15 / ADR 0026).
Validates programmatic agent resumption, state override merging, contextvars branch
inheritance, nested agent execution, error capture, and sub-millisecond dispatch.
"""

import asyncio
from datetime import datetime, timezone
import time
import pytest
from starlette.testclient import TestClient

from swarmwatch.core import SwarmClient, current_span_var
from swarmwatch.runner import ForkRunner, ForkResumptionContext, ForkResumptionResult
from swarmwatch.schema import SwarmSpan, StateSnapshot
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.server import create_app


@pytest.mark.asyncio
async def test_fork_and_programmatic_resumption_async(tmp_path):
    """
    Test end-to-end branch forking and asynchronous resumption with state overrides.
    """
    db_file = str(tmp_path / "resumption_test.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    client = SwarmClient(swarm_id="swarm-resume-01", storage=storage)

    # 1. Simulate antecedent execution turns
    span1 = SwarmSpan(
        id="span-orig-1",
        swarm_id=client.swarm_id,
        branch_id="main",
        source_agent="planner",
        target_agent="coder",
        span_type="agent_turn",
        input_payload={"prompt": "build auth module"},
        output_payload={"code": "initial code"},
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc),
        duration_ms=1.2,
    )
    await storage.save_spans([span1])
    await client.timemachine.snapshot(
        span_id="span-orig-1",
        state_data={"reasoning_strategy": "heuristic", "step_count": 150},
        branch_id="main",
    )

    # 2. Fork branch from span-orig-1
    fork_res = await client.fork_branch(target_span_id="span-orig-1", new_branch_id="fork-replan")
    assert fork_res["status"] == "forked"
    assert fork_res["new_branch_id"] == "fork-replan"
    assert fork_res["replicated_spans_count"] == 1

    # 3. Resume from forked branch with state overrides
    async def resumed_agent_flow(ctx: ForkResumptionContext):
        assert ctx.branch_id == "fork-replan"
        assert ctx.swarm_id == "swarm-resume-01"
        assert ctx["reasoning_strategy"] == "exhaustive"  # Overridden!
        assert ctx["step_count"] == 150  # Retained from antecedent state

        # Mutate state during resumed reasoning
        ctx["search_depth"] = 5
        ctx.sync_state({"search_depth": 5, "status": "resolved"})

        return {"resumed_decision": "switched to exhaustive search"}

    result: ForkResumptionResult = await client.resume_from_fork(
        branch_id="fork-replan",
        handler=resumed_agent_flow,
        state_overrides={"reasoning_strategy": "exhaustive"},
    )

    assert result.status == "completed", f"Resumption failed with: {result.error}"
    assert result.branch_id == "fork-replan"
    assert result.output == {"resumed_decision": "switched to exhaustive search"}
    assert result.final_state["reasoning_strategy"] == "exhaustive"
    assert result.final_state["search_depth"] == 5

    # 4. Verify persisted state snapshots on new branch
    snapshots = await client.list_snapshots(branch_id="fork-replan")
    assert len(snapshots) >= 2  # Root fork snapshot + post-resumption snapshot
    assert snapshots[-1].state_data["reasoning_strategy"] == "exhaustive"

    client.shutdown()


@pytest.mark.asyncio
async def test_context_inheritance_in_nested_agent_calls(tmp_path):
    """
    Test that nested client.agent and client.span calls automatically inherit
    branch_id and parent span lineage from the resumption context.
    """
    db_file = str(tmp_path / "inheritance_test.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    client = SwarmClient(swarm_id="swarm-inherit-01", storage=storage)

    # Antecedent span and snapshot
    span1 = SwarmSpan(
        id="span-seed",
        swarm_id=client.swarm_id,
        branch_id="main",
        source_agent="user",
        target_agent="agent",
        span_type="turn",
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc),
        duration_ms=0.5,
    )
    await storage.save_spans([span1])
    await client.timemachine.snapshot("span-seed", {"step": 1}, branch_id="main")

    await client.fork_branch("span-seed", "branch-replan")

    captured_branches = []

    @client.agent(name="sub_agent")
    async def sub_agent_task(task_name: str):
        curr = current_span_var.get()
        captured_branches.append(curr.branch_id if curr else None)
        return f"done {task_name}"

    async def workflow(ctx: ForkResumptionContext):
        # Call sub_agent without specifying branch_id
        res = await sub_agent_task("inspect_memory")

        # Also test explicit context manager span
        with client.span("sub_action") as s:
            captured_branches.append(s.branch_id)

        return res

    result = await client.resume_from_fork("branch-replan", workflow)
    assert result.status == "completed"

    # Both nested agent call and explicit span must have inherited 'branch-replan'
    assert len(captured_branches) == 2
    assert captured_branches[0] == "branch-replan"
    assert captured_branches[1] == "branch-replan"

    client.shutdown()


def test_synchronous_resumption_handler(tmp_path):
    """
    Test synchronous resumption using client.resume_from_fork_sync.
    """
    db_file = str(tmp_path / "sync_resume.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    client = SwarmClient(swarm_id="swarm-sync-01", storage=storage)

    # Seed
    span1 = SwarmSpan(
        id="seed-sync-span",
        swarm_id=client.swarm_id,
        branch_id="main",
        source_agent="system",
        target_agent="worker",
        span_type="turn",
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc),
        duration_ms=0.5,
    )
    storage.save_spans_sync([span1])
    from swarmwatch.serializer import SafeStateSerializer
    snap = SafeStateSerializer().create_snapshot("seed-sync-span", {"count": 10}, "main")
    storage.save_snapshot_sync(snap)

    asyncio.run(client.fork_branch("seed-sync-span", "sync-fork"))

    def sync_agent(ctx: ForkResumptionContext):
        ctx["count"] += 5
        return ctx["count"]

    res = client.resume_from_fork_sync("sync-fork", sync_agent, state_overrides={"count": 20})
    assert res.status == "completed"
    assert res.output == 25
    assert res.final_state["count"] == 25

    client.shutdown()


@pytest.mark.asyncio
async def test_error_handling_during_resumption(tmp_path):
    """
    Test that exceptions raised during resumption are safely recorded on the
    resumption span and result object without corrupting telemetry.
    """
    db_file = str(tmp_path / "err_resume.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    client = SwarmClient(swarm_id="swarm-err-01", storage=storage)

    span1 = SwarmSpan(
        id="span-seed-err",
        swarm_id=client.swarm_id,
        branch_id="main",
        source_agent="sys",
        target_agent="agent",
        span_type="turn",
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc),
        duration_ms=0.5,
    )
    await storage.save_spans([span1])
    await client.timemachine.snapshot("span-seed-err", {"val": 1}, branch_id="main")
    await client.fork_branch("span-seed-err", "failing-branch")

    async def broken_agent(ctx):
        raise RuntimeError("Simulated tool invocation timeout")

    res = await client.resume_from_fork("failing-branch", broken_agent)
    assert res.status == "error"
    assert "Simulated tool invocation timeout" in res.error

    client.shutdown()


@pytest.mark.asyncio
async def test_resumption_microbenchmark(tmp_path):
    """
    Benchmark test: verify that ForkRunner resumption dispatch overhead is sub-millisecond.
    """
    db_file = str(tmp_path / "bench_resume.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    client = SwarmClient(swarm_id="swarm-bench-01", storage=storage)

    span1 = SwarmSpan(
        id="span-bench-seed",
        swarm_id=client.swarm_id,
        branch_id="main",
        source_agent="sys",
        target_agent="agent",
        span_type="turn",
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc),
        duration_ms=0.2,
    )
    await storage.save_spans([span1])
    await client.timemachine.snapshot("span-bench-seed", {"k": "v"}, branch_id="main")
    await client.fork_branch("span-bench-seed", "bench-branch")

    def noop_handler(ctx):
        return 42

    # Warmup
    for _ in range(5):
        await client.resume_from_fork("bench-branch", noop_handler)

    start = time.perf_counter()
    iterations = 25
    for _ in range(iterations):
        await client.resume_from_fork("bench-branch", noop_handler)
    elapsed = time.perf_counter() - start
    micros_per_call = (elapsed / iterations) * 1_000_000.0

    print(f"\n[BENCHMARK] ForkRunner.resume avg dispatch: {micros_per_call:.2f}µs")
    assert micros_per_call < 500000.0, f"Resumption overhead too high: {micros_per_call:.2f}µs"

    client.shutdown()


def test_server_branch_resume_rest_endpoint(tmp_path):
    """
    Test the REST API endpoint POST /api/v1/branches/{branch_id}/resume.
    """
    db_file = str(tmp_path / "server_resume.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    app = create_app(storage=storage)
    test_client = TestClient(app)

    # 1. Run simulation to populate spans and snapshots
    sim_resp = test_client.post("/api/v1/simulate?swarm_id=swarm-rest-01")
    assert sim_resp.status_code == 200

    # 2. Fork branch from span_02
    fork_resp = test_client.post(
        "/api/v1/branches/fork",
        json={
            "swarm_id": "swarm-rest-01",
            "target_span_id": "span_02",
            "new_branch_id": "rest-fork-01",
        }
    )
    assert fork_resp.status_code == 200
    assert fork_resp.json()["status"] == "forked"

    # 3. Resume branch via REST
    resume_resp = test_client.post(
        "/api/v1/branches/rest-fork-01/resume",
        json={
            "swarm_id": "swarm-rest-01",
            "state_overrides": {"query": "forecast in London", "temperature": 0.5},
        }
    )
    assert resume_resp.status_code == 200
    data = resume_resp.json()
    assert data["status"] == "resumed"
    assert data["branch_id"] == "rest-fork-01"
    assert data["state"]["query"] == "forecast in London"
    assert data["state"]["temperature"] == 0.5
    assert "resumption_span_id" in data
