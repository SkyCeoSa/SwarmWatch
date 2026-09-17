# -*- coding: utf-8 -*-
"""
TDD Tests for SwarmWatch Microsoft Semantic Kernel Adapter (Seam 5 & ADR 0018).
Verifies function invocation filter execution, recursive tool loop halts,
and non-blocking async invocation support.
"""

from unittest.mock import MagicMock
import pytest

import swarmwatch
from swarmwatch.adapters.semantic_kernel import (
    SwarmWatchSemanticKernelFilter,
    FunctionInvocationContext,
)
from swarmwatch.circuit import SwarmCircuitBreakException, EntropyCircuitBreaker, CircuitPreset


def test_semantic_kernel_filter_sync_invocation(tmp_path):
    db_path = str(tmp_path / "sk_sync.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-sk-sync")

    sk_filter = SwarmWatchSemanticKernelFilter(swarm_id="swarm-sk-sync")

    ctx = FunctionInvocationContext(
        plugin_name="DatabasePlugin",
        function_name="execute_sql",
        arguments={"query": "SELECT balance FROM accounts WHERE id = 42;"},
    )

    def next_step(context):
        context.result = {"balance": 150000.0, "currency": "USD"}

    sk_filter.on_function_invocation(ctx, next_step)

    assert ctx.result == {"balance": 150000.0, "currency": "USD"}
    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-sk-sync")
    assert len(spans) == 1
    span = spans[0]
    assert span.span_type == "function_filter_invocation"
    assert span.source_agent == "DatabasePlugin"
    assert span.target_agent == "execute_sql"
    assert "SELECT balance" in span.input_payload["query"]
    assert span.output_payload["balance"] == 150000.0


def test_semantic_kernel_circuit_breaker_halts_recursive_tool_loop(tmp_path):
    db_path = str(tmp_path / "sk_loop.db")
    swarmwatch.init(db_path=db_path, swarm_id="swarm-sk-loop")

    circuit = EntropyCircuitBreaker(preset=CircuitPreset.STRICT)
    sk_filter = SwarmWatchSemanticKernelFilter(
        swarm_id="swarm-sk-loop",
        circuit_breaker=circuit,
    )

    loop_query = "SELECT * FROM schema_migrations WHERE applied = FALSE ORDER BY id ASC;"

    def mock_failing_query(context):
        context.result = {"error": "Deadlock detected during lock acquisition"}

    # Turn 1: Passes
    ctx1 = FunctionInvocationContext(
        plugin_name="MigrationTool",
        function_name="apply_migration",
        arguments={"query": loop_query},
    )
    sk_filter.on_function_invocation(ctx1, mock_failing_query)

    # Turn 2: Repetitive tool invocation issues warning
    ctx2 = FunctionInvocationContext(
        plugin_name="MigrationTool",
        function_name="apply_migration",
        arguments={"query": loop_query},
    )
    sk_filter.on_function_invocation(ctx2, mock_failing_query)

    # Turn 3: Consecutive warning limit reached -> Trips circuit breaker!
    ctx3 = FunctionInvocationContext(
        plugin_name="MigrationTool",
        function_name="apply_migration",
        arguments={"query": loop_query},
    )
    with pytest.raises(SwarmCircuitBreakException):
        sk_filter.on_function_invocation(ctx3, mock_failing_query)


@pytest.mark.asyncio
async def test_semantic_kernel_filter_async_invocation(tmp_path):
    db_path = str(tmp_path / "sk_async.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-sk-async")

    sk_filter = SwarmWatchSemanticKernelFilter(swarm_id="swarm-sk-async")

    ctx = FunctionInvocationContext(
        plugin_name="WebSearchPlugin",
        function_name="search",
        arguments={"query": "latest fed interest rate decision"},
    )

    async def async_next_step(context):
        context.result = {"results": ["Fed holds benchmark rate steady at 5.25%-5.50%"]}

    await sk_filter.on_function_invocation_async(ctx, async_next_step)

    assert "Fed holds" in ctx.result["results"][0]
    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-sk-async")
    assert len(spans) == 1
    assert spans[0].source_agent == "WebSearchPlugin"
    assert spans[0].target_agent == "search"
