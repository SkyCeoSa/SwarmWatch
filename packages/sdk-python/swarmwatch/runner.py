# -*- coding: utf-8 -*-
"""
SwarmWatch Live Fork Resumption Runner (Seam 15 / ADR 0026).
Provides programmatic resumption of autonomous agents from a forked state snapshot,
establishing scoped branch context, causal span linkage, and zero-token deterministic execution.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import inspect
import time
from typing import Any, Callable, Dict, Iterator, List, Optional
import uuid

from swarmwatch.schema import StateSnapshot, SwarmSpan
from swarmwatch.serializer import SafeStateSerializer


@dataclass
class ForkResumptionContext:
    """
    Scoped execution context provided to the resumed agent handler.
    Contains sanitized antecedent state, applied state overrides, active branch binding,
    and contextual helpers to emit linked child spans and record state snapshots.
    """
    branch_id: str
    swarm_id: str
    source_span_id: str
    resumption_span_id: str
    state: Dict[str, Any]
    state_overrides: Dict[str, Any] = field(default_factory=dict)
    client: Optional[Any] = None

    def span(
        self,
        name: str,
        source: str = "resumed_agent",
        target: str = "system",
        span_type: str = "span",
        input_payload: Optional[Dict[str, Any]] = None,
    ):
        """Spawns an execution span explicitly scoped to this resumption branch."""
        if self.client:
            return self.client.span(
                name=name,
                source=source,
                target=target,
                span_type=span_type,
                branch_id=self.branch_id,
                input_payload=input_payload,
            )
        raise RuntimeError("No active SwarmClient bound to ForkResumptionContext")

    def sync_state(self, state_data: Dict[str, Any]) -> StateSnapshot:
        """Captures a new state snapshot along this resumption branch."""
        if self.client:
            self.state.update(state_data)
            return self.client.sync_state(self.state, branch_id=self.branch_id)
        raise RuntimeError("No active SwarmClient bound to ForkResumptionContext")

    # Dict-like interface for transparent backwards compatibility
    def __getitem__(self, key: str) -> Any:
        return self.state[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.state[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self.state

    def get(self, key: str, default: Any = None) -> Any:
        return self.state.get(key, default)

    def keys(self):
        return self.state.keys()

    def values(self):
        return self.state.values()

    def items(self):
        return self.state.items()


@dataclass
class ForkResumptionResult:
    """
    Structured outcome of a live fork resumption run.
    """
    status: str  # "completed" | "error"
    branch_id: str
    swarm_id: str
    resumption_span_id: str
    output: Any = None
    final_state: Dict[str, Any] = field(default_factory=dict)
    duration_ms: float = 0.0
    error: Optional[str] = None


class ForkRunner:
    """
    Deep module orchestrating autonomous agent resumption from an antecedent state snapshot.
    Hides snapshot retrieval, override sanitization, boundary span linking, contextvars binding,
    and post-resumption snapshot capture behind a simple runner interface.
    """

    def __init__(self, client: Any):
        self.client = client
        self.serializer = SafeStateSerializer()

    async def resume(
        self,
        branch_id: str,
        handler: Callable[..., Any],
        state_overrides: Optional[Dict[str, Any]] = None,
    ) -> ForkResumptionResult:
        """
        Asynchronously resumes agent execution from the specified branch_id.
        """
        from swarmwatch.core import current_span_var

        perf_start = time.perf_counter()
        start_time = datetime.now(timezone.utc)

        # 1. Retrieve branch snapshot or branch spans
        snapshots = await self.client.list_snapshots(branch_id=branch_id)
        base_snapshot: Optional[StateSnapshot] = None
        source_span_id: str = f"fork_root_{branch_id}"

        if snapshots:
            base_snapshot = snapshots[-1]
            source_span_id = base_snapshot.span_id
            base_state = dict(base_snapshot.state_data)
        else:
            # Fallback: inspect spans for this branch
            raw_spans = await self.client.storage.get_swarm_spans(self.client.swarm_id)
            branch_spans = [s for s in raw_spans if (s.branch_id if hasattr(s, "branch_id") else s.get("branch_id")) == branch_id]
            if not branch_spans:
                raise ValueError(
                    f"Branch '{branch_id}' does not exist or has no recorded spans in swarm '{self.client.swarm_id}'"
                )
            last_span = branch_spans[-1]
            source_span_id = last_span.id if hasattr(last_span, "id") else last_span.get("id")
            base_state = {}

        # 2. Merge and sanitize state overrides
        merged_state = dict(base_state)
        sanitized_overrides: Dict[str, Any] = {}
        if state_overrides:
            sanitized_overrides = self.serializer.sanitize(state_overrides)
            merged_state.update(sanitized_overrides)

        resumption_span_id = str(uuid.uuid4())

        # 3. Create boundary resumption span
        resumption_span = SwarmSpan(
            id=resumption_span_id,
            swarm_id=self.client.swarm_id,
            project_id=getattr(self.client, "project_id", "default-project"),
            branch_id=branch_id,
            parent_span_id=source_span_id,
            source_agent="fork_runner",
            target_agent="resumed_agent",
            span_type="fork_resumption",
            input_payload={
                "branch_id": branch_id,
                "source_span_id": source_span_id,
                "state_overrides": sanitized_overrides,
                "initial_state": merged_state,
            },
            output_payload={},
            token_count=0,
            entropy_score=1.0,
            start_time=start_time,
            end_time=start_time,
            duration_ms=0.0,
        )

        ctx = ForkResumptionContext(
            branch_id=branch_id,
            swarm_id=self.client.swarm_id,
            source_span_id=source_span_id,
            resumption_span_id=resumption_span_id,
            state=merged_state,
            state_overrides=sanitized_overrides,
            client=self.client,
        )

        token = current_span_var.set(resumption_span)
        output: Any = None
        error_msg: Optional[str] = None
        status = "completed"

        try:
            # Pass ctx to handler if handler accepts arguments
            sig = inspect.signature(handler)
            if len(sig.parameters) > 0:
                result = handler(ctx)
            else:
                result = handler()

            if inspect.isawaitable(result):
                output = await result
            else:
                output = result

            # Record clean output payload
            if isinstance(output, dict):
                resumption_span.output_payload.update(output)
            elif output is not None:
                resumption_span.output_payload["result"] = str(output)
            resumption_span.output_payload["status"] = "completed"

            # Auto-save updated state snapshot at conclusion of resumption
            self.client.sync_state(ctx.state, branch_id=branch_id)
            self.client.flush()

        except Exception as e:
            status = "error"
            error_msg = str(e)
            resumption_span.output_payload["status"] = "error"
            resumption_span.output_payload["error"] = type(e).__name__
            resumption_span.output_payload["message"] = str(e)
        finally:
            end_time = datetime.now(timezone.utc)
            duration_ms = max(0.001, (time.perf_counter() - perf_start) * 1000.0)
            resumption_span.end_time = end_time
            resumption_span.duration_ms = duration_ms

            # Emit boundary resumption span
            self.client.transport.emit_span(resumption_span)
            current_span_var.reset(token)
            if hasattr(self.client, "flush"):
                self.client.flush()

        return ForkResumptionResult(
            status=status,
            branch_id=branch_id,
            swarm_id=self.client.swarm_id,
            resumption_span_id=resumption_span_id,
            output=output,
            final_state=ctx.state,
            duration_ms=duration_ms,
            error=error_msg,
        )

    def resume_sync(
        self,
        branch_id: str,
        handler: Callable[..., Any],
        state_overrides: Optional[Dict[str, Any]] = None,
    ) -> ForkResumptionResult:
        """
        Synchronously resumes agent execution using an active event loop or asyncio.run.
        """
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # In an active running loop, run in a background thread or create task
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    asyncio.run,
                    self.resume(branch_id=branch_id, handler=handler, state_overrides=state_overrides),
                )
                return future.result()
        else:
            return asyncio.run(
                self.resume(branch_id=branch_id, handler=handler, state_overrides=state_overrides)
            )
