# -*- coding: utf-8 -*-
"""
SwarmWatch Public Developer Surface.
Provides @swarmwatch.agent decorator, with swarmwatch.span(...) context manager,
and the singleton SwarmClient.
"""

import asyncio
import contextvars
from datetime import datetime, timezone
import functools
import inspect
import time
import traceback
from typing import Any, Callable, Dict, List, Optional
import uuid

from swarmwatch.schema import SwarmSpan, CircuitEvent
from swarmwatch.storage import SQLiteStorageAdapter, StorageAdapter
from swarmwatch.transport import TelemetryTransport, TransportConfig
from swarmwatch.webhooks import WebhookConfig, WebhookNotifier

current_span_var: contextvars.ContextVar[Optional[SwarmSpan]] = contextvars.ContextVar(
    "swarmwatch_current_span", default=None
)


class SpanContext:
    """
    Context manager for tracing arbitrary multi-agent blocks and tool executions.
    Supports both synchronous ('with') and asynchronous ('async with') syntax.
    """

    def __init__(
        self,
        client: "SwarmClient",
        name: str,
        source: str = "system",
        target: str = "system",
        span_type: str = "span",
        branch_id: Optional[str] = None,
        input_payload: Optional[Dict[str, Any]] = None,
    ):
        self.client = client
        self.name = name
        self.source = source
        self.target = target
        self.span_type = span_type
        self.parent_span: Optional[SwarmSpan] = current_span_var.get()
        self.branch_id = branch_id or (self.parent_span.branch_id if self.parent_span else "main")
        self.input_payload = input_payload or {}
        self.output_payload: Dict[str, Any] = {}
        self.token_count = 0
        self.entropy_score = 1.0

        self.span_id = str(uuid.uuid4())
        self.span: Optional[SwarmSpan] = None
        self._token: Optional[contextvars.Token] = None
        self.start_time: Optional[datetime] = None
        self._perf_start: Optional[float] = None

    def __enter__(self) -> SwarmSpan:
        self.start_time = datetime.now(timezone.utc)
        self._perf_start = time.perf_counter()
        self.span = SwarmSpan(
            id=self.span_id,
            swarm_id=self.client.swarm_id,
            branch_id=self.branch_id,
            parent_span_id=self.parent_span.id if self.parent_span else None,
            source_agent=self.source,
            target_agent=self.target,
            span_type=self.span_type,
            input_payload=self.input_payload,
            output_payload={},
            token_count=self.token_count,
            entropy_score=self.entropy_score,
            start_time=self.start_time,
            end_time=self.start_time,
            duration_ms=0.0,
        )
        self._token = current_span_var.set(self.span)
        return self.span

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        end_time = datetime.now(timezone.utc)
        elapsed = (time.perf_counter() - self._perf_start) * 1000.0 if self._perf_start else 0.0
        duration_ms = max(0.001, elapsed)

        if exc_val is not None:
            self.output_payload["error"] = exc_type.__name__ if exc_type else "Error"
            self.output_payload["message"] = str(exc_val)
            self.output_payload["traceback"] = "".join(traceback.format_tb(exc_tb))

        self.span.end_time = end_time
        self.span.duration_ms = duration_ms
        if self.output_payload:
            self.span.output_payload.update(self.output_payload)

        # Emit to non-blocking transport queue
        self.client.transport.emit_span(self.span)

        if self._token:
            current_span_var.reset(self._token)
        # Never swallow exception
        return False

    async def __aenter__(self) -> SwarmSpan:
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        return self.__exit__(exc_type, exc_val, exc_tb)


class SwarmClient:
    """
    Main client instance managing telemetry transport and span context.
    """

    def __init__(
        self,
        swarm_id: str = "default-swarm",
        storage: Optional[StorageAdapter] = None,
        transport_config: Optional[TransportConfig] = None,
        webhooks: Optional[List[WebhookConfig]] = None,
        db_path: str = "swarmwatch.db",
    ):
        self.swarm_id = swarm_id
        self.storage = storage or SQLiteStorageAdapter(db_path=db_path)
        self.transport = TelemetryTransport(storage=self.storage, config=transport_config)
        self.transport.start()
        self.webhooks = webhooks or []
        self.webhook_notifier = WebhookNotifier(configs=self.webhooks) if self.webhooks else None
        if self.webhook_notifier:
            self.webhook_notifier.start()

        from swarmwatch.serializer import TimeMachine
        from swarmwatch.runner import ForkRunner
        self.timemachine = TimeMachine(storage=self.storage)
        self.runner = ForkRunner(client=self)

    def record_circuit_event(self, event: CircuitEvent) -> None:
        """Records a circuit event in storage and dispatches to enterprise webhooks."""
        self.transport._call_storage("record_circuit_event", event)
        if self.webhook_notifier:
            self.webhook_notifier.notify(event)

    def span(
        self,
        name: str,
        source: str = "system",
        target: str = "system",
        span_type: str = "span",
        branch_id: Optional[str] = None,
        input_payload: Optional[Dict[str, Any]] = None,
    ) -> SpanContext:
        return SpanContext(
            client=self,
            name=name,
            source=source,
            target=target,
            span_type=span_type,
            branch_id=branch_id,
            input_payload=input_payload,
        )

    def agent(
        self,
        name: Optional[str] = None,
        role: Optional[str] = None,
        target: str = "environment",
        branch_id: Optional[str] = None,
    ) -> Callable:
        """
        Decorator wrapping an agent action or function.
        Supports both synchronous functions and async coroutines.
        """

        def decorator(fn: Callable) -> Callable:
            agent_name = name or fn.__name__
            sig = inspect.signature(fn)

            def extract_inputs(args, kwargs) -> Dict[str, Any]:
                try:
                    bound = sig.bind(*args, **kwargs)
                    bound.apply_defaults()
                    return {k: v for k, v in bound.arguments.items()}
                except Exception:
                    return {"args": list(args), "kwargs": kwargs}

            if asyncio.iscoroutinefunction(fn):
                @functools.wraps(fn)
                async def async_wrapper(*args, **kwargs):
                    inputs = extract_inputs(args, kwargs)
                    ctx = self.span(
                        name=f"agent:{agent_name}",
                        source=agent_name,
                        target=target,
                        span_type="agent_call",
                        branch_id=branch_id,
                        input_payload=inputs,
                    )
                    async with ctx:
                        result = await fn(*args, **kwargs)
                        if isinstance(result, dict):
                            ctx.output_payload.update(result)
                        else:
                            ctx.output_payload["result"] = result
                        return result

                return async_wrapper
            else:
                @functools.wraps(fn)
                def sync_wrapper(*args, **kwargs):
                    inputs = extract_inputs(args, kwargs)
                    ctx = self.span(
                        name=f"agent:{agent_name}",
                        source=agent_name,
                        target=target,
                        span_type="agent_call",
                        branch_id=branch_id,
                        input_payload=inputs,
                    )
                    with ctx:
                        result = fn(*args, **kwargs)
                        if isinstance(result, dict):
                            ctx.output_payload.update(result)
                        else:
                            ctx.output_payload["result"] = result
                        return result

                return sync_wrapper

        return decorator

    def sync_state(self, state_data: Dict[str, Any], branch_id: Optional[str] = None) -> Any:
        current_span = current_span_var.get()
        span_id = current_span.id if current_span else str(uuid.uuid4())
        branch = branch_id or (current_span.branch_id if current_span else "main")
        from swarmwatch.serializer import SafeStateSerializer
        from swarmwatch.schema import StateSnapshot
        serializer = SafeStateSerializer()
        sanitized = serializer.sanitize(state_data)
        state_hash = serializer.compute_hash(sanitized)
        snap = StateSnapshot(
            id=str(uuid.uuid4()),
            span_id=span_id,
            branch_id=branch,
            state_hash=state_hash,
            state_data=sanitized,
            created_at=datetime.now(timezone.utc),
        )
        self.transport._call_storage("save_snapshot", snap)
        return snap

    async def snapshot(self, state_data: Dict[str, Any], branch_id: Optional[str] = None) -> Any:
        current_span = current_span_var.get()
        span_id = current_span.id if current_span else str(uuid.uuid4())
        branch = branch_id or (current_span.branch_id if current_span else "main")
        return await self.timemachine.snapshot(span_id=span_id, state_data=state_data, branch_id=branch)

    async def step_back(self, target_span_id: str, new_branch_id: Optional[str] = None) -> Dict[str, Any]:
        return await self.timemachine.step_back(target_span_id=target_span_id, new_branch_id=new_branch_id)

    async def fork_branch(self, target_span_id: str, new_branch_id: str) -> Dict[str, Any]:
        return await self.timemachine.fork_branch(
            swarm_id=self.swarm_id,
            target_span_id=target_span_id,
            new_branch_id=new_branch_id,
        )

    async def list_branches(self) -> List[str]:
        return await self.timemachine.list_branches(swarm_id=self.swarm_id)

    async def list_snapshots(self, branch_id: Optional[str] = None) -> List[Any]:
        return await self.timemachine.get_snapshots(swarm_id=self.swarm_id, branch_id=branch_id)

    async def diff_snapshots(self, span_id_a: str, span_id_b: str) -> Any:
        return await self.timemachine.diff_snapshots(
            swarm_id=self.swarm_id,
            span_id_a=span_id_a,
            span_id_b=span_id_b,
            project_id=getattr(self, "project_id", "default-project"),
        )

    async def resume_from_fork(
        self,
        branch_id: str,
        handler: Callable[..., Any],
        state_overrides: Optional[Dict[str, Any]] = None,
    ) -> Any:
        return await self.runner.resume(branch_id=branch_id, handler=handler, state_overrides=state_overrides)

    def resume_from_fork_sync(
        self,
        branch_id: str,
        handler: Callable[..., Any],
        state_overrides: Optional[Dict[str, Any]] = None,
    ) -> Any:
        return self.runner.resume_sync(branch_id=branch_id, handler=handler, state_overrides=state_overrides)

    def flush(self) -> None:
        self.transport.flush()

    def shutdown(self) -> None:
        if self.webhook_notifier:
            self.webhook_notifier.shutdown()
        self.transport.shutdown()
        if hasattr(type(self.storage), "close_sync"):
            self.storage.close_sync()


# Global Client Singleton Management
_global_client: Optional[SwarmClient] = None


def init(
    swarm_id: str = "default-swarm",
    storage: Optional[StorageAdapter] = None,
    transport_config: Optional[TransportConfig] = None,
    webhooks: Optional[List[WebhookConfig]] = None,
    db_path: str = "swarmwatch.db",
) -> SwarmClient:
    global _global_client
    if _global_client is not None:
        _global_client.shutdown()
    _global_client = SwarmClient(
        swarm_id=swarm_id,
        storage=storage,
        transport_config=transport_config,
        webhooks=webhooks,
        db_path=db_path,
    )
    return _global_client


def get_client() -> SwarmClient:
    global _global_client
    if _global_client is None:
        _global_client = init()
    return _global_client


def record_circuit_event(event: CircuitEvent) -> None:
    get_client().record_circuit_event(event)


def span(
    name: str,
    source: str = "system",
    target: str = "system",
    span_type: str = "span",
    branch_id: Optional[str] = None,
    input_payload: Optional[Dict[str, Any]] = None,
) -> SpanContext:
    return get_client().span(
        name=name,
        source=source,
        target=target,
        span_type=span_type,
        branch_id=branch_id,
        input_payload=input_payload,
    )


def agent(
    name: Optional[str] = None,
    role: Optional[str] = None,
    target: str = "environment",
    branch_id: Optional[str] = None,
) -> Callable:
    return get_client().agent(name=name, role=role, target=target, branch_id=branch_id)


def sync_state(state_data: Dict[str, Any], branch_id: Optional[str] = None) -> Any:
    return get_client().sync_state(state_data, branch_id=branch_id)


async def snapshot(state_data: Dict[str, Any], branch_id: Optional[str] = None) -> Any:
    return await get_client().snapshot(state_data, branch_id=branch_id)


async def step_back(target_span_id: str, new_branch_id: Optional[str] = None) -> Dict[str, Any]:
    return await get_client().step_back(target_span_id=target_span_id, new_branch_id=new_branch_id)


async def fork_branch(target_span_id: str, new_branch_id: str) -> Dict[str, Any]:
    return await get_client().fork_branch(target_span_id=target_span_id, new_branch_id=new_branch_id)


async def list_branches() -> List[str]:
    return await get_client().list_branches()


async def list_snapshots(branch_id: Optional[str] = None) -> List[Any]:
    return await get_client().list_snapshots(branch_id=branch_id)


async def resume_from_fork(
    branch_id: str,
    handler: Callable[..., Any],
    state_overrides: Optional[Dict[str, Any]] = None,
) -> Any:
    return await get_client().resume_from_fork(
        branch_id=branch_id, handler=handler, state_overrides=state_overrides
    )


def resume_from_fork_sync(
    branch_id: str,
    handler: Callable[..., Any],
    state_overrides: Optional[Dict[str, Any]] = None,
) -> Any:
    return get_client().resume_from_fork_sync(
        branch_id=branch_id, handler=handler, state_overrides=state_overrides
    )


def flush() -> None:
    if _global_client is not None:
        _global_client.flush()


def shutdown() -> None:
    global _global_client
    if _global_client is not None:
        _global_client.shutdown()
        _global_client = None
