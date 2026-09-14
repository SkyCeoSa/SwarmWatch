# -*- coding: utf-8 -*-
"""
SwarmWatch OpenTelemetry GenAI Semantic Conventions & OTLP Native Exporter.
Provides direct OTLP/HTTP trace federation into enterprise APM collectors (Datadog, Grafana, Honeycomb)
without requiring heavy external OpenTelemetry SDK packages.
Guarantees sub-microsecond non-blocking submission.
"""
from datetime import datetime, timezone
import json
import logging
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional
import urllib.request

from swarmwatch.schema import SwarmSpan

logger = logging.getLogger("swarmwatch.otel")


def to_otel_genai_span(span: SwarmSpan) -> Dict[str, Any]:
    """
    Transforms a SwarmSpan into OpenTelemetry GenAI Semantic Conventions.
    Adheres strictly to OTel AI Observability standard attribute naming.
    """
    start_nano = int(span.start_time.timestamp() * 1e9)
    end_nano = int(span.end_time.timestamp() * 1e9)
    op_name = f"{span.source_agent} -> {span.target_agent}:{span.span_type}"

    return {
        "name": op_name,
        "trace_id": span.swarm_id,
        "span_id": span.id,
        "parent_span_id": span.parent_span_id,
        "start_time_unix_nano": start_nano,
        "end_time_unix_nano": end_nano,
        "attributes": {
            "gen_ai.system": "swarmwatch",
            "gen_ai.agent.name": span.source_agent,
            "gen_ai.target.agent": span.target_agent,
            "gen_ai.operation.name": op_name,
            "gen_ai.usage.total_tokens": span.token_count,
            "gen_ai.swarmwatch.entropy_score": float(span.entropy_score),
            "gen_ai.swarmwatch.branch_id": span.branch_id,
        },
    }


def format_otlp_traces_payload(
    spans: List[SwarmSpan],
    service_name: str = "swarmwatch",
) -> Dict[str, Any]:
    """
    Constructs an OTLP/HTTP JSON trace payload conforming to OpenTelemetry 1.0 specifications.
    Can be ingested by any standard OTLP endpoint (e.g. /v1/traces on port 4318).
    """
    otel_spans = []
    for s in spans:
        item = to_otel_genai_span(s)
        # Convert dictionary attributes to OTLP KeyValue format
        kv_attrs = []
        for k, v in item["attributes"].items():
            if isinstance(v, (int, bool)):
                kv_attrs.append({"key": k, "value": {"intValue": int(v)}})
            elif isinstance(v, float):
                kv_attrs.append({"key": k, "value": {"doubleValue": float(v)}})
            else:
                kv_attrs.append({"key": k, "value": {"stringValue": str(v)}})

        otel_spans.append({
            "traceId": item["trace_id"],
            "spanId": item["span_id"],
            "parentSpanId": item["parent_span_id"] or "",
            "name": item["name"],
            "kind": 1,  # SPAN_KIND_INTERNAL
            "startTimeUnixNano": item["start_time_unix_nano"],
            "endTimeUnixNano": item["end_time_unix_nano"],
            "attributes": kv_attrs,
            "status": {"code": 1},  # STATUS_CODE_OK
        })

    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service_name}},
                        {"key": "telemetry.sdk.name", "value": {"stringValue": "swarmwatch"}},
                        {"key": "telemetry.sdk.language", "value": {"stringValue": "python"}},
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {
                            "name": "swarmwatch.otel",
                            "version": "0.1.0",
                        },
                        "spans": otel_spans,
                    }
                ],
            }
        ]
    }


def _default_otlp_http_sender(url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> bool:
    """Standard HTTP sender targeting OTLP receiver endpoint."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        return resp.status in (200, 202)


class OtelGenAiExporter:
    """
    Asynchronous, non-blocking OpenTelemetry GenAI Exporter.
    Batches spans in an in-memory queue and flushes them to an OTLP HTTP trace collector.
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:4318/v1/traces",
        service_name: str = "swarmwatch",
        batch_size: int = 50,
        flush_interval_seconds: float = 0.5,
        max_queue_size: int = 5000,
        headers: Optional[Dict[str, str]] = None,
        sender: Optional[Callable[[str, Dict[str, Any], Dict[str, str]], bool]] = None,
    ):
        self.endpoint = endpoint
        self.service_name = service_name
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.headers = headers or {"Content-Type": "application/json"}
        self._sender = sender or _default_otlp_http_sender

        self._queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._is_started = False

    def start(self) -> None:
        """Starts background worker thread."""
        with self._lock:
            if self._is_started:
                return
            self._is_started = True
            self._stop_event.clear()
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="swarmwatch-otel-exporter",
                daemon=True,
            )
            self._worker_thread.start()

    def export_span(self, span: SwarmSpan) -> bool:
        """
        Submits a span for asynchronous OTLP export.
        Sub-microsecond return on caller thread.
        """
        try:
            self._queue.put_nowait(span)
            return True
        except queue.Full:
            logger.warning("SwarmWatch OtelGenAiExporter queue full; dropping span %s", span.id)
            return False

    def _flush_batch(self, batch: List[SwarmSpan]) -> None:
        if not batch:
            return
        payload = format_otlp_traces_payload(batch, service_name=self.service_name)
        try:
            self._sender(self.endpoint, payload, self.headers)
        except Exception as e:
            logger.error("Failed to export OTLP batch of %d spans: %s", len(batch), e)

    def _worker_loop(self) -> None:
        batch: List[SwarmSpan] = []
        last_flush = time.monotonic()

        while not self._stop_event.is_set():
            timeout = max(0.0, self.flush_interval_seconds - (time.monotonic() - last_flush))
            try:
                item = self._queue.get(timeout=timeout)
                if isinstance(item, tuple) and len(item) == 2 and item[0] == "__SWARMWATCH_FLUSH__":
                    if batch:
                        self._flush_batch(batch)
                        batch = []
                    item[1].set()
                    self._queue.task_done()
                    continue

                batch.append(item)
                self._queue.task_done()
            except queue.Empty:
                pass

            now = time.monotonic()
            if len(batch) >= self.batch_size or (batch and (now - last_flush) >= self.flush_interval_seconds):
                self._flush_batch(batch)
                batch = []
                last_flush = time.monotonic()

        # Drain remaining items
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                if isinstance(item, tuple) and len(item) == 2 and item[0] == "__SWARMWATCH_FLUSH__":
                    item[1].set()
                else:
                    batch.append(item)
                self._queue.task_done()
            except queue.Empty:
                break

        if batch:
            self._flush_batch(batch)

    def flush(self, timeout: float = 2.0) -> None:
        """Synchronously drains all queued spans."""
        with self._lock:
            worker_alive = self._worker_thread is not None and self._worker_thread.is_alive()

        if worker_alive:
            done = threading.Event()
            self._queue.put(("__SWARMWATCH_FLUSH__", done))
            done.wait(timeout=timeout)
        else:
            batch: List[SwarmSpan] = []
            while not self._queue.empty():
                try:
                    item = self._queue.get_nowait()
                    if isinstance(item, tuple) and len(item) == 2 and item[0] == "__SWARMWATCH_FLUSH__":
                        item[1].set()
                    else:
                        batch.append(item)
                    self._queue.task_done()
                except queue.Empty:
                    break
            if batch:
                self._flush_batch(batch)

    def shutdown(self, timeout: float = 2.0) -> None:
        """Stops worker thread and flushes queue."""
        with self._lock:
            if not self._is_started:
                self.flush()
                return
            self._is_started = False

        self._stop_event.set()
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)
        self.flush()
