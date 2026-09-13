# -*- coding: utf-8 -*-
"""
SwarmWatch SDK In-Process Transport & Background Bounded Queue Worker.
Guarantees sub-microsecond non-blocking span submission, asynchronous batch flushing,
and safe shutdown handling with atexit.
"""

import atexit
import asyncio
import concurrent.futures
import inspect
import logging
import queue
import threading
import time
from typing import List, Optional
from pydantic import BaseModel, Field

from swarmwatch.schema import SwarmSpan
from swarmwatch.storage import StorageAdapter

logger = logging.getLogger("swarmwatch.transport")


class TransportConfig(BaseModel):
    batch_size: int = Field(default=100, description="Max number of spans to flush per batch")
    flush_interval_seconds: float = Field(default=0.5, description="Time interval between batch flushes in seconds")
    max_queue_size: int = Field(default=10000, description="Maximum bounded queue capacity")
    worker_thread_name: str = Field(default="swarmwatch-transport-worker", description="Name of background daemon thread")


class TelemetryTransport:
    """
    High-performance telemetry pipeline running inside the host agent process.
    All span submission is purely non-blocking with zero network I/O on the calling thread.
    """

    def __init__(self, storage: StorageAdapter, config: Optional[TransportConfig] = None):
        self.storage = storage
        self.config = config or TransportConfig()
        self._queue: queue.Queue = queue.Queue(maxsize=self.config.max_queue_size)
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._dropped_count = 0
        self._lock = threading.Lock()
        self._is_started = False
        self._atexit_registered = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def dropped_count(self) -> int:
        with self._lock:
            return self._dropped_count

    def start(self) -> None:
        """Starts the background worker daemon thread."""
        with self._lock:
            if self._is_started:
                return
            self._is_started = True
            self._stop_event.clear()
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name=self.config.worker_thread_name,
                daemon=True,
            )
            self._worker_thread.start()
            if not self._atexit_registered:
                atexit.register(self.shutdown)
                self._atexit_registered = True

    def emit_span(self, span: SwarmSpan) -> bool:
        """
        Submits a span to the bounded queue.
        Executes in < 1 microsecond. Never blocks the calling thread.
        Returns True if queued, False if dropped due to capacity overflow.
        """
        try:
            self._queue.put_nowait(span)
            return True
        except queue.Full:
            with self._lock:
                self._dropped_count += 1
            logger.warning(
                "SwarmWatch telemetry queue full (%d items). Dropping span %s.",
                self.config.max_queue_size,
                span.id,
            )
            return False

    def _call_storage(self, method_name: str, *args, **kwargs):
        # 1. If storage class defines an explicit sync method (e.g. save_spans_sync), prefer it directly
        sync_method_name = f"{method_name}_sync"
        if hasattr(type(self.storage), sync_method_name):
            return getattr(self.storage, sync_method_name)(*args, **kwargs)

        # 2. Invoke method
        method = getattr(self.storage, method_name)
        res = method(*args, **kwargs)

        # 3. If result is a coroutine, resolve it safely across sync/async callers
        if inspect.iscoroutine(res):
            try:
                running_loop = asyncio.get_running_loop()
            except RuntimeError:
                running_loop = None

            if running_loop and running_loop.is_running():
                # Running inside an active async loop (e.g. pytest or async endpoint)
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    return pool.submit(asyncio.run, res).result()
            else:
                if self._loop is None or self._loop.is_closed():
                    self._loop = asyncio.new_event_loop()
                return self._loop.run_until_complete(res)
        return res

    def _flush_batch(self, batch: List[SwarmSpan]) -> None:
        if not batch:
            return
        try:
            self._call_storage("save_spans", batch)
        except Exception as e:
            logger.error("Failed to flush batch of %d spans to storage: %s", len(batch), e, exc_info=True)

    def _worker_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        batch: List[SwarmSpan] = []
        last_flush_time = time.monotonic()

        while not self._stop_event.is_set():
            timeout = max(0.0, self.config.flush_interval_seconds - (time.monotonic() - last_flush_time))
            try:
                item = self._queue.get(timeout=timeout)
                batch.append(item)
                self._queue.task_done()
            except queue.Empty:
                pass

            now = time.monotonic()
            if len(batch) >= self.config.batch_size or (batch and (now - last_flush_time) >= self.config.flush_interval_seconds):
                self._flush_batch(batch)
                batch = []
                last_flush_time = time.monotonic()

        # Drain any remaining items in queue before exiting loop
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                batch.append(item)
                self._queue.task_done()
            except queue.Empty:
                break

        if batch:
            self._flush_batch(batch)

        if self._loop and not self._loop.is_closed():
            self._loop.close()

    def flush(self) -> None:
        """
        Synchronously flushes all pending spans currently in the queue.
        """
        batch: List[SwarmSpan] = []
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                batch.append(item)
                self._queue.task_done()
            except queue.Empty:
                break

        if batch:
            self._flush_batch(batch)

    def shutdown(self, timeout: float = 2.0) -> None:
        """
        Gracefully flushes remaining telemetry and shuts down worker thread.
        """
        with self._lock:
            if not self._is_started:
                self.flush()
                return
            self._is_started = False

        self._stop_event.set()
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)

        self.flush()
