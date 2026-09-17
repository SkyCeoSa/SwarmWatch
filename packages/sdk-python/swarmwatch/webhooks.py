# -*- coding: utf-8 -*-
"""
SwarmWatch Enterprise Webhook Alerting Dispatcher (Seam 6 & 7 Extension / ADR 0007 & ADR 0023).
Provides non-blocking, asynchronous notifications to Slack Block Kit, Discord Embeds,
PagerDuty Events API v2, and custom webhooks when circuit breaker events occur.
Guarantees sub-microsecond non-blocking execution on agent threads with Dead-Letter Queue (DLQ) support.
"""
from datetime import datetime, timezone
import json
import logging
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional
import urllib.request
import uuid
from pydantic import BaseModel, Field

from swarmwatch.schema import CircuitEvent

logger = logging.getLogger("swarmwatch.webhooks")


class WebhookConfig(BaseModel):
    """Configuration for an enterprise webhook destination."""
    url: str
    format: str = Field(default="swarmwatch", description="'slack' | 'discord' | 'pagerduty' | 'swarmwatch'")
    secret: Optional[str] = Field(default=None, description="HMAC secret, Bearer token, or API key")
    routing_key: Optional[str] = Field(default=None, description="PagerDuty integration routing key")
    min_level: str = Field(default="warning", description="'warning' | 'trip'")
    max_retries: int = Field(default=3, description="Maximum retry attempts on network error")
    timeout: float = Field(default=5.0, description="HTTP timeout in seconds")


class DeadLetterRecord(BaseModel):
    """Record of an undeliverable webhook notification captured in the Dead-Letter Queue."""
    id: str = Field(default_factory=lambda: f"dlq-{uuid.uuid4().hex[:12]}")
    event_id: str
    swarm_id: str
    destination_url: str
    format: str
    attempt_count: int
    last_error: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Dict[str, Any]


def format_webhook_payload(event: CircuitEvent, config: WebhookConfig) -> Dict[str, Any]:
    """
    Formats CircuitEvent according to target webhook protocol:
    Slack Block Kit, Discord Embeds, PagerDuty Events API v2, or native SwarmWatch JSON.
    """
    fmt = config.format.lower()
    savings = event.counterfactual_savings

    if fmt == "slack":
        is_trip = event.event_type.lower() == "trip"
        header_text = f"{'[SWARMWATCH CIRCUIT TRIP]' if is_trip else '[SWARMWATCH CIRCUIT WARNING]'} Swarm: {event.swarm_id}"

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": header_text, "emoji": False},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Event*: {event.event_type.upper()}"},
                    {"type": "mrkdwn", "text": f"*Metric*: `{event.metric_val:.4f}`"},
                    {"type": "mrkdwn", "text": f"*Reason*: {event.reason}"},
                    {"type": "mrkdwn", "text": f"*Time*: {event.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}"},
                ],
            },
        ]

        if savings:
            savings_text = (
                f"*Counterfactual Savings (ADR 0006)*:\n"
                f"- Prevented: *{savings.tokens_saved:,} tokens* (~${savings.dollars_saved:.4f})\n"
                f"- Averted Turns: *{savings.turns_averted}*\n"
                f"- Budget Consumed: ${savings.budget_consumed:.4f}"
            )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": savings_text},
            })

        return {
            "text": header_text,
            "blocks": blocks,
        }

    elif fmt == "discord":
        is_trip = event.event_type.lower() == "trip"
        color = 0xEF4444 if is_trip else 0xF59E0B  # Crimson for trip, Amber for warning
        title = f"{'[SWARMWATCH CIRCUIT TRIP]' if is_trip else '[SWARMWATCH CIRCUIT WARNING]'} Swarm: {event.swarm_id}"

        fields = [
            {"name": "Status", "value": event.event_type.upper(), "inline": True},
            {"name": "Metric", "value": f"`{event.metric_val:.4f}`", "inline": True},
            {"name": "Reason", "value": event.reason, "inline": False},
        ]

        if savings:
            fields.append({
                "name": "Counterfactual Savings",
                "value": f"{savings.tokens_saved:,} tokens (~${savings.dollars_saved:.4f}) across {savings.turns_averted} averted turns",
                "inline": False,
            })

        return {
            "embeds": [
                {
                    "title": title,
                    "color": color,
                    "fields": fields,
                    "timestamp": event.created_at.isoformat(),
                }
            ]
        }

    elif fmt == "pagerduty":
        is_trip = event.event_type.lower() == "trip"
        severity = "critical" if is_trip else "warning"
        summary = f"[{'SWARMWATCH CIRCUIT TRIP' if is_trip else 'SWARMWATCH CIRCUIT WARNING'}] Swarm '{event.swarm_id}': {event.reason}"
        routing_key = config.routing_key or config.secret or ""

        custom_details: Dict[str, Any] = {
            "swarm_id": event.swarm_id,
            "span_id": event.span_id,
            "event_type": event.event_type,
            "metric_val": float(event.metric_val),
            "reason": event.reason,
            "created_at": event.created_at.isoformat(),
        }
        if savings:
            custom_details.update({
                "tokens_saved": savings.tokens_saved,
                "dollars_saved": savings.dollars_saved,
                "turns_averted": savings.turns_averted,
                "budget_consumed": savings.budget_consumed,
                "budget_protected": savings.budget_protected,
            })

        return {
            "routing_key": routing_key,
            "event_action": "trigger",
            "dedup_key": f"swarmwatch-{event.swarm_id}-{event.id}",
            "payload": {
                "summary": summary,
                "severity": severity,
                "source": "swarmwatch-circuit-breaker",
                "component": "agent_swarm",
                "group": "multi_agent_system",
                "class": "loop_prevention",
                "timestamp": event.created_at.isoformat(),
                "custom_details": custom_details,
            },
        }

    else:
        # Default SwarmWatch JSON schema
        data = {
            "id": event.id,
            "swarm_id": event.swarm_id,
            "span_id": event.span_id,
            "event_type": event.event_type,
            "reason": event.reason,
            "metric_val": float(event.metric_val),
            "created_at": event.created_at.isoformat(),
        }
        if savings:
            data["counterfactual_savings"] = savings.model_dump()
        return data


def _default_http_sender(url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> bool:
    """Standard HTTP sender using urllib."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        return resp.status in (200, 201, 202, 204)


class WebhookNotifier:
    """
    Asynchronous, non-blocking webhook dispatcher with Dead-Letter Queue (DLQ).
    Queues events and delivers them via background worker thread to guarantee sub-microsecond agent latency.
    """

    def __init__(
        self,
        configs: Optional[List[WebhookConfig]] = None,
        sender: Optional[Callable[[str, Dict[str, Any], Dict[str, str]], bool]] = None,
        max_queue_size: int = 2000,
        max_dlq_size: int = 500,
    ):
        self.configs = configs or []
        self._sender = sender or _default_http_sender
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._dlq_lock = threading.Lock()
        self._is_started = False
        self._max_dlq_size = max_dlq_size
        self._dead_letter_queue: List[DeadLetterRecord] = []
        self._stats = {
            "delivered": 0,
            "failed": 0,
            "retried": 0,
            "dlq_count": 0,
        }

    def start(self) -> None:
        """Starts background worker thread."""
        with self._lock:
            if self._is_started:
                return
            self._is_started = True
            self._stop_event.clear()
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="swarmwatch-webhook-dispatcher",
                daemon=True,
            )
            self._worker_thread.start()

    def notify(self, event: CircuitEvent) -> bool:
        """
        Submits an event for asynchronous webhook dispatch.
        Sub-microsecond return on caller thread.
        """
        if not self.configs:
            return True

        try:
            self._queue.put_nowait(event)
            return True
        except queue.Full:
            logger.warning("SwarmWatch WebhookNotifier queue is full; dropping event %s", event.id)
            return False

    def _deliver_event(self, event: CircuitEvent) -> None:
        for config in self.configs:
            # Check minimum alert level
            if config.min_level.lower() == "trip" and event.event_type.lower() != "trip":
                continue

            payload = format_webhook_payload(event, config)
            headers = {"Content-Type": "application/json"}
            if config.secret:
                headers["Authorization"] = f"Bearer {config.secret}"

            success = False
            last_err_msg = ""

            for attempt in range(1, config.max_retries + 1):
                try:
                    res = self._sender(config.url, payload, headers)
                    if res is not False:
                        success = True
                        with self._dlq_lock:
                            self._stats["delivered"] += 1
                        break
                    else:
                        raise RuntimeError(f"HTTP sender returned non-2xx status for {config.url}")
                except Exception as e:
                    last_err_msg = str(e)
                    with self._dlq_lock:
                        self._stats["retried"] += 1
                    if attempt == config.max_retries:
                        logger.error(
                            "Failed to send webhook to %s after %d attempts: %s",
                            config.url,
                            attempt,
                            e,
                        )
                    else:
                        time.sleep(0.02 * attempt)

            if not success:
                # Capture to Dead-Letter Queue
                with self._dlq_lock:
                    self._stats["failed"] += 1
                    dlq_record = DeadLetterRecord(
                        event_id=event.id,
                        swarm_id=event.swarm_id,
                        destination_url=config.url,
                        format=config.format,
                        attempt_count=config.max_retries,
                        last_error=last_err_msg or "Delivery attempts exhausted",
                        payload=payload,
                    )
                    self._dead_letter_queue.append(dlq_record)
                    if len(self._dead_letter_queue) > self._max_dlq_size:
                        self._dead_letter_queue.pop(0)
                    self._stats["dlq_count"] = len(self._dead_letter_queue)

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=0.1)
                if isinstance(item, tuple) and len(item) == 2 and item[0] == "__SWARMWATCH_FLUSH__":
                    item[1].set()
                    self._queue.task_done()
                    continue

                self._deliver_event(item)
                self._queue.task_done()
            except queue.Empty:
                pass

        # Drain remaining items
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                if isinstance(item, tuple) and len(item) == 2 and item[0] == "__SWARMWATCH_FLUSH__":
                    item[1].set()
                else:
                    self._deliver_event(item)
                self._queue.task_done()
            except queue.Empty:
                break

    def get_dlq(self) -> List[DeadLetterRecord]:
        """Returns shallow copy of Dead-Letter Queue records."""
        with self._dlq_lock:
            return list(self._dead_letter_queue)

    def clear_dlq(self) -> None:
        """Purges all records from Dead-Letter Queue."""
        with self._dlq_lock:
            self._dead_letter_queue.clear()
            self._stats["dlq_count"] = 0

    def replay_dlq(self) -> int:
        """
        Replays undelivered payloads currently stored in Dead-Letter Queue.
        Returns count of successfully replayed items.
        """
        with self._dlq_lock:
            records = list(self._dead_letter_queue)
            self._dead_letter_queue.clear()
            self._stats["dlq_count"] = 0

        replayed_count = 0
        for record in records:
            headers = {"Content-Type": "application/json"}
            try:
                res = self._sender(record.destination_url, record.payload, headers)
                if res is not False:
                    replayed_count += 1
                    with self._dlq_lock:
                        self._stats["delivered"] += 1
                else:
                    with self._dlq_lock:
                        self._dead_letter_queue.append(record)
                        self._stats["dlq_count"] = len(self._dead_letter_queue)
            except Exception as e:
                record.last_error = f"Replay failed: {e}"
                with self._dlq_lock:
                    self._dead_letter_queue.append(record)
                    self._stats["dlq_count"] = len(self._dead_letter_queue)

        return replayed_count

    def get_stats(self) -> Dict[str, int]:
        """Returns transmission statistics for monitoring."""
        with self._dlq_lock:
            return dict(self._stats)

    def flush(self, timeout: float = 2.0) -> None:
        """Synchronously drains all queued notifications."""
        with self._lock:
            worker_alive = self._worker_thread is not None and self._worker_thread.is_alive()

        if worker_alive:
            done_event = threading.Event()
            self._queue.put(("__SWARMWATCH_FLUSH__", done_event))
            done_event.wait(timeout=timeout)
        else:
            while not self._queue.empty():
                try:
                    item = self._queue.get_nowait()
                    if isinstance(item, tuple) and len(item) == 2 and item[0] == "__SWARMWATCH_FLUSH__":
                        item[1].set()
                    else:
                        self._deliver_event(item)
                    self._queue.task_done()
                except queue.Empty:
                    break

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
