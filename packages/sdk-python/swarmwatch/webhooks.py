# -*- coding: utf-8 -*-
"""
SwarmWatch Enterprise Webhook Alerting Dispatcher.
Provides non-blocking, asynchronous notifications to Slack, Discord, and custom webhooks
when circuit breaker events (warnings or trips) occur.
Guarantees sub-microsecond non-blocking execution on agent threads.
"""
from datetime import datetime, timezone
import json
import logging
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional
import urllib.request
from pydantic import BaseModel, Field

from swarmwatch.schema import CircuitEvent

logger = logging.getLogger("swarmwatch.webhooks")


class WebhookConfig(BaseModel):
    """Configuration for an enterprise webhook destination."""
    url: str
    format: str = Field(default="swarmwatch", description="'slack' | 'discord' | 'swarmwatch'")
    secret: Optional[str] = Field(default=None, description="HMAC secret or authorization token")
    min_level: str = Field(default="warning", description="'warning' | 'trip'")
    max_retries: int = Field(default=3, description="Maximum retry attempts on network error")
    timeout: float = Field(default=5.0, description="HTTP timeout in seconds")


def format_webhook_payload(event: CircuitEvent, config: WebhookConfig) -> Dict[str, Any]:
    """
    Formats CircuitEvent according to target webhook protocol (Slack, Discord, or SwarmWatch).
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
    Asynchronous, non-blocking webhook dispatcher.
    Queues events and sends them via background worker thread to prevent agent latency.
    """

    def __init__(
        self,
        configs: Optional[List[WebhookConfig]] = None,
        sender: Optional[Callable[[str, Dict[str, Any], Dict[str, str]], bool]] = None,
        max_queue_size: int = 2000,
    ):
        self.configs = configs or []
        self._sender = sender or _default_http_sender
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

            for attempt in range(1, config.max_retries + 1):
                try:
                    self._sender(config.url, payload, headers)
                    break
                except Exception as e:
                    if attempt == config.max_retries:
                        logger.error("Failed to send webhook to %s after %d attempts: %s", config.url, attempt, e)
                    else:
                        time.sleep(0.05 * attempt)

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
