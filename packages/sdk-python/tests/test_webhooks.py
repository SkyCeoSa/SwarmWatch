# -*- coding: utf-8 -*-
"""
TDD Tests for Enterprise Webhook Alerting Dispatcher (Seam 6 / Observability & Incident Alerting).
Validates non-blocking dispatch, Slack/Discord/Generic JSON formatting, and counterfactual savings embedding.
"""
from datetime import datetime, timezone
import pytest

from swarmwatch.schema import CircuitEvent, CounterfactualSavings
from swarmwatch.webhooks import WebhookConfig, WebhookNotifier, format_webhook_payload


def test_format_webhook_payload_slack():
    """Verify Slack block kit payload formatting."""
    event = CircuitEvent(
        id="ce-slack-1",
        swarm_id="swarm-prod-alpha",
        event_type="trip",
        reason="Low Shannon entropy detected (0.22 <= 0.40)",
        metric_val=0.22,
        counterfactual_savings=CounterfactualSavings(
            tokens_saved=25000,
            dollars_saved=0.50,
            turns_averted=5,
            budget_consumed=0.15,
            budget_protected=0.85,
        ),
        created_at=datetime.now(timezone.utc),
    )
    config = WebhookConfig(url="https://hooks.slack.com/services/test", format="slack")
    payload = format_webhook_payload(event, config)

    assert "text" in payload
    assert "blocks" in payload
    assert "[SWARMWATCH CIRCUIT TRIP]" in payload["text"]
    # Check that counterfactual savings are rendered
    text_content = str(payload["blocks"])
    assert "25,000 tokens" in text_content
    assert "$0.5000" in text_content or "$0.50" in text_content


def test_format_webhook_payload_discord():
    """Verify Discord embeds payload formatting."""
    event = CircuitEvent(
        id="ce-discord-1",
        swarm_id="swarm-prod-beta",
        event_type="warning",
        reason="High lexical repetition detected (0.89 >= 0.85)",
        metric_val=0.89,
        created_at=datetime.now(timezone.utc),
    )
    config = WebhookConfig(url="https://discord.com/api/webhooks/test", format="discord")
    payload = format_webhook_payload(event, config)

    assert "embeds" in payload
    embed = payload["embeds"][0]
    assert "WARNING" in embed["title"]
    assert embed["color"] == 0xF59E0B  # Amber color code


def test_webhook_notifier_non_blocking_delivery():
    """
    Verify that WebhookNotifier queues and delivers events without blocking caller.
    Uses mock HTTP transport.
    """
    delivered_payloads = []

    def mock_sender(url: str, payload: dict, headers: dict):
        delivered_payloads.append((url, payload))
        return True

    config = WebhookConfig(
        url="https://api.example.com/alerts",
        format="swarmwatch",
        min_level="warning",
    )
    notifier = WebhookNotifier(configs=[config], sender=mock_sender)
    notifier.start()

    event = CircuitEvent(
        id="ce-test-99",
        swarm_id="swarm-test",
        event_type="trip",
        reason="Maximum turns ceiling exceeded (51 > 50)",
        metric_val=51.0,
        counterfactual_savings=CounterfactualSavings(
            tokens_saved=10000,
            dollars_saved=0.20,
            turns_averted=2,
        ),
        created_at=datetime.now(timezone.utc),
    )

    # Calling notify must return immediately (non-blocking)
    accepted = notifier.notify(event)
    assert accepted is True

    # Flush notifier to process queue
    notifier.flush(timeout=2.0)
    notifier.shutdown()

    assert len(delivered_payloads) == 1
    url, data = delivered_payloads[0]
    assert url == "https://api.example.com/alerts"
    assert data["swarm_id"] == "swarm-test"
    assert data["counterfactual_savings"]["tokens_saved"] == 10000


def test_webhook_notifier_filters_below_min_level():
    """Verify that warnings are not sent if min_level is set to 'trip'."""
    delivered = []

    def mock_sender(url, payload, headers):
        delivered.append(payload)
        return True

    config = WebhookConfig(
        url="https://api.example.com/critical",
        format="swarmwatch",
        min_level="trip",  # Only trip events
    )
    notifier = WebhookNotifier(configs=[config], sender=mock_sender)
    notifier.start()

    warning_event = CircuitEvent(
        id="ce-warn-1",
        swarm_id="swarm-test",
        event_type="warning",
        reason="Warning level message",
        metric_val=0.45,
    )

    notifier.notify(warning_event)
    notifier.flush(timeout=1.0)
    notifier.shutdown()

    assert len(delivered) == 0, "Warning must be filtered out when min_level is 'trip'"


def test_format_webhook_payload_pagerduty():
    """Verify PagerDuty Events API v2 payload formatting and custom details."""
    event = CircuitEvent(
        id="ce-pd-1",
        swarm_id="swarm-prod-gamma",
        event_type="trip",
        reason="Information entropy collapse (0.12 <= 0.40)",
        metric_val=0.12,
        counterfactual_savings=CounterfactualSavings(
            tokens_saved=50000,
            dollars_saved=1.00,
            turns_averted=10,
            budget_consumed=0.30,
            budget_protected=1.70,
        ),
        created_at=datetime.now(timezone.utc),
    )
    config = WebhookConfig(
        url="https://events.pagerduty.com/v2/enqueue",
        format="pagerduty",
        routing_key="pd-routing-key-xyz123",
    )
    payload = format_webhook_payload(event, config)

    assert payload["routing_key"] == "pd-routing-key-xyz123"
    assert payload["event_action"] == "trigger"
    assert "swarmwatch-swarm-prod-gamma-ce-pd-1" in payload["dedup_key"]
    assert payload["payload"]["severity"] == "critical"
    assert payload["payload"]["source"] == "swarmwatch-circuit-breaker"
    assert "[SWARMWATCH CIRCUIT TRIP]" in payload["payload"]["summary"]

    details = payload["payload"]["custom_details"]
    assert details["tokens_saved"] == 50000
    assert details["dollars_saved"] == 1.00
    assert details["turns_averted"] == 10
    assert details["metric_val"] == 0.12


def test_webhook_notifier_dead_letter_queue_and_replay():
    """Verify that failed transmissions are captured in Dead-Letter Queue (DLQ) and can be replayed."""
    fail_count = 0

    def failing_sender(url, payload, headers):
        nonlocal fail_count
        fail_count += 1
        raise ConnectionResetError("Connection reset by peer simulated")

    config = WebhookConfig(
        url="https://unreachable.service.internal/alerts",
        format="pagerduty",
        max_retries=2,
    )
    notifier = WebhookNotifier(configs=[config], sender=failing_sender)
    notifier.start()

    event = CircuitEvent(
        id="ce-fail-1",
        swarm_id="swarm-dlq-test",
        event_type="trip",
        reason="Exhausted token quota",
        metric_val=100000.0,
    )

    notifier.notify(event)
    notifier.flush(timeout=2.0)

    # Verify DLQ capture
    dlq = notifier.get_dlq()
    assert len(dlq) == 1
    assert dlq[0].event_id == "ce-fail-1"
    assert dlq[0].destination_url == "https://unreachable.service.internal/alerts"
    assert dlq[0].format == "pagerduty"
    assert "Connection reset by peer" in dlq[0].last_error

    stats = notifier.get_stats()
    assert stats["failed"] == 1
    assert stats["retried"] == 2
    assert stats["dlq_count"] == 1

    # Now simulate recovered sender and replay DLQ
    replayed_success = []

    def recovered_sender(url, payload, headers):
        replayed_success.append((url, payload))
        return True

    notifier._sender = recovered_sender
    replayed_count = notifier.replay_dlq()

    assert replayed_count == 1
    assert len(replayed_success) == 1
    assert len(notifier.get_dlq()) == 0
    notifier.shutdown()


def test_server_alert_endpoints(tmp_path):
    """Verify control plane REST endpoints for testing alerts and managing the DLQ."""
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app
    from swarmwatch.storage import SQLiteStorageAdapter

    config = WebhookConfig(
        url="https://api.internal/webhook",
        format="slack",
        max_retries=1,
    )

    def mock_sender(url, payload, headers):
        raise TimeoutError("Simulated timeout")

    notifier = WebhookNotifier(configs=[config], sender=mock_sender)
    notifier.start()
    storage = SQLiteStorageAdapter(db_path=str(tmp_path / "alerts_test.db"))
    app = create_app(storage=storage, webhook_notifier=notifier)
    client = TestClient(app)

    # 1. Test alert generation endpoint
    res_test = client.post("/api/v1/alerts/test", json={
        "format": "pagerduty",
        "url": "https://events.pagerduty.com/v2/enqueue",
        "secret": "test-routing-key",
        "reason": "Test incident trigger",
    })
    assert res_test.status_code == 200
    data_test = res_test.json()
    assert data_test["status"] == "generated"
    assert data_test["format"] == "pagerduty"
    assert data_test["payload"]["routing_key"] == "test-routing-key"
    assert data_test["payload"]["event_action"] == "trigger"

    # 2. Trigger an event that fails into DLQ
    event = CircuitEvent(
        id="ce-server-fail",
        swarm_id="swarm-server",
        event_type="trip",
        reason="Dead-letter trigger",
        metric_val=1.0,
    )
    notifier.notify(event)
    notifier.flush(timeout=1.0)

    # 3. Inspect DLQ via REST API
    res_dlq = client.get("/api/v1/alerts/dlq")
    assert res_dlq.status_code == 200
    data_dlq = res_dlq.json()
    assert data_dlq["count"] >= 1
    assert data_dlq["records"][0]["event_id"] == "ce-server-fail"
    assert data_dlq["stats"]["failed"] >= 1

    # 4. Replay DLQ via REST API
    notifier._sender = lambda url, payload, headers: True
    res_replay = client.post("/api/v1/alerts/dlq/replay")
    assert res_replay.status_code == 200
    data_replay = res_replay.json()
    assert data_replay["replayed"] >= 1
    notifier.shutdown()

