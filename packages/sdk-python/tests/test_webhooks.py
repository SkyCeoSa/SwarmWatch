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
