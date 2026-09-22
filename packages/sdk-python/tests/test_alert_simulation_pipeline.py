# -*- coding: utf-8 -*-
"""
TDD Tests for Automated Alert Route Webhook Testing & Synthetic Simulation Pipeline (Seam 18 / ADR 0029).

Validates SyntheticAlertReceiver ephemeral port binding, schema validation for Slack, Discord,
and PagerDuty, programmable fault injection, Dead-Letter Queue (DLQ) buffering/recovery,
and live circuit breaker trip linkage with zero external cloud dependencies.
"""

import json
from urllib.error import HTTPError
import urllib.request
import pytest

from swarmwatch.synthetic_alert import (
    SyntheticAlertReceiver,
    AlertSimulationPipeline,
    AlertSimulationReport,
    validate_slack_block_kit,
    validate_discord_embed,
    validate_pagerduty_event,
)


def _send_post(url: str, payload: dict) -> tuple[int, dict]:
    """Helper to send HTTP POST to synthetic receiver."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return resp.status, body
    except HTTPError as e:
        body = json.loads(e.read().decode("utf-8"))
        return e.code, body


class TestSyntheticAlertReceiver:
    """Verifies synthetic receiver lifecycle, routes, schema checks, and fault injection."""

    @pytest.fixture
    def receiver(self):
        recv = SyntheticAlertReceiver()
        recv.start()
        yield recv
        recv.stop()

    def test_receiver_lifecycle_and_url_generation(self, receiver):
        assert receiver.base_url.startswith("http://127.0.0.1:")
        assert receiver.port > 0

        slack_url = receiver.url_for("slack")
        assert slack_url.endswith("/webhook/slack")

        pd_url = receiver.url_for("pagerduty")
        assert pd_url.endswith("/webhook/pagerduty")

    def test_slack_block_kit_schema_validation(self, receiver):
        valid_payload = {
            "text": "[SWARMWATCH CIRCUIT TRIP] Swarm: test-1",
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "Circuit Trip Alert"},
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": "*Event*: TRIP"},
                        {"type": "mrkdwn", "text": "*Metric*: 0.15"},
                    ],
                },
            ],
        }
        status, resp = _send_post(receiver.url_for("slack"), valid_payload)
        assert status == 200
        assert resp["schema_valid"] is True

        events = receiver.get_received_events("slack")
        assert len(events) == 1
        assert events[0].schema_valid is True

        # Invalid payload (missing header)
        invalid_payload = {"text": "plain text only", "blocks": []}
        status, resp = _send_post(receiver.url_for("slack"), invalid_payload)
        assert status == 200
        assert resp["schema_valid"] is False

        events = receiver.get_received_events("slack")
        assert len(events) == 2
        assert events[1].schema_valid is False
        assert len(events[1].schema_errors) > 0

    def test_discord_embed_schema_validation(self, receiver):
        valid_discord = {
            "embeds": [
                {
                    "title": "[SWARMWATCH CIRCUIT WARNING] Swarm: alpha",
                    "color": 0xF59E0B,
                    "fields": [
                        {"name": "Status", "value": "WARNING", "inline": True},
                        {"name": "Metric", "value": "`0.88`", "inline": True},
                    ],
                }
            ]
        }
        status, resp = _send_post(receiver.url_for("discord"), valid_discord)
        assert status == 200
        assert resp["schema_valid"] is True

        # Invalid discord (missing title and fields)
        invalid_discord = {"embeds": [{"color": 123}]}
        status, resp = _send_post(receiver.url_for("discord"), invalid_discord)
        assert status == 200
        assert resp["schema_valid"] is False

    def test_pagerduty_event_schema_validation(self, receiver):
        valid_pd = {
            "routing_key": "test-routing-key",
            "event_action": "trigger",
            "payload": {
                "summary": "Circuit Breaker Trip on Swarm alpha",
                "severity": "critical",
                "source": "swarmwatch",
                "custom_details": {"metric_val": 0.12, "dollars_saved": 4.50},
            },
        }
        status, resp = _send_post(receiver.url_for("pagerduty"), valid_pd)
        assert status == 202
        assert resp["schema_valid"] is True

        # Invalid pagerduty (missing routing_key)
        invalid_pd = {
            "event_action": "trigger",
            "payload": {"summary": "missing routing key"},
        }
        status, resp = _send_post(receiver.url_for("pagerduty"), invalid_pd)
        assert status == 202
        assert resp["schema_valid"] is False

    def test_fault_injection_and_recovery(self, receiver):
        route = "slack"
        target_url = receiver.url_for(route)

        # Inject 503 for 2 requests
        receiver.inject_fault(route, status_code=503, count=2)

        payload = {"text": "test", "blocks": [{"type": "header", "text": {}}]}

        # Request 1 -> 503
        s1, r1 = _send_post(target_url, payload)
        assert s1 == 503
        assert "Simulated fault" in r1["error"]

        # Request 2 -> 503
        s2, r2 = _send_post(target_url, payload)
        assert s2 == 503

        # Request 3 -> Recovered to 200
        s3, r3 = _send_post(target_url, payload)
        assert s3 == 200

        # Inject delay and clear
        receiver.inject_delay(route, delay_seconds=0.1)
        assert receiver.get_delay(route) == 0.1
        receiver.recover_route(route)
        assert receiver.get_delay(route) == 0.0


class TestAlertSimulationPipeline:
    """Verifies end-to-end multi-phase simulation pipeline orchestration."""

    def test_pipeline_happy_path(self):
        pipeline = AlertSimulationPipeline()
        report = pipeline.run(
            routes=["slack", "discord", "pagerduty"],
            include_faults=False,
            include_circuit_trip=False,
        )

        assert isinstance(report, AlertSimulationReport)
        assert report.all_passed is True
        assert report.total_routes_tested == 3
        assert report.phase_results["multi_channel"] is True
        assert report.counterfactual_savings_verified is True
        assert report.tokens_saved == 75000
        assert report.dollars_saved == 1.875

        for r in report.route_results:
            assert r.delivered is True
            assert r.schema_valid is True
            assert r.latency_ms > 0

    def test_pipeline_fault_injection_and_dlq_replay(self):
        pipeline = AlertSimulationPipeline()
        report = pipeline.run(
            routes=["slack", "pagerduty"],
            include_faults=True,
            include_circuit_trip=False,
        )

        assert report.phase_results["fault_and_dlq"] is True
        assert report.phase_results["recovery_and_replay"] is True

        # Slack was the fault target; verify DLQ accumulated and replayed
        slack_result = next(r for r in report.route_results if r.route == "slack")
        assert slack_result.dlq_accumulated >= 1
        assert slack_result.dlq_replayed >= 1

    def test_pipeline_live_swarm_circuit_trip_linkage(self):
        pipeline = AlertSimulationPipeline()
        report = pipeline.run(
            routes=["discord"],
            include_faults=False,
            include_circuit_trip=True,
        )

        assert report.phase_results["live_swarm_trip"] is True
        assert report.all_passed is True

    def test_pipeline_full_e2e_simulation_with_terminal_report(self):
        pipeline = AlertSimulationPipeline()
        report = pipeline.run(
            routes=["slack", "discord", "pagerduty"],
            include_faults=True,
            include_circuit_trip=True,
        )

        assert report.all_passed is True
        table = report.to_terminal_table()
        assert "SWARMWATCH SYNTHETIC ALERT ROUTE SIMULATION REPORT" in table
        assert "ALL ROUTES PASSED (100% HEALTHY)" in table
        assert "slack" in table
        assert "discord" in table
        assert "pagerduty" in table
        assert "Savings:    75,000 tokens" in table

        dict_report = report.to_dict()
        assert dict_report["all_passed"] is True
        assert len(dict_report["route_results"]) == 3
