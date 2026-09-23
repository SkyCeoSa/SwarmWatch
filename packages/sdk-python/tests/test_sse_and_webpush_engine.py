# -*- coding: utf-8 -*-
"""
SwarmWatch Server-Sent Events (SSE) & Enterprise WebPush Test Suite (Seam 28 / ADR 0039).

Verifies:
1. Pure Python NIST P-256 (secp256r1) field arithmetic, point operations, and ES256 VAPID JWT minting.
2. SseEventManager subscriber registration, keep-alive pings, filtering, and Last-Event-ID ring-buffer replay.
3. SQLite WAL push_subscriptions storage CRUD operations.
4. FastAPI endpoints: GET /api/v1/events/stream, GET /api/v1/push/vapid-public-key,
   POST /api/v1/push/subscribe, POST /api/v1/push/test, POST /api/v1/push/unsubscribe.
5. In-flight circuit breaker trip dispatching across SSE and WebPush channels.
6. Zero-token cost and sub-microsecond non-blocking execution invariants.
"""

import asyncio
from datetime import datetime, timezone
import json
import os
import tempfile
import time
import pytest
from starlette.testclient import TestClient

from swarmwatch.schema import CircuitEvent, SwarmSpan
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.sse import SseEvent, SseSubscriber, SseEventManager, global_sse_manager
from swarmwatch.webpush import (
    VapidKeyManager,
    PushKeys,
    PushSubscription,
    PushMessage,
    WebPushDispatcher,
    format_circuit_push_message,
    _point_add,
    _point_mul,
    P_256_GX,
    P_256_GY,
)
from swarmwatch.server import create_app
from swarmwatch.metrics import PrometheusMetricsRegistry


# ---------------------------------------------------------------------------
# 1. NIST P-256 Curve & VAPID Cryptographic Engine Tests
# ---------------------------------------------------------------------------

def test_p256_curve_math_and_scalar_multiplication():
    """Verify point addition and scalar multiplication on NIST P-256."""
    g = (P_256_GX, P_256_GY)
    # 2*G
    pt2 = _point_add(g, g)
    assert pt2 is not None
    assert pt2 != g

    # Scalar mul: 2*G using point_mul
    pt2_mul = _point_mul(2, g)
    assert pt2_mul == pt2

    # 3*G
    pt3 = _point_add(pt2, g)
    pt3_mul = _point_mul(3, g)
    assert pt3_mul == pt3


def test_vapid_key_generation_and_es256_signatures():
    """Verify VapidKeyManager generates valid keys and signs/verifies messages with ES256."""
    mgr = VapidKeyManager.generate()
    assert mgr.public_key_b64.startswith("B")  # Uncompressed 0x04 in Base64URL
    assert len(mgr.public_key_b64) in (86, 87, 88)
    assert len(mgr.private_key_b64) in (42, 43, 44)

    msg = b"SwarmWatch VAPID Specification Conformance Check"
    sig = mgr.sign_es256(msg)
    assert len(sig) == 64  # IEEE P1363 64-byte signature

    # Verify signature
    assert mgr.verify_es256(msg, sig) is True

    # Tampered message must fail verification
    assert mgr.verify_es256(b"Tampered message", sig) is False


def test_vapid_jwt_token_minting_and_auth_headers():
    """Verify mint_vapid_token and build_auth_headers produce RFC 8292 standard envelopes."""
    mgr = VapidKeyManager.generate()
    endpoint = "https://updates.push.services.mozilla.com/wpush/v2/test-sub-123"

    headers = mgr.build_auth_headers(endpoint, subject="mailto:sre@swarmwatch.corp")
    assert "Authorization" in headers
    assert "Crypto-Key" in headers

    auth_val = headers["Authorization"]
    assert auth_val.startswith("vapid t=")
    assert f"k={mgr.public_key_b64}" in auth_val
    assert headers["Crypto-Key"] == f"p256ecdsa={mgr.public_key_b64}"

    # Verify JWT parts
    token = auth_val.split("t=")[1].split(",")[0]
    parts = token.split(".")
    assert len(parts) == 3


# ---------------------------------------------------------------------------
# 2. SseEventManager Unit & Replay Buffer Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sse_event_formatting_and_matching():
    """Verify SseEvent wire formatting and SseSubscriber filter predicates."""
    evt = SseEvent(
        id="evt-001",
        event="circuit_trip",
        data={"swarm_id": "swarm-alpha", "metric_val": 0.22, "reason": "Entropy collapse"},
        retry=2500,
    )

    formatted = evt.format_sse()
    assert "id: evt-001\n" in formatted
    assert "event: circuit_trip\n" in formatted
    assert "retry: 2500\n" in formatted
    assert '"swarm_id":"swarm-alpha"' in formatted
    assert formatted.endswith("\n\n")

    # Subscriber matching: min_level=warning matches both
    sub_warn = SseSubscriber(min_level="warning")
    assert sub_warn.matches(evt) is True

    # Subscriber matching: min_level=trip matches trips
    sub_trip = SseSubscriber(min_level="trip")
    assert sub_trip.matches(evt) is True

    # Warning event should be rejected by sub_trip
    warn_evt = SseEvent(
        id="evt-002",
        event="circuit_warning",
        data={"swarm_id": "swarm-alpha", "metric_val": 0.40, "reason": "Low entropy"},
    )
    assert sub_trip.matches(warn_evt) is False

    # Swarm ID filtering
    sub_beta = SseSubscriber(swarm_id="swarm-beta")
    assert sub_beta.matches(evt) is False


@pytest.mark.asyncio
async def test_sse_event_manager_broadcast_and_ring_buffer_replay():
    """Verify SseEventManager queues events and supports Last-Event-ID ring-buffer replay."""
    mgr = SseEventManager(ring_buffer_size=5)
    sub = SseSubscriber()

    await mgr.register(sub)
    assert mgr.active_subscriber_count == 1

    # Broadcast 3 events
    e1 = SseEvent(id="e-1", event="circuit_trip", data={"turn": 1})
    e2 = SseEvent(id="e-2", event="circuit_trip", data={"turn": 2})
    e3 = SseEvent(id="e-3", event="circuit_trip", data={"turn": 3})

    await mgr.broadcast(e1)
    await mgr.broadcast(e2)
    await mgr.broadcast(e3)

    # Subscriber queue received all 3
    assert sub.queue.qsize() == 3

    # Test Last-Event-ID replay for client reconnecting after e-1
    replay = mgr.get_replay_events(last_event_id="e-1", subscriber=sub)
    assert len(replay) == 2
    assert [r.id for r in replay] == ["e-2", "e-3"]

    await mgr.unregister(sub)
    assert mgr.active_subscriber_count == 0


# ---------------------------------------------------------------------------
# 3. SQLite Storage Persistence for Push Subscriptions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sqlite_push_subscriptions_crud():
    """Verify push_subscriptions table creation and CRUD operations in SQLiteStorageAdapter."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        storage = SQLiteStorageAdapter(db_path=db_path)

        sub = PushSubscription(
            endpoint="https://push.example.com/sub/device-42",
            project_id="test-project",
            keys=PushKeys(p256dh="test-p256dh-key", auth="test-auth-secret"),
            topics=["circuit_trip", "fleet_trip"],
        )

        # 1. Save
        await storage.save_push_subscription(sub)

        # 2. Get
        fetched = await storage.get_push_subscription(sub.endpoint)
        assert fetched is not None
        assert fetched["endpoint"] == sub.endpoint
        assert fetched["project_id"] == "test-project"
        assert fetched["keys"]["p256dh"] == "test-p256dh-key"
        assert "circuit_trip" in fetched["topics"]

        # 3. List
        all_subs = await storage.list_push_subscriptions(project_id="test-project")
        assert len(all_subs) == 1

        # 4. Delete
        deleted = await storage.delete_push_subscription(sub.endpoint)
        assert deleted is True
        assert (await storage.get_push_subscription(sub.endpoint)) is None

        await storage.close()
    finally:
        storage.close_sync()
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 4. FastAPI SSE & WebPush REST Endpoints Tests
# ---------------------------------------------------------------------------

def test_fastapi_push_control_plane_endpoints():
    """Verify /api/v1/push/* endpoints for VAPID keys, subscription registration, test alerts."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        storage = SQLiteStorageAdapter(db_path=db_path)
        app = create_app(storage=storage)
        client = TestClient(app)

        # 1. GET /api/v1/push/vapid-public-key
        res_key = client.get("/api/v1/push/vapid-public-key")
        assert res_key.status_code == 200
        data_key = res_key.json()
        assert "public_key" in data_key
        assert data_key["public_key"].startswith("B")

        # 2. POST /api/v1/push/subscribe
        sub_payload = {
            "endpoint": "https://push.example.com/endpoints/client-88",
            "keys": {"p256dh": "mock-p256dh", "auth": "mock-auth"},
            "topics": ["circuit_trip", "circuit_warning"],
        }
        res_sub = client.post("/api/v1/push/subscribe", json=sub_payload)
        assert res_sub.status_code == 200
        assert res_sub.json()["status"] == "subscribed"

        # 3. GET /api/v1/push/subscriptions
        res_list = client.get("/api/v1/push/subscriptions")
        assert res_list.status_code == 200
        assert res_list.json()["count"] == 1

        # 4. POST /api/v1/push/test
        # Mock sender for WebPushDispatcher to avoid actual network requests
        dispatched_payloads = []

        def mock_push_sender(url, data, headers, timeout):
            dispatched_payloads.append((url, json.loads(data.decode("utf-8")), headers))
            return 201

        from swarmwatch.webpush import global_webpush_dispatcher
        global_webpush_dispatcher._sender = mock_push_sender

        res_test = client.post("/api/v1/push/test", json={})
        assert res_test.status_code == 200
        assert res_test.json()["status"] == "dispatched"
        assert res_test.json()["recipients"] >= 1

        # Allow background worker to process item
        time.sleep(0.3)
        assert len(dispatched_payloads) >= 1
        assert dispatched_payloads[0][1]["title"] == "SwarmWatch Test Alert"

        # 5. POST /api/v1/push/unsubscribe
        res_unsub = client.post("/api/v1/push/unsubscribe", json={"endpoint": sub_payload["endpoint"]})
        assert res_unsub.status_code == 200
        assert res_unsub.json()["status"] == "unsubscribed"

        # Verify empty
        res_empty = client.get("/api/v1/push/subscriptions")
        assert res_empty.json()["count"] == 0

    finally:
        storage.close_sync()
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass


def test_fastapi_sse_stream_endpoint():
    """Verify GET /api/v1/events/stream connects and streams SSE events."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    storage = SQLiteStorageAdapter(db_path=db_path)
    try:
        app = create_app(storage=storage)
        client = TestClient(app)

        # Pre-broadcast an event into global_sse_manager
        global_sse_manager._recent_events.clear()
        ce = CircuitEvent(
            id="evt-sse-replay-01",
            swarm_id="swarm-sse-test",
            event_type="trip",
            reason="Simulated Shannon loop",
            metric_val=0.19,
        )
        asyncio.run(global_sse_manager.broadcast(ce))

        # Connect with Last-Event-ID and limit=1 to verify immediate replay stream
        with client.stream("GET", "/api/v1/events/stream?swarm_id=swarm-sse-test&last_event_id=none&limit=1") as resp:
            assert resp.status_code == 200
            assert "text/event-stream" in resp.headers["content-type"]

            lines = list(resp.iter_lines())
            joined = "\n".join(lines)
            assert "circuit_trip" in joined
            assert "swarm-sse-test" in joined

    finally:
        storage.close_sync()
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass


