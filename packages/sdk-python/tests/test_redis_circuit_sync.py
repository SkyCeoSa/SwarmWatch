# -*- coding: utf-8 -*-
"""
TDD Test Suite for Distributed Redis Circuit Breaker & Fleet State Synchronization (Seam 22 / ADR 0033).
Validates atomic quota ledgers, sub-millisecond Pub/Sub trip broadcasting,
RESP2 protocol engine, dual-coordinator sync, FastAPI REST endpoints, and CLI interface.
"""

from io import BytesIO
import json
import pytest
from unittest.mock import MagicMock, patch

from swarmwatch.circuit import CircuitPolicy, SwarmCircuitBreakException
from swarmwatch.cli import main as cli_main
from swarmwatch.fleet import FleetEvent, FleetMetrics
from swarmwatch.redis_circuit import (
    MockRedisClient,
    RESPPrefix,
    RESPRedisClient,
    RedisCircuitCoordinator,
    RedisFleetBackend,
    RedisProtocolError,
    RedisTripBroadcast,
)
from swarmwatch.server import create_app
from fastapi.testclient import TestClient


# ============================================================================
# 1. MockRedisClient Unit Tests
# ============================================================================

def test_mock_redis_basic_ping_and_keys():
    client = MockRedisClient()
    assert client.ping() is True

    # Hash operations
    assert client.hset("hash1", "field1", "val1") == 1
    assert client.hset("hash1", "field1", "val1_updated") == 0
    assert client.hget("hash1", "field1") == "val1_updated"
    assert client.hget("hash1", "nonexistent") is None

    # Del key
    assert client.del_key("hash1") == 1
    assert client.hget("hash1", "field1") is None
    assert client.del_key("nonexistent") == 0


def test_mock_redis_atomic_increments():
    client = MockRedisClient()

    # Integer increment
    val = client.hincrby("ledger", "tokens", 100)
    assert val == 100
    val = client.hincrby("ledger", "tokens", 50)
    assert val == 150
    assert client.hget("ledger", "tokens") == "150"

    # Float increment
    c1 = client.hincrbyfloat("ledger", "cost", 0.05)
    assert pytest.approx(c1, rel=1e-5) == 0.05
    c2 = client.hincrbyfloat("ledger", "cost", 0.025)
    assert pytest.approx(c2, rel=1e-5) == 0.075
    assert pytest.approx(float(client.hget("ledger", "cost")), rel=1e-5) == 0.075


def test_mock_redis_set_operations():
    client = MockRedisClient()
    assert client.sadd("workers", "w1", "w2") == 2
    assert client.sadd("workers", "w1") == 0
    members = client.smembers("workers")
    assert members == {"w1", "w2"}

    assert client.srem("workers", "w1") == 1
    assert client.srem("workers", "w1") == 0
    assert client.smembers("workers") == {"w2"}


def test_mock_redis_pubsub():
    client = MockRedisClient()
    received_msgs = []

    def on_msg(channel: str, msg: str):
        received_msgs.append((channel, msg))

    token = client.subscribe("swarmwatch:trips", on_msg)

    # Publish message
    assert client.publish("swarmwatch:trips", "trip-payload-1") == 1
    assert len(received_msgs) == 1
    assert received_msgs[0] == ("swarmwatch:trips", "trip-payload-1")

    # Unsubscribe
    client.unsubscribe(token)
    assert client.publish("swarmwatch:trips", "trip-payload-2") == 0
    assert len(received_msgs) == 1

    client.close()


# ============================================================================
# 2. RESPRedisClient Protocol Parsing & Serialization Unit Tests
# ============================================================================

def test_resp_encode_command():
    client = RESPRedisClient(host="127.0.0.1", port=6379)
    cmd = client._encode_command("HSET", "key1", "field1", "val1")
    assert cmd == b"*4\r\n$4\r\nHSET\r\n$4\r\nkey1\r\n$6\r\nfield1\r\n$4\r\nval1\r\n"


def test_resp_read_response_primitives():
    client = RESPRedisClient()

    # Simple String
    r1 = client._read_response(BytesIO(b"+OK\r\n"))
    assert r1 == "OK"

    # Integer
    r2 = client._read_response(BytesIO(b":42\r\n"))
    assert r2 == 42

    # Bulk String
    r3 = client._read_response(BytesIO(b"$5\r\nhello\r\n"))
    assert r3 == "hello"

    # Bulk Nil
    r4 = client._read_response(BytesIO(b"$-1\r\n"))
    assert r4 is None

    # Error
    with pytest.raises(RedisProtocolError, match="WRONGTYPE"):
        client._read_response(BytesIO(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))


def test_resp_read_response_array():
    client = RESPRedisClient()
    # Array of two bulk strings: ["foo", "bar"]
    data = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    res = client._read_response(BytesIO(data))
    assert res == ["foo", "bar"]

    # Empty array
    assert client._read_response(BytesIO(b"*0\r\n")) == []

    # Nil array
    assert client._read_response(BytesIO(b"*-1\r\n")) is None


def test_resp_from_url():
    c1 = RESPRedisClient.from_url("redis://default:mysecret@redis.internal:6380/2")
    assert c1.host == "redis.internal"
    assert c1.port == 6380
    assert c1.password == "mysecret"
    assert c1.db == 2

    c2 = RESPRedisClient.from_url("redis://localhost:6379")
    assert c2.host == "localhost"
    assert c2.port == 6379
    assert c2.password is None
    assert c2.db == 0


# ============================================================================
# 3. RedisFleetBackend Unit Tests
# ============================================================================

def test_redis_fleet_backend_turn_recording():
    client = MockRedisClient()
    backend = RedisFleetBackend(client)
    policy = CircuitPolicy(max_turns=10, token_budget=1000)

    # Worker 1 records turn
    m1 = backend.record_turn_atomic("w1", "swarm-test", tokens=100, cost=0.01, turns=1, policy=policy)
    assert m1.total_tokens == 100
    assert m1.total_turns == 1
    assert pytest.approx(m1.total_cost, rel=1e-5) == 0.01
    assert m1.is_tripped is False
    assert "w1" in m1.active_workers

    # Worker 2 records turn
    m2 = backend.record_turn_atomic("w2", "swarm-test", tokens=250, cost=0.025, turns=2, policy=policy)
    assert m2.total_tokens == 350
    assert m2.total_turns == 3
    assert pytest.approx(m2.total_cost, rel=1e-5) == 0.035
    assert m2.active_workers == {"w1", "w2"}


def test_redis_fleet_backend_max_turns_trip():
    client = MockRedisClient()
    backend = RedisFleetBackend(client)
    policy = CircuitPolicy(max_turns=3)

    backend.record_turn_atomic("w1", "swarm-turns", tokens=10, cost=0.001, turns=2, policy=policy)
    assert backend.is_tripped("swarm-turns") is False

    # Exceeds max turns (total 3 >= 3)
    with pytest.raises(SwarmCircuitBreakException, match="max turns quota reached"):
        backend.record_turn_atomic("w2", "swarm-turns", tokens=10, cost=0.001, turns=1, policy=policy)

    assert backend.is_tripped("swarm-turns") is True
    metrics = backend.get_metrics("swarm-turns")
    assert metrics is not None
    assert metrics.is_tripped is True
    assert metrics.tripped_by == "w2"
    assert "max turns quota reached" in (metrics.trip_reason or "")


def test_redis_fleet_backend_token_budget_trip():
    client = MockRedisClient()
    backend = RedisFleetBackend(client)
    policy = CircuitPolicy(token_budget=500)

    backend.record_turn_atomic("w1", "swarm-tokens", tokens=400, cost=0.04, turns=1, policy=policy)
    assert backend.is_tripped("swarm-tokens") is False

    with pytest.raises(SwarmCircuitBreakException, match="token budget exceeded"):
        backend.record_turn_atomic("w2", "swarm-tokens", tokens=150, cost=0.015, turns=1, policy=policy)

    assert backend.is_tripped("swarm-tokens") is True


def test_redis_fleet_backend_cost_budget_trip():
    client = MockRedisClient()
    backend = RedisFleetBackend(client)
    policy = CircuitPolicy(cost_budget=1.0)

    backend.record_turn_atomic("w1", "swarm-cost", tokens=1000, cost=0.60, turns=1, policy=policy)
    assert backend.is_tripped("swarm-cost") is False

    with pytest.raises(SwarmCircuitBreakException, match="cost budget exceeded"):
        backend.record_turn_atomic("w2", "swarm-cost", tokens=1000, cost=0.50, turns=1, policy=policy)

    assert backend.is_tripped("swarm-cost") is True


def test_redis_fleet_backend_reset():
    client = MockRedisClient()
    backend = RedisFleetBackend(client)
    policy = CircuitPolicy(max_turns=2)

    with pytest.raises(SwarmCircuitBreakException):
        backend.record_turn_atomic("w1", "swarm-reset", tokens=10, cost=0.01, turns=2, policy=policy)

    assert backend.is_tripped("swarm-reset") is True
    backend.reset_swarm("swarm-reset")
    assert backend.is_tripped("swarm-reset") is False
    assert backend.get_metrics("swarm-reset") is None


# ============================================================================
# 4. RedisCircuitCoordinator & Cross-Node Pub/Sub Tests
# ============================================================================

def test_redis_coordinator_fast_path_cache():
    shared_client = MockRedisClient()
    coord = RedisCircuitCoordinator(policy=CircuitPolicy(max_turns=2), redis_client=shared_client)

    coord.record_turn("w1", "s-cache", tokens=50, turns=1)
    assert coord.is_tripped("s-cache") is False

    # Second turn trips coordinator
    with pytest.raises(SwarmCircuitBreakException):
        coord.record_turn("w2", "s-cache", tokens=50, turns=1)

    assert coord.is_tripped("s-cache") is True

    # Immediate fast-path rejection without calling backend
    with pytest.raises(SwarmCircuitBreakException, match="TRIPPED for swarm"):
        coord.record_turn("w1", "s-cache", tokens=10, turns=1)


def test_redis_coordinator_cross_node_pubsub_broadcast():
    shared_client = MockRedisClient()

    # Simulate Node A (Coordinator 1) and Node B (Coordinator 2) in separate worker processes
    coord_a = RedisCircuitCoordinator(policy=CircuitPolicy(token_budget=300), redis_client=shared_client)
    coord_b = RedisCircuitCoordinator(policy=CircuitPolicy(token_budget=300), redis_client=shared_client)

    events_b = []
    coord_b.subscribe(lambda ev: events_b.append(ev))

    # Node A runs turns within budget
    coord_a.record_turn("worker-node-a", "swarm-cluster", tokens=200, turns=1)
    assert coord_a.is_tripped("swarm-cluster") is False
    assert coord_b.is_tripped("swarm-cluster") is False

    # Node A breaches budget -> trips and broadcasts payload
    with pytest.raises(SwarmCircuitBreakException, match="token budget exceeded"):
        coord_a.record_turn("worker-node-a", "swarm-cluster", tokens=150, turns=1)

    assert coord_a.is_tripped("swarm-cluster") is True

    # Node B must immediately reflect tripped state via Pub/Sub event
    assert coord_b.is_tripped("swarm-cluster") is True
    assert len(events_b) >= 1
    trip_ev = next((e for e in events_b if e.event_type == "trip_broadcast"), None)
    assert trip_ev is not None
    assert trip_ev.swarm_id == "swarm-cluster"
    assert trip_ev.worker_id == "worker-node-a"

    # Any attempt on Node B is blocked immediately
    with pytest.raises(SwarmCircuitBreakException, match="TRIPPED for swarm"):
        coord_b.record_turn("worker-node-b", "swarm-cluster", tokens=10, turns=1)


def test_redis_coordinator_manual_trip_and_reset():
    shared_client = MockRedisClient()
    coord_1 = RedisCircuitCoordinator(redis_client=shared_client)
    coord_2 = RedisCircuitCoordinator(redis_client=shared_client)

    coord_1.trip("swarm-manual", reason="Human intervention", tripped_by="operator-alice")

    assert coord_1.is_tripped("swarm-manual") is True
    assert coord_2.is_tripped("swarm-manual") is True

    # Reset
    coord_1.reset_swarm("swarm-manual")
    assert coord_1.is_tripped("swarm-manual") is False
    # Reset from coordinator 2
    coord_2.reset_swarm("swarm-manual")
    assert coord_2.is_tripped("swarm-manual") is False


# ============================================================================
# 5. FastAPI Server Redis Fleet Endpoints Unit Tests
# ============================================================================

def test_server_redis_fleet_endpoints():
    mock_redis = MockRedisClient()
    coordinator = RedisCircuitCoordinator(policy=CircuitPolicy(max_turns=5), redis_client=mock_redis)

    coordinator.register_worker("worker-prod-1", "swarm-prod-99")
    coordinator.record_turn("worker-prod-1", "swarm-prod-99", tokens=450, cost=0.045, turns=2)

    app = create_app(redis_coordinator=coordinator)
    client = TestClient(app)

    # 1. GET metrics
    res = client.get("/api/v1/fleet/redis/metrics?swarm_id=swarm-prod-99")
    assert res.status_code == 200
    data = res.json()
    assert data["swarm_id"] == "swarm-prod-99"
    assert data["total_tokens"] == 450
    assert data["total_turns"] == 2
    assert data["is_tripped"] is False
    assert "worker-prod-1" in data["active_workers"]

    # 2. Non-existent swarm returns default zeroes with status 200
    res_unknown = client.get("/api/v1/fleet/redis/metrics?swarm_id=unknown-swarm")
    assert res_unknown.status_code == 200
    assert res_unknown.json()["total_tokens"] == 0
    assert res_unknown.json()["is_tripped"] is False

    # 3. POST trip
    res_trip = client.post("/api/v1/fleet/redis/trip", json={
        "swarm_id": "swarm-prod-99",
        "reason": "Security Alert: Prompt Injection detected",
        "tripped_by": "sec-bot-01",
        "metric_val": 99.0
    })
    assert res_trip.status_code == 200
    assert res_trip.json()["status"] == "tripped"

    # Confirm tripped in metrics endpoint
    res_after = client.get("/api/v1/fleet/redis/metrics?swarm_id=swarm-prod-99")
    assert res_after.json()["is_tripped"] is True
    assert res_after.json()["tripped_by"] == "sec-bot-01"

    # 4. POST reset
    res_reset = client.post("/api/v1/fleet/redis/reset", json={
        "swarm_id": "swarm-prod-99"
    })
    assert res_reset.status_code == 200
    assert res_reset.json()["status"] == "reset"

    # Confirm metrics reset
    res_reset_metrics = client.get("/api/v1/fleet/redis/metrics?swarm_id=swarm-prod-99")
    assert res_reset_metrics.status_code == 200
    assert res_reset_metrics.json()["total_tokens"] == 0
    assert res_reset_metrics.json()["is_tripped"] is False


# ============================================================================
# 6. CLI `swarmwatch fleet` Subcommand Tests
# ============================================================================

def test_cli_fleet_status_and_actions(capsys):
    mock_client = MockRedisClient()
    test_coordinator = RedisCircuitCoordinator(redis_client=mock_client)
    test_coordinator.register_worker("worker-cli", "swarm-cli-01")
    test_coordinator.record_turn("worker-cli", "swarm-cli-01", tokens=200, cost=0.02, turns=1)

    with patch("swarmwatch.redis_circuit.RedisCircuitCoordinator", return_value=test_coordinator):
        # Test CLI fleet status (JSON)
        with pytest.raises(SystemExit) as exc_info:
            cli_main(["fleet", "status", "--swarm-id", "swarm-cli-01", "--format", "json"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        status_data = json.loads(captured.out)
        assert status_data["swarm_id"] == "swarm-cli-01"
        assert status_data["total_tokens"] == 200
        assert status_data["is_tripped"] is False

        # Test CLI fleet trip
        with pytest.raises(SystemExit) as exc_info:
            cli_main(["fleet", "trip", "--swarm-id", "swarm-cli-01", "--reason", "Manual Operator Halt", "--tripped-by", "operator-bob"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "Circuit tripped across Redis fleet" in captured.out
        assert test_coordinator.is_tripped("swarm-cli-01") is True

        # Test CLI fleet reset
        with pytest.raises(SystemExit) as exc_info:
            cli_main(["fleet", "reset", "--swarm-id", "swarm-cli-01"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "Fleet state reset" in captured.out
        assert test_coordinator.is_tripped("swarm-cli-01") is False

