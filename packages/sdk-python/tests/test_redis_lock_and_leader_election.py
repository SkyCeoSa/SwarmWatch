# -*- coding: utf-8 -*-
"""
Automated Unit and Integration Tests for Distributed Redis Lock & Leader Election (Seam 24 / ADR 0035).
"""

import time
import pytest
from starlette.testclient import TestClient

from swarmwatch.redis_circuit import MockRedisClient
from swarmwatch.redis_lock import (
    RedisDistributedLock,
    RedisLeaderElector,
    ServerNodeRole,
    ClusterLeaderStatus,
)
from swarmwatch.server import create_app


def test_redis_distributed_lock_acquire_and_release():
    client = MockRedisClient()
    lock = RedisDistributedLock(client, resource_name="lock:resource:alpha", ttl_ms=2000)

    # Acquire lock
    assert lock.acquire(blocking=False) is True
    assert lock.is_held is True
    assert lock.token is not None
    assert lock.is_locked() is True

    # Second acquire on same resource fails
    lock2 = RedisDistributedLock(client, resource_name="lock:resource:alpha", ttl_ms=2000)
    assert lock2.acquire(blocking=False) is False
    assert lock2.is_held is False

    # Extend lock
    assert lock.extend(additional_ttl_ms=3000) is True

    # Release lock
    assert lock.release() is True
    assert lock.is_held is False
    assert lock.is_locked() is False

    # Second client can now acquire
    assert lock2.acquire(blocking=False) is True
    assert lock2.release() is True


def test_redis_distributed_lock_context_manager():
    client = MockRedisClient()
    lock = RedisDistributedLock(client, resource_name="lock:resource:ctx", ttl_ms=1000)

    with lock:
        assert lock.is_held is True
        assert lock.is_locked() is True

    assert lock.is_held is False
    assert lock.is_locked() is False


def test_redis_leader_elector_single_node():
    client = MockRedisClient()
    elector = RedisLeaderElector(
        client=client,
        node_id="srv-node-test-1",
        election_key="test:leader:lock",
        roster_key="test:cluster:nodes",
        lease_ttl_ms=1000,
        heartbeat_interval_s=0.1,
    )

    elected_events = []
    stepped_down_events = []
    elector.on_elected(lambda: elected_events.append(True))
    elector.on_stepped_down(lambda: stepped_down_events.append(True))

    elector.start()
    time.sleep(0.2)

    try:
        assert elector.is_leader is True
        assert elector.role == ServerNodeRole.LEADER
        assert len(elected_events) >= 1

        status = elector.get_leader_status()
        assert isinstance(status, ClusterLeaderStatus)
        assert status.leader_node_id == "srv-node-test-1"
        assert status.is_leader is True
        assert status.local_role == ServerNodeRole.LEADER
        assert status.nodes_count >= 1
    finally:
        elector.stop()

    assert elector.is_leader is False


def test_redis_leader_elector_failover_and_step_down():
    client = MockRedisClient()
    elector_a = RedisLeaderElector(
        client=client,
        node_id="srv-node-alpha",
        election_key="test:failover:lock",
        roster_key="test:failover:nodes",
        lease_ttl_ms=800,
        heartbeat_interval_s=0.1,
    )
    elector_b = RedisLeaderElector(
        client=client,
        node_id="srv-node-beta",
        election_key="test:failover:lock",
        roster_key="test:failover:nodes",
        lease_ttl_ms=800,
        heartbeat_interval_s=0.1,
    )

    elector_a.start()
    time.sleep(0.2)
    assert elector_a.is_leader is True

    elector_b.start()
    time.sleep(0.2)
    assert elector_b.is_leader is False
    assert elector_b.role == ServerNodeRole.STANDBY

    status = elector_b.get_leader_status()
    assert status.leader_node_id == "srv-node-alpha"
    assert status.is_leader is False
    assert status.local_role == ServerNodeRole.STANDBY
    assert status.nodes_count == 2

    # Node A steps down voluntarily
    assert elector_a.step_down() is True
    assert elector_a.is_leader is False

    # Node B discovers vacancy and promotes to LEADER
    time.sleep(0.3)
    assert elector_b.is_leader is True
    assert elector_b.role == ServerNodeRole.LEADER

    status_after = elector_b.get_leader_status()
    assert status_after.leader_node_id == "srv-node-beta"
    assert status_after.is_leader is True

    elector_a.stop()
    elector_b.stop()


def test_server_cluster_leader_endpoints():
    client = MockRedisClient()
    elector = RedisLeaderElector(
        client=client,
        node_id="srv-node-server-test",
        lease_ttl_ms=2000,
        heartbeat_interval_s=0.1,
    )
    app = create_app(leader_elector=elector)
    test_client = TestClient(app)

    time.sleep(0.2)
    assert elector.is_leader is True

    # 1. GET /api/v1/cluster/leader
    res = test_client.get("/api/v1/cluster/leader")
    assert res.status_code == 200
    data = res.json()
    assert data["leader_node_id"] == "srv-node-server-test"
    assert data["is_leader"] is True
    assert data["local_role"] == "LEADER"
    assert data["nodes_count"] >= 1
    assert len(data["roster"]) >= 1

    # 2. GET /api/v1/fleet/redis/metrics includes cluster_leader
    m_res = test_client.get("/api/v1/fleet/redis/metrics")
    assert m_res.status_code == 200
    m_data = m_res.json()
    assert "cluster_leader" in m_data
    assert m_data["cluster_leader"]["leader_node_id"] == "srv-node-server-test"
    assert m_data["cluster_leader"]["is_leader"] is True

    # 3. POST /api/v1/cluster/leader/step-down
    sd_res = test_client.post("/api/v1/cluster/leader/step-down")
    assert sd_res.status_code == 200
    sd_data = sd_res.json()
    assert sd_data["status"] == "stepped_down"
    assert sd_data["node_id"] == "srv-node-server-test"

    elector.stop()
