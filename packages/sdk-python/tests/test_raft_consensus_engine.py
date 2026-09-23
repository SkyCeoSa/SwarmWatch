# -*- coding: utf-8 -*-
"""
Unit and integration test suite for SwarmWatch Multi-Cluster High-Availability
Raft Consensus Engine (Seam 30 / ADR 0041).
"""

import asyncio
import json
import pytest
from fastapi.testclient import TestClient

from swarmwatch.raft import (
    RaftNodeRole,
    RaftLogEntry,
    RequestVoteRpc,
    RequestVoteResponse,
    AppendEntriesRpc,
    AppendEntriesResponse,
    SwarmRaftStateMachine,
    RaftConsensusEngine,
    MockRaftCluster,
    global_raft_engine,
)
from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.cli import main as cli_main


@pytest.fixture
def clean_raft_engine():
    """Resets global_raft_engine state before and after each test."""
    global_raft_engine.node_id = "local-leader-01"
    global_raft_engine.peers = []
    global_raft_engine.current_term = 0
    global_raft_engine.voted_for = None
    global_raft_engine.log = [
        RaftLogEntry(term=0, index=0, command="SENTINEL", payload={})
    ]
    global_raft_engine.commit_index = 0
    global_raft_engine.last_applied = 0
    global_raft_engine.role = RaftNodeRole.FOLLOWER
    global_raft_engine.leader_id = None
    global_raft_engine.state_machine = SwarmRaftStateMachine()
    yield global_raft_engine
    global_raft_engine.role = RaftNodeRole.FOLLOWER


def test_raft_state_machine_deterministic_transitions():
    sm = SwarmRaftStateMachine()

    # 1. Quota increment
    e1 = RaftLogEntry(
        term=1,
        index=1,
        command="quota_inc",
        payload={"swarm_id": "swarm-test-01", "turns": 8, "tokens": 2400, "cost": 0.048, "worker_id": "worker-alpha"},
    )
    s1 = sm.apply_entry(e1)
    assert s1["swarm_id"] == "swarm-test-01"
    assert s1["total_turns"] == 8
    assert s1["total_tokens"] == 2400
    assert s1["total_cost"] == 0.048
    assert s1["active_workers"] == ["worker-alpha"]
    assert s1["is_tripped"] is False

    # 2. Circuit trip
    e2 = RaftLogEntry(
        term=1,
        index=2,
        command="circuit_trip",
        payload={"swarm_id": "swarm-test-01", "reason": "Entropy collapse > 0.88", "tripped_by": "raft-consensus"},
    )
    s2 = sm.apply_entry(e2)
    assert s2["is_tripped"] is True
    assert s2["trip_reason"] == "Entropy collapse > 0.88"
    assert s2["tripped_by"] == "raft-consensus"

    # 3. Circuit reset
    e3 = RaftLogEntry(
        term=1,
        index=3,
        command="circuit_reset",
        payload={"swarm_id": "swarm-test-01"},
    )
    s3 = sm.apply_entry(e3)
    assert s3["is_tripped"] is False
    assert s3["trip_reason"] is None
    assert s3["total_turns"] == 0
    assert s3["total_tokens"] == 0

    # 4. List and get state
    assert sm.get_swarm_state("swarm-test-01") is not None
    assert len(sm.list_swarms()) == 1


@pytest.mark.asyncio
async def test_raft_engine_single_node_standalone():
    engine = RaftConsensusEngine(node_id="standalone-01", peers=[])
    assert engine.cluster_size == 1
    assert engine.quorum_size == 1
    assert engine.role == RaftNodeRole.FOLLOWER

    won = await engine.start_election()
    assert won is True
    assert engine.role == RaftNodeRole.LEADER
    assert engine.current_term == 1

    success, entry, leader_id = await engine.propose(
        "trip",
        {"swarm_id": "swarm-alpha", "reason": "Single-node trip proposal"},
    )
    assert success is True
    assert entry is not None
    assert entry.index == 1
    assert engine.commit_index == 1

    state = engine.state_machine.get_swarm_state("swarm-alpha")
    assert state is not None
    assert state["is_tripped"] is True
    assert state["trip_reason"] == "Single-node trip proposal"


@pytest.mark.asyncio
async def test_mock_raft_cluster_three_node_election():
    cluster = MockRaftCluster(node_count=3)
    assert len(cluster.node_ids) == 3

    leader = await cluster.elect_leader("node-01")
    assert leader.node_id == "node-01"
    assert leader.role == RaftNodeRole.LEADER
    assert leader.current_term == 1

    node2 = cluster.nodes["node-02"]
    node3 = cluster.nodes["node-03"]
    assert node2.role == RaftNodeRole.FOLLOWER
    assert node3.role == RaftNodeRole.FOLLOWER
    assert node2.current_term == 1
    assert node3.current_term == 1
    assert node2.leader_id == "node-01"
    assert node3.leader_id == "node-01"


@pytest.mark.asyncio
async def test_mock_raft_cluster_proposal_and_quorum_replication():
    cluster = MockRaftCluster(node_count=3)
    leader = await cluster.elect_leader("node-01")

    success, entry = await cluster.propose_command(
        "trip",
        {"swarm_id": "swarm-distributed-01", "reason": "Quorum trip reached"},
    )
    assert success is True
    assert entry is not None
    assert entry.index == 1
    assert leader.commit_index == 1

    # Verify leader state machine
    leader_state = leader.stateMachine.get_swarm_state("swarm-distributed-01") if hasattr(leader, 'stateMachine') else leader.state_machine.get_swarm_state("swarm-distributed-01")
    assert leader_state["is_tripped"] is True

    # Verify followers received replicated log
    for nid in ["node-02", "node-03"]:
        follower = cluster.nodes[nid]
        assert follower.last_log_index == 1
        assert follower.log[1].command == "trip"


@pytest.mark.asyncio
async def test_raft_failover_and_vote_rejection():
    cluster = MockRaftCluster(node_count=3)
    leader1 = await cluster.elect_leader("node-01")
    assert leader1.current_term == 1

    # Non-leader node rejects proposals
    follower = cluster.nodes["node-02"]
    success, entry, leader_id = await follower.propose("trip", {"swarm_id": "test"})
    assert success is False
    assert entry is None
    assert leader_id == "node-01"

    # Leader steps down
    leader1.step_down(2)
    assert leader1.role == RaftNodeRole.FOLLOWER

    # Stale vote rejected
    stale_rpc = RequestVoteRpc(term=1, candidate_id="node-03", last_log_index=1, last_log_term=1)
    resp = leader1.handle_request_vote(stale_rpc)
    assert resp.vote_granted is False
    assert resp.term == 2

    # Node-02 starts election and wins
    leader2 = await cluster.elect_leader("node-02")
    assert leader2.node_id == "node-02"
    assert leader2.role == RaftNodeRole.LEADER
    assert leader2.current_term >= 2


def test_raft_fastapi_endpoints(clean_raft_engine, tmp_path):
    db_file = str(tmp_path / "test_raft.db")
    storage = SQLiteStorageAdapter(db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. GET /api/v1/raft/status
    resp = client.get("/api/v1/raft/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "node_id" in data
    assert "role" in data
    assert "term" in data
    assert "commit_index" in data

    # 2. POST /api/v1/raft/propose
    prop_resp = client.post(
        "/api/v1/raft/propose",
        json={"command": "trip", "payload": {"swarm_id": "swarm-fastapi-01", "reason": "FastAPI proposal"}},
    )
    assert prop_resp.status_code == 200
    prop_data = prop_resp.json()
    assert prop_data["status"] == "committed"
    assert prop_data["commit_index"] >= 1

    # Verify status reflects update
    status_resp = client.get("/api/v1/raft/status")
    assert status_resp.status_code == 200
    st_data = status_resp.json()
    assert len(st_data.get("swarms", [])) >= 1

    # 3. POST /api/v1/raft/rpc/request-vote
    vote_rpc = {
        "term": 5,
        "candidate_id": "node-remote-99",
        "last_log_index": 5,
        "last_log_term": 5,
    }
    vote_resp = client.post("/api/v1/raft/rpc/request-vote", json=vote_rpc)
    assert vote_resp.status_code == 200
    assert "vote_granted" in vote_resp.json()

    # 4. POST /api/v1/raft/rpc/append-entries
    append_rpc = {
        "term": 5,
        "leader_id": "node-remote-99",
        "prev_log_index": 0,
        "prev_log_term": 0,
        "entries": [],
        "leader_commit": 1,
    }
    app_resp = client.post("/api/v1/raft/rpc/append-entries", json=append_rpc)
    assert app_resp.status_code == 200
    assert "success" in app_resp.json()

    # 5. POST /api/v1/raft/step-down
    sd_resp = client.post("/api/v1/raft/step-down?new_term=6")
    assert sd_resp.status_code == 200
    assert sd_resp.json()["status"] == "stepped_down"
    assert sd_resp.json()["role"] == RaftNodeRole.FOLLOWER


def test_raft_cli_commands(clean_raft_engine, capsys):
    # Test CLI status
    with pytest.raises(SystemExit) as excinfo:
        cli_main(["raft", "status", "--format", "json"])
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "node_id" in out

    # Test CLI cluster simulation
    with pytest.raises(SystemExit) as excinfo:
        cli_main(["raft", "cluster", "--nodes", "3", "--format", "json"])
    assert excinfo.value.code == 0
    cluster_out = capsys.readouterr().out
    assert "leader" in cluster_out
    assert "proposal_success" in cluster_out
