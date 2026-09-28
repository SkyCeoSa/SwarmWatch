# -*- coding: utf-8 -*-
"""
Tests for Consensus Authority Binding & Linearizable Quorum Fencing (Seam 61 / ADR 0072).
Verifies majority quorum commitment, minority partition fail-closed rejection,
replicated state machine transitions, linearizable read consistency, and zero-token invariants.
"""

import pytest
from swarmwatch.consensus_authority import (
    ConsensusAuthorityBinding,
    ConsensusLogEntry,
    ConsensusPermitResult,
    ConsensusPermitStatus,
    ConsensusRearmResult,
    ConsensusTripResult,
    CommittedTenantState,
)
from swarmwatch.safety_authority import AuthorityState


class TestConsensusAuthorityBinding:
    """Test suite for Seam 61 consensus authority binding."""

    def test_single_node_quorum_lifecycle(self):
        """Verifies single-node cluster (N=1, Q=1) full permit lifecycle."""
        binding = ConsensusAuthorityBinding(node_id="node-1", cluster_nodes=["node-1"], initial_epoch=1)
        assert binding.quorum_required == 1
        assert binding.has_quorum is True

        # Configure tenant
        binding.configure_tenant("tenant-alpha", max_cost=50.0, max_tokens=50000, max_turns=100)
        state0 = binding.read_committed("tenant-alpha")
        assert state0.available_cost == 50.0
        assert state0.available_tokens == 50000
        assert state0.last_committed_index == 0

        # Reserve permit
        res = binding.reserve("tenant-alpha", action="agent_turn", cost_estimate=10.0, tokens_estimate=5000)
        assert res.is_accepted is True
        assert res.status == ConsensusPermitStatus.ACCEPTED
        assert res.quorum_achieved is True
        assert res.ack_count == 1
        assert res.log_index == 1
        assert res.permit_hash is not None

        # Verify reserved state reflected in committed read
        state1 = binding.read_committed("tenant-alpha")
        assert state1.reserved_cost == 10.0
        assert state1.available_cost == 40.0
        assert state1.last_committed_index == 1

        # Commit permit with actual cost 8.5
        commit_res = binding.commit(res.permit_id, actual_cost=8.5, actual_tokens=4200)
        assert commit_res.is_accepted is True
        assert commit_res.log_index == 2

        state2 = binding.read_committed("tenant-alpha")
        assert state2.spent_cost == 8.5
        assert state2.reserved_cost == 0.0
        assert state2.available_cost == 41.5
        assert state2.last_committed_index == 2

    def test_multi_node_cluster_quorum_acceptance(self):
        """Verifies 3-node cluster (N=3, Q=2) quorum-committed reservations."""
        cluster = ["node-1", "node-2", "node-3"]
        binding = ConsensusAuthorityBinding(node_id="node-1", cluster_nodes=cluster, initial_epoch=1)
        assert binding.quorum_required == 2
        assert binding.has_quorum is True
        assert len(binding.reachable_nodes) == 3

        binding.configure_tenant("tenant-beta", max_cost=100.0, max_tokens=100000)

        res = binding.reserve("tenant-beta", cost_estimate=25.0, tokens_estimate=10000)
        assert res.is_accepted is True
        assert res.ack_count == 3
        assert res.quorum_required == 2
        assert res.quorum_achieved is True

        # Release unexecuted permit
        rel_res = binding.release(res.permit_id, reason="task_cancelled")
        assert rel_res.is_accepted is True
        state = binding.read_committed("tenant-beta")
        assert state.reserved_cost == 0.0
        assert state.available_cost == 100.0

    def test_minority_partition_fail_closed(self):
        """
        Verifies fail-closed execution during minority partition:
        When node-1 is partitioned from node-2 and node-3, it cannot issue permits.
        """
        cluster = ["node-1", "node-2", "node-3"]
        binding = ConsensusAuthorityBinding(node_id="node-1", cluster_nodes=cluster, initial_epoch=1)
        binding.configure_tenant("tenant-gamma", max_cost=100.0)

        # Simulate partition isolating node-1 from node-2 and node-3
        binding.simulate_partition(["node-2", "node-3"])
        assert binding.has_quorum is False
        assert len(binding.reachable_nodes) == 1

        # Attempt to reserve -> must fail closed with QUORUM_LOSS
        res = binding.reserve("tenant-gamma", cost_estimate=15.0)
        assert res.is_accepted is False
        assert res.status == ConsensusPermitStatus.QUORUM_LOSS
        assert res.quorum_achieved is False
        assert "Quorum loss" in str(res.rejection_reason)

        # Committed state must be untouched
        state = binding.read_committed("tenant-gamma")
        assert state.reserved_cost == 0.0
        assert state.available_cost == 100.0

        # Attempt trip in minority partition -> must fail closed
        trip_res = binding.trip("tenant-gamma", reason="partitioned_trip")
        assert trip_res.tripped is False
        assert trip_res.quorum_achieved is False

        # Restore partition -> operations immediately succeed with majority quorum
        binding.restore_partition()
        assert binding.has_quorum is True
        res_after = binding.reserve("tenant-gamma", cost_estimate=15.0)
        assert res_after.is_accepted is True
        assert res_after.status == ConsensusPermitStatus.ACCEPTED

    def test_replicated_state_machine_peer_replication(self):
        """
        Verifies log entry replication across peer nodes, ensuring identical committed state.
        """
        cluster = ["node-1", "node-2", "node-3"]
        leader = ConsensusAuthorityBinding(node_id="node-1", cluster_nodes=cluster, initial_epoch=1)
        follower = ConsensusAuthorityBinding(node_id="node-2", cluster_nodes=cluster, initial_epoch=1)

        leader.configure_tenant("tenant-shared", max_cost=100.0, max_tokens=50000)
        follower.configure_tenant("tenant-shared", max_cost=100.0, max_tokens=50000)

        # Leader executes operations
        p1 = leader.reserve("tenant-shared", cost_estimate=20.0, tokens_estimate=5000)
        leader.commit(p1.permit_id, actual_cost=18.0, actual_tokens=4500)
        p2 = leader.reserve("tenant-shared", cost_estimate=10.0, tokens_estimate=2000)

        assert leader.committed_index == 3

        # Follower initially has committed_index 0
        assert follower.committed_index == 0

        # Replicate to follower
        applied = leader.replicate_to_peer(follower)
        assert applied == 3
        assert follower.committed_index == 3

        # Linearizable reads on both nodes must match exactly
        s_lead = leader.read_committed("tenant-shared")
        s_foll = follower.read_committed("tenant-shared")

        assert s_lead.spent_cost == s_foll.spent_cost == 18.0
        assert s_lead.reserved_cost == s_foll.reserved_cost == 10.0
        assert s_lead.available_cost == s_foll.available_cost == 72.0
        assert s_lead.last_committed_index == s_foll.last_committed_index == 3

    def test_trip_and_rearm_monotonic_epoch(self):
        """Verifies quorum circuit trip and monotonic epoch rearm."""
        cluster = ["node-1", "node-2", "node-3"]
        binding = ConsensusAuthorityBinding(node_id="node-1", cluster_nodes=cluster, initial_epoch=5)
        binding.configure_tenant("tenant-secure", max_cost=100.0)

        # Quorum trip
        trip_res = binding.trip("tenant-secure", reason="anomalous_entropy_spike")
        assert trip_res.tripped is True
        assert trip_res.quorum_achieved is True
        assert trip_res.epoch == 5

        state = binding.read_committed("tenant-secure")
        assert state.state == AuthorityState.TRIPPED
        assert state.trip_reason == "anomalous_entropy_spike"

        # Subsequent reserve must be rejected as TRIPPED
        res = binding.reserve("tenant-secure", cost_estimate=5.0)
        assert res.is_accepted is False
        assert res.status == ConsensusPermitStatus.TRIPPED

        # Quorum rearm with monotonic epoch bump
        rearm_res = binding.rearm("tenant-secure", new_epoch=6)
        assert rearm_res.rearmed is True
        assert rearm_res.old_epoch == 5
        assert rearm_res.new_epoch == 6
        assert binding.epoch == 6

        state_after = binding.read_committed("tenant-secure")
        assert state_after.state == AuthorityState.NOMINAL
        assert state_after.epoch == 6

        # Reserve succeeds under new epoch
        res_new = binding.reserve("tenant-secure", cost_estimate=5.0)
        assert res_new.is_accepted is True
        assert res_new.epoch == 6

    def test_tamper_detection_in_log_entries(self):
        """Verifies canonical hash verification rejects forged or corrupted peer log entries."""
        binding = ConsensusAuthorityBinding(node_id="node-1", cluster_nodes=["node-1", "node-2"])
        entry = ConsensusLogEntry(
            index=1,
            epoch=1,
            term=1,
            op_type="RESERVE",
            payload={"tenant_id": "t1", "cost_estimate": 10.0},
        )
        assert entry.entry_hash is not None

        # Tamper payload without updating hash
        entry.payload["cost_estimate"] = 9999.0
        success = binding.apply_replicated_entry(entry)
        assert success is False
        assert binding.committed_index == 0

    def test_export_status_reporting(self):
        """Verifies cluster metrics export."""
        binding = ConsensusAuthorityBinding(node_id="node-1", cluster_nodes=["node-1", "node-2", "node-3"])
        status = binding.export_status()
        assert status["node_id"] == "node-1"
        assert status["quorum_required"] == 2
        assert status["has_quorum"] is True
        assert len(status["cluster_nodes"]) == 3
        assert status["committed_index"] == 0

    def test_rest_api_consensus_endpoints(self):
        """Verifies consensus authority REST endpoints on FastAPI server."""
        from fastapi.testclient import TestClient
        from swarmwatch.server import create_app

        app = create_app()
        client = TestClient(app)

        # GET status
        res_status = client.get("/api/v1/consensus/status")
        assert res_status.status_code == 200
        data = res_status.json()
        assert "quorum_required" in data
        assert "has_quorum" in data

        # Reserve
        res_res = client.post(
            "/api/v1/consensus/reserve",
            json={
                "tenant_id": "tenant-api-test",
                "action": "agent_turn",
                "cost_estimate": 5.0,
                "tokens_estimate": 1000,
            },
        )
        assert res_res.status_code == 200
        p_data = res_res.json()
        assert p_data["status"] == "ACCEPTED"
        permit_id = p_data["permit_id"]

        # Commit
        res_com = client.post(
            "/api/v1/consensus/commit",
            json={
                "permit_id": permit_id,
                "actual_cost": 4.5,
                "actual_tokens": 900,
            },
        )
        assert res_com.status_code == 200
        assert res_com.json()["status"] == "ACCEPTED"

        # Read committed
        res_read = client.get("/api/v1/consensus/read-committed?tenant_id=tenant-api-test")
        assert res_read.status_code == 200
        read_data = res_read.json()
        assert read_data["spent_cost"] == 4.5

    def test_cli_consensus_commands(self, capsys):
        """Verifies consensus authority CLI subcommands."""
        import json
        from swarmwatch.cli import main

        # Test CLI status
        with pytest.raises(SystemExit) as exc:
            main(["consensus", "status", "--format", "json"])
        assert exc.value.code == 0
        status_data = json.loads(capsys.readouterr().out)
        assert "cluster_status" in status_data
        assert "committed_state" in status_data

        # Test CLI reserve
        with pytest.raises(SystemExit) as exc2:
            main([
                "consensus", "reserve",
                "--tenant", "cli-consensus-tenant",
                "--action-name", "code_eval",
                "--cost", "1.5",
                "--tokens", "2000",
                "--format", "json",
            ])
        assert exc2.value.code == 0
        res_data = json.loads(capsys.readouterr().out)
        assert res_data["status"] == "ACCEPTED"
        assert res_data["quorum_achieved"] is True
