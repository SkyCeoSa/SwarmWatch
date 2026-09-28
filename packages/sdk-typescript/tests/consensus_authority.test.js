/**
 * Tests for Consensus Authority Binding & Linearizable Quorum Fencing (Seam 61 / ADR 0072 - TypeScript Edition).
 * Verifies majority quorum commitment, minority partition fail-closed rejection,
 * replicated state machine transitions, linearizable read consistency, and zero-token invariants.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  ConsensusAuthorityBinding,
  ConsensusLogEntry,
  ConsensusPermitStatus,
  AuthorityState,
} from "../src/index.ts";

test("Consensus Authority Binding & Linearizable Quorum Fencing (Seam 61 / ADR 0072)", async (t) => {
  await t.test("single node cluster quorum lifecycle (N=1, Q=1)", () => {
    const binding = new ConsensusAuthorityBinding("node-1", ["node-1"], 1);
    assert.equal(binding.quorumRequired, 1);
    assert.equal(binding.hasQuorum, true);

    binding.configureTenant("tenant-alpha", 50.0, 50000, 100);
    const state0 = binding.readCommitted("tenant-alpha");
    assert.equal(state0.availableCost, 50.0);
    assert.equal(state0.availableTokens, 50000);
    assert.equal(state0.lastCommittedIndex, 0);

    // Reserve permit
    const res = binding.reserve("tenant-alpha", "agent_turn", 10.0, 5000);
    assert.equal(res.isAccepted, true);
    assert.equal(res.status, ConsensusPermitStatus.ACCEPTED);
    assert.equal(res.quorumAchieved, true);
    assert.equal(res.ackCount, 1);
    assert.equal(res.logIndex, 1);
    assert.ok(res.permitHash);

    const state1 = binding.readCommitted("tenant-alpha");
    assert.equal(state1.reservedCost, 10.0);
    assert.equal(state1.availableCost, 40.0);
    assert.equal(state1.lastCommittedIndex, 1);

    // Commit permit
    const commitRes = binding.commit(res.permitId, 8.5, 4200);
    assert.equal(commitRes.isAccepted, true);
    assert.equal(commitRes.logIndex, 2);

    const state2 = binding.readCommitted("tenant-alpha");
    assert.equal(state2.spentCost, 8.5);
    assert.equal(state2.reservedCost, 0.0);
    assert.equal(state2.availableCost, 41.5);
    assert.equal(state2.lastCommittedIndex, 2);
  });

  await t.test("multi node cluster quorum acceptance (N=3, Q=2)", () => {
    const cluster = ["node-1", "node-2", "node-3"];
    const binding = new ConsensusAuthorityBinding("node-1", cluster, 1);
    assert.equal(binding.quorumRequired, 2);
    assert.equal(binding.hasQuorum, true);
    assert.equal(binding.reachableNodes.length, 3);

    binding.configureTenant("tenant-beta", 100.0, 100000);

    const res = binding.reserve("tenant-beta", "agent_turn", 25.0, 10000);
    assert.equal(res.isAccepted, true);
    assert.equal(res.ackCount, 3);
    assert.equal(res.quorumRequired, 2);
    assert.equal(res.quorumAchieved, true);

    // Release unexecuted permit
    const relRes = binding.release(res.permitId, "task_cancelled");
    assert.equal(relRes.isAccepted, true);
    const state = binding.readCommitted("tenant-beta");
    assert.equal(state.reservedCost, 0.0);
    assert.equal(state.availableCost, 100.0);
  });

  await t.test("minority partition fail-closed rejection", () => {
    const cluster = ["node-1", "node-2", "node-3"];
    const binding = new ConsensusAuthorityBinding("node-1", cluster, 1);
    binding.configureTenant("tenant-gamma", 100.0);

    // Simulate partition isolating node-1
    binding.simulatePartition(["node-2", "node-3"]);
    assert.equal(binding.hasQuorum, false);
    assert.equal(binding.reachableNodes.length, 1);

    // Attempt reserve -> must fail closed with QUORUM_LOSS
    const res = binding.reserve("tenant-gamma", "agent_turn", 15.0);
    assert.equal(res.isAccepted, false);
    assert.equal(res.status, ConsensusPermitStatus.QUORUM_LOSS);
    assert.equal(res.quorumAchieved, false);
    assert.ok(res.rejectionReason && res.rejectionReason.includes("Quorum loss"));

    // State untouched
    const state = binding.readCommitted("tenant-gamma");
    assert.equal(state.reservedCost, 0.0);
    assert.equal(state.availableCost, 100.0);

    // Trip in minority partition fails closed
    const tripRes = binding.trip("tenant-gamma", "partitioned_trip");
    assert.equal(tripRes.tripped, false);
    assert.equal(tripRes.quorumAchieved, false);

    // Restore partition
    binding.restorePartition();
    assert.equal(binding.hasQuorum, true);
    const resAfter = binding.reserve("tenant-gamma", "agent_turn", 15.0);
    assert.equal(resAfter.isAccepted, true);
    assert.equal(resAfter.status, ConsensusPermitStatus.ACCEPTED);
  });

  await t.test("replicated state machine peer replication and linearizable reads", () => {
    const cluster = ["node-1", "node-2", "node-3"];
    const leader = new ConsensusAuthorityBinding("node-1", cluster, 1);
    const follower = new ConsensusAuthorityBinding("node-2", cluster, 1);

    leader.configureTenant("tenant-shared", 100.0, 50000);
    follower.configureTenant("tenant-shared", 100.0, 50000);

    const p1 = leader.reserve("tenant-shared", "agent_turn", 20.0, 5000);
    leader.commit(p1.permitId, 18.0, 4500);
    const p2 = leader.reserve("tenant-shared", "agent_turn", 10.0, 2000);

    assert.equal(leader.committedIndex, 3);
    assert.equal(follower.committedIndex, 0);

    const applied = leader.replicateToPeer(follower);
    assert.equal(applied, 3);
    assert.equal(follower.committedIndex, 3);

    const sLead = leader.readCommitted("tenant-shared");
    const sFoll = follower.readCommitted("tenant-shared");

    assert.equal(sLead.spentCost, 18.0);
    assert.equal(sFoll.spentCost, 18.0);
    assert.equal(sLead.reservedCost, 10.0);
    assert.equal(sFoll.reservedCost, 10.0);
    assert.equal(sLead.availableCost, 72.0);
    assert.equal(sFoll.availableCost, 72.0);
    assert.equal(sLead.lastCommittedIndex, sFoll.lastCommittedIndex);
  });

  await t.test("circuit trip and monotonic epoch rearm", () => {
    const cluster = ["node-1", "node-2", "node-3"];
    const binding = new ConsensusAuthorityBinding("node-1", cluster, 5);
    binding.configureTenant("tenant-secure", 100.0);

    const tripRes = binding.trip("tenant-secure", "anomalous_entropy_spike");
    assert.equal(tripRes.tripped, true);
    assert.equal(tripRes.quorumAchieved, true);
    assert.equal(tripRes.epoch, 5);

    const state = binding.readCommitted("tenant-secure");
    assert.equal(state.state, AuthorityState.TRIPPED);
    assert.equal(state.tripReason, "anomalous_entropy_spike");

    const res = binding.reserve("tenant-secure", "agent_turn", 5.0);
    assert.equal(res.isAccepted, false);
    assert.equal(res.status, ConsensusPermitStatus.TRIPPED);

    const rearmRes = binding.rearm("tenant-secure", 6);
    assert.equal(rearmRes.rearmed, true);
    assert.equal(rearmRes.oldEpoch, 5);
    assert.equal(rearmRes.newEpoch, 6);
    assert.equal(binding.epoch, 6);

    const stateAfter = binding.readCommitted("tenant-secure");
    assert.equal(stateAfter.state, AuthorityState.NOMINAL);
    assert.equal(stateAfter.epoch, 6);

    const resNew = binding.reserve("tenant-secure", "agent_turn", 5.0);
    assert.equal(resNew.isAccepted, true);
    assert.equal(resNew.epoch, 6);
  });

  await t.test("tamper detection and entry hash verification", () => {
    const binding = new ConsensusAuthorityBinding("node-1", ["node-1", "node-2"]);
    const entry = new ConsensusLogEntry({
      index: 1,
      epoch: 1,
      term: 1,
      opType: "RESERVE",
      payload: { tenant_id: "t1", cost_estimate: 10.0 },
    });
    assert.ok(entry.entryHash);

    // Tamper payload without updating hash
    entry.payload.cost_estimate = 9999.0;
    const success = binding.applyReplicatedEntry(entry);
    assert.equal(success, false);
    assert.equal(binding.committedIndex, 0);
  });

  await t.test("export status metrics reporting", () => {
    const binding = new ConsensusAuthorityBinding("node-1", ["node-1", "node-2", "node-3"]);
    const status = binding.exportStatus();
    assert.equal(status.node_id, "node-1");
    assert.equal(status.quorum_required, 2);
    assert.equal(status.has_quorum, true);
    assert.equal(status.cluster_nodes.length, 3);
    assert.equal(status.committed_index, 0);
  });
});
