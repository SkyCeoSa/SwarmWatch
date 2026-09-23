import { test, describe } from 'node:test';
import assert from 'node:assert';

import {
  RaftNodeRole,
  SwarmRaftStateMachine,
  RaftConsensusEngine,
  MockRaftCluster,
} from '../src/raft.ts';

describe('Multi-Cluster High-Availability Raft Consensus Engine (TypeScript / ADR 0041)', () => {
  test('SwarmRaftStateMachine deterministically applies trip, reset, and quota entries', () => {
    const sm = new SwarmRaftStateMachine();

    // 1. Quota increment
    const entry1 = {
      term: 1,
      index: 1,
      command: 'quota_inc',
      payload: { swarm_id: 'swarm-omega', turns: 5, tokens: 1200, cost: 0.024, worker_id: 'pod-worker-01' },
      timestamp: new Date().toISOString(),
    };
    const s1 = sm.applyEntry(entry1);
    assert.strictEqual(s1.swarmId, 'swarm-omega');
    assert.strictEqual(s1.totalTurns, 5);
    assert.strictEqual(s1.totalTokens, 1200);
    assert.strictEqual(s1.totalCost, 0.024);
    assert.deepStrictEqual(s1.activeWorkers, ['pod-worker-01']);
    assert.strictEqual(s1.isTripped, false);

    // 2. Circuit trip
    const entry2 = {
      term: 1,
      index: 2,
      command: 'circuit_trip',
      payload: { swarm_id: 'swarm-omega', reason: 'Entropy collapse > 0.85', tripped_by: 'raft-coordinator' },
      timestamp: new Date().toISOString(),
    };
    const s2 = sm.applyEntry(entry2);
    assert.strictEqual(s2.isTripped, true);
    assert.strictEqual(s2.tripReason, 'Entropy collapse > 0.85');
    assert.strictEqual(s2.trippedBy, 'raft-coordinator');

    // 3. Circuit reset
    const entry3 = {
      term: 1,
      index: 3,
      command: 'circuit_reset',
      payload: { swarm_id: 'swarm-omega' },
      timestamp: new Date().toISOString(),
    };
    const s3 = sm.applyEntry(entry3);
    assert.strictEqual(s3.isTripped, false);
    assert.strictEqual(s3.totalTurns, 0);
    assert.strictEqual(s3.totalTokens, 0);
    assert.strictEqual(s3.tripReason, null);
  });

  test('Single-node standalone RaftConsensusEngine immediately self-elects and commits proposals', async () => {
    const engine = new RaftConsensusEngine({
      nodeId: 'standalone-01',
      peers: [],
    });

    assert.strictEqual(engine.clusterSize, 1);
    assert.strictEqual(engine.quorumSize, 1);
    assert.strictEqual(engine.role, RaftNodeRole.FOLLOWER);

    const won = await engine.startElection();
    assert.strictEqual(won, true);
    assert.strictEqual(engine.role, RaftNodeRole.LEADER);
    assert.strictEqual(engine.currentTerm, 1);

    const propResult = await engine.propose('circuit_trip', {
      swarm_id: 'swarm-alpha',
      reason: 'Budget threshold crossed',
    });

    assert.strictEqual(propResult.success, true);
    assert.ok(propResult.entry);
    assert.strictEqual(propResult.entry.index, 1);
    assert.strictEqual(engine.commitIndex, 1);

    const state = engine.stateMachine.getSwarmState('swarm-alpha');
    assert.ok(state);
    assert.strictEqual(state.isTripped, true);
    assert.strictEqual(state.tripReason, 'Budget threshold crossed');
  });

  test('MockRaftCluster elects leader across 3 nodes with strict quorum (N/2 + 1)', async () => {
    const cluster = new MockRaftCluster(3);
    assert.strictEqual(cluster.nodeIds.length, 3);

    const leader = await cluster.electLeader('node-01');
    assert.strictEqual(leader.nodeId, 'node-01');
    assert.strictEqual(leader.role, RaftNodeRole.LEADER);
    assert.strictEqual(leader.currentTerm, 1);

    // Follower states
    const node2 = cluster.nodes.get('node-02');
    const node3 = cluster.nodes.get('node-03');
    assert.ok(node2);
    assert.ok(node3);
    assert.strictEqual(node2.role, RaftNodeRole.FOLLOWER);
    assert.strictEqual(node3.role, RaftNodeRole.FOLLOWER);
    assert.strictEqual(node2.currentTerm, 1);
    assert.strictEqual(node3.currentTerm, 1);
  });

  test('MockRaftCluster replicates log entries and reaches quorum consensus', async () => {
    const cluster = new MockRaftCluster(3);
    const leader = await cluster.electLeader('node-01');

    const { success, entry } = await cluster.proposeCommand('trip', {
      swarm_id: 'swarm-fleet-99',
      reason: 'Global Fleet Circuit Quorum Tripped',
    });

    assert.strictEqual(success, true);
    assert.ok(entry);
    assert.strictEqual(entry.index, 1);
    assert.strictEqual(leader.commitIndex, 1);

    // Verify leader state machine
    const leaderState = leader.stateMachine.getSwarmState('swarm-fleet-99');
    assert.ok(leaderState);
    assert.strictEqual(leaderState.isTripped, true);

    // Verify followers received replicated log
    for (const nid of ['node-02', 'node-03']) {
      const follower = cluster.nodes.get(nid);
      assert.ok(follower);
      assert.strictEqual(follower.lastLogIndex, 1);
      assert.strictEqual(follower.log[1].command, 'trip');
    }
  });

  test('Raft handles leader failover and election term monotonicity', async () => {
    const cluster = new MockRaftCluster(3);
    const leader1 = await cluster.electLeader('node-01');
    assert.strictEqual(leader1.currentTerm, 1);

    // Leader-01 steps down
    leader1.stepDown(2);
    assert.strictEqual(leader1.role, RaftNodeRole.FOLLOWER);

    // Node-02 starts election and becomes new leader
    const leader2 = await cluster.electLeader('node-02');
    assert.strictEqual(leader2.nodeId, 'node-02');
    assert.strictEqual(leader2.role, RaftNodeRole.LEADER);
    assert.ok(leader2.currentTerm >= 2);

    // Proposal on new leader succeeds
    const { success } = await cluster.proposeCommand('reset', {
      swarm_id: 'swarm-fleet-99',
    }, 'node-02');
    assert.strictEqual(success, true);
  });

  test('Raft node rejects proposals when not in LEADER role', async () => {
    const cluster = new MockRaftCluster(3);
    await cluster.electLeader('node-01');

    const follower = cluster.nodes.get('node-02');
    assert.ok(follower);

    const res = await follower.propose('trip', { swarm_id: 'test' });
    assert.strictEqual(res.success, false);
    assert.strictEqual(res.entry, null);
    assert.strictEqual(res.leaderId, 'node-01');
  });
});
