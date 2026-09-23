/**
 * SwarmWatch Multi-Cluster High-Availability Raft Consensus Engine (Seam 30 / ADR 0041).
 *
 * TypeScript SDK parity implementation with Node 22 type-stripping compliance.
 * Implements pure standard library Raft distributed consensus protocol (Ongaro & Ousterhout)
 * providing leader election, monotonic term epochs, log replication, deterministic state machine,
 * and Byzantine-free quorum commits (N/2 + 1) without external Redis.
 */

export const RaftNodeRole = {
  LEADER: 'LEADER',
  CANDIDATE: 'CANDIDATE',
  FOLLOWER: 'FOLLOWER',
} as const;

export type RaftNodeRole = (typeof RaftNodeRole)[keyof typeof RaftNodeRole];

export interface RaftLogEntry {
  term: number;
  index: number;
  command: string;
  payload: Record<string, any>;
  timestamp: string;
}

export interface RequestVoteRpc {
  term: number;
  candidateId: string;
  lastLogIndex: number;
  lastLogTerm: number;
}

export interface RequestVoteResponse {
  term: number;
  voteGranted: boolean;
}

export interface AppendEntriesRpc {
  term: number;
  leaderId: string;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: RaftLogEntry[];
  leaderCommit: number;
}

export interface AppendEntriesResponse {
  term: number;
  success: boolean;
  matchIndex: number;
}

export interface SwarmRaftState {
  swarmId: string;
  totalTurns: number;
  totalTokens: number;
  totalCost: number;
  isTripped: boolean;
  tripReason?: string | null;
  trippedBy?: string | null;
  activeWorkers: string[];
  lastUpdated: string;
}

export class SwarmRaftStateMachine {
  private swarms: Map<string, SwarmRaftState>;

  constructor() {
    this.swarms = new Map();
  }

  applyEntry(entry: RaftLogEntry): SwarmRaftState {
    const cmd = (entry.command || '').toLowerCase();
    const payload = entry.payload || {};
    const swarmId = payload.swarm_id || payload.swarmId || 'default-swarm';

    let state = this.swarms.get(swarmId);
    if (!state) {
      state = {
        swarmId,
        totalTurns: 0,
        totalTokens: 0,
        totalCost: 0.0,
        isTripped: false,
        tripReason: null,
        trippedBy: null,
        activeWorkers: [],
        lastUpdated: entry.timestamp,
      };
      this.swarms.set(swarmId, state);
    }

    state.lastUpdated = entry.timestamp;

    if (cmd === 'trip' || cmd === 'circuit_trip' || cmd === 'fleet_trip') {
      state.isTripped = true;
      state.tripReason = payload.reason || 'Raft consensus circuit trip';
      state.trippedBy = payload.tripped_by || payload.trippedBy || 'raft-cluster';
    } else if (cmd === 'reset' || cmd === 'circuit_reset' || cmd === 'fleet_reset') {
      state.isTripped = false;
      state.tripReason = null;
      state.trippedBy = null;
      state.totalTurns = 0;
      state.totalTokens = 0;
      state.totalCost = 0.0;
    } else if (cmd === 'quota_inc' || cmd === 'update_quota') {
      state.totalTurns += Number(payload.turns || 0);
      state.totalTokens += Number(payload.tokens || 0);
      state.totalCost += Number(payload.cost || 0.0);
      const workerId = payload.worker_id || payload.workerId;
      if (workerId && !state.activeWorkers.includes(workerId)) {
        state.activeWorkers.push(workerId);
      }
    } else if (cmd === 'register_worker') {
      const workerId = payload.worker_id || payload.workerId;
      if (workerId && !state.activeWorkers.includes(workerId)) {
        state.activeWorkers.push(workerId);
      }
    }

    return { ...state, activeWorkers: [...state.activeWorkers] };
  }

  getSwarmState(swarmId: string): SwarmRaftState | null {
    const state = this.swarms.get(swarmId);
    if (!state) return null;
    return { ...state, activeWorkers: [...state.activeWorkers] };
  }

  listSwarms(): SwarmRaftState[] {
    return Array.from(this.swarms.values()).map((s) => ({
      ...s,
      activeWorkers: [...s.activeWorkers],
    }));
  }
}

export type RaftRpcSender = (
  peer: string,
  rpcType: 'request_vote' | 'append_entries',
  data: any
) => Promise<any>;

export class RaftConsensusEngine {
  nodeId: string;
  peers: string[];
  rpcSender?: RaftRpcSender;
  electionTimeoutRange: [number, number];
  heartbeatInterval: number;

  currentTerm: number;
  votedFor: string | null;
  log: RaftLogEntry[];

  commitIndex: number;
  lastApplied: number;
  role: RaftNodeRole;
  leaderId: string | null;

  nextIndex: Map<string, number>;
  matchIndex: Map<string, number>;
  stateMachine: SwarmRaftStateMachine;

  constructor(options?: {
    nodeId?: string;
    peers?: string[];
    rpcSender?: RaftRpcSender;
    electionTimeoutRange?: [number, number];
    heartbeatInterval?: number;
  }) {
    this.nodeId = options?.nodeId || 'node-01';
    this.peers = options?.peers || [];
    this.rpcSender = options?.rpcSender;
    this.electionTimeoutRange = options?.electionTimeoutRange || [0.15, 0.3];
    this.heartbeatInterval = options?.heartbeatInterval || 0.05;

    this.currentTerm = 0;
    this.votedFor = null;
    this.log = [
      {
        term: 0,
        index: 0,
        command: 'SENTINEL',
        payload: {},
        timestamp: new Date().toISOString(),
      },
    ];

    this.commitIndex = 0;
    this.lastApplied = 0;
    this.role = RaftNodeRole.FOLLOWER;
    this.leaderId = null;

    this.nextIndex = new Map();
    this.matchIndex = new Map();
    this.stateMachine = new SwarmRaftStateMachine();
  }

  get isLeader(): boolean {
    return this.role === RaftNodeRole.LEADER;
  }

  get lastLogIndex(): number {
    return this.log[this.log.length - 1].index;
  }

  get lastLogTerm(): number {
    return this.log[this.log.length - 1].term;
  }

  get clusterSize(): number {
    return this.peers.length + 1;
  }

  get quorumSize(): number {
    return Math.floor(this.clusterSize / 2) + 1;
  }

  handleRequestVote(rpc: RequestVoteRpc): RequestVoteResponse {
    // 1. If term < currentTerm, reject vote
    if (rpc.term < this.currentTerm) {
      return { term: this.currentTerm, voteGranted: false };
    }

    // 2. If term > currentTerm, update term and step down to follower
    if (rpc.term > this.currentTerm) {
      this.currentTerm = rpc.term;
      this.role = RaftNodeRole.FOLLOWER;
      this.votedFor = null;
      this.leaderId = null;
    }

    // 3. Check if we already voted for someone else
    const canVote = this.votedFor === null || this.votedFor === rpc.candidateId;

    // 4. Election Safety: Candidate log completeness check
    const ourLastIndex = this.lastLogIndex;
    const ourLastTerm = this.lastLogTerm;
    const logOk =
      rpc.lastLogTerm > ourLastTerm ||
      (rpc.lastLogTerm === ourLastTerm && rpc.lastLogIndex >= ourLastIndex);

    if (canVote && logOk) {
      this.votedFor = rpc.candidateId;
      return { term: this.currentTerm, voteGranted: true };
    }

    return { term: this.currentTerm, voteGranted: false };
  }

  handleAppendEntries(rpc: AppendEntriesRpc): AppendEntriesResponse {
    // 1. Reply false if term < currentTerm
    if (rpc.term < this.currentTerm) {
      return { term: this.currentTerm, success: false, matchIndex: this.lastLogIndex };
    }

    // 2. Recognize leader
    if (rpc.term > this.currentTerm || this.role === RaftNodeRole.CANDIDATE) {
      this.currentTerm = rpc.term;
      this.role = RaftNodeRole.FOLLOWER;
      this.votedFor = null;
    }
    this.leaderId = rpc.leaderId;

    // 3. Reply false if log doesn't contain an entry at prevLogIndex matching prevLogTerm
    if (rpc.prevLogIndex >= this.log.length) {
      return { term: this.currentTerm, success: false, matchIndex: this.lastLogIndex };
    }

    if (rpc.prevLogIndex > 0 && this.log[rpc.prevLogIndex].term !== rpc.prevLogTerm) {
      this.log = this.log.slice(0, rpc.prevLogIndex);
      return { term: this.currentTerm, success: false, matchIndex: this.lastLogIndex };
    }

    // 4. Append new entries not already in log
    if (rpc.entries && rpc.entries.length > 0) {
      for (let i = 0; i < rpc.entries.length; i++) {
        const entry = rpc.entries[i];
        const idx = rpc.prevLogIndex + 1 + i;
        if (idx < this.log.length) {
          if (this.log[idx].term !== entry.term) {
            this.log = this.log.slice(0, idx);
            this.log.push(entry);
          }
        } else {
          this.log.push(entry);
        }
      }
    }

    // 5. If leaderCommit > commitIndex, advance commitIndex and apply to state machine
    if (rpc.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(rpc.leaderCommit, this.lastLogIndex);
      this.applyCommittedEntries();
    }

    return { term: this.currentTerm, success: true, matchIndex: this.lastLogIndex };
  }

  applyCommittedEntries(): void {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      if (this.lastApplied < this.log.length) {
        const entry = this.log[this.lastApplied];
        if (entry.command !== 'SENTINEL') {
          this.stateMachine.applyEntry(entry);
        }
      }
    }
  }

  async startElection(): Promise<boolean> {
    this.currentTerm++;
    this.role = RaftNodeRole.CANDIDATE;
    this.votedFor = this.nodeId;
    this.leaderId = null;

    let votesGranted = 1; // Vote for self

    if (this.clusterSize <= 1) {
      this.becomeLeader();
      return true;
    }

    if (!this.rpcSender) {
      return false;
    }

    const rpc: RequestVoteRpc = {
      term: this.currentTerm,
      candidateId: this.nodeId,
      lastLogIndex: this.lastLogIndex,
      lastLogTerm: this.lastLogTerm,
    };

    const promises = this.peers.map((peer) =>
      this.rpcSender!(peer, 'request_vote', rpc).catch(() => null)
    );

    const responses = await Promise.all(promises);

    for (const res of responses) {
      if (res && typeof res === 'object') {
        const resp = res as RequestVoteResponse;
        if (resp.term > this.currentTerm) {
          this.stepDown(resp.term);
          return false;
        }
        if (resp.voteGranted) {
          votesGranted++;
        }
      }
    }

    if (votesGranted >= this.quorumSize && this.role === RaftNodeRole.CANDIDATE) {
      this.becomeLeader();
      return true;
    }

    return false;
  }

  becomeLeader(): void {
    this.role = RaftNodeRole.LEADER;
    this.leaderId = this.nodeId;

    for (const peer of this.peers) {
      this.nextIndex.set(peer, this.lastLogIndex + 1);
      this.matchIndex.set(peer, 0);
    }
  }

  async broadcastHeartbeats(): Promise<void> {
    if (!this.isLeader || !this.rpcSender) return;
    const promises = this.peers.map((peer) => {
      const prevIdx = this.lastLogIndex;
      const prevTerm = this.lastLogTerm;
      const rpc: AppendEntriesRpc = {
        term: this.currentTerm,
        leaderId: this.nodeId,
        prevLogIndex: prevIdx,
        prevLogTerm: prevTerm,
        entries: [],
        leaderCommit: this.commitIndex,
      };
      return this.rpcSender!(peer, 'append_entries', rpc).catch(() => null);
    });
    await Promise.all(promises);
  }


  stepDown(newTerm?: number): void {
    if (newTerm !== undefined && newTerm > this.currentTerm) {
      this.currentTerm = newTerm;
    }
    this.role = RaftNodeRole.FOLLOWER;
    this.votedFor = null;
    this.leaderId = null;
  }

  async propose(
    command: string,
    payload: Record<string, any>
  ): Promise<{ success: boolean; entry: RaftLogEntry | null; leaderId: string | null }> {
    if (!this.isLeader) {
      return { success: false, entry: null, leaderId: this.leaderId };
    }

    const entry: RaftLogEntry = {
      term: this.currentTerm,
      index: this.lastLogIndex + 1,
      command,
      payload,
      timestamp: new Date().toISOString(),
    };
    this.log.push(entry);

    // Single-node commit
    if (this.clusterSize <= 1) {
      this.commitIndex = entry.index;
      this.applyCommittedEntries();
      return { success: true, entry, leaderId: null };
    }

    if (!this.rpcSender) {
      return { success: false, entry, leaderId: null };
    }

    let replicationSuccesses = 1; // Self
    const promises = this.peers.map((peer) => {
      const prevIdx = entry.index - 1;
      const prevTerm = prevIdx < this.log.length ? this.log[prevIdx].term : 0;
      const rpc: AppendEntriesRpc = {
        term: this.currentTerm,
        leaderId: this.nodeId,
        prevLogIndex: prevIdx,
        prevLogTerm: prevTerm,
        entries: [entry],
        leaderCommit: this.commitIndex,
      };
      return this.rpcSender!(peer, 'append_entries', rpc).catch(() => null);
    });

    const responses = await Promise.all(promises);

    for (const res of responses) {
      if (res && typeof res === 'object') {
        const resp = res as AppendEntriesResponse;
        if (resp.term > this.currentTerm) {
          this.stepDown(resp.term);
          return { success: false, entry: null, leaderId: null };
        }
        if (resp.success) {
          replicationSuccesses++;
        }
      }
    }

    if (replicationSuccesses >= this.quorumSize) {
      this.commitIndex = entry.index;
      this.applyCommittedEntries();
      return { success: true, entry, leaderId: null };
    }

    return { success: false, entry, leaderId: null };
  }

  status(): Record<string, any> {
    return {
      node_id: this.nodeId,
      role: this.role,
      term: this.currentTerm,
      leader_id: this.leaderId,
      commit_index: this.commitIndex,
      last_log_index: this.lastLogIndex,
      peers: this.peers,
      cluster_size: this.clusterSize,
      quorum_size: this.quorumSize,
    };
  }
}

export class MockRaftCluster {
  nodeIds: string[];
  nodes: Map<string, RaftConsensusEngine>;

  constructor(nodeCount: number = 3) {
    this.nodeIds = Array.from({ length: nodeCount }, (_, i) => `node-${String(i + 1).padStart(2, '0')}`);
    this.nodes = new Map();

    for (const nid of this.nodeIds) {
      const peers = this.nodeIds.filter((p) => p !== nid);
      const engine = new RaftConsensusEngine({
        nodeId: nid,
        peers,
        rpcSender: this.makeSender(nid),
      });
      this.nodes.set(nid, engine);
    }
  }

  private makeSender(senderId: string): RaftRpcSender {
    return async (targetId: string, rpcType: 'request_vote' | 'append_entries', data: any) => {
      const target = this.nodes.get(targetId);
      if (!target) {
        throw new Error(`Peer node '${targetId}' not found in cluster`);
      }

      if (rpcType === 'request_vote') {
        return target.handleRequestVote(data);
      } else if (rpcType === 'append_entries') {
        return target.handleAppendEntries(data);
      }
      throw new Error(`Unknown RPC type: ${rpcType}`);
    };
  }

  async electLeader(preferredCandidate?: string): Promise<RaftConsensusEngine> {
    const candidateId = preferredCandidate || this.nodeIds[0];
    const candidate = this.nodes.get(candidateId);
    if (!candidate) {
      throw new Error(`Candidate '${candidateId}' not found in cluster`);
    }

    const won = await candidate.startElection();
    if (!won) {
      throw new Error(`Candidate '${candidateId}' failed to win Raft election`);
    }
    await candidate.broadcastHeartbeats();
    return candidate;
  }

  async proposeCommand(
    command: string,
    payload: Record<string, any>,
    leaderId?: string
  ): Promise<{ success: boolean; entry: RaftLogEntry | null }> {
    let leader: RaftConsensusEngine | undefined;
    if (leaderId) {
      leader = this.nodes.get(leaderId);
    } else {
      for (const node of this.nodes.values()) {
        if (node.isLeader) {
          leader = node;
          break;
        }
      }
    }

    if (!leader) {
      throw new Error('No active Raft leader found in cluster to receive proposal');
    }

    const result = await leader.propose(command, payload);
    return { success: result.success, entry: result.entry };
  }
}
