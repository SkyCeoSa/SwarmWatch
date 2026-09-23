# ADR 0041: Multi-Cluster High-Availability Raft Consensus Engine

## Status
Accepted

## Context
SwarmWatch coordinates multi-agent swarm sessions, distributed atomic quota ceilings, and immediate circuit trips across parallel worker nodes. In Seams 22–24 (ADRs 0033–0035), distributed state synchronization was established using Redis as the high-throughput coordination backbone.

However, enterprise deployments in multi-cloud, edge, on-premises air-gapped data centers, or zero-trust Kubernetes topologies often prohibit external Redis infrastructure or require autonomous self-contained clusters. To provide enterprise-grade high availability, split-brain immunity, and Byzantine-free quorum coordination without any external dependencies, SwarmWatch requires an embedded Raft distributed consensus engine.

## Decision Drivers
1. **Zero External Dependency Invariant**: Must operate entirely using standard library primitives (`asyncio`, `time`, `random`, `json`, `dataclasses`) without external distributed key-value stores (etcd, Consul, ZooKeeper, Redis).
2. **Quorum Safety & Split-Brain Prevention**: Enforces standard Raft leader election ($N/2 + 1$), term monotonic epochs, and replicated log matching invariants.
3. **Replicated State Machine for AI Swarms**: Replicated log entries directly apply atomic fleet quota counters, swarm circuit trips, and policy updates across all nodes in the cluster.
4. **Dual Transport Support**:
   - In-memory event loop transport for lightning-fast sub-50ms deterministic local testing.
   - Standard HTTP/REST RPC transport for multi-node production Kubernetes / cloud deployments.
5. **Dual-SDK Parity**: Symmetrical Raft consensus engine and state machine in both Python and TypeScript (`@swarmwatch/sdk`).
6. **Zero-Token Cost Guarantee**: All election mechanics, log replication, and state transitions execute with sub-millisecond local latency and zero remote LLM token usage.

## Architecture

### 1. Raft Node Roles & States
- **`RaftNodeRole`**:
  - `FOLLOWER`: Responds to incoming RPCs from candidates and leaders. Converts to `CANDIDATE` if election timeout fires.
  - `CANDIDATE`: Solicits votes via `RequestVoteRpc`. Transitions to `LEADER` upon obtaining a quorum ($N/2 + 1$).
  - `LEADER`: Emits periodic heartbeats via `AppendEntriesRpc`, coordinates client proposals, and advances commit index upon quorum acknowledgment.
- **`RaftTerm`**: Monotonically increasing epoch number. A node observing a higher term immediately steps down to `FOLLOWER`.

### 2. RPC Protocols
- **`RequestVoteRpc`**:
  - `term`: Candidate's term.
  - `candidate_id`: Candidate identifier.
  - `last_log_index`: Index of candidate's last log entry.
  - `last_log_term`: Term of candidate's last log entry.
  - *Response*: `term`, `vote_granted` (True if term $\ge$ currentTerm, votedFor is null or candidate_id, and candidate log is at least as up-to-date as receiver).
- **`AppendEntriesRpc`**:
  - `term`: Leader's term.
  - `leader_id`: Leader identifier.
  - `prev_log_index`: Index of log entry immediately preceding new ones.
  - `prev_log_term`: Term of `prev_log_index` entry.
  - `entries`: Array of `RaftLogEntry` objects to replicate.
  - `leader_commit`: Leader's `commit_index`.
  - *Response*: `term`, `success`, `match_index`.

### 3. Replicated Swarm State Machine (`SwarmRaftStateMachine`)
Applies committed log entries sequentially:
- `COMMAND_TRIP`: Halts a swarm session cluster-wide (`is_tripped = True`, reason, tripping worker).
- `COMMAND_RESET`: Re-arms circuits and resets counters.
- `COMMAND_QUOTA_INC`: Atomically increments cumulative tokens, turns, and cost.

## Consequences
### Positive
- Fully standalone, self-healing clustering without external Redis or database infrastructure.
- 100% deterministic local offline testing across Windows, macOS, and Linux.
- Strict quorum guarantees ($N/2 + 1$) preventing split-brain during network partitions.
- Dual-SDK parity across Python and TypeScript runtimes.

### Negative
- A minimum of 3 nodes is required for fault tolerance ($2F + 1$ formula: 3 nodes tolerate 1 failure, 5 nodes tolerate 2 failures). Single-node clusters function as immediate self-leaders.
