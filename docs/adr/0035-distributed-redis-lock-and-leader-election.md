# ADR 0035: Distributed Redis Lock & Leader Election for Multi-Server SwarmWatch Instances

## Status
Accepted

## Context
When SwarmWatch instances are scaled horizontally across multiple servers, worker nodes, or Kubernetes pods behind an ingress or load balancer, certain background jobs and control plane routines must operate strictly as singletons. These include:
1. Periodic Dead-Letter Queue (DLQ) webhook retries and alerting replays.
2. Scheduled synthetic alert simulation runs and agent audit log rotations.
3. Fleet-wide metric aggregation rollups and counterfactual budget reports.
4. Cluster-wide circuit breaker state flushes.

Without a distributed coordination mechanism, running multiple server instances concurrently leads to split-brain execution, duplicate alert dispatches to external systems (Slack, Discord, PagerDuty), and race conditions during cluster resets.

## Decision
We introduce **Seam 24**: **Distributed Redis Lock & Lease-Based Leader Election** (`RedisDistributedLock` & `RedisLeaderElector`) into SwarmWatch.

### Architectural Invariants:
1. **Atomic Mutex Locking (`RedisDistributedLock`)**:
   - Implements distributed mutual exclusion using Redis `SET resource token NX PX ttl_ms`.
   - Generates unique cryptographic node/lock tokens (`uuid4`) to guarantee safe ownership semantics.
   - Provides Python context manager (`with lock:`) and async/sync acquisition interfaces.
   - Releases lock safely via an atomic Lua script:
     ```lua
     if redis.call("get", KEYS[1]) == ARGV[1] then
         return redis.call("del", KEYS[1])
     else
         return 0
     end
     ```
   - Supports lease extension (`extend(ms)`) via an atomic Lua script:
     ```lua
     if redis.call("get", KEYS[1]) == ARGV[1] then
         return redis.call("pexpire", KEYS[1], ARGV[2])
     else
         return 0
     end
     ```

2. **Lease-Based Leader Election Manager (`RedisLeaderElector`)**:
   - Each SwarmWatch server instance is assigned a unique node identifier (`node_id`, e.g. `srv-node-7b8c9d1e`).
   - Nodes compete for the distributed lease key `swarmwatch:leader:lock` with a configurable lease TTL (default 5000ms).
   - The elected `LEADER` runs a background heartbeat daemon renewing the lease every `heartbeat_interval_s` (default 1.5s).
   - If the Leader node crashes, becomes partitioned, or stops renewing, the lease naturally expires in Redis. Standby nodes (`STANDBY`) detect expiry and compete to elect a new Leader with zero downtime and no split-brain.
   - Nodes register their presence in `swarmwatch:cluster:nodes` with heartbeat epoch timestamps, allowing the cluster to maintain an active node roster.
   - Provides voluntary step-down (`step_down()`) allowing operators or rolling updates to smoothly relinquish leadership to a standby node.
   - Provides lifecycle hooks `on_elected(callback)` and `on_stepped_down(callback)` to start and stop background singleton workers dynamically.

3. **Dual-SDK Parity (TypeScript)**:
   - Identical locking and leader election primitives implemented in `@swarmwatch/sdk` (`packages/sdk-typescript/src/redis_lock.ts`).

4. **Control Plane REST & WebSocket Endpoints**:
   - `GET /api/v1/cluster/leader`: Returns current leader node ID, lease remaining ms, role of querying instance, and active cluster roster.
   - `POST /api/v1/cluster/leader/step-down`: Voluntarily relinquishes leadership, triggering election of another node.
   - WebSocket broadcast: Emits `cluster_leader_changed` event over active sessions when leadership transitions.

5. **CLI & Console Observability**:
   - CLI: `swarmwatch cluster status` and `swarmwatch cluster step-down`.
   - Console: Leader status badge and cluster node count in `#fleetBar`.

6. **Zero-Token Cost Guarantee**:
   - 100% deterministic local in-process execution using pure socket RESP2 or mock in-memory client. Zero remote LLM API calls.

## Consequences
- Multi-server and multi-pod SwarmWatch deployments can safely run singleton workers without duplicate executions or race conditions.
- Automatic failover is achieved within one lease TTL period (e.g. $<5\text{s}$) upon leader crash or partition.
- Full observability into cluster node roles directly from the developer console and CLI.
