# ADR 0057: Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer

## Status
Accepted

## Context
In distributed multi-agent systems, agents emit intense, heterogeneous streams of telemetry, state snapshots, entropy calculations, bounded model checking problems, and multi-modal asset inspections. 

Without intelligent workload sharding:
1. **Node Starvation vs. Congestion**: Uniform round-robin or static key sharding causes hot spots on compute-constrained nodes (e.g. edge worker nodes with high CPU or memory load) while under-utilizing high-capacity nodes.
2. **Loss of Session Locality**: Naive stateless load balancing splits turns from the same swarm session across random nodes, destroying in-memory causal DAG cache locality and requiring expensive state synchronization.
3. **Single Point of Failure (SPOF)**: Relying on a centralized cloud reverse proxy or broker introduces latency overhead and fails under network partitions.
4. **LLM Cost Violation**: Using an LLM to decide task allocation introduces prohibitive latency (>1-3 seconds) and violates SwarmWatch's **Zero-Token Cost Guarantee**.

## Decision
We implement Seam 46: Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer as a deep module with full dual-SDK parity across Python (`swarmwatch.sharding`) and TypeScript (`@swarmwatch/sdk/src/sharding.ts`).

### 1. Compute Capacity Scoring (`ComputeCapacityScore`)
Each physical node $i$ evaluates its real-time compute health using normalized standard metrics ($C_i \in [0.0, 1.0]$):
$$C_i = w_{\text{cpu}} \cdot (1 - u_{\text{cpu}}) + w_{\text{mem}} \cdot (1 - u_{\text{mem}}) + w_{\text{queue}} \cdot (1 - \min(1.0, q / q_{\text{max}}))$$
Default weights: $w_{\text{cpu}} = 0.40$, $w_{\text{mem}} = 0.35$, $w_{\text{queue}} = 0.25$.
Nodes with $C_i < 0.15$ are flagged `OVERLOADED` and temporarily drained of new task admissions.

### 2. Capacity-Weighted Consistent Hash Ring (`ConsistentHashRing`)
The hash ring maps 32-bit integer keys into a circular coordinate space $[0, 2^{32}-1]$.
- Physical nodes register $V_i = \max(1, \lfloor V_{\text{base}} \times C_i \rfloor)$ virtual nodes (vnodes) where $V_{\text{base}} = 64$.
- High-capacity nodes naturally occupy a larger proportion of the ring, while degraded nodes yield hash slices gracefully.
- Virtual node hash tokens: $\text{hash}(`\text{node\_id}::\text{vnode\_idx}`)$ using deterministic 32-bit Murmur/FNV/SHA-256 slicing.
- Routing query: $O(\log M)$ binary search along the sorted ring array to find the nearest clockwise vnode.

### 3. Causal Session Affinity & Graceful Spillover
- Swarm turns, bounded model checks, and spans are keyed by `ShardRoutingKey` (typically `swarm_id` or `coalition_id`).
- When the primary node is healthy ($C_i \ge C_{\text{min}}$), it handles the workload, ensuring warm memory DAG cache hits.
- If the primary node saturates ($C_i < C_{\text{min}}$), the balancer transparently walks to the next successor replica node on the ring (Spillover Routing) without dropping spans.

### 4. Zero-Token Cost Guarantee
100% in-process deterministic mathematical scoring, binary search ring lookup, and hash calculation ($<25\mu\text{s}$ per decision). Zero remote LLM API calls, zero external proxy dependencies.

### 5. Dual-SDK Parity
- **Python**: `packages/sdk-python/swarmwatch/sharding.py` (`NodeHealthTier`, `NodeComputeMetrics`, `ComputeCapacityScore`, `VirtualNode`, `ConsistentHashRing`, `ShardRouteResult`, `ShardingStatusReport`, `ComputeAwareBalancer`, `global_compute_balancer`).
- **TypeScript**: `packages/sdk-typescript/src/sharding.ts` (`NodeHealthTier`, `NodeComputeMetrics`, `ComputeCapacityScore`, `VirtualNode`, `ConsistentHashRing`, `ShardRouteResult`, `ShardingStatusReport`, `ComputeAwareBalancer`, `globalComputeBalancer`) conforming strictly to Node 22 `--experimental-strip-types`.

### 6. Control Plane, CLI & Web Console Integration
- **REST Endpoints**:
  - `GET /api/v1/sharding/status`: Retrieve active ring size, node capacity distribution, and routing statistics.
  - `GET /api/v1/sharding/ring`: List virtual node ring topology and key ownership percentages.
  - `POST /api/v1/sharding/route`: Resolve target node for a given `shard_key`.
  - `POST /api/v1/sharding/node-capacity`: Report or update live compute capacity metrics for a node.
  - `POST /api/v1/sharding/rebalance`: Trigger immediate capacity-weighted ring reconstruction.
- **CLI Subcommand**:
  - `swarmwatch shard status`: Monospace ASCII summary of node capacities and ring balance.
  - `swarmwatch shard ring`: Detailed ASCII table of virtual node distributions.
  - `swarmwatch shard route <key>`: Route a session key to its primary and spillover nodes.
  - `swarmwatch shard rebalance`: Trigger manual ring capacity recalculation.
- **Web Console**:
  - `#shardBadge` in `.fleet-left-group` of `#fleetBar` in `dashboard.html`.
  - Dynamic status indicator: Balanced (`SHARDING: BALANCED (N NODES)` in green), Spillover (`SHARDING: SPILLOVER` in amber), Saturated (`SHARDING: SATURATED` in red).
  - 100% compliant with High-Density Craft Standard (0 anti-patterns `[]`).

## Consequences
### Positive
- Prevents node saturation and memory exhaustion across distributed swarm deployments.
- Preserves cache locality for causal DAG traversal while supporting seamless failover.
- Rapid $O(\log N)$ lookup in $<25\mu\text{s}$ with zero LLM token costs.

### Negative
- Dynamic capacity changes require periodic vnode updates; amortized by only updating vnodes when capacity shifts by more than $\Delta C \ge 0.10$.
