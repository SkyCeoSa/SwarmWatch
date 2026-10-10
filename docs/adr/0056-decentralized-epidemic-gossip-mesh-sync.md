# ADR 0056: Decentralized Epidemic Gossip Telemetry Mesh Sync

## Status
Accepted

## Context
As enterprise multi-agent deployments scale across edge nodes, hybrid clouds, air-gapped data centers, and ephemeral serverless worker containers, relying solely on centralized brokers (e.g., central Redis or a single master database) creates single-points-of-failure (SPOFs), cross-region latency bottlenecks, and network partition vulnerability.

When agents operate across decentralized clusters, they require:
1. **Peer-to-Peer Telemetry Dissemination**: Rapid propagation of causal spans, circuit trip interventions, and watermarked artifacts between autonomous nodes without depending on a central cloud broker.
2. **Causal Event Ordering via Vector Clocks**: Ensuring distributed nodes can order events, detect concurrent mutations, and achieve eventual consistency without synchronized physical wall clocks.
3. **Partition Tolerance & Anti-Entropy**: Epidemic gossip protocols (combining eager Rumor Mongering with periodic Anti-Entropy digests) that guarantee complete state convergence even under intermittent network splits and transient node dropouts.
4. **Zero-Token Cost Guarantee**: Dissemination, digest calculation, and causal merge must execute 100% in-process with zero remote LLM API calls, sub-millisecond latencies ($<50\mu\text{s}$ per operation), and zero heavy external messaging brokers.

## Decision
We implement Seam 45: Decentralized Epidemic Gossip Telemetry Mesh Sync as a deep module with full dual-SDK parity across Python (`swarmwatch.gossip`) and TypeScript (`@swarmwatch/sdk/src/gossip.ts`).

### 1. Vector Clock Algebra (`VectorClock`)
Every mesh node maintains an in-memory vector clock $V \in \mathbb{N}^{\mathcal{N}}$ where each entry $V[i]$ represents the latest monotonic sequence counter observed from node $i$.
- **Increment**: When node $i$ creates a local span or trips a circuit, it increments its own counter:
  $$V_i[i] \leftarrow V_i[i] + 1$$
- **Causal Dominance**: Clock $V_A$ dominates $V_B$ ($V_A \ge V_B$) iff $\forall k, V_A[k] \ge V_B[k]$ and $\exists k, V_A[k] > V_B[k]$.
- **Concurrency**: Clocks $V_A$ and $V_B$ are concurrent ($V_A \parallel V_B$) iff neither dominates the other, signaling branch divergence that must be causally merged.
- **Merge**:
  $$V_{\text{merged}}[k] = \max(V_A[k], V_B[k]) \quad \forall k$$

### 2. Mesh Node Membership & Failure Detection (`MeshNode`)
Nodes maintain a dynamic peer roster tracking:
- `node_id`: Unique cryptographic identifier of the peer.
- `endpoint`: Direct network or in-process address (`host:port` or virtual URI).
- `status`: State machine transitioning through `ACTIVE`, `SUSPECT`, `DEAD`.
- `last_heartbeat`: Timestamp of the latest gossip exchange.
- `generation`: Monotonic epoch counter to distinguish process restarts.

### 3. Epidemic Dissemination Engine (`GossipMeshManager`)
The mesh manager operates two complementary gossip protocols:
1. **Rumor Mongering (Eager Push)**: Upon recording a high-priority event (such as a circuit breaker halt or quarantine isolation), the node probabilistically selects $k$ random active peers (fanout $k=3$) and pushes the delta immediately.
2. **Anti-Entropy (Pairwise Pull/Push Reconciliation)**: Periodically (every anti-entropy epoch, e.g. 5000ms), nodes exchange compact `GossipDigest` objects containing vector clocks and event hashes. If peer $B$ is missing events known to peer $A$, $A$ transmits a `GossipDelta` containing the missing spans, trips, and watermarks.

### 4. Zero-Token Cost Guarantee
100% in-process deterministic hash comparisons, vector clock math, and JSON/binary delta serialization ($<50\mu\text{s}$ per turn). Zero external LLM calls, zero centralized broker dependencies.

### 5. Dual-SDK Parity
- **Python**: `packages/sdk-python/swarmwatch/gossip.py` (`MeshNodeStatus`, `MeshNode`, `VectorClock`, `GossipDigest`, `GossipDelta`, `MeshStatusReport`, `GossipMeshManager`, `global_gossip_manager`).
- **TypeScript**: `packages/sdk-typescript/src/gossip.ts` (`MeshNodeStatus`, `MeshNode`, `VectorClock`, `GossipDigest`, `GossipDelta`, `MeshStatusReport`, `GossipMeshManager`, `globalGossipManager`) conforming strictly to Node 22 `--experimental-strip-types`.

### 6. Control Plane, CLI & Web Console Integration
- **REST Endpoints**:
  - `GET /api/v1/mesh/status`: Retrieve mesh convergence score, active peers, and vector clock state.
  - `GET /api/v1/mesh/peers`: List active and suspect peers in the mesh roster.
  - `POST /api/v1/mesh/peers/register`: Register or update a remote peer node in the local mesh table.
  - `POST /api/v1/mesh/gossip/round`: Trigger an on-demand epidemic gossip round.
  - `POST /api/v1/mesh/gossip/sync`: Reconcile an incoming `GossipDigest` or `GossipDelta`.
- **CLI Subcommand**:
  - `swarmwatch mesh status`: Monospace ASCII report of mesh peers, vector clock, and convergence.
  - `swarmwatch mesh peers`: Tabular list of active, suspect, and dead mesh peers.
  - `swarmwatch mesh join <endpoint>`: Join a cluster by advertising to a seed peer.
  - `swarmwatch mesh sync`: Trigger manual anti-entropy synchronization round.
- **Web Console**:
  - `#meshBadge` in `.fleet-left-group` of `#fleetBar` in `dashboard.html`.
  - Dynamic status indicator: Converged (`MESH: CONVERGED (N PEERS)` in green), Syncing (`MESH: SYNCING` in accent), Partitioned (`MESH: PARTITIONED` in red).
  - 100% compliant with High-Density Craft Standard (0 anti-patterns `[]`).

## Consequences
### Positive
- Fully decentralized peer-to-peer telemetry synchronization with no single point of failure.
- Guaranteed eventual consistency across transient network splits and partitioned clusters.
- Causal event ordering through Vector Clocks eliminates clock-skew anomalies.
- Zero external LLM tokens or centralized message broker dependencies.

### Negative
- Gossip message volume scales with $O(k \cdot N \log N)$ where $k$ is fanout and $N$ is node count; bounded by digest pruning and anti-entropy intervals.
