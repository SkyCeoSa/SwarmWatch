# ADR 0083: Dynamic Agent Federation & Swarm Mesh Topology Routing

## Status
Accepted

## Context
As autonomous agent swarms expand across heterogeneous infrastructure (multi-cloud Kubernetes clusters, edge nodes, and segregated multi-tenant enclaves), agents must collaborate across process and network boundaries without central bottlenecks. However, existing multi-agent communication frameworks suffer from critical failure modes:
1. **Ambient Trust & Unauthenticated Handshakes**: Ad-hoc agent routing frameworks rely on plain IP addresses or synthetic identifiers, exposing the swarm to spoofed agent identities, man-in-the-middle packet injection, and unauthorized cross-tenant data leakage.
2. **Cascading Routing Loops & Forwarding Storms**: Dynamic agent delegation graphs frequently generate forwarding loops where agents endlessly re-delegate tasks to each other, consuming network bandwidth and memory under load.
3. **Partition Isolation & Lack of Capability Fencing**: Uncontrolled inter-cluster routing allows agents to invoke tools and resources across tenant boundaries without checking cryptographic permissions or respecting fail-closed network partitions (ADR 0081).
4. **Disconnection from Safety Circuits**: When safety thresholds or budget circuit breakers trip (ADRs 0011, 0081, 0082), inter-agent message transit continues unabated, failing to contain runaway autonomous execution.

Previous seams established cryptographic capability tokens (ADR 0076 / Seam 65), effect-aware compensation (ADR 0077 / Seam 66), datacenter disaster recovery (ADR 0081 / Seam 70), and autonomous action gateways (ADR 0082 / Seam 71). Seam 72 completes the distributed communication fabric by engineering a verifiable, zero-trust **Swarm Mesh Federation & Routing Engine**.

## Decision
We implement a zero-trust, cryptographically verified **Dynamic Agent Federation & Swarm Mesh Topology Routing Engine** (`MeshFederationRouter` / `MeshTopology`) with strict dual-SDK parity across Python 3.12+ and Node 22 native type-stripping.

### 1. Mesh Topology Graph & Formal Routing Model
The federation topology is modeled as a weighted directed multigraph $\mathcal{G} = (\mathcal{V}, \mathcal{E}, \mathcal{W})$, where:
- $\mathcal{V}$ is the set of authenticated `PeerNode` instances, each possessing a unique `node_id`, `cluster_id`, `tenant_id`, and NIST P-256 public key.
- $\mathcal{E} \subseteq \mathcal{V} \times \mathcal{V}$ is the set of active, bidirectional or unidirectional authenticated routing links (`MeshLink`).
- $\mathcal{W}: \mathcal{E} \to \mathbb{R}^+$ assigns non-negative latency and cost weights to each link.

Optimal, loop-free routing paths are determined dynamically via Dijkstra's shortest path algorithm with loop-freedom constraints:
$$\pi^*(u, v) = \arg\min_{\pi \in \Pi(u, v)} \sum_{e \in \pi} \mathcal{W}(e)$$
where $\Pi(u, v)$ is the set of simple paths from $u$ to $v$. If no valid path exists, routing fails closed with `MeshDestinationUnreachableError`.

### 2. Zero-Trust Peer Discovery & Mutual Handshake
Nodes join the federation through mutual cryptographic handshakes:
1. Candidate nodes emit a signed `MeshPeerAdvertisement` binding their endpoint, cluster identity, tenant capability scope, and ECDSA signature.
2. The receiving router verifies the signature against the peer's public key; invalid signatures immediately raise `MeshPeerAuthenticationError`.
3. Periodic heartbeats maintain peer liveness. Peers failing to emit a heartbeat within `stale_timeout_seconds` are evicted from the active routing topology.

### 3. Loop Freedom & Hop-Limit Enforcement (TTL)
Every inter-agent message is encapsulated in an immutable `MeshPacket`:
- Each packet carries `max_hops` (TTL, default 16) and a monotonically incremented `hop_count`.
- Intermediate relays append their identifier to `path_traversed`.
- If a relay observes its own identifier in `path_traversed`, routing aborts immediately with `MeshRoutingLoopError`.
- If `hop_count \ge max_hops`, the packet is dropped fail-closed with `MeshHopLimitExceededError`.

### 4. Tenant & Capability Boundary Fencing
Inter-node packet transit is strictly bounded by tenant isolation rules:
- Cross-tenant message transit is prohibited unless the packet carries an attenuated `CapabilityToken` (ADR 0076) explicitly delegating cross-tenant communication.
- Unauthorized packets are rejected fail-closed with `MeshCapabilityFencingError`.

### 5. Fail-Closed Circuit Breaker Coupling
The federation router synchronizes directly with SwarmWatch safety circuit breakers (ADRs 0011, 0081, 0082):
- When a circuit trips (e.g., token exhaustion, runaway entropy, datacenter partition), `MeshFederationRouter` transitions to a locked state.
- In the locked state, all unprivileged inter-agent packet transit is rejected with `MeshCircuitTrippedError`, preventing runaway cascades across cluster nodes.

### 6. Tamper-Evident Routing Receipts
Every packet transit produces a deterministic `MeshRoutingReceipt`:
- Canonical SHA-256 digest (`sw:mesh:v1:...`) binding `(receipt_id, packet_id, source_node_id, dest_node_id, tenant_id, path_traversed, status, duration_us, timestamp_ns)`.
- Recorded in an append-only in-memory audit log accessible via control plane APIs.

## Invariants
1. **Loop Freedom Invariant**: Shortest-path routing with cycle detection guarantees zero packet loops; packets exceeding `max_hops` are dropped fail-closed (`MeshHopLimitExceededError`).
2. **Capability & Tenant Fencing**: Packets cannot cross tenant boundaries or access unauthorized nodes without cryptographic capability validation (`MeshCapabilityFencingError`).
3. **Fail-Closed Safety Circuit Coupling**: When safety circuits trip, mesh routing immediately halts all unprivileged packet transit (`MeshCircuitTrippedError`).
4. **Sub-250µs Execution SLO**: Shortest path computation and routing decision execute in $<50\,\mu\text{s}$ for bounded topologies ($\le 128$ nodes).
5. **Zero-Token Cost Guarantee**: 100% deterministic local graph algorithms (Dijkstra, DFS cycle detection, ECDSA/SHA-256 verification) with zero external LLM API calls.
6. **Dual-SDK Parity**: 100% symmetrical semantics, models, and errors between Python 3.12+ and Node 22 native type-stripping.

## Consequences
- **Positive**: Enables decentralized, zero-trust inter-agent communication across multi-cloud clusters while eliminating forwarding loops and cross-tenant data leaks.
- **Positive**: Seamlessly bridges with capability tokens (ADR 0076) and disaster recovery partitions (ADR 0081).
- **Positive**: Sub-microsecond local routing with zero LLM API token overhead.
- **Negative**: Dynamic mesh topology graph updates require lock synchronization and periodic heartbeat house-keeping.
