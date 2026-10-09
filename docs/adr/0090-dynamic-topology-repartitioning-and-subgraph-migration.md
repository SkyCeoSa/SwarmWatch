# ADR 0090: Dynamic Topology Re-Partitioning & Adaptive Sub-Graph Migration

## Status
Accepted

## Context & Problem Statement
In multi-tenant, distributed multi-cluster agent architectures (established across Seams 74 through 78), agent workloads exhibit dynamic, non-uniform communication topologies. Statically partitioned hyper-graphs $\mathcal{H} = (\mathcal{V}_H, \mathcal{E}_H)$ across autonomous heterogeneous clusters ($\mathcal{C}_1, \dots, \mathcal{C}_k$) quickly lead to:
1. **Severe Hyper-Edge Cut Hotspotting**: When interacting agent groups and shared memory leases are split across distant clusters, multi-party hyper-edge coordination requires repeated cross-cluster Byzantine consensus (ADR 0085) and anti-entropy synchronization (ADR 0088), causing high inter-cluster network traffic and latency spikes.
2. **Compute & Context Load Imbalances**: Resource-intensive agent swarms may saturate a single cluster's memory or execution budget while peer clusters remain underutilized.
3. **Causal Vector Drift & State Divergence**: Migrating active agent sub-graphs across cluster boundaries without mathematical guarantees risks causal vector regression, duplicate agent states, or orphan dangling hyper-edges.

Existing load balancers (e.g., Seam 46) route individual stateless tasks, but do not provide atomic, zero-downtime, causally monotonic migration of stateful $n$-ary hyper-graph partitions.

## Decision
We implement **Seam 79: Dynamic Topology Re-Partitioning & Adaptive Sub-Graph Migration** (`DynamicTopologyMigrator`), providing:
1. **Mathematical Objective & Graph Partitioning Heuristic**:
   A deterministic, zero-token cost cut-minimization and load variance balancing objective function:
   $$J(\mathcal{P}) = \alpha \cdot \text{CutWeight}(\mathcal{P}) + \beta \cdot \text{LoadVariance}(\mathcal{P}) + \gamma \cdot \text{Cost}_{\text{migration}}$$
   where $\text{CutWeight}(\mathcal{P}) = \sum_{e \in \mathcal{E}_{\text{cut}}} w(e)$, identifying hyper-edges spanning two or more distinct clusters ($|\{ \text{cluster}(v) \mid v \in e \}| > 1$).
2. **Sub-Graph Boundary Isolation & Contract Model**:
   A candidate migration sub-graph $\mathcal{S}_{\text{mig}} = (\mathcal{V}_{\text{mig}}, \mathcal{E}_{\text{mig}})$ is extracted with formal boundary contracts:
   - **Internal Hyper-Edges**: $e \in \mathcal{E}_H$ where $e \subseteq \mathcal{V}_{\text{mig}}$ migrate fully to the destination cluster.
   - **Boundary Cut Hyper-Edges**: $e \in \mathcal{E}_H$ where $e \cap \mathcal{V}_{\text{mig}} \neq \emptyset$ and $e \setminus \mathcal{V}_{\text{mig}} \neq \emptyset$ are converted to cross-cluster boundary contracts (`SubGraphBoundaryContract`) ensuring continuous reachability without orphan references.
3. **Deterministic 7-Phase Migration Automaton (`MigrationState`)**:
   - `IDLE`: Continuous monitoring of topology and cluster resource metrics.
   - `PROFILING_SKEW`: Evaluation of load variance and inter-cluster hyper-edge cut weights.
   - `PLAN_SYNTHESIZED`: Formulation of a deterministic `MigrationPlan` with explicit boundary contracts and expected cut reduction.
   - `PREPARING_TRANSFER`: Quarantining source sub-graph nodes, synthesizing bundle digest, and validating source Proof-of-Execution certificates.
   - `TRANSFERRING_SUBGRAPH`: Atomic ingestion of nodes, internal edges, and boundary contracts on the destination cluster.
   - `COMMITTING_MIGRATION`: Monotonic vector clock handover and source node eviction.
   - `COMPLETED_STABLE`: Transition to verified stable state.
   - `CIRCUIT_TRIPPED`: Immediate fail-closed lockout upon invariant violation.
4. **Three Strict Operational Invariants**:
   - **Invariant M1 (Causal Vector Monotonicity Handover)**:
     The target cluster's post-migration vector clock $V_{\text{dst\_post}}$ must satisfy:
     $$\forall k, \quad V_{\text{dst\_post}}[k] \ge \max(V_{\text{src}}[k], V_{\text{dst\_pre}}[k])$$
     and $V_{\text{dst\_post}}[\mathcal{C}_{\text{dst}}] = \max(V_{\text{src}}[\mathcal{C}_{\text{dst}}], V_{\text{dst\_pre}}[\mathcal{C}_{\text{dst}}]) + 1$.
     Any regression ($V_{\text{dst\_post}}[k] < V_{\text{src}}[k]$) raises `MigrationCausalRegressionError` and trips the circuit.
   - **Invariant M2 (Hyper-Graph Boundary Closure & No Orphan Cut Edges)**:
     All hyper-edges incident to migrated nodes must either migrate completely or have a verified `SubGraphBoundaryContract`. Any dangling edge referencing missing nodes raises `OrphanSubGraphEdgeError` and trips the circuit fail-closed.
   - **Invariant M3 (Cryptographic Proof-of-Execution Attestation & Target Verification)**:
     Migration plans and execution receipts are bound to canonical SHA-256 domain-tagged hashes (`sw:mig:plan:v1:<sha256>`, `sw:mig:rcpt:v1:<sha256>`) and verified against issuer committee roots.
5. **Fail-Closed Safety Circuit Coupling**:
   Unverifiable migration bundles, boundary violations, or negative clock regressions immediately latch the safety circuit, halting further migrations until explicit administrative rearm.
6. **Zero-Token Cost Guarantee**:
   All graph partitioning evaluations, bundle packing, and cryptographic verifications execute 100% locally in-process with $0.00 USD LLM API token expenditure.
7. **Sub-250µs Execution Latency SLO**:
   All in-memory evaluations, bundle serialization, and atomic handovers complete under 250µs for typical agent cluster graph topologies.

## Consequences

### Positive
- Sub-graphs can be dynamically relocated to relieve overloaded clusters and co-locate communicating agents.
- Cross-cluster coordination overhead and hyper-edge synchronization latency are minimized.
- Zero risk of causal state corruption or orphan nodes due to strict Invariants M1 and M2.
- 100% dual-SDK parity across Python and TypeScript (Node 22 native type-stripping).

### Negative / Trade-offs
- Slight transient memory overhead during sub-graph bundle serialization and atomic handover.
- Boundary hyper-edges spanning clusters retain cross-cluster synchronization overhead until further consolidated.
