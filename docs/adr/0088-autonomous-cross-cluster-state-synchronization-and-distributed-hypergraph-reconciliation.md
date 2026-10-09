# ADR 0088: Autonomous Cross-Cluster State Synchronization & Distributed Hyper-Graph Reconciliation

## Status
Accepted

## Context
As autonomous agent swarms scale across geographically distributed datacenters, sovereign cloud regions, and multi-tenant organizational perimeters (ADR 0076, ADR 0081, ADR 0083), each cluster operates with its own localized state machines, Byzantine consensus committees (ADR 0085), optimistic rollup engines (ADR 0086), and epistemic arbitration authorities (ADR 0087).

When swarms collaborate across cluster boundaries on shared objectives—such as co-authoring distributed pipelines, orchestrating multi-agent joint task leases, or executing multi-region disaster failover—they must synchronize complex relational and transactional state. Traditional synchronization approaches exhibit fatal vulnerabilities in decentralized agent networks:
1. **Unverifiable Cross-Cluster Attestations (Invariant I1)**: Prior consensus verification models verified Proof-of-Execution (PoE) certificates against the *local* cluster's active committee. However, in cross-cluster sync, certificates are issued by remote clusters under independent epochs and distinct validator committee public key roots (`committee_root`). Without binding the issuing cluster's epoch committee root and quorum into verification, remote clusters either reject valid foreign state or accept forged certificates from unrecognized entities.
2. **Pairwise Edge Brittleness in Complex Agent Topologies**: Standard graph synchronization models pairs of nodes $(u, v)$. In real-world multi-agent coordination, transactions frequently involve $k \ge 3$ agents simultaneously (e.g., a 3-agent joint escrow lease, an n-way consensus vote, or a multi-model cognitive pipeline). Representing multi-agent relations via pairwise binary edges causes relational loss, combinatorial edge bloat, and synchronization races.
3. **Causal Anomaly and Split-Brain Partition Thrashing**: Asynchronous network latencies and transient WAN partitions produce concurrent conflicting mutations. Simple Last-Write-Wins (LWW) based on non-synchronized wall-clock time causes silent data loss and clock-skew exploits. Conversely, locking distributed databases across multi-region clusters introduces hundreds of milliseconds of latency, violating SwarmWatch's sub-250 microsecond execution SLO.
4. **Decoupling from Safety Circuits**: When cross-cluster state divergence indicates an unresolvable split-brain fork or cryptographic forgery, classical sync engines attempt endless retries. Swarms require an automated, fail-closed mechanism to halt state mutation and latch the safety circuit immediately.

Seam 77 resolves these challenges by introducing an in-process, zero-token **Autonomous Cross-Cluster State Synchronization & Distributed Hyper-Graph Reconciliation Engine** (`CrossClusterSyncEngine`).

## Decision
We implement a deterministic, in-process **Autonomous Cross-Cluster State Synchronization & Distributed Hyper-Graph Reconciliation Engine** with complete dual-SDK parity across Python 3.12+ (`swarmwatch.cross_cluster_sync`) and TypeScript Node 22 native type-stripping (`@swarmwatch/sdk/src/cross_cluster_sync.ts`).

### 1. Distributed Hyper-Graph State Model
State is formalized as an immutable, conflict-free distributed hyper-graph $\mathcal{H} = (\mathcal{V}_H, \mathcal{E}_H)$:
- **Hyper-Nodes** $\mathcal{V}_H$: Atomic agent entities, resources, or contexts:
  $$\mathcal{V}_H = \langle \text{node\_id}, \text{cluster\_id}, \text{state\_type}, \text{state\_payload}, \mathbf{V}, \text{epoch}, t, \text{digest} \rangle$$
  where $\mathbf{V}$ is a vector clock $\{C_1: v_1, C_2: v_2, \dots\}$.
- **Hyper-Edges** $\mathcal{E}_H$: $n$-ary relations connecting multi-party subsets of hyper-nodes ($\mathcal{N} \subseteq \mathcal{V}_H, |\mathcal{N}| \ge 2$):
  $$\mathcal{E}_H = \langle \text{edge\_id}, \mathcal{N}_{\text{node\_ids}}, \text{edge\_type}, \text{properties}, w, \mathbf{V}, \text{epoch}, t, \text{digest} \rangle$$
  where $w \in [0.0, 1.0]$ is the edge strength/validity weight.
- **Integrity Invariant**: Every referenced `node_id` in a hyper-edge must exist within the hyper-graph. Dangling hyper-edges immediately raise `HyperGraphIntegrityError`.

### 2. Cross-Cluster Proof-of-Execution Verification (Invariant I1)
When Cluster $B$ ingests a sync packet emitted by Cluster $A$, it executes **Issuer-Bound Certificate Verification**:
- The sync packet includes a `ProofOfExecutionCertificate` ($\mathcal{P}$).
- The verifier validates $\mathcal{P}$ against Cluster $A$'s registered epoch committee root (`committee_root`):
  $$\text{Verify}(\mathcal{P}) \iff \mathcal{P}.\text{epoch} = e_A \land \mathcal{P}.\text{committee\_root} = \text{Root}(K_A^{(e_A)}) \land |\mathcal{P}.\text{signers}| \ge \mathcal{P}.\text{quorum\_required}$$
- If $\mathcal{P}$ is tampered with, carries insufficient signatures, or diverges from the issuing cluster's epoch committee root, verification fails closed, raising `UnverifiableCertificateError`.

### 3. Vector Clock Causal Analysis
Given local vector clock $\mathbf{V}_L$ and incoming remote vector clock $\mathbf{V}_R$, the causal relationship is strictly classified:
$$\text{Verdict}(\mathbf{V}_L, \mathbf{V}_R) = \begin{cases}
\text{CAUSALLY\_AHEAD} & \text{if } \forall k, \mathbf{V}_R[k] \ge \mathbf{V}_L[k] \land \mathbf{V}_R \ne \mathbf{V}_L \text{ (gap-free)} \\
\text{CAUSALLY\_BEHIND} & \text{if } \forall k, \mathbf{V}_R[k] \le \mathbf{V}_L[k] \\
\text{CAUSAL\_GAP\_DETECTED} & \text{if } \exists k, \mathbf{V}_R[k] > \mathbf{V}_L[k] + 1 \\
\text{CONCURRENT} & \text{if } \exists i, j \text{ s.t. } \mathbf{V}_R[i] > \mathbf{V}_L[i] \land \mathbf{V}_R[j] < \mathbf{V}_L[j]
\end{cases}$$

- Gaps raise `CausalDiscontinuityError` to prevent processing out-of-order state transitions.
- Concurrent updates trigger CRDT algebraic semilattice reconciliation.

### 4. CRDT Join Semilattice Reconciliation Algebra
Concurrent state mutations are merged deterministically via a join semilattice $(\mathcal{H}, \sqcup)$:
- **Vector Clock Merge**: $\mathbf{V}_{\text{merged}}[k] = \max(\mathbf{V}_L[k], \mathbf{V}_R[k])$.
- **Hyper-Node LWW Resolution**: For conflicting node mutations $u_L, u_R$:
  $$u_{\text{winner}} = \begin{cases}
  u_R & \text{if } (e_R, t_R, c_R) > (e_L, t_L, c_L) \\
  u_L & \text{otherwise}
  \end{cases}$$
- **Hyper-Edge Monotone Join**: For concurrent hyper-edge modifications:
  - Node membership $\mathcal{N} = \mathcal{N}_L \cup \mathcal{N}_R$.
  - Weight joined monotonically: $w = \max(w_L, w_R)$.
  - Properties merged via dictionary union with LWW precedence.
- **Split-Brain Fork Rejection**: Irreconcilable structural conflicts (e.g., mutually exclusive state assertions across disconnected partitions) raise `SplitBrainForkError` and trip the safety circuit.

### 5. Tamper-Evident Sync Reconciliation Receipts
Upon successful reconciliation, the engine issues an immutable cryptographic receipt:
$$\text{Receipt Hash} = \text{"sw:sync:v1:"} \,\|\, \text{SHA256}(\text{canonical\_json}(\text{SyncReconciliationReceipt}))$$
recording source cluster, target cluster, nodes reconciled count, edges reconciled count, merged vector clock, and composite hyper-graph root digest.

### 6. Fail-Closed Safety Circuit Coupling
The synchronization engine couples directly with SwarmWatch circuit breakers:
- Active circuit trips immediately reject outgoing and incoming sync packets (`SyncCircuitTrippedError`).
- Invariant breaches (unverifiable certificates or split-brain forks) immediately latch the safety circuit, halting cross-cluster mutations until administrator rearm.

## Invariants
1. **Invariant I1 (Issuer-Bound PoE Verification)**: Foreign certificates are validated strictly against the issuing cluster's epoch committee root and quorum threshold.
2. **Zero-Token Cost Guarantee**: All vector clock evaluations, hyper-graph merges, and cryptographic validations execute locally with zero external LLM API calls ($0.00 USD cost).
3. **Sub-250µs Execution Latency SLO**: Ingestion, causal classification, and reconciliation execute in $<250\,\mu\text{s}$.
4. **Hyper-Graph Structural Integrity**: All hyper-edges must strictly link $\ge 2$ valid, existing hyper-nodes; dangling edges fail closed.
5. **Causal Monotonicity & CRDT Convergence**: Replicas applying the same set of concurrent updates converge to identical hyper-graph digests regardless of arrival order.
6. **Dual-SDK Parity**: 100% identical types, causal classifications, and receipt hashes across Python 3.12+ and TypeScript Node 22 native type-stripping.

## Consequences
- **Positive**: Enables multi-cluster swarms to synchronize complex n-ary relational state without pairwise edge bloat.
- **Positive**: Eliminates split-brain silent data corruption via rigorous vector clock causality and CRDT joins.
- **Positive**: Prevents cross-cluster forgery by enforcing issuer-bound committee verification (Invariant I1).
- **Negative**: Requires clusters to register foreign committee public key roots per epoch prior to accepting cross-cluster packets.
