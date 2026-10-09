# ADR 0091: Autonomous Cross-Cluster Causality Barrier & Multi-Swarm Epoch Cut-Over Synchronization

## Status
Accepted

## Context & Problem Statement
In distributed multi-cluster agent architectures (established across Seams 74 through 79), autonomous swarms continuously execute state mutations, hyper-graph partition evolutions, and Byzantine consensus rounds across independent clusters ($\mathcal{C}_1, \dots, \mathcal{C}_k$).

However, when critical cluster-wide state transitions occur—such as:
1. **Periodic Committee Key Rotation & Epoch Advancement** ($e \to e + 1$): Rotating validator public keys and epoch committee roots $\mathcal{R}_e$.
2. **Global Schema & Storage Architecture Migrations** (ADR 0053/0054): Applying structural updates across heterogeneous clusters without data corruption.
3. **Coordinated Multi-Swarm Disaster Recovery & Consistent Checkpoint Freezes** (ADR 0075/0088): Establishing an immutable causal baseline across all clusters simultaneously.

Standard asynchronous CRDT join semilattices and point-to-point vector clock synchronization allow concurrent operations to interleave indefinitely. Without a **Causality Barrier**, in-flight operations with dependencies spanning old and new epochs cause:
- **Split-Epoch State Corruption**: An agent in Cluster A executes in Epoch $e+1$ while acting on uncommitted state from Cluster B in Epoch $e$, generating invalid Proof-of-Execution certificates bound to obsolete committee roots.
- **Phantom In-Flight Mutations**: Messages delayed in the network arrive after epoch cut-over, applying stale mutations that regress causality vectors.
- **Distributed Deadlocks**: Naive 2-phase locking across autonomous swarms risks systemic deadlocks if an agent cluster stalls during synchronization.

## Decision
We implement **Seam 80: Autonomous Cross-Cluster Causality Barrier & Multi-Swarm Epoch Cut-Over Synchronization** (`CrossClusterCausalityBarrier`), providing:

1. **Deterministic $k$-Phase Barrier State Automaton (`BarrierState`)**:
   - `IDLE`: Regular multi-cluster operations under active epoch $e$.
   - `ARMING_BARRIER`: Coordinator issues a barrier proposal with target cut-off vector clock $\vec{V}_{\text{barrier}}$ and target epoch $e_{\text{target}} = e + 1$.
   - `DRAINING_INFLIGHT`: Participating clusters acknowledge the barrier, pause admission of new ingress transactions, and drain all in-flight operations causally precedent to $\vec{V}_{\text{barrier}}$.
   - `BARRIER_SYNCHRONIZED`: All participating clusters achieve verified causal quiescence ($\forall i \in \mathcal{C}, V_i \succeq \vec{V}_{\text{barrier}}$ with zero pending in-flight operations).
   - `COMMITTING_CUTOVER`: Atomic epoch advancement ($e \to e + 1$), committee root rotation ($\mathcal{R}_{e+1}$), and cryptographic threshold attestation.
   - `COMPLETED_EPOCH_ADVANCED`: Barrier lifted; post-barrier operations admitted exclusively under epoch $e+1$.
   - `CIRCUIT_TRIPPED`: Immediate fail-closed lockout upon invariant violation or timeout.

2. **Generalized Vector Barrier Quiescence Equation**:
   For a committee of participating clusters $\mathcal{C} = \{C_1, \dots, C_n\}$ with local vector clocks $V_i$:
   $$\vec{V}_{\text{barrier}}[k] = \max_{i \in \mathcal{C}} V_i[k]$$
   Causal quiescence is mathematically satisfied if and only if:
   $$\forall i \in \mathcal{C}, \quad V_i \succeq \vec{V}_{\text{barrier}} \quad \land \quad \text{InFlightCount}(C_i) = 0$$

3. **Three Strict Operational Invariants**:
   - **Invariant B1 (Barrier Monotonicity & Causal Quiescence)**:
     No operation with causal timestamp $V < \vec{V}_{\text{barrier}}$ may be admitted after the barrier enters `BARRIER_SYNCHRONIZED`. Any delayed message arriving with an outdated timestamp post-cutover is rejected with `StaleCausalityBarrierError`.
   - **Invariant B2 (Atomic Multi-Cluster Epoch Cut-Over)**:
     Epoch advancement $e \to e + 1$ is atomic and irreversible across all participating clusters. The new epoch root $\mathcal{R}_{e+1}$ must be signed by Byzantine quorum $Q \ge 2f + 1$. Zero partial commit is permitted across heterogeneous clusters.
   - **Invariant B3 (Cryptographic Cut-Over Certificate & Foreign Attestation)**:
     The cut-over process mints a tamper-evident `EpochCutOverCertificate` and `BarrierReceipt` bound to canonical domain-tagged hash (`sw:barrier:rcpt:v1:<sha256>`). Foreign clusters verify this attestation before admitting any post-barrier state transition.

4. **Fail-Closed Safety Circuit Coupling**:
   Any causal regression, sequence gap during draining, Byzantine equivocation, or unverified foreign attestation immediately trips the safety circuit with `BarrierCircuitTrippedError`, halting further state mutations until administrative rearm.

5. **Zero-Token Cost Guarantee**:
   All barrier handshakes, vector clock evaluations, and cryptographic threshold attestations execute 100% locally in-process with zero external LLM API token expenditure ($0.00 USD).

6. **Sub-250µs Execution Latency SLO**:
   In-memory barrier evaluations, quiescence verifications, and atomic epoch transitions execute in under 250µs.

## Consequences

### Positive
- Guarantees clean, atomic epoch cut-overs across distributed heterogeneous swarms without data races or orphaned in-flight mutations.
- Eliminates split-epoch cryptographic verification failures and obsolete committee root references.
- Provides mathematical proof of causal quiescence prior to committing global system transitions.
- 100% dual-SDK parity across Python and TypeScript (Node 22 native type-stripping).

### Negative / Trade-offs
- Ingress transactions are briefly buffered or queued while the barrier is in `DRAINING_INFLIGHT` and `COMMITTING_CUTOVER` phases.
