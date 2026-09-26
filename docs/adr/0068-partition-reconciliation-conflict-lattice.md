# ADR 0068: Partition Reconciliation & Conflict Lattice Engine (Seam 57)

## Status
Accepted

## Context
In distributed multi-agent systems, network partitions, transient split-brains, and worker isolations inevitably occur. When partitioned clusters or gossip mesh replicas heal and rejoin, SwarmWatch must resolve divergent states without compromising critical safety guarantees:
1. **Silent Trip Clearance Hazard**: If Partition A tripped a circuit breaker due to runaway entropy or honeypot trigger while Partition B remained nominal, naively adopting majority vote or latest timestamp could silently clear the safety trip, resuming dangerous agent execution.
2. **Loss of Provenance on Concurrent Spans**: When two agents execute concurrently in partitioned network segments, standard Last-Write-Wins (LWW) silently discards telemetry from one branch, destroying causal traceability and creating forensic blindspots.
3. **Authority vs. Gossip Conflation**: Safety authority (fencing epochs, linearizable execution permits) must remain strictly decoupled from gossip propagation to prevent gossip anti-entropy from overriding authoritative epochs or granting invalid permits.

## Decision
We implement **Seam 57: Partition Reconciliation (`MeshReconciler`)**:
1. **Safety Trip Monotonicity Invariant**:
   - Circuit breaker trips and security interventions are strictly monotonic. If ANY partitioned node or replica reports a `TRIPPED` state for a swarm or tenant, the reconciled state MUST be `TRIPPED`. A safety trip can never be cleared by a rejoining nominal node.
2. **Explicit Conflict Lattice (`ConflictLattice`)**:
   - Uses vector-clock partial order to identify causal dominance ($V_A \le V_B$ or $V_B \le V_A$).
   - For concurrent spans ($V_A \parallel V_B$), the engine resolves via a deterministic conflict lattice that preserves provenance metadata (`PROVENANCE_RETAINED`) rather than silently dropping either span.
3. **Authoritative Epoch Fencing**:
   - Reconciled authority epoch is computed monotonically: $E_{reconciled} = \max(E_{local}, E_{remote}) + (\Delta \text{ if conflict})$.
   - Stale permits from pre-partition epochs are invalidated fail-closed.
4. **Deep Module Interface**:
   - `MeshReconciler.diff(local_state, remote_state) -> ReconciliationPlan`: Identifies diverging keys, missing sequences, and conflicting vector clocks.
   - `MeshReconciler.mergeEvidence(local_events, remote_events) -> MergeResult`: Merges telemetry streams under causal order, deduplicating identical hashes and retaining provenance for concurrent events.
   - `MeshReconciler.reconcileAuthority(local_auth, remote_auth) -> AuthorityReconciliationResult`: Enforces trip monotonicity and authoritative epoch convergence.
   - `MeshReconciler.status(tenant_id) -> Dict[str, Any]`
   - `MeshReconciler.reset() -> None`
5. **Zero-Token & Dual-SDK Guarantee**:
   - 100% deterministic local in-memory execution ($<25\mu\text{s}$). Zero external LLM calls.
   - Full dual-SDK parity across Python (`packages/sdk-python/swarmwatch/mesh_reconciler.py`) and TypeScript (`packages/sdk-typescript/src/mesh_reconciler.ts`).

## Consequences
- **Positive**:
  - Guarantees safety circuit trips never disappear upon partition healing.
  - Eliminates silent telemetry loss during network partitions through explicit provenance retention.
  - Strictly decouples linearizable safety authority from eventually consistent gossip.
- **Negative**:
  - Reconciling concurrent authority states increments the fencing epoch, requiring active agents to refresh permits.
