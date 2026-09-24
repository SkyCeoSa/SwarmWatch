# ADR 0053: Bounded Model Checking & Multi-Agent Invariant Verification

- **Status**: Accepted
- **Date**: 2026-09-24
- **Context**: SwarmWatch Seam 42 (Bounded Model Checking & Multi-Agent Invariant Verification)

---

## 1. Context and Problem Statement
Multi-agent collaborative workflows often suffer from emergent conversational deadlocks, livelocks, uncontrolled mutual delegation cycles, or budget exhaustion when agents interact asynchronously. Runtime detection (like circuit breakers) stops loops *after* they begin, but cannot mathematically prove that a given multi-agent topology is guaranteed to be safe and deadlock-free before runtime execution or deployment.

SwarmWatch requires an in-process, deterministic **Bounded Model Checking (BMC)** engine capable of systematically exploring state transition spaces up to a bounded depth $k$, verifying safety and liveness invariants without external SAT/SMT solvers or LLM API calls.

## 2. Decision & Architectural Principles
We implement `BoundedModelChecker`, `AgentTransitionModel`, and `StateSpaceExplorer` as a deep module:

1. **State Transition Model (`AgentTransitionModel`)**:
   - Represents the discrete state of an interacting swarm:
     \[
     S = (\text{active\_agent}, \text{turn\_depth}, \text{flags}, \text{cumulative\_tokens}, \text{call\_stack})
     \]
   - Encapsulates valid transitions between agent roles, handoffs, tool calls, and final states.

2. **Invariants Verified**:
   - **`NO_DEADLOCK`**: Every non-terminal state must have at least one valid transition or explicitly transition to an approved `FINAL` state.
   - **`NO_UNBOUNDED_CYCLES`**: No circular delegation loop ($A \to B \to A$) can repeat indefinitely without consuming monotonic progress counters.
   - **`BOUNDED_BUDGET`**: Across all execution paths of depth $\le k$, cumulative token/turn usage must not exceed defined safety quotas.
   - **`MUTUAL_EXCLUSION`**: Incompatible agent roles (e.g. `executor` and `verifier`) cannot execute simultaneously within a shared critical section.

3. **Bounded Depth Search & Counterexample Synthesis**:
   - Systematically explores all reachable paths up to bound $k \in [1, 25]$ using depth-limited state hashing ($O(|V| + |E|)$).
   - If an invariant is breached, the model checker synthesizes an exact deterministic counterexample trace ($S_0 \xrightarrow{t_1} S_1 \xrightarrow{t_2} \dots \xrightarrow{t_n} S_{\text{violation}}$) enabling developers to inspect the exact path leading to failure.

4. **Zero-Token Cost & Sub-Millisecond Execution**:
   - 100% in-process deterministic graph traversal ($<1.0\text{ms}$ for hundreds of states). Zero external cloud solvers or LLM calls.

5. **Dual-SDK Parity & Impeccable Craft Floor**:
   - Python: `packages/sdk-python/swarmwatch/model_checker.py`
   - TypeScript: `packages/sdk-typescript/src/model_checker.ts` adhering strictly to Node 22 type-stripping rules (`node --experimental-strip-types`).
   - Web Console: `#modelCheckBadge` in `#fleetBar` of `dashboard.html` with 0 anti-patterns (`[]`).

## 3. Consequences
- **Positive**:
  - Mathematically proves safety and deadlock-freedom before swarm execution.
  - Produces actionable counterexample traces for debugging.
  - Zero performance overhead on live agent execution paths.
- **Negative**:
  - Bound $k$ must be finite to prevent state space combinatorial explosion.
