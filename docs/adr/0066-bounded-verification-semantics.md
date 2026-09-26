# ADR 0066: Bounded Verification Semantics & Explicit Counterexample Engine (Seam 55)

## Status
Accepted

## Context
In Seam 42 (ADR 0053), SwarmWatch implemented `BoundedModelChecker` to explore agent transition state spaces up to depth $k$. However, the Senior Code & Architecture Audit raised a critical scientific and evidentiary limitation:
1. **Misleading `VERIFIED` Claims**: When bounded model checking terminates at depth $k$ without finding a bug, returning a blanket verdict of `VERIFIED` misleads users into believing that the system is unconditionally deadlock-free or safe for all time ($k \to \infty$). In reality, finite exploration cannot prove unbounded liveness or safety unless the reachable state space is completely closed.
2. **Ambiguity Under Resource Limits**: If exploration is cut short because a state limit (`max_states`) or timeout is hit, returning `VERIFIED` is dangerous. It must explicitly return `INDETERMINATE`.
3. **Missing Explored Coverage & Assumptions**: Formal engineering standards demand that every verification output explicitly declare its model assumptions, exploration bound $k$, actual depth reached, and state space coverage metric.
4. **Reproducible Counterexample Traces**: When an invariant is violated, an explicit, step-by-step counterexample trace must be generated, and an independent validator (`verify_trace`) must be able to replay and verify the trace soundly without SAT solvers or LLM calls.

## Decision
We implement **Seam 55: Bounded Verification Semantics (`SafetyVerifier`)**:
1. **Truthful Verdict Nomenclature (`BoundedVerdict`)**:
   - `BOUNDED_SAFE`: The requested properties hold strictly for all states reachable within depth $k$, but the reachable state space is not closed beyond $k$.
   - `VIOLATED`: A counterexample state was discovered violating an invariant within depth $k$; accompanied by a deterministic minimal trace.
   - `INDETERMINATE`: Search was truncated by resource constraints (`max_states`, `timeout_ms`) before checking all states within bound $k$.
   - `UNBOUNDED_PROVEN`: The complete reachable state space was exhaustively explored (the exploration frontier became empty at depth $d \le k$), proving the property for all possible executions.
   - `VACUOUS`: The model has zero transitions or an empty initial configuration.
2. **State Space Coverage Metric**:
   - Report records `total_states_explored`, `frontier_states_remaining`, and `state_coverage_ratio`:
     $$\text{coverage} = \frac{|V_{\text{explored}}|}{|V_{\text{explored}}| + |V_{\text{frontier}}|}$$
3. **Independent Trace Verification**:
   - `SafetyVerifier.verify_trace(model, property, trace)`: An isolated, sub-millisecond validator that replays the counterexample trace step-by-step against model transitions to verify validity and violation soundness.
4. **Deep Module Interface**:
   - `SafetyVerifier.verify(model, properties, limits, model_id) -> VerificationReport`
   - `SafetyVerifier.verify_trace(model, property, trace) -> Tuple[bool, Optional[str]]`
   - `SafetyVerifier.status(tenant_id) -> Dict[str, Any]`
5. **Zero-Token & Dual-SDK Guarantee**:
   - 100% deterministic local in-memory BFS execution. Zero remote LLM API calls.
   - Full dual-SDK parity across Python (`packages/sdk-python/swarmwatch/safety_verifier.py`) and TypeScript (`packages/sdk-typescript/src/safety_verifier.ts`).

## Consequences
- **Positive**:
  - Eliminates false security guarantees by strictly distinguishing between `BOUNDED_SAFE` ($k$), `INDETERMINATE`, and `UNBOUNDED_PROVEN`.
  - Provides mathematical and audit credibility complying with formal method standards.
- **Negative**:
  - Requires explicit limits specification (`max_depth_k`, `max_states`, `timeout_ms`).
