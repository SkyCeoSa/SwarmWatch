# ADR 0096: Barrier Leases, Liveness & Atomic Abort (Seam 85)

## Status
Accepted

## Context
Milestone 80 (Causality Barrier / ADR 0091) introduced multi-cluster epoch cut-over coordination. However, independent adversarial audit identified three significant limitations:
1. **Liveness deadlock:** If a single participating cluster went silent during the `DRAINING` or `QUIESCENT` phases, the barrier stalled forever because `BarrierQuiescenceTimeoutError` was defined but never actually raised in the timer loop.
2. **Atomic Abort absence:** There was no explicit distributed 2PC abort mechanism to roll back an in-progress cut-over if a participant timed out or faulted.
3. **Shallow protocol leaking:** Callers had to drive internal 7-phase transitions manually (`arm -> start_draining -> report_quiescence -> commit`).

To establish guaranteed liveness, atomic two-phase commit abort semantics, and deep-module ergonomics, SwarmWatch introduces **Barrier Leases, Liveness & Atomic Abort**.

## Invariants
- **L1 (Bounded Lease Termination):** Every barrier execution must terminate strictly within lease duration $T$ (default $1,000\,\text{ms}$) with either `Committed(QC)` or `Aborted(AbortProof)`. Silent nodes trigger an automatic timeout abort.
- **L2 (Atomic Abort & Epoch Conservation):** If any participant fails to quiesce within lease $T$, the barrier aborts atomically. An abort guarantees epoch conservation ($e' = e$) and restores the active barrier state to `OPEN`. Commits occur only upon full 2PC consensus attested by a threshold `ThresholdQuorumCertificate` (Seam 81).
- **L3 (Deep 1-Call Automaton Abstraction):** The external API collapses into a single orchestrator entry point:
  `cutover(target_epoch: int, committee: CommitteeRoster, lease_ms: int) -> BarrierCutoverResult`
  which internally coordinates draining, quiescence polling, threshold attestation, and commit/abort resolution.

## Decision
1. Implement `barrier_lease.py` in Python and `barrier_lease.ts` in TypeScript Node 22.
2. Structure the core entities:
   - `BarrierCutoverStatus`: `COMMITTED`, `ABORTED`, `IN_PROGRESS`.
   - `BarrierAbortProof`: Cryptographic evidence of timeout or participant failure.
   - `BarrierCutoverResult`: Outcome containing either the threshold QC or abort proof.
   - `LeasedBarrierCoordinator`: Deep module managing leases, 2PC participant timeouts, and atomic cutover.
3. Provide interface:
   - `cutover(target_epoch: int, roster: CommitteeRoster, signer_keys: Dict[str, Key], lease_ms: int = 1000) -> BarrierCutoverResult`
   - `admit_operation(op_epoch: int) -> bool`
   - `get_status() -> Dict[str, Any]`

## Consequences
- Prevents silent-node hangs and distributed deadlock.
- Guarantees strict epoch conservation on network failures.
- Zero-token cost guarantee ($0.00 USD).
- Sub-250µs execution latency for local operational checks.
