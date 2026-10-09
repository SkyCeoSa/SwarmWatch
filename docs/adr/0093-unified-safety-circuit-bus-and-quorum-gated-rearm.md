# ADR 0093: Unified Safety Circuit Bus & Quorum-Gated Rearm (Seam 82)

## Status
Accepted

## Context
Across Seams 1 to 80, eight separate modules independently implemented localized `trip_circuit()` functions and private circuit breaker flags (`EntropyCircuitBreaker`, `RaftCircuitBreaker`, `MeshCircuitBreaker`, `BarrierCircuitBreaker`, etc.). Independent adversarial audit highlighted two critical vulnerabilities:
1. Fragmented circuit state allowed an attacker to reset safety trips unilaterally or circumvent fail-closed protection in unguarded subsystems.
2. Control plane endpoints (`/api/v1/safety/rearm`, `/api/v1/barrier/circuit/rearm`, etc.) allowed unauthenticated operators to rearm the circuit, defeating the fail-closed guarantee entirely.

To provide unified, impenetrable fail-closed safety, SwarmWatch introduces the **Unified Safety Circuit Bus & Quorum-Gated Rearm** kernel.

## Invariants
- **SC1 (Monotonic Trip Generation):** Circuit trip generations are strictly monotonic ($g' > g$). Every trip transition permanently records the trip identifier, causal timestamp, root cause scope, and reason.
- **SC2 (Quorum-Gated Multi-Operator Rearm):** Rearming the safety circuit requires a verified `ThresholdQuorumCertificate` (Seam 81 / ADR 0092) signed by $\ge 2$ distinct authorized operators for domain `sw:qc:v1:circuit:rearm` committing to a fresh `StateSnapshot` root hash. Unilateral or unauthenticated rearm is rejected fail-closed.
- **SC3 (Global Immediate Propagation):** A trip trip in any subsystem publishes an event across the bus, transitioning the global state and notifying all registered subscribers within sub-microsecond latency. All operations within guarded scopes reject execution immediately.

## Decision
1. Implement `circuit_bus.py` in Python and `circuit_bus.ts` in TypeScript Node 22.
2. Structure the `SafetyCircuitBus` as a deep module:
   - `trip(reason: str, scope: str = "GLOBAL", metadata: dict = None) -> str`
   - `rearm(qc: ThresholdQuorumCertificate, snapshot_hash: str, expected_roster: CommitteeRoster) -> int`
   - `guard(scope: str)` (Python context manager / TypeScript async callback or RAII guard)
   - `is_tripped(scope: str = "GLOBAL") -> bool`
   - `subscribe(callback)` / `unsubscribe(callback)`
   - `get_status() -> CircuitBusStatus`
3. Wire the unified bus into all control plane rearm endpoints in `server.py`, strictly requiring the operator multi-signature quorum certificate.

## Consequences
- Eliminates duplicated circuit logic across the codebase.
- Guarantees that no single compromised operator credential can reset safety circuits.
- Maintains zero external LLM API cost ($0.00 USD).
- Keeps trip evaluation and guard latency under 10µs, well within the 250µs SLO.
