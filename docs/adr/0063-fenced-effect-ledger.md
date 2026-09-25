# ADR 0063: Fenced Effect Ledger & Monotonic Idempotency Barrier

## Status
Accepted (Seam 52)

## Context
In distributed multi-agent systems and swarm architectures, agents execute external side-effects (e.g. database writes, remote tool execution, payment transactions, external API dispatches). In the presence of worker crashes, retries, network partitions, or leader election handovers, former leaders (stale leaders) or parallel workers can attempt duplicate executions of the exact same side-effect.

While Seam 51 (`SafetyAuthority`) governs overall resource budgets and execution permits with fencing epochs, executing external actions requires an explicit, durable **Fenced Effect Ledger**. Without this:
1. A worker holding an expired or stale lease can execute an irreversible external action after another worker has already been elected.
2. Network retries can execute side-effectful tools multiple times.
3. Graph state rollbacks cannot undo external effects that were already executed without an idempotency barrier.

## Decision
We implement a deep, linearizable **Fenced Effect Ledger (`EffectLedger`)** with compare-and-swap (CAS) transitions and monotonic fencing epoch validation:

1. **Idempotency & Effect Key Invariant**:
   Every side-effectual action is identified by a unique `effect_key` and caller-provided `idempotency_key`. Each `effect_key` commits at most once:
   $$\forall k \in \text{EffectKeys}, \quad |\{ e \in \text{Ledger} : e.\text{effect\_key} = k \land e.\text{status} = \text{COMMITTED} \}| \le 1$$

2. **Monotonic Fencing Epoch & Stale Leader Barrier**:
   Any claim or completion attempt submitted with an epoch older than the current authoritative epoch ($\text{epoch} < \text{current\_epoch}$) is rejected fail-closed with `STALE_EPOCH`:
   $$\text{claim}(k, \text{epoch}) \implies \text{epoch} \ge \text{current\_epoch}$$

3. **Compare-And-Swap (CAS) Claim Lifecycle**:
   - `CLAIMED`: The effect key is reserved by a specific worker under the active epoch with a lease timeout.
   - `AUTHORIZED`: The claim is validated against an active `SafetyPermit` hash from `SafetyAuthority`.
   - `COMMITTED`: The effect has completed successfully, saving the result payload and generating an immutable `EffectReceipt`.
   - `ABORTED`: The claim failed or was released, freeing the key for retry under a newer epoch if desired.

4. **Zero-Token Local Execution**:
   The entire state machine operates strictly in-memory with thread-safe atomics, canonical domain tagging (`sw:eff:v1`), and zero external LLM calls.

## Consequences
- Prevents split-brain workers from executing duplicate external tool actions.
- Guarantees exactly-once semantics for all critical tool interventions.
- Provides cryptographic, canonical receipts for auditability across both Python and TypeScript runtimes.
