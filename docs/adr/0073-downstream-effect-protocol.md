# ADR 0073: Downstream Effect Protocol & 2PC Outcome-Unknown Reconciliation

## Status
Accepted

## Context
In distributed multi-agent systems and tool-execution architectures, agents trigger external real-world side effects (e.g., payment gateways, database writes, webhooks, cloud provisioning, third-party API mutations). 

Prior to Seam 62, Seam 52 established a fenced `EffectLedger` enforcing upstream compare-and-swap idempotency and monotonic epoch fences. However, as identified in the foundational architectural audit, no upstream ledger alone can guarantee true "exactly-once" side effects across an uncoordinated network boundary without explicit 2-Phase Commit (2PC) or downstream-enforced idempotency keys:
1. **The In-Flight Network Partition Gap**: If an agent worker dispatches an HTTP POST or RPC mutation to a downstream provider, and the network drops or times out before the HTTP response arrives, the upstream worker faces the classic Two Generals' Problem: did the downstream system execute the mutation, or was the request dropped before processing?
2. **The Danger of Blind Retries**: If the caller blindly retries upon timeout, it risks catastrophic duplicate execution (e.g., duplicate billing or duplicate resource creation) if the downstream provider did not enforce idempotency.
3. **The Danger of Silent Aborts**: If the caller assumes failure and aborts or reallocates state, the downstream effect may remain committed, resulting in desynchronized reality and uncontainable state divergence.
4. **False "Exactly-Once" Semantics**: Marketing or claiming "exactly-once execution" across external distributed boundaries without formal downstream verification violates physical distributed systems laws.

## Decision
We implement **Seam 62: Downstream Effect Protocol & 2PC Outcome-Unknown Reconciliation (`DownstreamEffectBoundary`)** across both Python 3.12+ and TypeScript Node 22 SDKs with zero external LLM token overhead:

1. **Explicit Five-State Effect Lifecycle Machine**:
   - `PREPARED`: Upstream effect is validated, assigned a cryptographic idempotency key `idempotency_key`, target URI, canonical payload digest, and monotonic fencing epoch $\varepsilon$.
   - `DISPATCHED`: The mutation request is transmitted across the downstream network boundary carrying the canonical idempotency header `X-Swarm-Idempotency-Key` and epoch fence.
   - `COMMITTED`: Explicit downstream acknowledgment receipt received and validated with downstream confirmation ID and result digest.
   - `OUTCOME_UNKNOWN`: Network timeout, connection reset, 5xx gateway error, or process restart occurs while state is `DISPATCHED` before receipt of downstream acknowledgment. The system explicitly fails closed into `OUTCOME_UNKNOWN`, refusing to report either success or failure.
   - `RECONCILED`: An active reconciliation probe (`reconcile()`) queries the downstream service status using the idempotency key, determining whether the mutation succeeded or never arrived, transitioning state to `COMMITTED` or `ABORTED`.
   - `ABORTED`: Explicit downstream rejection (e.g. 4xx client error, validation failure) or upstream cancellation prior to dispatch.

2. **Downstream Idempotency Contract & Fencing**:
   - Every dispatch requires an explicit downstream idempotency contract specifying whether the downstream endpoint supports atomic deduplication (`DOWNSTREAM_FENCED`) or is uncoordinated (`UNCOORDINATED`).
   - For `UNCOORDINATED` downstream services, retries upon timeout are strictly rejected fail-closed with `OUTCOME_UNKNOWN_BLOCKED`.
   - For `DOWNSTREAM_FENCED` services, retry carries the identical idempotency key and epoch fence.

3. **Active Reconciliation Protocol**:
   - The boundary provides `.reconcile(effect_id, query_fn)` which executes a read-only idempotent probe against the downstream target.
   - If `query_fn` confirms downstream commit: transitions to `COMMITTED` with synthesized `DownstreamReceipt` and `reconciled = True`.
   - If `query_fn` confirms non-existence: transitions to `ABORTED` (or safe for fresh dispatch).
   - If probe times out: remains `OUTCOME_UNKNOWN` with incremented retry count and logged backoff.

4. **Zero-Token Cost Guarantee**:
   - All state transitions, hashing, epoch validation, and outcome tracking execute locally in $<25\mu\text{s}$ with zero external LLM judge calls.

5. **Dual-SDK Parity**:
   - Exact mathematical symmetry between Python (`downstream_effect.py`) and TypeScript (`downstream_effect.ts`).

## Consequences
### Positive
- Eliminates false "exactly-once" claims in multi-agent tool execution.
- Prevents catastrophic duplicate external mutations during network partitions and timeouts.
- Provides a formal, auditable trail of in-flight downstream effects and automated reconciliation.

### Negative
- Applications calling uncoordinated external APIs must implement or register a reconciliation probe function for automated recovery from `OUTCOME_UNKNOWN` states.
