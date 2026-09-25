# ADR 0062: Unified Safety Authority & Linearizable Execution Permit Engine

## Status
Accepted

## Context
Following the comprehensive architectural audit (March 2026 / Seam 50-60 Roadmap), SwarmWatch identified a critical operational risk: **No Single Authoritative Safety Decision**.

Prior to this ADR, safety decisions and budget checks were fragmented across multiple subsystems:
1. Distributed Redis Fleet circuit quotas (RESP increments),
2. Raft consensus state machine,
3. Decentralized epidemic Gossip mesh updates,
4. Local in-memory circuit breakers (`EntropyCircuitBreaker`),
5. Dynamic workload load balancer,
6. Autonomous topology auto-healing.

During network partitions or asynchronous Pub/Sub message delays, these decoupled subsystems could diverge, creating split-brain windows where workers could continue initiating high-cost agent execution turns after the global swarm budget or entropy threshold had been tripped.

## Decision
We introduce **Seam 51: Unified Safety Authority (`SafetyAuthority`)**, enforcing a single linearizable execution permit state machine across all swarms and tenants.

### 1. The Core Invariant
> **"No agent turn, tool invocation, or state modification starts without an unexpired, tenant-scoped execution permit (`SafetyPermit`) issued by the authoritative epoch."**

### 2. Linearizable State Machine Lifecycle
Every execution follows an atomic three-stage lifecycle:
$$\text{reserve} \longrightarrow \text{commit / release} \longrightarrow \text{trip}$$

1. **Reserve (`reserve`)**:
   - Caller requests a permit specifying `tenant_id`, `action`, `cost_estimate`, `token_budget`, and `timeout_ms`.
   - The authority verifies tenant quotas, authority state, and fencing epoch.
   - Atomically holds the reservation and returns a cryptographically signed `SafetyPermit`.
   - Status is explicitly `ACCEPTED`, `REJECTED`, or `INDETERMINATE`.

2. **Commit (`commit`)**:
   - Upon completing the turn/action, worker presents `permit_id` with `actual_cost` and `actual_tokens`.
   - Unused reserved budget is returned to the pool; permanent expenditures are debited.

3. **Release (`release`)**:
   - If execution fails or aborts before invocation, entire reservation is released back to the tenant pool.

4. **Trip (`trip`)**:
   - When any invariant (entropy, turns, tokens, budget, policy) trips, the authority increments the monotonic `epoch` counter and sets status to `TRIPPED`.
   - All previously issued permits for $\le \text{current\_epoch}$ are immediately invalidated and revoked.

### 3. Canonical Domain Separation
Permits are canonically serialized and hashed using `CanonicalCodec` with domain tag:
$$\text{permit\_hash} = \text{SHA256}(\text{"sw:pmt:v1"} \parallel \text{0x00} \parallel C_v(\text{permit\_data}))$$

### 4. Fail-Closed Boundary
If the authority is unreachable, partitioned, or experiencing lease expiration, callers MUST fail-closed (`REJECTED` or `INDETERMINATE`), halting outbound agent actions until quorum is re-established.

## Consequences
- **Positive**: Eliminates split-brain execution loops and duplicate-effect execution windows across clusters.
- **Positive**: Strict bounded authority before autonomous agent actions execute.
- **Positive**: Zero external token consumption (100% deterministic local/cluster arithmetic).
- **Positive**: Dual-SDK parity across Python and TypeScript runtimes with identical permit hashing.
