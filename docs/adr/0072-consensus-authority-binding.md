# ADR 0072: Consensus Authority Binding & Linearizable Quorum Fencing

## Status
Accepted (Seam 61)

## Context
In Seam 51 (ADR 0062), SwarmWatch introduced the `SafetyAuthority` and `SafetyPermit` to issue epoch-fenced execution permits within a single node. While `SafetyAuthority` enforced local thread-safe linearizable permits, the external architecture audit (AUDIT REPORT.md) identified that in a distributed fleet, local epoch fencing without distributed quorum consensus creates a vulnerability: network partitions or leader isolation could allow a minority partition to continue issuing conflicting permits (split-brain condition).

To achieve true enterprise-grade distributed safety, execution permits must be bound to a replicated state machine (RSM) where every permit reservation, commitment, trip, and re-arm operation requires an explicit quorum of cluster nodes:
$$Q = \left\lfloor \frac{N}{2} \right\rfloor + 1$$

Under network partitions or minority isolation, any node lacking quorum consensus must immediately **fail closed**, rejecting execution permit requests with explicit `REJECTED` or `QUORUM_LOSS` status, preventing uncommitted or split-brain state transitions.

## Invariants & Formal Model
1. **Quorum Commitment Invariant**:
   Every permit operation ($\text{RESERVE}$, $\text{COMMIT}$, $\text{RELEASE}$, $\text{TRIP}$, $\text{REARM}$) is assigned a monotonic log index $i \in \mathbb{N}$ and epoch $e \in \mathbb{N}$. A transition is linearizable if and only if acknowledged by a majority quorum $Q \ge \lfloor N / 2 \rfloor + 1$.
2. **Minority Fail-Closed Invariant**:
   If reachable peer count $P < Q$, the node is in a minority partition. All mutation requests are rejected immediately without altering the local committed state.
3. **Linearizable Reads Invariant**:
   Linearizable read requests (`read_committed`) reflect only state up to the highest quorum-committed index. Dirty uncommitted entries are inaccessible.
4. **Deterministic RSM Transition State Machine**:
   $$\text{RESERVED} \longrightarrow \text{COMMITTED} \lor \text{RELEASED} \longrightarrow \text{TRIPPED} \lor \text{REARMED}$$
5. **Zero-Token Cost Guarantee**:
   All consensus replication, quorum counting, cryptographic digests, and state transitions execute entirely via local deterministic CPU operations, requiring zero external LLM tokens or cloud services.

## Deep-Module Interface Design
The deep module `AuthorityBinding` hides all distributed RPC, vote collecting, heartbeats, and replicated log mechanics behind a clean, focused API:

```python
class AuthorityBinding:
    def reserve(self, tenant_id: str, action: str, cost: float, tokens: int, timeout_ms: float) -> ConsensusPermitResult: ...
    def commit(self, permit_id: str, actual_cost: float, actual_tokens: int) -> ConsensusPermitResult: ...
    def release(self, permit_id: str, reason: str) -> ConsensusPermitResult: ...
    def trip(self, tenant_id: str, reason: str) -> ConsensusTripResult: ...
    def rearm(self, tenant_id: str, new_epoch: Optional[int] = None) -> ConsensusRearmResult: ...
    def read_committed(self, tenant_id: str) -> CommittedTenantState: ...
```

## Consequences
- Single-process and multi-node deployments share the exact same authority binding semantics ($N=1 \implies Q=1$).
- Full parity between Python 3.12+ and Node 22 (TypeScript native type stripping).
- Partition and split-brain states cannot cause unauthorized execution leases or unmetered agent turns.
