# ADR 0098: BFT View-Change & Accountable Primary Rotation (Seam 87)

## Status
Accepted

## Context
Seam 74 (Byzantine Consensus Oracle / ADR 0085) implemented PBFT pre-prepare, prepare, and commit phases. However, the independent adversarial audit identified that:
1. The oracle operated as an in-process vote aggregator without a decentralized view-change protocol, leaving the primary susceptible to liveness lockup if faulty or partitioned.
2. Slashing faulty nodes reshuffled primaries mid-view because primaries were selected dynamically from the active list.

To provide decentralized consensus liveness and accountable slashing, SwarmWatch implements **BFT View-Change & Accountable Primary Rotation**.

## Invariants
- **V1 (Prepared-Proof View Safety):** No two conflicting decisions can be committed for the same sequence number across view changes. A new view $v+1$ is valid if and only if it carries $\ge 2f+1$ valid `ViewChangeVote` messages containing the latest prepared certificate proofs.
- **V2 (Third-Party Accountable Slashing):** Equivocation produces a self-contained `SlashingProof` containing two distinct signatures over conflicting digests for identical $(view, sequence)$. Any node can verify this proof without trust and apply slashing.
- **V3 (Deterministic Primary Rotation):** The primary for view $v$ is strictly assigned as $\text{roster.sorted\_members}[v \pmod N]$. Primary rotation occurs strictly upon view change, never arbitrarily during a view.

## Decision
1. Implement `bft_view_change.py` in Python and `bft_view_change.ts` in TypeScript Node 22.
2. Structure the core entities:
   - `ViewChangeVote`: Signed attestation proposing transition to view $v+1$.
   - `NewViewCertificate`: Multi-party collection of $\ge 2f+1$ view change votes.
   - `SlashingProof`: Proof of dual-signing equivocation.
   - `BftViewCoordinator`: Deep module managing view advancement and primary health.
3. Provide interface:
   - `suspect_primary(current_view: int, reason: str, signer_id: str, signer_key: Ed25519PrivateKey) -> ViewChangeVote`
   - `install_view(votes: List[ViewChangeVote], roster: CommitteeRoster) -> NewViewCertificate`
   - `prove_equivocation(sig1: Dict[str, Any], sig2: Dict[str, Any], roster: CommitteeRoster) -> SlashingProof`
   - `get_primary(view: int, roster: CommitteeRoster) -> str`

## Consequences
- Guarantees liveness under Byzantine or offline primaries.
- Eliminates trusted aggregator assumptions.
- Integrates seamlessly with Epistemic Arbitration (Seam 76).
- Zero external LLM cost ($0.00 USD).
- Sub-250µs execution latency.
