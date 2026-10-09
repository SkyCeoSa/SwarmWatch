# ADR 0104: Decentralized Cryptographic Proof Marketplace & Verifier Node Staking Pool

## Status
Accepted

## Context
In large-scale multi-region agent observability and distributed verification across SwarmWatch clusters, validating high volumes of cryptographic execution rollups, epistemic dispute assertions, and causal state transition proofs requires elastic verification throughput. Self-hosted clusters and cloud marketplace participants need an autonomous, incentive-aligned mechanism to distribute verification workloads across independent verifier nodes without trusting any single evaluator.

To prevent sybil attacks, lazy verification, and malicious attestation of corrupted proofs, the verification subsystem requires an economic bonding and staking model:
1. **Collateral Staking & Slashing**: Nodes must stake cryptographic collateral before bidding on proof verification jobs. Submitting fraudulent or equivocating verification receipts results in deterministic slashing of the bonded stake.
2. **Double-Auction Matching**: Proof bounties submitted by agent swarms must be matched against verifier bids deterministically under bounded deadline windows and market clearing rules.
3. **Cryptographic Multi-Signature Quorum Verification**: Proof validity is finalized if and only if a Byzantine-fault-tolerant quorum $K \ge \lceil(N + f + 1)/2\rceil$ of staked verifiers submit matching, cryptographically signed receipts.
4. **Safety Circuit Bus Coupling**: Any solvency collapse in the staking pool or detection of collusive malicious receipts immediately latches the fail-closed safety circuit bus.

## Decision
We implement Seam 93 (`ProofMarketplaceCoordinator`) across Python (`swarmwatch.proof_marketplace`) and TypeScript (`@swarmwatch/sdk/src/proof_marketplace.ts`) with 100% dual-SDK parity, sub-250 microsecond execution latency SLO, and zero external LLM API costs ($0.00 USD).

### 1. Mathematical Invariants
- **Invariant CPM1 (Staking & Collateral Slashing Invariant)**:
  Each verifier node $V_i$ must maintain bonded stake $S(V_i) \ge S_{\min}$. If an equivocating or forged verification receipt $R(V_i)$ is attested, the bonded stake is slashed by penalty fraction $\gamma \cdot S(V_i)$ where $\gamma \in (0, 1]$, and the node is permanently barred (`is_slashed = True`, `is_active = False`).
- **Invariant CPM2 (Double-Auction Proof Market Clearing Invariant)**:
  Proof verification requests $P_j = \langle \text{proof\_id}, \text{complexity\_weight}, \text{max\_bounty}, \text{deadline\_epoch} \rangle$ are matched against verifier bids $B_i = \langle V_i, \text{fee\_bid}, \text{staked\_collateral} \rangle$. Bids clear if and only if $\text{fee\_bid} \le \text{max\_bounty}$ and verifier stake $S(V_i) \ge \omega \cdot \text{complexity\_weight}$.
- **Invariant CPM3 (K-of-N Cryptographic Proof Verification Quorum)**:
  A proof verification task is finalized if and only if a quorum of $K \ge \lceil(N + f + 1)/2\rceil$ staked verifiers submit matching cryptographic verification receipts $R_i = \text{Sign}_{V_i}(\text{proof\_id} \parallel \text{digest} \parallel \text{verdict})$ within the deadline window.
- **Invariant CPM4 (Fail-Closed Safety Circuit Latching)**:
  If the aggregate staked solvency of the active verifier pool falls below safety threshold $S_{\text{threshold}}$, or if colluding malicious verifier receipts exceed fault budget $f = \lfloor(N-1)/3\rfloor$, the coordinator raises `MarketplaceSolvencyBreachError` and latches the `SafetyCircuitBus` in fail-closed state (`PROOF_MARKETPLACE_SOLVENCY_BREACH`).

### 2. Deep Module Architecture & Types
- **Data Models**:
  - `VerifierNode`: Unique ID, Ed25519 public key, bonded stake, reputation score, activity and slashing flags.
  - `ProofBounty`: Bounty ID, proof type, proof digest, max bounty, required quorum, status, deadline epoch.
  - `VerifierBid`: Bid ID, bounty ID, verifier ID, fee bid, staked collateral, signature.
  - `ProofVerificationReceipt`: Receipt ID, bounty ID, verifier ID, verdict, proof hash, signature, timestamp.
  - `SettlementCertificate`: Settlement ID, bounty ID, cleared fee, payouts, slashed verifiers, finalized verdict.
- **Coordinator**:
  - `ProofMarketplaceCoordinator`: Manages node registration, stake bonding/unbonding, bounty creation, bid matching, receipt aggregation, quorum settlement, and automated slashing.

## Consequences
- **Positive**: Elastic, decentralized proof verification capacity without central trust bottlenecks.
- **Positive**: Strict economic guarantees preventing fraudulent attestation through deterministic stake slashing.
- **Positive**: Sub-250µs execution latency with zero external LLM API cost ($0.00 USD).
- **Negative**: Requires verifier nodes to maintain active cryptographic keypairs and collateral balances.
