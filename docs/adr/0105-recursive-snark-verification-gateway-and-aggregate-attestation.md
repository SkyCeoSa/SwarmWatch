# ADR 0105: Zero-Knowledge Aggregate Attestation & Recursive SNARK Verification Gateway

## Status
Accepted

## Context
As agent swarms scale across distributed clusters, validating individual state transition receipts, causal barrier certificates, and epistemic claims generates substantial verification overhead. Transmitting $M$ separate zero-knowledge proofs over WAN mesh networks induces latency, bandwidth exhaustion, and verification queue congestion.

To achieve constant-time $O(1)$ global state verification across multi-cluster swarms, SwarmWatch requires a recursive proof folding gateway:
1. **Succinct Proof Aggregation**: Folding $M$ independent proof witnesses $\pi_1, \ldots, \pi_M$ into a single succinct recursive proof $\Pi^*$ whose verification time is strictly bounded and independent of batch size $M$.
2. **Cryptographic Soundness & Non-Malleability**: Tampering with any single constituent proof witness must invalidate the recursive fold fail-closed, preventing tainted state transitions from being aggregate-certified.
3. **Deterministic Tree Folding**: Binary hierarchical proof folding allowing verifiable membership proofs and bounded logarithmic tree depth $h \le \lceil \log_2 M \rceil$.
4. **Safety Circuit Bus Coupling**: Detection of polynomial commitment mismatches or recursive forgery immediately triggers the fail-closed safety circuit bus.

## Decision
We implement Seam 94 (`RecursiveSnarkGateway`) across Python (`swarmwatch.recursive_snark`) and TypeScript (`@swarmwatch/sdk/src/recursive_snark.ts`) with 100% dual-SDK parity, sub-250 microsecond execution latency SLO, and zero external LLM API costs ($0.00 USD).

### 1. Mathematical Invariants
- **Invariant RZK1 (Succinct Compression Invariant)**:
  For an aggregate batch of $M$ valid proof witnesses, the recursive proof $\Pi^*$ maintains constant size $O(1)$ byte overhead and verifies in bounded constant time.
- **Invariant RZK2 (Recursive Soundness & Non-Malleability Invariant)**:
  If any constituent proof witness $\pi_k$ is tainted, forged, or inconsistent with its committed state transition digest, the recursive aggregator rejects folding fail-closed (`RecursiveProofSoundnessBreachError`).
- **Invariant RZK3 (Deterministic Tree Folding Invariant)**:
  Aggregate proofs fold deterministically along a binary Merkle folding tree of depth $h = \lceil \log_2 M \rceil$, yielding deterministic root hash derivation.
- **Invariant RZK4 (Fail-Closed Safety Circuit Latching)**:
  If recursive verification detects an invalid aggregate proof signature or an unresolvable commitment mismatch, the engine raises `RecursiveVerificationFailedError` and trips the `SafetyCircuitBus` (`RECURSIVE_SNARK_VERIFICATION_FAILURE`).

### 2. Deep Module Architecture & Types
- **Data Models**:
  - `ConstituentProof`: `proof_id: str`, `batch_epoch: int`, `state_digest: str`, `proof_witness: str`, `is_valid: bool`.
  - `AggregateSnarkReceipt`: `aggregate_id: str`, `constituent_count: int`, `aggregate_root: str`, `recursive_proof_bytes: str`, `verified: bool`, `created_at_hlc: str`.
  - `FoldingStep`: `step_index: int`, `left_hash: str`, `right_hash: str`, `folded_hash: str`.
- **Gateway**:
  - `RecursiveSnarkGateway`: Manages proof admission, binary tree folding, recursive proof synthesis, and constant-time verification.

## Consequences
- **Positive**: $O(1)$ constant-time verification of massive multi-agent execution traces.
- **Positive**: Drastic reduction in network bandwidth across multi-cloud and cross-border federation.
- **Positive**: Zero-token cost guarantee ($0.00 USD) and sub-250µs execution latency SLO.
- **Negative**: Requires strict canonical serialization of constituent proof witnesses prior to folding.
