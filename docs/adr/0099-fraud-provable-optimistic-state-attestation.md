# ADR 0099: Fraud-Provable Optimistic State Attestation (Seam 88)

## Status
Accepted

## Context
Milestone 75 (Zero-Knowledge State Transition Rollup / ADR 0086) implemented state transition batching. However, independent adversarial audit identified that:
1. The module was architecturally an optimistic rollup engine using Fiat-Shamir polynomial commitments, rather than a true zero-knowledge SNARK proof.
2. The prover public key verification was optional, permitting attackers to submit self-signed attestations without pinning.
3. There was no explicit challenge window or interactive fraud verification mechanism.

To establish honest nomenclature, cryptographic soundness, and verifiable fraud detection, SwarmWatch introduces **Fraud-Provable Optimistic State Attestation**.

## Invariants
- **F1 (Deterministic Step Fraud Proof):** Any invalid state transition in an optimistic batch can be challenged and proven fraudulent within challenge window $W$ (default $5,000\,\text{ms}$) via single-step deterministic re-execution.
- **F2 (Timelocked Finalization & Pinned Prover Pinning):** An attestation cannot be finalized into an immutable receipt until challenge window $W$ elapses without valid challenge. Attestations with unknown or mismatched prover public keys are rejected fail-closed.
- **F3 (Pluggable Proof Verification Slot):** Architecture encapsulates attestation verification behind a pluggable `ProofVerifier` interface, enabling future real arithmetic circuits without modifying caller contracts.

## Decision
1. Implement `optimistic_attestation.py` in Python and `optimistic_attestation.ts` in TypeScript Node 22.
2. Structure the core entities:
   - `TransitionStep`: Sequence index, pre-state hash, action payload, post-state hash.
   - `OptimisticAttestation`: Prover Ed25519 signature over batch root, challenge deadline.
   - `FraudProof`: Proof showing discrepancy in step re-execution.
   - `FinalizedStateReceipt`: Cryptographically sealed receipt after challenge window.
   - `OptimisticAttestationEngine`: Deep module governing attestations, challenges, and finalization.
3. Provide interface:
   - `attest(steps: List[TransitionStep], prover_id: str, prover_key: Ed25519PrivateKey, challenge_window_ms: int = 5000) -> OptimisticAttestation`
   - `challenge(att: OptimisticAttestation, step_index: int, claim_pre: str, claim_post: str) -> Optional[FraudProof]`
   - `finalize(att: OptimisticAttestation, now_ns: Optional[int] = None) -> FinalizedStateReceipt`

## Consequences
- Honest domain vocabulary: eliminates misleading "ZK" claims while retaining strong optimistic security.
- Eliminates forgeable proof risks by enforcing pinned prover keys.
- Preserves zero external LLM API cost ($0.00 USD).
- Sub-250µs execution latency for local attestations and challenges.
