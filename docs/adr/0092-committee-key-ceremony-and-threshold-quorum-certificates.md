# ADR 0092: Committee Key Ceremony & Threshold Quorum Certificates (Seam 81)

## Status
Accepted

## Context
Milestone 70 to 80 introduced distributed cryptographic coordination primitives across agent clusters (Byzantine Consensus, State Rollups, Cross-Cluster Sync, and Causality Barriers). However, independent adversarial audit identified that certain certificate verification paths previously accepted unkeyed SHA-256 digests or attacker-supplied quorum counts, permitting forgery of cutover receipts and cross-cluster Proofs-of-Execution.

To establish mathematically unbreakable cryptographic trust boundaries across all distributed seams, SwarmWatch requires an explicit **Committee Key Ceremony and Threshold Quorum Certificate (QC)** kernel.

## Invariants
- **QC1 (Threshold Quorum Completeness):** A `QuorumCertificate` is valid if and only if it carries $\ge \lceil(N+f+1)/2\rceil$ distinct, valid Ed25519 signatures over $H(\text{domain} \parallel \text{epoch} \parallel \text{digest})$ from public keys belonging to the verified `CommitteeRoot(epoch) = MerkleRoot(\text{sorted}(PK_1, \dots, PK_N))$.
- **QC2 (Certified Epoch Rotation Chain):** A committee transition from epoch $e$ to epoch $e+1$ must be certified by a valid $QC(e)$ committing to the next `CommitteeRoot(e+1)`. Uncertified key rotations are rejected fail-closed.
- **QC3 (Replay & Domain Separation Defense):** Every attestation includes an explicit domain tag (`sw:qc:v1:<domain>`), monotonic epoch number, and payload digest preventing cross-seam or cross-epoch signature replay.

## Decision
1. Implement a unified deep module `qc.py` (Python) and `qc.ts` (TypeScript Node 22).
2. Represent committees as immutable rosters of member IDs, Ed25519 public keys, and Merkle tree roots.
3. Define `ThresholdQuorumCertificate` containing:
   - `cert_id`: Deterministic SHA-256 identifier.
   - `domain`: Domain tag string (e.g., `barrier:cutover`, `sync:poe`, `circuit:rearm`).
   - `epoch`: Monotonic integer epoch.
   - `digest`: Hex-encoded SHA-256 payload digest.
   - `committee_root`: Hex-encoded Merkle root of the authorized committee.
   - `signatures`: Mapping of `member_id` to Ed25519 signature hex string.
   - `signers`: Sorted list of participating member IDs.
4. Provide a deep module interface:
   - `ceremony(members: List[CommitteeMember], epoch: int) -> CommitteeRoster`
   - `attest(roster: CommitteeRoster, digest: str, domain: str, signers: Dict[str, PrivateKey]) -> ThresholdQuorumCertificate`
   - `verify(cert: ThresholdQuorumCertificate, expected_roster: CommitteeRoster | None, expected_root: str | None) -> bool`
   - `rotate(current_roster: CommitteeRoster, next_members: List[CommitteeMember], signer_keys: Dict[str, PrivateKey]) -> Tuple[CommitteeRoster, ThresholdQuorumCertificate]`
5. Wire threshold QC verification into Seam 80 (Causality Barrier B3), Seam 77 (Cross-Cluster Sync I1), Seam 72 (Mesh Router), and Seam 82 (Unified Safety Circuit Rearm).

## Consequences
- Unkeyed checksums and forgeable mock signatures are permanently eradicated.
- Verification cannot be spoofed by omitting signatures or injecting attacker-controlled quorum requirements.
- Zero-token cost guarantee is strictly preserved ($0.00 USD, 100% deterministic local Ed25519 and SHA-256).
- Latency overhead for threshold signature verification remains within the sub-250µs SLO.
