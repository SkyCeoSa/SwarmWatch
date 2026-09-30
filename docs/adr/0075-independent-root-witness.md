# ADR 0075: Independent Root Witness & Public-Key Chains

## Status
Accepted

## Context
In SwarmWatch, root epoch anchors (ADR 0065) originally utilized symmetric HMAC signatures (`SWARMWATCH_AUDIT_SECRET_KEY`) to authenticate epoch Merkle roots. While computationally lightweight, symmetric authentication suffers from fundamental structural vulnerabilities:
1. **Symmetric Forgery & Non-Repudiation Failure**: Any entity or node possessing the secret key to verify epoch roots is equally capable of forging historical roots, retroactively altering audit logs, or creating divergent states without detection.
2. **Whole-Chain Replacement & Split-Brain Attacks**: Without asymmetric digital signatures and cryptographic proof of tree consistency, a compromised server or adversarial node can present entirely fabricated root chains to isolated consumers.
3. **Absence of Public Verifiability**: External auditors, regulators, or decentralized participants cannot verify audit proofs without gaining access to the cluster's private HMAC secret.
4. **NIST SP 800-57 Key Lifecycle Non-Compliance**: Lack of a formal key rotation protocol with cryptographic proof of possession leaves long-lived deployments vulnerable to key compromise.

The external audit report (`AUDIT REPORT.md`, Foundational Gap Closure #5) explicitly mandated:
> "Introduce an independent root witness verifying append-only consistency proofs with asymmetric key signatures and rotation logs."

## Decision
We implement **Seam 64: Independent Root Witness & Public-Key Chains (`RootWitness`)** across both Python 3.12+ and TypeScript Node 22 SDKs, providing an asymmetric, publicly verifiable, append-only checkpoint witness:

1. **Zero-Dependency Asymmetric Signature Standard**:
   - **Algorithm**: Elliptic Curve Digital Signature Algorithm (ECDSA) over NIST P-256 (FIPS 186-4 / SECG secp256r1) with SHA-256 (ES256).
   - **Signature Format**: IEEE P1363 64-byte $(r \parallel s)$ canonical signatures (with BIP 62 low-$s$ constraint $s \le \lfloor N/2 \rfloor$), URL-safe Base64 encoded.
   - **Public Key Format**: Standard 65-byte uncompressed EC point ($0x04 \parallel X \parallel Y$), URL-safe Base64 encoded.
   - **Zero External Dependencies**:
     - Python: Pure-Python stdlib curve arithmetic and scalar multiplication in `WitnessKeyManager`.
     - Node.js: Native `node:crypto` with `prime256v1` and `ieee-p1363` encoding.
     - 100% cross-runtime signature verification parity.

2. **Signed Checkpoints (`SignedCheckpoint`)**:
   - Each checkpoint cryptographically binds:
     - `epoch`: Strictly monotonic 64-bit integer ($0, 1, 2, \dots$).
     - `tree_size`: Strictly non-decreasing total count of committed leaves ($M \le N$).
     - `root_hash`: 64-character hex SHA-256 Merkle root.
     - `prev_checkpoint_hash`: SHA-256 digest of the immediate preceding checkpoint (or 64 zeros for epoch 0).
     - `checkpoint_hash`: Canonical SHA-256 digest:
       `"sw:cp:v1:<epoch>:<tree_size>:<root_hash>:<prev_checkpoint_hash>:<timestamp_ns>:<witness_public_key>"`
     - `witness_public_key`: Base64URL-encoded uncompressed public key.
     - `signature`: Base64URL-encoded 64-byte IEEE P1363 signature over `checkpoint_hash`.
     - `timestamp_ns`: High-resolution nanosecond timestamp.

3. **RFC 6962 Logarithmic Consistency Proof Engine**:
   - Employs the Certificate Transparency standard (RFC 6962 Section 2.1.2) binary Merkle tree with explicit domain separation:
     - Leaf prefix: $\text{SHA256}(0x00 \parallel \text{leaf\_data})$
     - Interior node prefix: $\text{SHA256}(0x01 \parallel \text{left\_hash} \parallel \text{right\_hash})$
   - `get_consistency_proof(m, n)` generates a logarithmic consistency proof ($\mathcal{O}(\log n)$ intermediate hashes) demonstrating that the tree of size $m$ is an exact prefix of the tree of size $n$ without historical modification.
   - `verify_consistency(old_cp, new_cp, proof)` reconstructs both $R_m$ and $R_n$ from the proof path. Raises `ConsistencyVerificationError` if the trees diverge.

4. **Attack Mitigations**:
   - **Rollback Attack Protection**: `verify_checkpoint()` and `submit_checkpoint()` detect and reject backward epochs ($epoch \le last\_epoch$) or truncated tree sizes ($tree\_size < last\_tree\_size$) with `RollbackAttackError`.
   - **Split-Brain / Whole-Chain Replacement Protection**: Divergent parent hashes or failed consistency proofs raise `SplitBrainForkError`.

5. **NIST SP 800-57 Verifiable Key Rotation Protocol (`KeyRotationLog`)**:
   - `rotate_key(new_key_manager)` produces a cryptographically signed `KeyRotationRecord`:
     - `challenge`: $\text{SHA256}(\text{"sw:rot:v1:"} \parallel old\_pub \parallel new\_pub \parallel epoch \parallel timestamp)$
     - `proof_of_possession`: Signature of challenge by the *new* private key.
     - `authorization_signature`: Signature of challenge by the *old* private key.
   - Any external verifier can trace and validate the entire chain of trust from genesis public key to active witness key (`verify_rotation_log()`).

6. **Public Verification & Zero-Token Cost Guarantee**:
   - External auditors can verify any checkpoint using solely the Witness's public key; zero shared secrets or private keys required.
   - Consistency proof verification executes in $< 250\,\mu\text{s}$ (audit SLO).
   - Zero external LLM calls or token expenditure; 100% deterministic local computation.

## Consequences
### Positive
- Closes Foundational Gap #5 from the external audit report.
- Eliminates symmetric forgery vulnerabilities and delivers true non-repudiation.
- Fully compatible with Certificate Transparency / RFC 6962 standards.
- Enables public, unauthenticated auditability without compromising security.

### Negative
- Asymmetric signature generation requires elliptic curve scalar multiplication; however, checkpoints are issued at epoch boundaries, preserving sub-250µs verification latency on the hot path.
