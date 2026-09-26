# ADR 0065: Durable Audit Anchor & Merkle Audit Receipt Engine (Seam 54)

## Status
Accepted

## Context
In previous iterations (specifically Seam 49 / ADR 0060), SwarmWatch implemented an in-memory `CausalProofAccumulator` capable of computing session Merkle roots and inclusion proofs. However, the Senior Code & Architecture Audit revealed critical evidentiary limitations:
1. **Mutable Roots & Historical Rewriting**: An in-memory accumulator can be reset or lost upon worker crash/restart, allowing malicious or malfunctioning coordinators to rewrite historical execution claims without detection.
2. **Missing Post-Crash Chain of Custody**: When a server restarts, there was no durable anchor tying historical roots together across restarts.
3. **Misleading Nomenclature**: Proofs of inclusion in a Merkle tree were previously marketed alongside "zero-knowledge" claims, whereas they actually establish qualified inclusion and authenticity relative to an authority, not cryptographic zero-knowledge.

To make SwarmWatch enterprise critical-infrastructure ready, the platform requires an append-only, durable root ledger where signed root epochs form an unbreakable cryptographic hash chain surviving process crashes, with explicit *Merkle audit receipts*.

## Decision
We implement **Seam 54: Durable Audit Anchor (`AuditAnchor`)**:
1. **Append-Only Root Epoch Chain**:
   - Each epoch $e \ge 1$ records an immutable `RootEpoch`: monotonic sequence $e$, Merkle root $R_e$, previous chain digest $H_{chain}^{(e-1)}$, new chain digest $H_{chain}^{(e)} = \text{SHA256}(H_{chain}^{(e-1)} \parallel e \parallel R_e \parallel \text{tenant\_id})$, timestamp, and cryptographic HMAC signature.
   - Genesis epoch $e=0$ is deterministically anchored.
   - Historical epochs are persisted to durable storage (append-only file or SQLite table) and verified from genesis upon restart.
2. **Merkle Audit Receipts (Not ZK Proofs)**:
   - Receipts are strictly designated as `MerkleAuditReceipt`, containing the leaf hash, $O(\log n)$ sibling path, epoch root, chain digest, and cryptographic signature.
   - Verification independently reconstructs the Merkle root, confirms inclusion in the durable epoch chain, and verifies the hash-chain link to Genesis.
3. **Deep Module Interface**:
   - `AuditAnchor.append_root(root_hash, tenant_id, metadata)`: Appends an epoch root, computes the hash chain, and commits to durable storage.
   - `AuditAnchor.issue_receipt(epoch, leaf_hash, audit_path, tenant_id)`: Issues an authentic `MerkleAuditReceipt`.
   - `AuditAnchor.verify_receipt(receipt)`: Validates signature, Merkle reconstruction, epoch existence, and chain continuity.
   - `AuditAnchor.audit_chain(since_epoch)`: Audits full chain integrity from genesis.
   - `AuditAnchor.status(tenant_id)`: Diagnostic health and epoch count.
4. **Zero-Token Cost Guarantee**:
   - 100% deterministic local in-memory cryptographic execution (SHA-256 and HMAC-SHA-256). Zero remote LLM API calls.
5. **Cross-Runtime Parity**:
   - Strict dual-SDK parity between Python (`packages/sdk-python/swarmwatch/audit_anchor.py`) and TypeScript (`packages/sdk-typescript/src/audit_anchor.ts`).

## Consequences
- **Positive**:
  - Process crashes and restarts cannot rewrite or truncate historical execution commitments.
  - Complete chain of custody across worker lifecycles.
  - Honest cryptographic labeling complying with auditor findings.
- **Negative**:
  - Requires durable storage writes on root commitment (sub-millisecond local I/O).
