# ADR 0060: Zero-Knowledge Causal Proof Generation & Cryptographic Audit Engine

## Status
Accepted

## Context
In regulated enterprise environments (EU AI Act High-Risk systems, SOC2 Type II, HIPAA, FedRAMP, and quantitative financial auditing):
1. **Verifiable Execution Lineage vs Confidentiality Paradox**: Auditors demand mathematical proof that an autonomous agent swarm complied with strict safety policies (e.g. circuit breaker tripped immediately upon anomaly detection, tools were invoked only by authorized agents, and causal DAG transitions followed approved state machines). However, disclosing raw message spans or conversational payloads leaks proprietary intellectual property, user PII, API tokens, and confidential financial instructions.
2. **Post-Hoc Trace Tampering Vulnerability**: In distributed fleets, compromised or rogue worker nodes can tamper with stored trace histories, drop offending spans, re-order turns, or synthesize counterfactual histories to conceal policy infractions.
3. **Absence of Cryptographic Non-Repudiation**: Traditional observability dumps rely on mutable log files or database rows without cryptographic immutability guarantees.
4. **Zero-Token Cost Constraint**: Proof generation and verification must be mathematically deterministic and run entirely in-process ($<500\mu\text{s}$ proof generation, $<20\mu\text{s}$ verification) without calling third-party LLMs or remote proof services.

## Decision
We implement Seam 49: Zero-Knowledge Causal Proof Generation & Cryptographic Audit Engine as an in-process, zero-token deep module with full dual-SDK parity across Python (`swarmwatch.zk_proof`) and TypeScript (`@swarmwatch/sdk/src/zk_proof.ts`).

### 1. Cryptographic Causal Accumulator & Binary Merkle Tree
- Each causal span $S_i$ is mapped into a cryptographic leaf node $L_i$:
  $$L_i = \text{SHA256}\left(\text{span\_id} \parallel \text{turn\_idx} \parallel \text{agent\_id} \parallel H(\text{payload}) \parallel \bigoplus_{p \in \text{Parents}(S_i)} \text{hash}(p) \parallel \text{circuit\_tripped}\right)$$
- An immutable, balanced binary Merkle tree is deterministically computed over ordered leaves $[L_0, L_1, \dots, L_{n-1}]$, yielding session Merkle root $R_{\mathcal{W}}$.
- The root $R_{\mathcal{W}}$ forms a non-repudiable cryptographic anchor for the entire swarm session.

### 2. Zero-Knowledge Audit Receipts & Proof Verification
- **Merkle Audit Path ($\vec{\sigma}$)**: Proves that an audited event (e.g. circuit trip, tool call, policy enforcement) was included at turn $k$ within root $R_{\mathcal{W}}$ with $O(\log n)$ proof length.
- **Zero-Knowledge Guarantee**: The verifier verifies inclusion without inspecting plaintext message contents or conversational prompts; only cryptographic commitments and metadata are processed.
- **Causal Lineage Proof**: Mathematically validates that span $S_b$ causally succeeds span $S_a$ ($S_a \prec S_b$) along the acyclic delegation DAG.

### 3. Proof Types (`ProofCommitmentType`)
- `TURN_INCLUSION`: Proves turn $k$ was executed and committed in the immutable session history.
- `CIRCUIT_TRIP`: Proves safety circuit breaker tripped at exact turn $k$ with recorded reason hash.
- `CAUSAL_LINEAGE`: Proves causal parent-child antecedent link between two spans.
- `POLICY_COMPLIANCE`: Proves evaluated policy rules conformed to declarative constraints.

### 4. Zero-Token Cost Guarantee
- 100% in-process SHA-256 Merkle tree calculation. Zero external LLM API calls, zero remote ZK-prover infrastructure costs.

### 5. Dual-SDK Parity
- **Python**: `packages/sdk-python/swarmwatch/zk_proof.py` (`ProofCommitmentType`, `SpanCommitmentLeaf`, `MerkleAuditProof`, `CausalAuditReceipt`, `CausalProofAccumulator`, `ZkProofEngine`, `global_zk_proof_engine`).
- **TypeScript**: `packages/sdk-typescript/src/zk_proof.ts` (`ProofCommitmentType`, `SpanCommitmentLeaf`, `MerkleAuditProof`, `CausalAuditReceipt`, `CausalProofAccumulator`, `ZkProofEngine`, `globalZkProofEngine`) strictly conforming to Node 22 `--experimental-strip-types`.

### 6. Control Plane, CLI & Web Console Integration
- **REST Endpoints**:
  - `GET /api/v1/swarms/{id}/proof/root`: Retrieves active session Merkle root and leaf count.
  - `POST /api/v1/swarms/{id}/proof/generate`: Generates verifiable zero-knowledge audit receipt for span/turn.
  - `POST /api/v1/swarms/{id}/proof/verify`: Verifies cryptographic validity of audit proof against root.
  - `GET /api/v1/swarms/{id}/proof/manifest`: Complete audit manifest including all leaves and tree topology.
- **CLI Subcommand**:
  - `swarmwatch proof root --swarm-id <id>`: Inspect session Merkle root.
  - `swarmwatch proof generate --swarm-id <id> --span-id <sid>`: Generate cryptographic audit receipt.
  - `swarmwatch proof verify --swarm-id <id> --receipt <json_or_file>`: Validate proof integrity.
  - `swarmwatch proof manifest --swarm-id <id>`: Export full session audit manifest.
- **Web Console (`dashboard.html`)**:
  - `#zkProofBadge` in `.fleet-left-group` of `#fleetBar`.
  - Dynamic status polling every 11000ms: displays `ZK-PROOF: VERIFIED (N)` in green.
  - 100% compliant with High-Density Craft Standard.

## Consequences
### Positive
- Satisfies strict regulatory requirements (EU AI Act, SOC2) without disclosing sensitive prompt content.
- Cryptographically guarantees non-repudiation and causal tamper-evidence.
- Microsecond execution latency ($<500\mu\text{s}$ generation, $<20\mu\text{s}$ verification) with zero token overhead.

### Negative / Trade-offs
- Verification requires access to the verified Merkle root anchor.
