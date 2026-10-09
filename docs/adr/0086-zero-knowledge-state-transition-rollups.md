# ADR 0086: Optimistic State Transition Rollups & Succinct Asymmetric Attestations

## Status
Accepted (Amended for Milestone H Hardening)

## Context
As autonomous agent swarms scale across clusters, regions, and organizational boundaries (established in ADRs 0081, 0083, 0084, and 0085), swarms generate high-frequency streams of state mutations—including tool side-effects, memory updates, consensus decisions, and causal lineage events.

Transmitting, auditing, and persisting every individual state mutation introduces critical architectural vulnerabilities:
1. **Bandwidth and Verification Bottlenecks ($O(M)$ Scaling)**: Replicating and verifying thousands of individual state mutations across cross-cluster gateways or external audit anchors (ADR 0054) creates quadratic communication bottlenecks. Remote clusters or audit observers must sequentially re-execute every transition to verify final swarm state.
2. **Confidentiality and Scratchpad Leakage**: Fine-grained agent state transitions often contain private reasoning scratchpads, internal token embeddings, proprietary prompts, and confidential customer parameters. Broadcasting raw state transitions across untrusted or semi-trusted clusters violates strict data privacy boundaries.
3. **Decoupling from Safety Invariants and Absence of Fraud Detection**: In the absence of mathematical state transition constraints, corrupted or compromised agents can forge arbitrary state mutations, skip sequence numbers, or fork state branches without detection until catastrophic damage occurs.
4. **Third-Party Verifiability vs. Shared-Secret MACs (Milestone H B-3)**: Symmetric HMAC MACs require verifiers to possess a shared secret seed, which creates symmetric forging vulnerabilities (any entity that can verify a receipt can also forge one) and prevents public third-party audits. Asymmetric digital signatures are necessary so that anyone holding the Prover's public key can independently verify state transition validity without acquiring private signing authority.
5. **Merkle Leaf Domain Separation & Ambiguous Encodings (Milestone H B-2)**: Leaf parsers that dynamically decode even-length hex vs. odd-length UTF-8 strings introduce encoding collision vulnerabilities (e.g., `"616263"` and `"abc"` hashing to identical leaves). Enforcing strict 64-character lowercase hexadecimal hash strings eliminates all leaf-encoding collisions.

Seam 75 solves these challenges by implementing an in-process, zero-token **Optimistic State Transition Rollup & Succinct Attestation Engine** (`OptimisticRollupEngine` / `ZkRollupEngine`).

## Decision
We implement an in-process, deterministic **Optimistic State Transition Rollup & Succinct Attestation Engine** with complete dual-SDK parity across Python 3.12+ (`swarmwatch.zk_rollup`) and TypeScript Node 22 native type-stripping (`@swarmwatch/sdk/src/zk_rollup.ts`).

> [!NOTE]
> **Cryptographic Implementation Clarification**: While early design drafts informally referenced "Zero-Knowledge Rollup", this module is mathematically and architecturally implemented as an **Optimistic State Transition Rollup with Fiat-Shamir Polynomial Commitments and Asymmetric Ed25519 Attestations** (hence the primary class name `OptimisticRollupEngine`). It does not execute a heavy zk-SNARK / zk-STARK arithmetic circuit or rely on trusted setups, thus preserving our sub-250µs execution SLO and zero external LLM API cost guarantees.

### 1. Sparse State Tree & Strict Merkle Invariants
The engine maintains an in-memory sparse state slot registry representing agent working memory:
- **State Slot**: Key-value association $\langle k_i, v_i \rangle$ where each slot possesses a 64-character lowercase hex digest $h_i = \text{SHA256}(v_i)$.
- **Strict Merkle Leaf Inputs**: Merkle leaf hashes must strictly match `^[0-9a-f]{64}$`, raising `InvalidStateTransitionError` otherwise. Leaves are prefixed with RFC 6962 domain separation byte `0x00`, and internal nodes with `0x01`. Solitary nodes are promoted without duplicate hashing.
- **State Merkle Root**: The global state root $\Sigma = \text{MerkleRoot}(\{ (k_i, h_i) \})$ commits deterministically to the aggregate system state.

### 2. Rollup Batching & Sequence Monotonicity
The engine aggregates $M$ contiguous atomic state transitions into an immutable `RollupBatch`:
$$\mathcal{B} = \{ \tau_1, \tau_2, \dots, \tau_M \} \quad \text{with} \quad \text{seq}_{i+1} = \text{seq}_i + 1$$
- **Pre-State Root ($\Sigma_{\text{pre}}$)**: The system Merkle root immediately prior to applying $\tau_1$.
- **Post-State Root ($\Sigma_{\text{post}}$)**: The system Merkle root immediately after applying $\tau_M$.
- **Batch Digest ($D$)**:
  $$D = \text{SHA256}(\tau_1.\text{digest} \parallel \tau_2.\text{digest} \parallel \dots \parallel \tau_M.\text{digest})$$
- Strict monotonicity rules reject out-of-order, duplicate, or gapped sequences with `SequenceDiscontinuityError`.

### 3. Succinct Fiat-Shamir State Polynomial Evaluation
To bind state transitions into a succinct polynomial evaluation without re-executing transitions:
1. **Fiat-Shamir Non-Interactive Challenge ($z$)**:
   $$z = \text{SHA256}(\Sigma_{\text{pre}} \parallel \Sigma_{\text{post}} \parallel D \parallel \text{delta\_merkle\_root}) \pmod p$$
   evaluated over the standard BN254 scalar field prime $p = 21888242871839275222246405745257275088548364400416034343698204186575808495617$.
2. **Transition Polynomial Evaluation ($P(z)$)**:
   $$P(z) = c_0 + c_1 z + c_2 z^2 + c_3 z^3 \pmod p$$
   where $c_0 = \Sigma_{\text{pre}}$, $c_1 = D$, $c_2 = \text{delta\_merkle\_root}$, $c_3 = \Sigma_{\text{post}}$.

### 4. Asymmetric Third-Party Verifiable Attestation
The Prover signs the canonical receipt hash using their private Ed25519 key:
$$\text{Receipt Hash} = \text{"sw:rollup:v1:"} \,\|\, \text{SHA256}(\text{canonical\_json}(\text{AttestationPayload}))$$
$$\sigma_{\text{prover}} = \text{Sign}_{\text{Ed25519}}(\text{Receipt Hash})$$
- Anyone in possession of the Prover's public key `prover_public_key_hex` can independently verify the receipt without holding any secret seed or private key.
- Receipts cannot be forged by verifiers or third parties.

### 5. Fraud Challenge & Replay Defense
External verifiers or monitoring nodes can challenge a rollup batch in $O(1)$ time:
- **State Invariant Verification**: Confirms that applying the transition delta Merkle root to $\Sigma_{\text{pre}}$ deterministically produces $\Sigma_{\text{post}}$.
- **Sequence Continuity Verification**: Confirms continuous monotonic numbering from $\text{seq}_{\text{start}}$ to $\text{seq}_{\text{end}}$.
- **Asymmetric Signature Verification**: Confirms valid Ed25519 digital signature against the Prover's pinned public key.
- Any discrepancy raises `RollupVerificationFailedError` and triggers immediate fraud challenge isolation.

### 6. Fail-Closed Safety Circuit Coupling
The Rollup Engine synchronizes directly with SwarmWatch's circuit breaker:
- **Lockout Enforcement**: When the circuit breaker is tripped, queuing new transitions and committing rollup batches immediately halts with `RollupCircuitTrippedError`.
- **Autonomous Fault Trip**: If a fraudulent transition, sequence gap, state forgery, or proof verification failure occurs, the engine autonomously trips the global safety circuit to prevent state divergence.

## Invariants
1. **Zero-Token Cost Guarantee**: All state transitions, sparse Merkle updates, Fiat-Shamir challenge derivations, and proof verifications execute 100% locally with zero external LLM API calls ($0.00 USD token cost).
2. **Sub-250µs Execution Latency**: Atomic transition queuing and succinct attestation synthesis execute with microsecond latency.
3. **Asymmetric Third-Party Verifiability**: State transition attestations are signed using Ed25519; anyone can verify receipts using the Prover's public key with zero shared secrets.
4. **Collision-Free Merkle Tree**: Leaf hashes must be strictly 64 lowercase hexadecimal characters matching `^[0-9a-f]{64}$`; ambiguous encodings are strictly rejected.
5. **Strict Sequence Monotonicity**: State sequences must be contiguous and strictly monotonic; sequence gaps or duplicates are deterministically rejected.
6. **Fail-Closed Latching**: Any failed proof verification or state conflict trips the safety circuit and blocks subsequent batch commitments.
7. **Dual-SDK Parity**: 100% identical state transitions, cryptographic hashing, and proof receipts across Python 3.12+ and TypeScript Node 22 native type-stripping.

## Consequences
- **Positive**: Compresses thousands of discrete agent state mutations into a single succinct $O(1)$ verifiable attestation.
- **Positive**: Enables open third-party verification without sharing private cryptographic seeds.
- **Positive**: Eliminates Merkle leaf-encoding collisions completely across both SDKs.
- **Positive**: Zero external cloud, trusted setup, or heavy C++ cryptographic library dependencies; operates entirely with native standard cryptographic primitives.
