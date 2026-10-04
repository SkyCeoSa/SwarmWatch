# ADR 0086: Zero-Knowledge State Transition Rollups & Succinct Proof Verification

## Status
Accepted

## Context
As autonomous agent swarms scale across clusters, regions, and organizational boundaries (established in ADRs 0081, 0083, 0084, and 0085), swarms generate high-frequency streams of state mutations—including tool side-effects, memory updates, consensus decisions, and causal lineage events.

Transmitting, auditing, and persisting every individual state mutation introduces critical architectural vulnerabilities:
1. **Bandwidth and Verification Bottlenecks ($O(M)$ Scaling)**: Replicating and verifying thousands of individual state mutations across cross-cluster gateways or external audit anchors (ADR 0054) creates quadratic communication bottlenecks. Remote clusters or audit observers must sequentially re-execute every transition to verify final swarm state.
2. **Confidentiality and Scratchpad Leakage**: Fine-grained agent state transitions often contain private reasoning scratchpads, internal token embeddings, proprietary prompts, and confidential customer parameters. Broadcasting raw state transitions across untrusted or semi-trusted clusters violates strict data privacy boundaries.
3. **Decoupling from Safety Invariants and Absence of Fraud Detection**: In the absence of mathematical state transition constraints, corrupted or compromised agents can forge arbitrary state mutations, skip sequence numbers, or fork state branches without detection until catastrophic damage occurs.
4. **Zero-Token Cost & Real-Time Latency Constraints**: Heavy external zk-SNARK prover libraries (e.g., Circom, Groth16 setups, Libsnark) require multi-gigabyte trusted setups, heavy compilation toolchains, and tens of seconds of prover latency. Furthermore, using LLMs to summarize or verify transitions violates SwarmWatch's **Zero-Token Cost Guarantee** and sub-millisecond execution requirements.

Seam 75 solves these challenges by implementing an in-process, zero-token **Zero-Knowledge State Transition Rollup & Succinct Proof Verification Engine** (`ZkRollupEngine`).

## Decision
We implement an in-process, deterministic **Zero-Knowledge State Transition Rollup & Succinct Proof Verification Engine** with complete dual-SDK parity across Python 3.12+ (`swarmwatch.zk_rollup`) and TypeScript Node 22 native type-stripping (`@swarmwatch/sdk/src/zk_rollup.ts`).

### 1. Sparse State Tree & Atomic Transitions
The engine maintains an in-memory sparse state slot registry representing agent working memory:
- **State Slot**: Key-value association $\langle k_i, v_i \rangle$ where each slot possesses a cryptographic digest $h_i = \text{SHA256}(v_i)$.
- **State Merkle Root**: The global state root $\Sigma = \text{MerkleRoot}(\{ (k_i, h_i) \})$ commits deterministically to the aggregate system state.
- **Atomic State Transition**:
  $$\tau = \langle \text{transition\_id}, \text{seq}, \text{agent\_id}, \text{slot\_key}, h_{\text{from}}, h_{\text{to}}, h_{\Delta}, \text{mask}, \text{meta}, t \rangle$$
  where each transition verifies that the current slot value matches $h_{\text{from}}$ before updating to $h_{\text{to}}$.

### 2. Rollup Batching & Sequence Monotonicity
The engine aggregates $M$ contiguous atomic state transitions into an immutable `RollupBatch`:
$$\mathcal{B} = \{ \tau_1, \tau_2, \dots, \tau_M \} \quad \text{with} \quad \text{seq}_{i+1} = \text{seq}_i + 1$$
- **Pre-State Root ($\Sigma_{\text{pre}}$)**: The system Merkle root immediately prior to applying $\tau_1$.
- **Post-State Root ($\Sigma_{\text{post}}$)**: The system Merkle root immediately after applying $\tau_M$.
- **Batch Digest ($D$)**:
  $$D = \text{SHA256}(\tau_1.\text{digest} \parallel \tau_2.\text{digest} \parallel \dots \parallel \tau_M.\text{digest})$$
- Strict monotonicity rules reject out-of-order, duplicate, or gapped sequences with `InvalidStateTransitionError`.

### 3. Succinct Zero-Knowledge Proof Construction
To guarantee that $\Sigma_{\text{pre}}$ validly transitioned to $\Sigma_{\text{post}}$ without revealing private execution scratchpads or payload contents, the engine synthesizes a succinct zero-knowledge argument:
1. **Private Blinding Factor ($\beta$)**: Generated from an in-process cryptographic seed and batch digest:
   $$\beta = \text{HMAC-SHA256}(D, \text{secret\_seed})$$
2. **Fiat-Shamir Non-Interactive Challenge ($z$)**:
   $$z = \text{SHA256}(\Sigma_{\text{pre}} \parallel \Sigma_{\text{post}} \parallel D \parallel \beta) \pmod p$$
3. **Succinct Proof Polynomial Evaluation**:
   The engine computes a polynomial evaluation proof binding the state transition invariant:
   $$\Sigma_{\text{post}} \equiv \text{SHA256}(\Sigma_{\text{pre}} \parallel D \parallel \text{delta\_root})$$
4. **Tamper-Evident Proof Receipt**:
   The rollup receipt seals the verification payload with domain tag:
   $$\text{Receipt Hash} = \text{"sw:rollup:v1:"} \,\|\, \text{SHA256}(\text{canonical\_json}(\text{RollupProofReceipt}))$$

### 4. Fraud Challenge & Replay Defense
External verifiers or monitoring nodes can challenge a rollup batch in $O(1)$ time:
- **State Invariant Verification**: Confirms that applying the transition delta Merkle root to $\Sigma_{\text{pre}}$ deterministically produces $\Sigma_{\text{post}}$.
- **Sequence Continuity Verification**: Confirms continuous monotonic numbering from $\text{seq}_{\text{start}}$ to $\text{seq}_{\text{end}}$.
- **Zero-Knowledge Proof Verification**: Confirms that the Fiat-Shamir challenge point $z$ and cryptographic commitment proof match without inspecting the private payload masks.
- Any discrepancy raises `RollupVerificationFailedError` and triggers immediate fraud challenge isolation.

### 5. Fail-Closed Safety Circuit Coupling
The ZK Rollup Engine synchronizes directly with SwarmWatch's circuit breaker:
- **Lockout Enforcement**: When the circuit breaker is tripped, queuing new transitions and committing rollup batches immediately halts with `RollupCircuitTrippedError`.
- **Autonomous Fault Trip**: If a fraudulent transition, sequence gap, state forgery, or proof verification failure occurs, the engine autonomously trips the global safety circuit to prevent state divergence.

## Invariants
1. **Zero-Token Cost Guarantee**: All state transitions, sparse Merkle updates, Fiat-Shamir challenge derivations, and proof verifications execute 100% locally with zero external LLM API calls ($0.00 USD token cost).
2. **Sub-250µs Execution SLO**: Atomic transition queuing and succinct proof verification execute in $<250\,\mu\text{s}$ per batch under normal load.
3. **Zero-Knowledge Privacy**: Proof receipts verify state transitions without leaking private scratchpad payloads or internal agent memory.
4. **Strict Sequence Monotonicity**: State sequences must be contiguous and strictly monotonic; sequence gaps or duplicates are deterministically rejected.
5. **Fail-Closed Latching**: Any failed proof verification or state conflict trips the safety circuit and blocks subsequent batch commitments.
6. **Dual-SDK Parity**: 100% identical state transitions, cryptographic hashing, and proof receipts across Python 3.12+ and TypeScript Node 22 native type-stripping.

## Consequences
- **Positive**: Compresses thousands of discrete agent state mutations into a single succinct $O(1)$ verifiable proof.
- **Positive**: Preserves strict zero-knowledge privacy for internal agent reasoning and confidential data.
- **Positive**: Enables cross-cluster verification and audit anchoring without transferring heavy state payloads.
- **Positive**: Zero external cloud, trusted setup, or heavy C++ cryptographic library dependencies; operates entirely with native standard cryptographic primitives.
- **Negative**: Rollup batch size must be tuned (default 100 transitions) to balance latency versus compression ratio.
