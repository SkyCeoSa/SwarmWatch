# ADR 0084: Multi-Modal Cognitive Grounding & Vector Evidence Fusion

## Status
Accepted

## Context
As autonomous agent swarms execute increasingly complex tasks across heterogeneous modalities (natural language prompts, visual UI screenshots, audio inputs, structured JSON payloads, and automated external tool invocations), swarms become acutely vulnerable to **cognitive drift** and **ungrounded inference**:
1. **Hallucinatory Drift**: Autonomous agents generate confident assertions, synthesized conclusions, and risky tool parameters based on non-existent, obsolete, or hallucinated premises rather than authenticated swarm observations.
2. **Exorbitant LLM Re-Inspection & Vector DB Latency**: Traditional retrieval-augmented generation (RAG) and hallucination-detection frameworks rely on secondary LLM critic calls or third-party cloud vector databases (Pinecone, Weaviate, Milvus). These approaches introduce prohibitive per-turn token costs ($0.03–$0.15/turn) and excessive network latency (>500–1,500ms), catastrophically violating SwarmWatch's **Zero-Token Cost Guarantee** and sub-250µs execution SLO.
3. **Cross-Modal Disconnect**: Multimodal assets (such as watermarked visual artifacts from ADR 0055), downstream action execution receipts (ADR 0082), mesh routing packets (ADR 0083), and cryptographic root witness checkpoints (ADR 0075) exist in isolated silos without a unified mathematical substrate to fuse evidentiary weight.
4. **Decoupling from Safety Circuit Breakers**: When an agent begins hallucinating facts or drifting from ground truth, traditional systems allow the agent to continue executing downstream actions and financial tool calls, compounding catastrophic error cascades.

Seam 73 resolves these critical vulnerabilities by engineering an in-process, deterministic **Multi-Modal Cognitive Grounding & Vector Evidence Fusion Engine** (`CognitiveGroundingEngine`).

## Decision
We implement an in-process, zero-token deterministic **Multi-Modal Cognitive Grounding & Vector Evidence Fusion Engine** (`CognitiveGroundingEngine` / `VectorEvidenceStore`) with strict dual-SDK parity across Python 3.12+ and Node 22 native type-stripping (`@swarmwatch/sdk`).

### 1. Deterministic In-Process Sparse-to-Dense Semantic Projection
To eliminate all external LLM embedding API calls and heavy deep-learning dependencies while maintaining zero-token operation, we establish a deterministic, hashed n-gram semantic projection operator:
$$\vec{\phi}: \mathcal{S} \to \mathbb{R}^D \quad (D = 64)$$
where $\mathcal{S}$ is an arbitrary string, multimodal artifact fingerprint, or canonical receipt hash.
- The projection tokenizes content into normalized character shingles and sub-word n-grams ($n \in \{1, 2, 3\}$).
- Each token $t$ is mapped via stable 64-bit hashing (Murmur/FNV/SHA-256) into coordinate index $k = \text{hash}(t) \pmod D$ and sign bit $s \in \{-1, +1\}$.
- Feature counts are weighted via sub-linear term frequency $w(t) = 1 + \ln(1 + \text{count}(t))$.
- The final dense vector is $L_2$-normalized to unit sphere:
  $$\vec{v} = \frac{\vec{u}}{\|\vec{u}\|_2 + \epsilon}, \quad \|\vec{v}\|_2 = 1.0$$
- This mathematical projection is 100% deterministic, zero-token, and executes in $<5\,\mu\text{s}$ per claim on CPU.

### 2. Multi-Modal Evidence Space & Metric Algebra
Authentic evidence is registered across 5 fundamental modalities:
- `TEXT_CLAIM`: Prompt constraints, retrieved corpus facts, agent observations.
- `VISUAL_ARTIFACT`: Visual telemetry digests and perceptual watermarks (ADR 0055).
- `ACTION_RECEIPT`: Downstream tool execution proofs (`ActionExecutionReceipt`, ADR 0082).
- `ROOT_WITNESS`: Cryptographic state checkpoint proofs (`RootWitness`, ADR 0075).
- `STRUCTURED_TELEMETRY`: Normalized SwarmSpan telemetry payloads and state snapshots.

Cosine similarity between any claim vector $\vec{c} = \vec{\phi}(C)$ and evidence vector $\vec{e} = \vec{\phi}(E)$ is calculated as the exact inner product:
$$\text{sim}(\vec{c}, \vec{e}) = \langle \vec{c}, \vec{e} \rangle = \sum_{i=1}^D c_i \cdot e_i \in [-1.0, 1.0]$$

### 3. Evidentiary Fusion Operator
Given a candidate agent claim $C$ and active evidence registry $\mathcal{E}$, the engine evaluates evidence scores across each registered modality $m \in \mathcal{M}$:
$$S_m(C) = \max_{e \in \mathcal{E}_m} \big( w(e) \cdot \text{sim}(\vec{\phi}(C), \vec{\phi}(e)) \big)$$
where $w(e) \in (0.0, 1.0]$ is the evidence confidence weight.

The unified **Fused Grounding Score** $S_{\text{fused}}(C)$ is computed across requested modalities via normalized weighted fusion:
$$S_{\text{fused}}(C) = \frac{\sum_{m \in \mathcal{M}} \alpha_m S_m(C)}{\sum_{m \in \mathcal{M}} \alpha_m}$$
where $\alpha_m$ are modality significance weights (default uniform $\alpha_m = 1.0$).

### 4. Grounding Verdict Lattice
The engine classifies the claim into a formal 4-tier verdict lattice:
1. `GROUNDED`: $S_{\text{fused}}(C) \ge \theta_{\text{ground}}$ (default $\theta_{\text{ground}} = 0.70$). Strong mathematical alignment with authenticated evidence.
2. `PARTIALLY_GROUNDED`: $\theta_{\text{margin}} \le S_{\text{fused}}(C) < \theta_{\text{ground}}$ (default $\theta_{\text{margin}} = 0.45$). Weak or tangential alignment; execution allowed with cautionary warning.
3. `UNGROUNDED`: $S_{\text{fused}}(C) < \theta_{\text{margin}}$. No evidentiary basis discovered. In strict mode or before high-risk actions, raises `UngroundedInferenceError`.
4. `DRIFT_DETECTED`: Orthogonal or strongly negative correlation ($S_{\text{fused}}(C) < \theta_{\text{drift}}$, default $0.15$) indicating semantic divergence or direct contradiction with authenticated ground truth.

### 5. Fail-Closed Safety Circuit Coupling & Auto-Trip Governor
The cognitive grounding engine couples directly with SwarmWatch's mathematical circuit breakers (ADRs 0011, 0081, 0082, 0083):
- **Monotonic Locked Transit**: When safety circuits trip, all grounding verifications fail closed with `GroundingCircuitTrippedError`.
- **Runaway Drift Governor**: The engine tracks consecutive `UNGROUNDED` and `DRIFT_DETECTED` assertions per agent session. When consecutive drift incidents breach `max_drift_count_before_trip` (default 3), the engine autonomously trips the global safety circuit, halting the swarm before catastrophic downstream actions occur.

### 6. Tamper-Evident Grounding Receipts
Every verification operation generates an immutable `GroundingReceipt`:
- Canonical SHA-256 digest (`sw:ground:v1:...`) binding `(receipt_id, claim_id, tenant_id, verdict, overall_score, duration_us, timestamp_ns)`.
- Recorded in an in-memory, thread-safe audit ledger for regulatory compliance.

## Invariants
1. **Zero-Token Cost Guarantee**: 100% deterministic local vector projection, cosine algebra, and evidence fusion with zero external LLM API calls ($0.00 USD cost).
2. **Sub-50µs Execution SLO**: Complete vector projection, evidence indexing, and cosine similarity evaluation execute in $<50\,\mu\text{s}$ for active working stores ($\le 256$ evidence units).
3. **Multi-Modal Completeness**: Native support for text assertions, visual artifact digests, tool action receipts, root witness checkpoints, and structured spans.
4. **Fail-Closed Safety Coupling**: Latching circuit breaker immediately blocks ungrounded inferences (`GroundingCircuitTrippedError`); persistent drift breaches trip the swarm circuit.
5. **Dual-SDK Parity**: 100% identical semantics, projection hashing, and receipt structure between Python 3.12+ and Node 22 native type-stripping.

## Consequences
- **Positive**: Prevents ungrounded hallucinations from triggering dangerous downstream agent side-effects.
- **Positive**: Unifies multimodal telemetry, tool action proofs, and state snapshots into a single verifiable vector space.
- **Positive**: Zero external cloud vector DB or LLM dependencies; runs 100% local and offline.
- **Negative**: Hashed n-gram semantic projection captures lexical-semantic overlap and phrasing proximity rather than deep contextual reasoning, requiring calibrated thresholding.
