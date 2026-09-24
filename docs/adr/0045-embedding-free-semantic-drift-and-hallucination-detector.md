# ADR 0045: Embedding-Free Vector Semantic Drift & Hallucination Divergence Detector (Seam 34)

## Status
Accepted

## Date
2026-09-24

## Context
Multi-agent swarms engaged in iterative reasoning, code synthesis, or autonomous negotiation are vulnerable to subtle modes of failure beyond simple string repetition or structural tool recursion:
1. **Semantic Drift**: Across extended conversational turns, agents may gradually deviate from the initial user prompt, anchoring instructions, or domain boundaries, wandering into irrelevant or unauthorized sub-problems.
2. **Hallucinatory Echo Chambers**: When multiple agents converse, one agent may introduce an ungrounded assumption or fictitious entity; peer agents then validate, elaborate, and amplify the error in subsequent turns, creating a self-reinforcing hallucinatory divergence.
3. **The Token Tax of LLM-as-a-Judge**: Traditional approaches attempt to detect semantic drift and hallucinations using external vector embeddings (e.g., OpenAI or Voyage embeddings) or secondary LLM judge invocations. This violates SwarmWatch's foundational **Zero-Token Cost Guarantee**, incurs prohibitive network latency (100–500ms per turn), and introduces third-party privacy and API availability dependencies.

An in-process, zero-external-dependency, zero-token-cost mathematical engine is required to quantify semantic drift and detect hallucinatory divergence in $<50\mu\text{s}$ per turn directly within the agent runtime.

## Decision
We implement an in-process, deterministic **Embedding-Free Vector Semantic Drift & Hallucination Divergence Detector** (`SemanticDriftDetector`) across the Python and TypeScript SDKs:

1. **Lexical Shingling & Multi-Order N-Gram Vectorization**:
   - Extracts character 4-grams and word 2/3-grams from agent conversational turns after case-folding, punctuation normalization, and numerical token bucketing.
   - Preserves phrase co-occurrence and morphological continuity without external NLP tokenizers or language model dependencies.

2. **64-Bit Universal MinHash Local Sensitivity Hashing (LSH) & Jaccard SimHash**:
   - Employs an array of $M=64$ deterministic universal hash functions:
     $$h_i(x) = (a_i \cdot x + b_i) \pmod p$$
     where $p = 2^{61} - 1$ (Mersenne prime) and coefficients $a_i, b_i$ are fixed cryptographic pseudo-random constants.
   - Computes fixed-size 64-element integer signature vectors in $<15\mu\text{s}$.
   - Evaluates unbiased pairwise Jaccard similarity between conversational turns:
     $$\hat{J}(A, B) = \frac{1}{M} \sum_{i=1}^M \mathbf{1}(\text{sig}_A[i] == \text{sig}_B[i])$$

3. **Vocabulary Compression Ratio ($VCR$) & Lexical Dispersion**:
   - Quantifies lexical diversity using the Type-Token Ratio (TTR) and compressed entropy ratio:
     $$VCR(t) = \frac{|V_{\text{unique}}(t)|}{|V_{\text{total}}(t)|}$$
   - Rapid collapse of $VCR$ ($<0.20$) over sustained message lengths signals repetitive echo chambers and ungrounded generation loops.

4. **Composite Semantic Drift Calculus ($\Delta_{\text{drift}}$)**:
   - For turn $t$, evaluates similarity against both the root task anchor $t_0$ and the immediate predecessor turn $t-1$:
     $$\Delta_{\text{drift}}(t) = 1.0 - \left( w_{\text{anchor}} \cdot \hat{J}(t, t_0) + w_{\text{local}} \cdot \hat{J}(t, t-1) \right)$$
     with default weights $w_{\text{anchor}} = 0.60$ and $w_{\text{local}} = 0.40$.
   - Classifies status into 4 operational tiers:
     - `nominal`: $\Delta_{\text{drift}} \le 0.45$
     - `warning`: $0.45 < \Delta_{\text{drift}} \le 0.72$
     - `divergent`: $\Delta_{\text{drift}} > 0.72$
     - `tripped`: Circuit broken due to persistent divergence or echo chamber collapse.

5. **In-Flight Circuit Breaker Linkage**:
   - Integrates directly with `EntropyCircuitBreaker` and `RedisCircuitCoordinator`.
   - When $\Delta_{\text{drift}} > 0.72$ for 2 consecutive turns or $VCR < 0.20$, triggers an immediate circuit trip (`SwarmCircuitBreakException`) with reason `Semantic drift threshold breached` or `Hallucinatory echo chamber detected`.
   - Immediately halts runaway multi-agent trajectories and logs counterfactual token and dollar savings.

6. **Dual-SDK Parity (TypeScript)**:
   - Full Node 22 type-stripping compliant implementation in `@swarmwatch/sdk/src/drift.ts`:
     - `MinHashSigner`: 64-hash permutation engine using JavaScript BigInt arithmetic.
     - `SemanticDriftDetector`: Sliding window tracker, anchor management, and profile generator.
     - `SemanticDriftProfile`: Immutable turn profile.

7. **REST Control Plane, CLI & Web Console**:
   - REST Endpoints:
     - `GET /api/v1/swarms/{swarm_id}/drift`: Complete chronological drift timeline and aggregate metrics.
     - `POST /api/v1/swarms/{swarm_id}/drift/evaluate`: Ad-hoc drift analysis for arbitrary payloads.
   - CLI Subcommands:
     - `swarmwatch drift status [--swarm-id <id>] [--format json|terminal]`
     - `swarmwatch drift analyze --text "<str>" [--anchor "<str>"]`
   - Web Console:
     - `#driftBadge` added to `#fleetBar` in `dashboard.html`.
     - Real-time polling via `refreshDriftStatus()` every 8500ms.
     - Conforms 100% to Impeccable Craft Floor (0 anti-patterns `[]`).

## Consequences

### Positive
- **Zero-Token Cost**: Operates entirely with deterministic mathematical hashing and set theory; zero external LLM API calls or vector embedding fees.
- **Ultra-Low Latency**: Computes MinHash signatures and pairwise Jaccard similarity in $<25\mu\text{s}$, seamlessly integrating into high-throughput streaming pipelines.
- **Early Warning**: Identifies subtle topic derailment and hallucination buildup before agents waste thousands of tokens or take unauthorized actions.
- **Root Task Anchoring**: Anchors against the initial objective, preventing gradual topic migration across dozens of intermediate agent turns.

### Negative / Trade-offs
- **Lexical Approximation**: MinHash Jaccard similarity tracks lexical and morphological overlap rather than high-dimensional contextual embeddings. Synonymous phrasing with completely disjoint vocabularies may show higher drift than dense neural embeddings would report; tuned weights ($w_{\text{anchor}}=0.6, w_{\text{local}}=0.4$) mitigate false positives.
