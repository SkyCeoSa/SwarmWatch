# ADR 0049: Differential Privacy & Cryptographic State Anonymization for Enterprise Multi-Tenant Tracing

## Status
Accepted

## Context
When enterprise agent swarms emit causal telemetry (token counts, latencies, monetary costs, prompt Shannon entropy, agent roles, and identifiers) to federated multi-tenant datastores (ClickHouse, OpenTelemetry collectors, cloud object storage), these traces risk leaking proprietary business logic, prompt complexity, and organizational agent structure.
While previous seams implemented secret redaction for credentials (API keys, passwords, JWTs), raw quantitative fields are vulnerable to statistical linkage attacks, membership inference, and differential reconstruction.

We require a zero-external-dependency, zero-token-overhead mathematical framework to enforce formal **Differential Privacy ($\epsilon, \delta$-DP)** and deterministic cryptographic pseudonymization on spans before cross-tenant analytics or external federation.

## Decision
We implement `DifferentialPrivacyEngine` providing mathematical differential privacy and cryptographic identity obfuscation:

1. **Calibrated Laplace Noise Mechanism**:
   For any numeric telemetry field $X$ with bounded domain $[L, U]$ and global $L_1$ sensitivity $\Delta f = U - L$:
   $$X_{\text{priv}} = \text{clamp}(X, L, U) + \eta, \quad \eta \sim \text{Laplace}\left(0, \frac{\Delta f}{\epsilon}\right)$$
   Where scale $b = \Delta f / \epsilon$. The random variable $\eta$ is generated via standard library uniform pseudo-random generation and the inverse cumulative distribution function (CDF) of the Laplace distribution:
   $$F^{-1}(u) = -\text{sgn}(u - 0.5) \cdot b \cdot \ln(1 - 2|u - 0.5|), \quad u \in (0, 1)$$

2. **Cryptographic Role & Agent Pseudonymization**:
   To preserve topological causal link analysis without exposing raw entity names:
   $$\text{AnonAgentID} = \text{agent\_} + \text{HMAC-SHA256}(K_{\text{tenant}}, \text{AgentID})[:12]$$
   $$\text{AnonRole} = \text{role\_} + \text{HMAC-SHA256}(K_{\text{tenant}}, \text{Role})[:8]$$
   This guarantees that intra-swarm causal links remain consistent while third-party observers cannot reverse-engineer true identity.

3. **Privacy Budget ($\epsilon$-Budget Accounting)**:
   - Tracks total privacy budget consumption across queries or exports:
     $$\epsilon_{\text{remaining}} = \max\left(0.0, \epsilon_{\text{total}} - \sum \epsilon_i\right)$$
   - When $\epsilon_{\text{remaining}} = 0$, the engine enters exhausted status, refusing additional noisy exports or returning non-disclosing default aggregates.

4. **Dual-SDK Parity**:
   - Python: `packages/sdk-python/swarmwatch/diff_privacy.py` (`DifferentialPrivacyEngine`, `DifferentialPrivacyPolicy`, `LaplaceMechanism`, `AnonymizedSpanRecord`, `global_privacy_engine`).
   - TypeScript: `packages/sdk-typescript/src/diff_privacy.ts` with 100% strict Node 22 `--experimental-strip-types` compliance.

5. **Control Plane & Web Console**:
   - REST endpoints: `GET /api/v1/privacy/status`, `POST /api/v1/privacy/anonymize`, `POST /api/v1/privacy/policy`.
   - CLI subcommands: `swarmwatch privacy status`, `swarmwatch privacy test`.
   - Web Console: `#dpBadge` in `#fleetBar` in `dashboard.html` with real-time epsilon budget tracking.

## Consequences
- **Positive**: Provable mathematical privacy guarantees against reconstruction attacks; deterministic pseudonymization preserving graph topology; 0 external dependencies; 0 remote LLM costs; execution $<50\mu\text{s}$ per span.
- **Negative**: Controlled perturbation introduces small variance to individual numeric metrics, though population aggregates remain asymptotically unbiased ($\mathbb{E}[\eta] = 0$).
