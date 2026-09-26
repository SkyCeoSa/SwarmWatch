# ADR 0069: Privacy Release Ledger & Differential Privacy Accountant (Seam 58)

## Status
Accepted

## Context
In multi-tenant multi-agent deployments, telemetry exports, aggregated metrics, and agent performance reports can leak sensitive internal state or proprietary prompt contents through repeated querying (differential reconstruction attacks).
1. **Repeated-Query Reconstruction Vulnerability**: Without tracking cumulative privacy loss across queries, an adversarial tenant can issue multiple overlapping queries with slight variations to reconstruct individual agent tokens, private prompt parameters, or secret system instructions.
2. **Unbounded Sensitivity Hazard**: If arbitrary raw telemetry values are perturbed without global/local sensitivity clipping, outlier values can blow up scale parameters or leak signal through the variance of the noise.
3. **Lack of Composition Accounting**: Privacy budgets must be accounted for across multiple releases using mathematically sound composition theorems (linear $\epsilon$-DP and advanced $(\epsilon, \delta)$-DP composition).
4. **Reproducibility vs. Cryptographic Security**: Production release mechanisms must use cryptographically secure random number generation (CSPRNG) to prevent pseudorandom seed prediction, while audit and automated test fixtures require deterministic seeded distributions to verify exact numerical parity.

## Decision
We implement **Seam 58: Privacy Release Ledger (`PrivacyRelease`)**:
1. **Privacy Release Ledger Invariant**:
   - An export or query evaluation occurs only when its tenant's remaining privacy budget ($\epsilon_{\text{remaining}}, \delta_{\text{remaining}}$) and contribution rules permit it.
   - When the cumulative budget is exhausted ($\epsilon_{\text{spent}} + \epsilon_q > \epsilon_{\text{total}}$), all further release requests fail-closed with `PRIVACY_BUDGET_EXHAUSTED`.
2. **Specified Adjacency & Clipped Sensitivity ($L_1$)**:
   - Adjacency $D \sim D'$ is defined as differing by at most one agent turn record or execution trace.
   - Queries clip inputs strictly within configured sensitivity boundaries $[L_{\text{min}}, L_{\text{max}}]$, guaranteeing bounded $L_1$ sensitivity $\Delta f \le L_{\text{max}} - L_{\text{min}}$.
3. **Dual Composition Accounting**:
   - **Linear Composition**: $\epsilon_{\text{total}} = \sum_{i=1}^k \epsilon_i$.
   - **Advanced $(\epsilon, \delta)$-Composition**: For $k$ queries with $(\epsilon, \delta)$, total privacy bound $\epsilon_g = \sqrt{2k \ln(1/\delta')} \epsilon + k\epsilon(e^\epsilon - 1)$ with $\delta_g = k\delta + \delta'$.
4. **Calibrated Laplace Mechanism**:
   - Scale $b = \frac{\Delta f}{\epsilon_q}$. Noise drawn from $\text{Laplace}(0, b)$.
   - Cryptographic CSPRNG in production; deterministic seed option for reproducible audit verification.
5. **Deep Module Interface**:
   - `PrivacyRelease.reserve(tenant_id, epsilon, delta=0.0, sensitivity=1.0, query_id=None) -> PrivacyReservation`: Reserves privacy budget before query execution.
   - `PrivacyRelease.release(reservation_id, raw_value, noise_seed=None) -> PrivacyReleaseResult`: Applies calibrated perturbation to clipped values and commits budget consumption to the ledger.
   - `PrivacyRelease.remaining(tenant_id) -> Dict[str, float]`: Returns remaining $\epsilon$ and $\delta$ budget.
   - `PrivacyRelease.status(tenant_id) -> Dict[str, Any]`: Returns ledger diagnostics, total releases, and budget breakdown.
   - `PrivacyRelease.set_tenant_budget(tenant_id, total_epsilon, total_delta=0.0) -> None`: Configures or re-provisions tenant budget.
   - `PrivacyRelease.reset() -> None`: Clears in-memory ledger.
6. **Zero-Token & Dual-SDK Guarantee**:
   - 100% in-process mathematical computation ($<25\mu\text{s}$). Zero external LLM calls.
   - Full dual-SDK parity across Python (`packages/sdk-python/swarmwatch/privacy_ledger.py`) and TypeScript (`packages/sdk-typescript/src/privacy_ledger.ts`).
   - Canonical hashing with tag `CanonicalDomainTag.PRIVACY` (`"sw:priv:v1"`).

## Consequences
- **Positive**:
  - Eliminates differential reconstruction attacks across repeated queries.
  - Guarantees provable mathematical privacy bounds with clipped sensitivity.
  - Fail-closed behavior prevents unauthorized telemetry leakage once budgets expire.
- **Negative**:
  - Telemetry exports with high precision requirements consume budget quickly and require administrative replenishment.
