# ADR 0050: Predictive Multi-Agent Failure Forecasting & Early-Warning Entropy Martingales (Seam 39)

## Context & Problem Statement
Currently, SwarmWatch's `EntropyCircuitBreaker` (ADR 0002) and `RedisCircuitCoordinator` (ADR 0033) operate *reactively*: they detect semantic collapse or token budget exhaustion once an anomalous turn has already concluded and tripped threshold limits ($H(X) \le 0.35$ bits).

In high-concurrency enterprise multi-agent workflows (e.g., automated coding, financial trade reconciliation, legal document drafting), by the time a reactive circuit halts execution:
1. Two to four expensive turns of compute and prompt tokens have already been wasted.
2. Downstream agent contexts have already been partially contaminated (Context Poisoning, ADR 0043).
3. The multi-agent swarm suffers a hard, abrupt interruption rather than a smooth, pre-emptive adaptation.

Existing telemetry monitoring tools lack formal mathematical models to predict multi-agent conversational failure *before* threshold breaches occur without relying on expensive, recursive secondary LLM observer calls.

## Decision
We implement **Seam 39: Predictive Multi-Agent Failure Forecasting & Early-Warning Entropy Martingales**, introducing an in-process, zero-token mathematical forecasting engine based on discrete-time supermartingale convergence and Azuma-Hoeffding concentration inequalities.

### Mathematical Foundations
1. **Entropy Drift Series**:
   Let $H_t$ denote the Shannon entropy of turn $t \in \{1, \dots, n\}$.
   The turn-to-turn entropy delta is:
   $$\Delta H_t = H_t - H_{t-1}$$
   When a swarm functions nominally, $\Delta H_t$ exhibits mean-zero random fluctuation. As conversational degeneration begins (repetitive retries, prompt echo chambers, stuck tools), $\Delta H_t$ exhibits consistent negative drift:
   $$\mathbb{E}[\Delta H_t \mid \mathcal{F}_{t-1}] < 0$$
   transforming the sequence $\{H_t\}_{t \ge 1}$ into a strict **supermartingale** with respect to the causal filtration $\mathcal{F}_{t-1}$.

2. **Centered Martingale Difference Sequence**:
   To detect structural drift distinct from natural variance, we compute the centered martingale difference:
   $$D_t = \Delta H_t - \mu_{\text{baseline}}$$
   where $\mu_{\text{baseline}}$ is estimated over a sliding baseline calibration window. The cumulative martingale sequence is:
   $$M_n = \sum_{t=1}^n D_t$$

3. **Azuma-Hoeffding Concentration Bound**:
   Assuming bounded turn differences $|D_t| \le c$, Azuma's inequality guarantees that for a forecast horizon of $h$ turns into the future, the probability that entropy declines by more than critical gap $\lambda = H_n - H_{\text{threshold}}$ satisfies:
   $$P\left(H_{n+h} \le H_{\text{threshold}} \mid \mathcal{F}_n\right) \ge 1 - \exp\left(-\frac{2\lambda^2}{h \cdot c^2}\right)$$

4. **Early-Warning Horizon & Risk Classification**:
   - **Nominal ($P_{\text{risk}} < 0.30$ or $T_{\text{forecast}} > 5$ turns)**: Multi-agent dialogue is statistically stable.
   - **Early-Warning ($0.30 \le P_{\text{risk}} < 0.70$, $T_{\text{forecast}} \in [3, 5]$)**: Negative entropy velocity detected; pre-emptive alert logged.
   - **Imminent Collapse ($P_{\text{risk}} \ge 0.70$, $T_{\text{forecast}} \le 2$ turns)**: High-confidence impending failure; pre-emptive notification dispatched to callers and self-healing controller (ADR 0042).

### Architectural Invariants
1. **Zero-Token Cost Guarantee**: All supermartingale calculations and Azuma-Hoeffding bounds execute locally in pure Python / TypeScript floating-point arithmetic in $<30\mu\text{s}$ per turn. Zero remote LLM API calls.
2. **Dual-SDK Parity**:
   - Python: `swarmwatch.forecast.EntropyMartingaleForecaster`
   - TypeScript: `@swarmwatch/sdk/src/forecast.ts` complying strictly with Node 22 type-stripping rules (`node --experimental-strip-types`).
3. **Control Plane & Web Console**:
   - REST Endpoints: `GET /api/v1/swarms/{id}/forecast`, `POST /api/v1/swarms/{id}/forecast/evaluate`, `POST /api/v1/swarms/{id}/forecast/reset`.
   - CLI Subcommand: `swarmwatch forecast status|predict`.
   - Web Console: `#forecastBadge` in `#fleetBar` conforming strictly to High-Density Craft Standard (0 anti-patterns `[]`).

## Consequences
### Positive
- Enterprise operators and self-healing pipelines receive advance notice 2 to 4 turns *before* circuit trips occur.
- Eliminates context poisoning and wasted token expenditure before irreversible deadlock.
- 100% deterministic, reproducible, and verifiable with pure mathematical properties.

### Negative / Trade-offs
- Requires at least 3 turns of initial telemetry to calibrate the filtration baseline before issuing high-confidence forecasts.
