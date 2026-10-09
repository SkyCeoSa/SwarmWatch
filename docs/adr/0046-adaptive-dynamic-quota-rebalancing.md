# ADR 0046: Adaptive Dynamic Quota Re-balancing (Seam 35)

## Status
Accepted

## Date
2026-09-24

## Context
Multi-agent swarms operating in distributed production environments face fundamental resource allocation challenges when executing multi-turn reasoning and tool orchestration:
1. **The Rogue Agent Quota Drain**: An agent experiencing high semantic drift, prompt injection, or hallucinatory loops can rapidly consume the shared swarm token/budget pool before the global circuit breaker trips, prematurely halting productive agents that are converging on the user's objective.
2. **Static Quota Stranding & Starvation**: Static equal partitioning of token/turn budgets among agents causes complex reasoning agents to hit artificial budget limits mid-task while simpler or idle agents leave tens of thousands of tokens stranded and unutilized.
3. **Absence of Real-Time Utility-Aware Scheduling**: Traditional distributed throughput throttlers and static token managers treat all agents uniformly without regard for real-time semantic convergence, Shannon entropy stability, or task utility.

An in-process, zero-external-dependency, zero-token-cost mathematical rebalancer is required to continuously evaluate agent utility and dynamically re-allocate token and turn quotas across distributed swarms in $<50\mu\text{s}$ per turn.

## Decision
We implement an in-process, deterministic **Adaptive Dynamic Quota Rebalancer** (`AdaptiveQuotaRebalancer`) across the Python and TypeScript SDKs:

1. **Composite Agent Utility Function ($U_a$)**:
   - For agent $a$ at turn $t$, evaluates real-time telemetry:
     - Semantic Drift: $\Delta_a \in [0.0, 1.0]$ (from Seam 34 / ADR 0045)
     - Vocabulary Compression Ratio: $\text{VCR}_a \in [0.0, 1.0]$ (lexical diversity)
     - Shannon Entropy Alignment: $E_a = \max\left(0.0, 1.0 - \frac{|H_a - 0.75|}{0.75}\right)$
     - Fault Rate: $f_a \in [0.0, 1.0]$ (proportion of tool failures or policy violations)
   - Composite Utility Calculus:
     $$U_a = \text{clamp}\left( w_d \cdot (1.0 - \Delta_a) + w_v \cdot \text{VCR}_a + w_e \cdot E_a - w_f \cdot f_a, 0.0, 1.0 \right)$$
     with default weights $w_d = 0.45$, $w_v = 0.25$, $w_e = 0.20$, and $w_f = 0.10$.

2. **Four-Tier Operational Classification**:
   - `BOOSTED` / `CONVERGING`: $U_a \ge 0.70$ (eligible for surge quota grants from reclaimed reserve).
   - `NOMINAL`: $0.40 \le U_a < 0.70$ (maintains standard baseline quota).
   - `THROTTLED`: $0.20 \le U_a < 0.40$ (quota trimmed to safety floor to prevent rogue drain).
   - `QUARANTINED`: $U_a < 0.20$ or tripped (all unspent quota reclaimed to buffer; execution blocked).

3. **Dynamic Quota Re-balancing Calculus**:
   - Total Fleet Quota: $Q_{\text{total}}$ (tokens / turns / budget).
   - Reclaimed Buffer: $Q_{\text{buffer}}$ aggregated from throttled, inactive, or quarantined workers.
   - For active non-quarantined agents:
     $$q_a^* = q_{\text{floor}} + Q_{\text{distributable}} \cdot \frac{U_a^\kappa}{\sum_{j \in \text{active}} U_j^\kappa}$$
     where $\kappa = 1.5$ is the sensitivity exponent and $q_{\text{floor}}$ guarantees a non-zero operational safety floor.
   - Surge Grants: Agents in `BOOSTED` tier reaching $>85\%$ utilization automatically receive surge slices from $Q_{\text{buffer}}$ without expanding global fleet ceilings.

4. **Integration with Fleet and Circuit Breakers**:
   - Hooks into `FleetCircuitCoordinator` and span ingestion pipelines.
   - Enforces per-agent dynamic quotas, throwing `AgentQuotaExceededException` when a throttled agent exceeds its dynamic quota while allowing converging agents to proceed.

5. **Dual-SDK Parity (TypeScript)**:
   - Full Node 22 type-stripping compliant implementation in `@swarmwatch/sdk/src/rebalancer.ts`:
     - `AdaptiveQuotaRebalancer`, `AgentTier`, `AgentAllocation`, `RebalancePlan`.
   - Exported in `packages/sdk-typescript/src/index.ts`.

6. **REST Control Plane, CLI & Web Console**:
   - REST Endpoints:
     - `GET /api/v1/swarms/{swarm_id}/rebalancer`: Dynamic allocations, utility scores, tiers, and reclaimed pool.
     - `POST /api/v1/swarms/{swarm_id}/rebalancer/rebalance`: Trigger immediate rebalancing epoch.
     - `POST /api/v1/swarms/{swarm_id}/rebalancer/policy`: Configure weights, sensitivity, and safety floors.
   - CLI Subcommands:
     - `swarmwatch rebalance status <swarm_id>`: Formatted tabular summary of agent quotas and tiers.
     - `swarmwatch rebalance run <swarm_id>`: Trigger immediate rebalance.
   - Web Console:
     - Add `#rebalancerBadge` in `#fleetBar` of `dashboard.html`.
     - 100% compliant with Impeccable Craft Floor (0 anti-patterns `[]`).

## Consequences

### Positive
- Prevents rogue or divergent agents from exhausting the fleet-wide token budget.
- Automatically directs surplus tokens to high-performing, converging agents.
- Operates entirely in-process with zero external LLM API calls ($<50\mu\text{s}$ per turn).
- Dual-SDK parity across Python and TypeScript.

### Negative
- Requires maintaining agent-level turn metrics alongside fleet-level aggregates.
- Extremely rapid task changes may require short rebalancing epoch windows (e.g., every 3-5 turns).
