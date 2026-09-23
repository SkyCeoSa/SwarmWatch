# ADR 0042: Autonomous Counterfactual Swarm Optimization & Self-Healing Policy Controller

## Status
Accepted

## Date
2026-09-23

## Context
As LLM agent swarms scale across distributed multi-cluster fleets, anomalies such as recurring tool invocation failures (e.g., downstream API timeouts, malformed SQL queries), localized Shannon entropy collapses (repetitive response loops), and unpredicted token budget acceleration can rapidly degrade fleet reliability and exhaust cloud budgets before human operators can intervene.

Prior to Seam 31, SwarmWatch relied on reactive circuit breakers (`EntropyCircuitBreaker`) and declarative policy engines (`PolicyEngine`) that either warn or trip the entire swarm session upon violation. However:
1. **Coarse-Grained Trips**: Tripping an entire swarm because a single non-critical tool is failing stops productive multi-agent workflows unnecessarily.
2. **Manual Remediation Burden**: Operators had to manually inspect post-mortem reports, update policy YAML/JSON configurations, and restart or fork the swarm session.
3. **Lack of Counterfactual Insight**: Operators lacked quantified visibility into what specific alternative configurations (e.g. model down-routing, tool blacklisting, quota tightening) would have saved in dollars and tokens prior to enacting changes.

There is a critical need for an embedded, autonomous, zero-token cost self-healing controller that continuously inspects causal spans, detects emerging failure modes in real time, computes counterfactual optimization savings, and enacts reversible, surgical remediation actions without manual intervention.

## Decision
We implement the **Autonomous Counterfactual Swarm Optimization & Self-Healing Policy Controller** (`SwarmSelfHealingController`) within SwarmWatch:

1. **Deterministic Anomaly Detection Engine**:
   - `REPEATED_TOOL_FAILURES`: Tracks consecutive failures per tool/agent; triggers when failure count $\ge N$ (default 3) or error rate $\ge 40\%$.
   - `ENTROPY_VOLATILITY`: Detects abrupt Shannon entropy drops ($H < 0.35$) indicating repetitive generative degeneration.
   - `BUDGET_RUNAWAY`: Detects token consumption velocity exceeding projected session quota limits.
   - `CASCADE_ERROR`: Identifies when an upstream agent's failure propagates errors across downstream dependency spans.

2. **Reversible Remediation Action Taxonomy (`SelfHealingActionType`)**:
   - `BLACKLIST_TOOL`: Dynamically appends a failing tool to the agent's `prohibited_tools` in the active `PolicyEngine`, isolating the fault without halting the swarm.
   - `ROUTE_MODEL_FALLBACK`: Dynamically routes an agent from an expensive or erratic model (e.g., `gpt-4o`) to a lighter, stable fallback model (e.g., `gpt-4o-mini`).
   - `CLAMP_QUOTA`: Preemptively clamps max turns or token limits to prevent runaway spend before circuit trips occur.
   - `AUTO_RESET_ON_STABILIZATION`: Automatically clears circuit trip conditions after a configurable probation window if agent metrics remain nominal.
   - `INJECT_BACKOFF`: Introduces artificial throttling delay to relieve downstream rate limits.

3. **In-Process Counterfactual Optimization Calculus**:
   - Computes dollar savings $S_{\$} = \Delta_{\text{tokens}} \times \text{cost\_per\_token} + \text{avoided\_cost}$.
   - Evaluates alternative execution trajectories to quantify counterfactual efficiency.
   - Operates with 0 external LLM API calls, strictly using local mathematical algorithms.

4. **100% Reversible Rollbacks**:
   - Every `SelfHealingAction` captures `original_state`, allowing instantaneous automated or operator-driven rollback via `POST /api/v1/self-healing/rollback` or CLI `swarmwatch heal rollback`.

5. **Dual-SDK Parity & Node 22 Type-Stripping Compliance**:
   - Implemented in Python (`packages/sdk-python/swarmwatch/selfhealing.py`) and TypeScript (`packages/sdk-typescript/src/selfhealing.ts`).

## Consequences

### Positive
- **Reduced Swarm Downtime**: Isolated tool blacklisting and model fallback preserve swarm execution without full session terminations.
- **Automated Cost Control**: Counterfactual optimization clamps runaways proactively, saving thousands of tokens per incident.
- **Zero-Token Overhead**: No remote LLM calls required for anomaly detection or rule synthesis.
- **Full Operator Auditing**: Every autonomous intervention is recorded, queryable via REST/CLI, and fully reversible.

### Negative / Trade-offs
- Overly aggressive blacklisting of critical tools could degrade agent task completion if fallback alternatives are unavailable. Mitigation: configurable probation windows and manual rollback overrides.
