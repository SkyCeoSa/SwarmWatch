# ADR 0006: Counterfactual Token & Budget Savings Estimation Model

## Status
Accepted

## Context
When the Entropy Circuit Breaker halts an autonomous multi-agent swarm in a repetitive loop, developers and engineering leaders need immediate visibility into the financial and compute damage averted. Because the loop was intercepted early, future runaway token burn has not occurred in reality. Without a clearly defined calculation model, users question how "$4.83 / 241,500 tokens prevented" is computed.

## Decision
We adopt a Counterfactual Savings model with three tiered estimation strategies:
1. **Configured Loop Ceiling (Default)**:
   When an agent or swarm defines a `max_iterations` or loop timeout ceiling N_max, the averted turns are:
   Delta_turns = N_max - N_halt
   Prevented tokens are computed as Delta_turns * T_bar, where T_bar is the empirical rolling average token count per turn in the current swarm context.
2. **Deterministic Task Budget Quota**:
   If the user supplies a session cost budget cap B_session, protected budget is reported as B_protected = B_session - B_consumed.
3. **Canonical Benchmark Baseline (Demo / Simulation)**:
   For synthetic benchmark scenarios and simulations, metrics reflect the empirical unconstrained failure run (50 turns at 5,366 tokens/turn = 241,500 tokens / $4.83 at standard frontier model pricing).

## Consequences
- **Positive**: Provides clear, defensible ROI metrics on dashboard and Slack/webhook alerts.
- **Positive**: Eliminates confusion around predicting the future by tying savings directly to user-configured execution bounds.
- **Negative**: Counterfactual numbers represent conservative ceiling estimates rather than exact deterministic logs of events that never happened.
