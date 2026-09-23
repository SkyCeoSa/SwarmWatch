# ADR 0043: Distributed Multi-Agent Root-Cause Causal Inference Engine

## Status
Accepted

## Date
2026-09-23

## Context
In distributed multi-agent systems, operational failures, circuit breaker trips, and runaway loops rarely originate at the location where the failure manifests. For example, a downstream agent (such as a `Coder` or `Executor`) might repeatedly fail to execute generated code on turn 7 or trip the Shannon entropy circuit breaker. However, the true underlying root cause almost invariably occurred several turns earlier in the conversational DAG—such as an upstream `Researcher` or `Planner` injecting corrupted schema information, hallucinated constraints, or failing a tool invocation on turn 2.

Prior to Seam 32, SwarmWatch's incident diagnosis attributed failure solely to the triggering span (`trigger_span_id`, `source_agent`, `target_agent`). This had several critical limitations:
1. **Symptom Blaming**: The agent that tripped the breaker was blamed for the outage, obscuring the upstream culprit.
2. **Context Poisoning Invisibility**: Downstream agents that consumed flawed or hallucinated outputs from upstream peers continued to propagate poisoned context, amplifying error cascading without detection.
3. **Suboptimal Time-Machine Resumption**: When an operator wanted to fork or resume from a past turn using the Time-Machine, identifying which exact turn to fork from required manual forensic inspection.

There is a vital need for an embedded, zero-token cost causal inference engine (`CausalInferenceEngine`) based on Structural Causal Models (SCM) and Judea Pearl's Do-Calculus principles that automatically tracks ancestral DAG lineages, isolates the true root cause, scores context poisoning contagion, and recommends the exact Time-Machine fork point for seamless recovery.

## Decision
We implement the **Distributed Multi-Agent Root-Cause Causal Inference Engine** (`CausalInferenceEngine`) within SwarmWatch:

1. **Structural Causal Model (SCM) & Ancestral DAG Traversal**:
   - Reconstructs the directed acyclic graph of agent execution from `SwarmSpan` causality (`parent_span_id`, chronological turn indexing, and message passing topology).
   - Performs backward ancestral graph traversal ($Anc(S_{\text{symptom}})$) from any observed symptom, failure, or circuit halt.

2. **Causal Attribution & Do-Calculus Counterfactual Heuristic**:
   - Scores each ancestral node using multi-factor causal attribution:
     - Error/exception status flags and failure payloads ($E(v)$)
     - Abrupt Shannon entropy collapses ($\Delta H(v)$)
     - Repetition and high similarity surges
     - Topological distance attenuation (giving causal precedence to root triggers over symptom cascades)
   - Evaluates counterfactual intervention ($do(V = \text{nominal})$): identifying the earliest ancestral span whose non-occurrence would have prevented downstream propagation.

3. **Context Poisoning Score ($CPS \in [0.0, 1.0]$)**:
   - Quantifies the degree of tainted context contamination injected into downstream agent prompts.
   - Identifies the origin agent, poison classification (`INVALID_JSON`, `TOOL_FAILURE`, `ENTROPY_COLLAPSE`, `SCHEMA_CORRUPTION`), and the list of contaminated downstream agents.

4. **Automated Time-Machine Fork Point Recommendation**:
   - Automatically computes `recommended_fork_turn = root_cause_turn - 1`.
   - Directly equips the SwarmWatch Time-Machine with the optimal turn for non-destructive DAG branching.

5. **Zero-Token Cost Guarantee**:
   - 100% deterministic graph mathematics running in-process in $<5\text{ms}$ with zero external LLM API calls.

6. **Dual-SDK Parity & Node 22 Type-Stripping Compliance**:
   - Implemented in Python (`packages/sdk-python/swarmwatch/causal_inference.py`) and TypeScript (`packages/sdk-typescript/src/causal_inference.ts`).

## Consequences

### Positive
- **Accurate Fault Attribution**: Eliminates false attribution to downstream victim agents.
- **Actionable Time-Machine Resumption**: Operators and autonomous agents know precisely which turn to fork from.
- **Zero-Token Overhead**: Pure in-process graph analysis without external LLM inference costs.
- **Context Poisoning Visibility**: Detailed breakdown of how corrupted tokens spread across agent prompts.

### Negative / Trade-offs
- Deeply nested DAGs with hundreds of cross-agent edges require efficient adjacency indexation to maintain $<5\text{ms}$ evaluation times.
