# ADR 0016: Interactive Chaos & Loop Simulator in Developer Console

## Status
Accepted

## Date
2026-09-16

## Context
Validating multi-agent systems and safety circuit breakers in staging or local development presents unique challenges:
1. Triggering subtle failure modes (such as lexical entropy collapse, recursive tool loops, or conversational deadlocks) by querying commercial LLMs is unpredictable, slow, and expensive.
2. Developers evaluating SwarmWatch require immediate, tactile proof that `EntropyCircuitBreaker`, real-time WebSocket telemetry streaming, Prometheus metric gauges, and webhook alerts respond accurately when multi-agent swarms malfunction.
3. Automated end-to-end and browser-based smoke tests require a deterministic, local mechanism to inject synthetic failure conditions without network dependencies or API keys.

## Decision
We implement the **Interactive Chaos & Loop Simulator** as a deep module (`packages/sdk-python/swarmwatch/chaos.py`), exposing a backend control plane API (`POST /api/v1/chaos/inject`) and an austere UI panel in the Developer Web Console (`dashboard.html`):

1. **`ChaosSimulator` Deep Module (`swarmwatch/chaos.py`)**:
   - Encapsulates synthetic scenario generators for 4 canonical multi-agent failure patterns:
     - `entropy_collapse`: Simulated code-refactoring ping-pong between `BackendEngineer` and `SecurityAuditor` that collapses lexical Shannon entropy below the safety threshold ($H_{norm} \le 0.35$).
     - `infinite_tool_recursion`: Agent repeatedly calling `PythonCodeRunner` with recursive indentation or syntax errors, exceeding the tool repetition ceiling.
     - `budget_quota_exhaustion`: Agent triggering rapid token consumption exceeding the configured dollar/token budget limit.
     - `conversational_deadlock`: AutoGen-style conversational pleasantry ping-pong ("Thank you" / "You're welcome") with high token similarity ($>0.85$).
   - Yields `SwarmSpan` events sequentially, evaluates each turn against `EntropyCircuitBreaker`, captures state snapshots via `TimeMachine`, and computes prevented token/cost metrics.

2. **Backend Control Plane (`server.py`)**:
   - Exposes `POST /api/v1/chaos/inject?scenario={scenario}&swarm_id={swarm_id}&step_delay={delay}`.
   - Emits spans into `StorageAdapter`, broadcasts updates via `WebSocketManager`, and records Prometheus counters (`swarmwatch_spans_total`, `swarmwatch_circuit_trips_total`, `swarmwatch_tokens_saved_total`).

3. **Developer Web Console Integration (`dashboard.html`)**:
   - Built to high-density craft standard standards (`#080B11` palette, tabular nums, zero emojis, zero neon glows, zero nested cards).
   - Provides quick-action scenario triggers with real-time visual feedback, circuit trip banners, and direct URL query support (`?chaos={scenario}`) for automated browser inspection.

## Consequences

### Positive
- One-click deterministic verification of circuit breakers and observability dashboards.
- 100% zero external LLM token cost.
- Full end-to-end integration across storage, WebSocket streaming, Prometheus metrics, and UI.
- Deterministic test fixtures for CI/CD and local development.

### Negative
- Synthetic scenarios are simulated models of agent failure; developers must still configure realistic production thresholds for domain-specific swarms.
