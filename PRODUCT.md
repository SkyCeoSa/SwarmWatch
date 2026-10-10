# Product


## Platform

web

## Users

AI Engineers, Multi-Agent Systems Architects, and Software Engineering Leads building and operating autonomous LLM swarms (using LangGraph, CrewAI, AutoGen, or custom agent protocols).

## Product Purpose

SwarmWatch provides real-time causal observability, runaway loop circuit-breaking, and interactive time-machine state branching for autonomous AI agent swarms. It eliminates costly semantic infinite loops and enables deterministic diagnosis of agent communication failures.

## Positioning

Unlike passive tracing tools (LangSmith, Arize Phoenix, Helicone) that only log token expenditure post-mortem, SwarmWatch actively guards swarms during live runtime:
1. Detects cyclic deadlocks and repetitive reasoning using Shannon entropy and causal graph analysis.
2. Trips circuit breakers to halt runaway loops before API budgets are destroyed.
3. Provides non-destructive time-machine execution branching to rewind and replay swarms from any intermediate turn.

## Operating Context

Developer workstation and production monitoring consoles. Dark-mode, high information density, tabular monospace metrics, zero-latency inspection. Evaluated alongside tools like Vercel, Linear, and Datadog.

## Capabilities and Constraints

- Capabilities:
  - Causal span ingestion via HTTP and WebSocket (/api/spans, /ws).
  - Native framework adapters: LangGraph Callback Handler, CrewAI Step/Task Callbacks.
  - Runtime Circuit Breaker: Shannon entropy anomaly detection, max turn thresholds, cost protection.
  - Interactive Developer Console: Turn-by-turn causal dialogue stream, inspector drawer, state diff viewer.
  - Interactive Swarm Simulation: Built-in deterministic software engineering swarm scenario (ProductArchitect, BackendEngineer, CodeReviewer, SecurityAuditor).
- Constraints:
  - Strictly software engineering and autonomous agent systems domain. Zero financial/trading domain contamination.
  - Zero toy canvas gimmicks or decorative noise; pure information density and clarity.
  - WCAG AAA contrast, accessible DOM, responsive layout.

## Brand Commitments

- Aesthetic: Austere, minimal, quiet authority, high-density developer console.
- Tone: Technical, precise, objective, unembellished.
- Palette: Deep charcoal-slate foundation (#080B11, #0E1422), 1px structural hairline borders (#1D2840), precision Instrument Cyan accent (#38BDF8), clear status semantics (Green #10B981, Amber #F59E0B, Crimson #EF4444).

## Evidence on Hand

- Python SDK with 100% passing test suite (40 unit & integration tests).
- FastAPI dashboard server with real-time SSE / WebSocket streaming.
- Canonical software engineering swarm demo scripts in examples/multi_agent_demo.py and examples/langgraph_crewai_demo.py.

## Product Principles

1. **Content is the Hero**: The developer focus is on what agents communicated, the causal order, token burn, and loop detection--not decorative UI chrome.
2. **Quiet Power in Simplicity**: Flawless visual hierarchy, dual-font discipline (Inter for UI, JetBrains Mono for data/payloads), zero visual slop.
3. **Actionable Intervention**: A circuit breaker alert must immediately expose the exact token/dollar savings and offer a one-click time-machine fork.
4. **Honesty and Precision**: Real metrics, real payloads, tabular numbers, zero fake marketing cheer.
