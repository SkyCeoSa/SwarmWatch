# 03. Project Policies & Engineering Standards

All contributors and AI pairing sessions must strictly adhere to the following non-negotiable engineering policies.

---

## 1. Deep Module Architecture Policy (Matt Pocock / John Ousterhout)

1. **Maximize Leverage**: Modules must feature small, cohesive interfaces that hide substantial implementation complexity.
2. **Clean Seams**: Alterations in behavior (swapping SQLite for ClickHouse, or adding a new framework adapter) must occur at defined seams without modifying caller code.
3. **No Pass-Through Wrappers**: Reject shallow classes that merely rename methods without adding abstraction depth or locality.

---

## 2. Zero-Token Cost Guarantee

1. **Zero LLM API Calls for Observability**: The circuit breaker, entropy calculator, and telemetry tracer must never call external LLM APIs to perform evaluations.
2. **Sub-Microsecond Caller Latency**: Emitting a span or capturing context must take $<1\mu s$ on the agent thread. Synchronous HTTP network calls during agent execution are prohibited.

---

## 3. High-Density Craft Standard & UI Rules

Every UI surface must conform to the high-density craft standard standard:
1. **Palette**: Strictly dark slate console (`#080B11` canvas, `#0E1422` surface, `#1D2840` border, `#38BDF8` accent).
2. **Zero Forbidden Elements**:
   - Zero dark glows or blurred neon box-shadows (`box-shadow: 0 0 20px rgba(...)`).
   - Zero emojis in UI code, status messages, or CLI logs (`\U0001f441`, etc.).
   - Zero pulsing colored dots or novelty animations.
   - Zero gradient text or decorative eyebrows.
   - Zero nested cards (cards inside cards).
3. **Typography & Layout**:
   - Minimum font size: 12px.
   - Tabular numerals (`font-variant-numeric: tabular-nums`) for all metrics and durations.
   - Code and payloads formatted in monospace (`JetBrains Mono`, `SF Mono`).
4. **Mechanical Detector Compliance**: `python -c "from swarmwatch.acceptance import global_acceptance_harness; print(global_acceptance_harness.audit_dashboard_artifact('dashboard.html'))"` must return 0 findings (`[]`).

---

## 4. Security & Data Privacy Policy

1. **Permanent Secret Masking**: Any string matching known API key patterns (`sk-[a-zA-Z0-9_-]{15,}`, `bearer ...`) or keys containing sensitive names (`password`, `secret`, `api_key`) must be permanently redacted before disk or network transmission.
2. **Local-First Default**: By default, telemetry is persisted strictly to local SQLite on the developer's workstation. No external telemetry is sent without explicit user opt-in.
3. **Cycle-Safe Serialization**: Complex cyclic objects in agent memory must be resolved cleanly without infinite recursion or stack overflows.

---

## 5. Testing & Zero-Regression Policy

1. **100% Green Test Suite**: All unit and integration tests under `packages/sdk-python/tests/` must pass before any commit is pushed.
2. **TDD Discipline**: When fixing bugs or introducing features, create a failing (Red) test first, implement the minimal solution (Green), and refactor cleanly.
