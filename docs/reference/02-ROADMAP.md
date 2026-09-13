# 02. Project Roadmap & Milestones

This document charts the development milestones of **SwarmWatch**, from local foundational architecture to enterprise multi-tenant cloud operations.

---

## Milestone Status Overview

- [x] **Phase 1: Core Engine & In-Process Telemetry** (Complete)
- [x] **Phase 2: Mathematical Circuit Breaker & Safe Time-Machine** (Complete)
- [x] **Phase 3: Real-Time Console & Ecosystem Adapters** (Complete)
- [ ] **Phase 4: Scale, Columnar Storage & Enterprise Alerts** (In Progress)
- [ ] **Phase 5: Multi-Language SDKs & OpenTelemetry Federation** (Planned)
- [ ] **Phase 6: SwarmWatch Cloud SaaS & Marketplace** (Future)

---

## Phase Breakdown

### Phase 1: Core Engine & In-Process Telemetry (Delivered)
- **Zero-Latency In-Memory Ring Buffer**: Thread-safe producer-consumer queue (`queue.Queue`) maintaining $<1\mu s$ caller return.
- **SQLite WAL Storage Adapter**: Thread-local connection pool enforcing `PRAGMA journal_mode=WAL` and transactional batching.
- **Core Schema**: Immutable `SwarmSpan`, `StateSnapshot`, and `CircuitEvent` models.
- **TDD Test Suite**: Storage lifecycle, sentinel draining, and concurrent emission verification.

### Phase 2: Mathematical Circuit Breaker & Safe Time-Machine (Delivered)
- **Zero-Token Circuit Breaker**: Normalized Shannon entropy and token-level Jaccard similarity engine running in $<0.5ms$.
- **Two-Step Intervention**: Soft context warning hints progressing to hard `SwarmCircuitBreakException` halts.
- **Safe State Serializer**: Deep cycle detection, custom object fallback, and regex masking of sensitive API keys (`sk-...`, Bearer tokens).
- **DAG State Branching**: Non-destructive `TimeMachine.snapshot` and `TimeMachine.step_back` state rewinds.

### Phase 3: Real-Time Console & Ecosystem Adapters (Delivered)
- **Impeccable Developer Console**: Austere `#080B11` slate theme, zero glows, tabular numerals, and 0 anti-patterns on mechanical detector.
- **WebSocket Control Plane**: Live span streaming with 350ms pacing and remote command handling (`CMD_PAUSE`, `CMD_FORK_BRANCH`).
- **Framework Adapters**:
  - `SwarmWatchCallbackHandler` for LangGraph & LangChain.
  - `SwarmWatchCrewListener` for CrewAI.
- **CLI Subcommand**: `swarmwatch ui --port 8765` for instant local execution.

### Phase 4: Scale, Columnar Storage & Enterprise Alerts (Active Horizon)
- **ClickHouse Storage Adapter**: Columnar ingestion engine for teams processing millions of spans daily.
- **Counterfactual Budget Quotas**: Session-level cost caps with dynamic `dollars_saved` and `tokens_saved` calculation.
- **Enterprise Webhook Notifications**: Real-time Slack, Discord, and PagerDuty alerts triggered when circuits trip.
- **Dynamic Policy UI**: Real-time slider adjustments for entropy thresholds and window sizes.

### Phase 5: Multi-Language SDKs & OpenTelemetry Federation
- **TypeScript / Node.js SDK**: Native `@swarmwatch/sdk` for Vercel AI SDK, Mastra, and LangChain.js.
- **OpenTelemetry Native Exporter**: Bi-directional mapping between `SwarmSpan` and OTel GenAI Semantic Conventions.
