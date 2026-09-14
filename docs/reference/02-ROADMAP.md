# 02. Project Roadmap & Milestones

This document charts the development milestones of **SwarmWatch**, from local foundational architecture to enterprise multi-tenant cloud operations.

---

## Milestone Status Overview

- [x] **Phase 1: Core Engine & In-Process Telemetry** (Complete)
- [x] **Phase 2: Mathematical Circuit Breaker & Safe Time-Machine** (Complete)
- [x] **Phase 3: Real-Time Console & Ecosystem Adapters** (Complete)
- [x] **Phase 4: Scale, Columnar Storage & Enterprise Alerts** (Complete)
- [x] **Phase 5: Multi-Language SDKs & OpenTelemetry Federation** (Complete)
- [x] **Phase 6: Multi-Tenant Architecture & Distributed Fleet Sync** (Complete)
- [ ] **Phase 7: SwarmWatch Cloud SaaS & Marketplace** (Future)

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

### Phase 4: Scale, Columnar Storage & Enterprise Alerts (Delivered)
- **ClickHouse Storage Adapter**: Columnar ingestion engine for teams processing millions of spans daily.
- **Counterfactual Budget Quotas**: Session-level cost caps with dynamic `dollars_saved` and `tokens_saved` calculation.
- **Enterprise Webhook Notifications**: Real-time Slack, Discord, and PagerDuty alerts triggered when circuits trip.
- **Dynamic Policy UI**: Real-time slider adjustments for entropy thresholds and window sizes.

### Phase 5: Multi-Language SDKs & OpenTelemetry Federation (Delivered)
- **OpenTelemetry GenAI Semantic Conventions Exporter**:
  - Direct conversion of `SwarmSpan` to OpenTelemetry GenAI Semantic Conventions (`gen_ai.system`, `gen_ai.agent.name`, `gen_ai.usage.total_tokens`, `gen_ai.swarmwatch.entropy_score`).
  - Standard OTLP/HTTP traces payload formatting (`/v1/traces`) with zero heavy C-extensions or gRPC overhead.
  - Background async exporter with retry semantics and graceful batch shutdown.
- **First-Class Native TypeScript SDK (`@swarmwatch/sdk`)**:
  - Zero external runtime dependencies in Node.js 22+ ESM.
  - Normalized Shannon entropy ($H_{norm}$) and sliding-window lexical Jaccard similarity engine.
  - In-process non-blocking ring buffer queue with async batch flusher.
  - Quota budget enforcement (`max_turns`, `token_budget`, `cost_budget`) and counterfactual savings calculation.
  - `SafeStateSerializer` with circular reference breaking, sensitive credential masking, and deterministic SHA-256 state hashing.
  - Seamless context propagation via `AsyncLocalStorage` for parent-child agent spans.
  - Ecosystem adapters for Vercel AI SDK (`swarmWatchVercelOnFinish`) and LangChain.js (`SwarmWatchLangChainCallbackHandler`).
- **Comprehensive Dual Test Suite**:
  - 45 Python unit/integration tests passing (100%).
  - 42 TypeScript unit/integration tests passing (100%).
  - 87 total automated tests running 100% locally with zero external network or GitHub Actions dependencies.

### Phase 6: Multi-Tenant Architecture & Distributed Fleet Sync (Delivered)
- **Cloud-Agnostic Multi-Tenancy Hierarchy**:
  - `Organization`: Top-level enterprise tenant boundary.
  - `Project`: Scoped workspaces with isolated DAG branches and telemetry histories.
  - `ApiKey`: Cryptographic bearer tokens (`sw_live_...` / `sw_test_...`) with fine-grained capability scopes.
  - `TenantContext`: Contextual propagation across async workers and API endpoints.
- **Zero-Knowledge Cryptographic Key Management**:
  - SHA-256 one-way hashing with constant-time verification (`hmac.compare_digest`).
  - In-memory fast-path verification cache.
  - Raw secret keys displayed strictly once at creation time.
- **Row-Level Tenant Isolation**:
  - SQLite and ClickHouse adapters partitioned by `project_id` across spans, snapshots, and circuit events.
  - Fast composite indexing on `(project_id, swarm_id, branch_id, start_time)`.
- **FastAPI Authentication Middleware & Control Plane Endpoints**:
  - Header parsing for `Authorization: Bearer <key>` and `X-API-Key: <key>`.
  - Full CRUD REST API for `/api/v1/organizations`, `/api/v1/projects`, `/api/v1/keys`.
  - Non-breaking fallback for single-tenant local runs to `default-project`.
- **Distributed Fleet Circuit Coordinator (`FleetCircuitCoordinator`)**:
  - Synchronizes cumulative token, turn, and cost metrics across distributed worker threads and processes.
  - Instantaneous trip broadcasting to halt runaway swarm agents across parallel replicas.


