# 05. System Architecture & Seams

This document details the architectural topology, defined seams, and runtime dataflow of SwarmWatch.

---

## 1. System Topology Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                   Multi-Agent Application Space                        │
│                                                                        │
│   LangGraph Graph           CrewAI Crew          Custom Python Agents  │
│          │                       │                         │           │
│   SwarmWatchCallback    SwarmWatchListener        @swarmwatch.agent    │
└──────────┬───────────────────────┬─────────────────────────┬───────────┘
           │                       │                         │
           ▼                       ▼                         ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        SwarmWatch Client Core                          │
│                                                                        │
│   [Seam 1] Telemetry Ingestion (<1µs non-blocking queue.Queue)         │
│   [Seam 2] Mathematical Circuit Breaker (<0.5ms Shannon Entropy)       │
│   [Seam 3] State Time-Machine (Cycle detection, Redaction, GZIP)       │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼ Background Worker Thread
┌────────────────────────────────────────────────────────────────────────┐
│                   Storage & Control Plane Tier                         │
│                                                                        │
│   [Seam 4] Storage Adapter Interface                                   │
│            ├── SQLiteStorageAdapter (WAL Mode, Thread-Local Pools)     │
│            └── ClickHouseStorageAdapter (Columnar Enterprise Scale)    │
│                                                                        │
│   [Seam 5] Real-Time FastAPI Server & WebSocket Manager                │
│            ├── REST Ingestion: POST /api/v1/spans, POST /api/v1/simulate│
│            ├── State Forks: POST /api/v1/branches/fork                 │
│            └── WebSocket Streaming: /ws/swarm/{swarm_id}               │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼ WebSocket Stream
┌────────────────────────────────────────────────────────────────────────┐
│               [Seam 6] Impeccable Developer Console                    │
│   - Live Execution Stream (350ms Cadence)                              │
│   - Real-Time Shannon Entropy & Metric Cards                           │
│   - Time-Machine Turn Inspector & State Forking                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Defined Seams Description

1. **Telemetry Ingestion Seam**:
   - Primitives: `SwarmClient.span(...)`, `@swarmwatch.agent()`, `SwarmClient.sync_state(...)`.
   - Invariant: Zero synchronous disk or network I/O on caller threads.
2. **Entropy Circuit Breaker Seam**:
   - Primitives: `EntropyCircuitBreaker.evaluate(message, agent_id, swarm_id) -> CircuitDecision`.
   - Invariant: Zero LLM API calls, zero token burn.
3. **State Time-Machine Seam**:
   - Primitives: `TimeMachine.snapshot(...)`, `TimeMachine.step_back(...)`.
   - Invariant: Secret redaction and cycle-breaking before serialization.
4. **Storage Adapter Seam**:
   - Primitives: `StorageAdapter` abstract base class.
   - Invariant: Thread safety; SQLite connections never shared across threads.
5. **Ecosystem Adapter Seam**:
   - Primitives: `SwarmWatchCallbackHandler` (LangGraph/LangChain), `SwarmWatchCrewListener` (CrewAI), `SwarmWatchLlamaIndexHandler` (LlamaIndex), `SwarmWatchAutoGenListener` (Microsoft AutoGen), `SwarmWatchDSPyTracker` (Stanford DSPy), and `SwarmWatchSemanticKernelFilter` (Microsoft Semantic Kernel).
   - Invariant: Public official interfaces only; zero monkey-patching; proactive Shannon entropy circuit breaking.

6. **Real-Time Observability & Chaos Injection Seam**:
   - Primitives: `create_app(storage)` FastAPI factory, `WebSocketManager`, `ChaosSimulator`, and `dashboard.html`.
   - Invariant: Graceful disconnection handling, fallback replay, deterministic zero-cost synthetic failure injection, and strict Impeccable UI craft standards.
7. **Package Distribution Integrity Seam**:
   - Primitives: `PackageReleaseValidator.validate_wheel(...)`, `validate_tarball(...)`, `validate_distribution(...)`.
   - Invariant: Zero external dependencies; multi-format validation (Wheel and npm) with bit-level integrity and secret scanning before release.
8. **Enterprise Metrics & Prometheus Telemetry Seam**:
   - Primitives: `PrometheusMetricsRegistry` and FastAPI `GET /metrics` exposition route.
   - Invariant: Sub-microsecond atomic counters; zero external monitoring dependencies; strict Prometheus 0.0.4 text format compliance.
9. **Visual Causal Topology Seam**:
   - Primitives: `CausalTopologyGraph.build_from_spans(spans) -> CausalTopology`, `GET /api/v1/topology`, and vector SVG DAG layout in `dashboard.html`.
   - Invariant: Non-blocking deterministic cycle detection; pure SVG rendering with zero heavy visualization library dependencies; negligible CPU utilization (<0.05%).
10. **Enterprise Security & Secret Redaction Seam**:
    - Primitives: `SensitiveDataMasker.mask_text(text)`, `SensitiveDataMasker.mask_payload(payload)`, and automatic ingestion sanitization in `TelemetryTransport.emit_span()`.
    - Invariant: Pre-compiled regex catalog executing in $<0.5\mu s$; raw secrets (cloud keys, database DSNs, JWTs, PEM keys, credit cards, PII) NEVER reach disk storage or live client streams; zero LLM token cost.
11. **Incident Post-Mortem & Financial ROI Engine Seam**:
    - Primitives: `IncidentReportGenerator.generate_report(swarm_id, format)`, CLI `swarmwatch report --swarm-id <id>`, and REST endpoint `GET /api/v1/swarms/{id}/report`.
    - Invariant: Zero-token deterministic forensic analysis; exact counterfactual dollar/token ROI computation; root-cause agent pair attribution and entropy trajectory diagnosis; $<5ms$ execution.
12. **Declarative Guardrail Policy Engine & Policy DSL Seam (Dual-SDK Parity / ADR 0021 & ADR 0022)**:
    - Primitives (Python): `PolicyEngine`, `GuardrailPolicy`, `RuleConfig`, `DeclarativeRule`, `RuleCondition`, `PolicyEvaluationResult`, `load_policy(path_or_content)`, `GET /api/v1/policies`, `POST /api/v1/policies`.
    - Primitives (TypeScript): `PolicyEngine`, `SafePolicyParser`, `GuardrailPolicy`, `RuleConfig`, `DeclarativeRule`, `RuleCondition`, `PolicyEvaluationResult`, `EntropyCircuitBreaker(policyEngine)`.
    - Invariant: Zero-dependency YAML/JSON parsing; sub-microsecond ($<1\mu s$) deterministic rule evaluation; role-based inheritance with fallback defaults; zero external LLM tokens; zero-downtime hot-reloading; strict parity between Python and Node.js runtimes.
13. **Enterprise Incident Alerting & Dead-Letter Dispatcher Seam (Dual-SDK Parity / ADR 0007 & ADR 0023)**:
    - Primitives (Python): `WebhookNotifier`, `WebhookConfig`, `DeadLetterRecord`, `format_webhook_payload`, `GET /api/v1/alerts/dlq`, `POST /api/v1/alerts/dlq/replay`, `POST /api/v1/alerts/test`.
    - Primitives (TypeScript): `WebhookNotifier`, `WebhookConfig`, `DeadLetterRecord`, `formatWebhookPayload`, `defaultHttpSender`.
    - Invariant: Non-blocking sub-microsecond queue enqueue on caller threads; native PagerDuty Events API v2 with deduplication, Slack Block Kit, and Discord embeds; in-memory bounded Dead-Letter Queue (DLQ) with retry and replay; zero remote token burn.
14. **Visual Time-Machine Controls & State Branching Seam (Dual-SDK Parity / ADR 0024 & ADR 0025)**:
    - Primitives (Python & Console): `#timeMachineBar` (Step-Back, Step-Forward, Scrubber, Branch Selector, Fork Action), `#forkModal`, `TimeMachine` (`serializer.py`), `GET /api/v1/swarms/{swarm_id}/snapshots`, `POST /api/v1/branches/fork` (with antecedent span replication), `StorageAdapter.get_snapshots_for_swarm()`, `StorageAdapter.list_branches()`, WebSocket `branch_forked` event.
    - Primitives (TypeScript): `TimeMachine` (`timemachine.ts`), `ForkBranchResult`, `StorageSink` extensions (`getSnapshot`, `getSnapshotsForSwarm`, `forkBranch`, `listBranches`), `InMemoryStorageSink`, `HttpStorageSink`, and `SwarmClient` contextual helpers (`snapshot`, `stepBack`, `forkBranch`, `listBranches`, `listSnapshots`).
    - Invariant: Non-destructive historical state navigation with zero latency; antecedent spans preserved and replicated into new branch namespace; in-app non-blocking toasts (`showToast()`) replacing modal popups; single-key keyboard navigation (`[`, `]`, `f`, `Esc`); Impeccable Craft Floor design standards (#080B11 palette, tabular numerals, zero emojis, zero neon glows); 100% parity across Python and TypeScript SDKs.
15. **Live Fork Resumption Runner Seam (Dual-SDK Parity / ADR 0026)**:
    - Primitives (Python): `ForkRunner` (`runner.py`), `ForkResumptionContext`, `ForkResumptionResult`, `client.resume_from_fork()`, `client.resume_from_fork_sync()`, REST endpoint `POST /api/v1/branches/{branch_id}/resume`, and WebSocket `branch_resumed` broadcast.
    - Primitives (TypeScript): `ForkRunner` (`runner.ts`), `ForkResumptionContext`, `ForkResumptionResult`, `client.resumeFromFork()`, `resumeFromFork()`.
    - Invariant: Scoped context propagation across `contextvars` and `AsyncLocalStorage`; nested `@agent` and `span()` calls automatically inherit the active branch namespace; boundary `fork_resumption` span maintains causal lineage to historical antecedent turns; sub-millisecond execution overhead (<500µs in Python, <400µs in TypeScript); zero remote LLM token usage; 100% dual-SDK parity.
