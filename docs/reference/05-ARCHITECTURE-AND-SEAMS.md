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
16. **State Snapshot Diff Inspector Seam (Dual-SDK Parity & Console / ADR 0027)**:
    - Primitives (Python): `compute_state_diff()` (`serializer.py`), `StateDiffReport`, `DiffEntry`, `DiffSummary`, REST endpoint `GET /api/v1/swarms/{swarm_id}/snapshots/diff`.
    - Primitives (TypeScript): `computeStateDiff()` (`timemachine.ts`), `StateDiffReport`, `DiffEntry`, `DiffSummary`.
    - Primitives (Console): Inspector tab bar (`#tabPayload`, `#tabSnapshot`, `#tabDiff`), `#diffViewContainer`, `#diffTargetSelector`, `#diffModeSelector` (Unified vs Split), delta counter badges (`#diffBadgeAdded`, `#diffBadgeModified`, `#diffBadgeRemoved`), filter chips (`#diffFilterAll`, `#diffFilterChanged`, `#diffFilterRedacted`).
    - Invariant: 100% deterministic recursive object comparison; sub-millisecond execution dispatch; deep secret mask awareness (`is_redacted`); SHA-256 fingerprint tracking; Impeccable Craft Floor design standards (#080B11 canvas, #0E1422 surfaces, #1D2840 borders, monospace tabular numerals, zero emojis, zero neon glows); zero remote LLM token usage.
17. **Industrial Production Monitoring Stack & Grafana Preset Seam (ADR 0028)**:
    - Primitives: `docker-compose.monitoring.yml`, `deploy/prometheus/prometheus.yml`, `deploy/grafana/provisioning/datasources/datasources.yml`, `deploy/grafana/provisioning/dashboards/dashboards.yml`, `deploy/grafana/dashboards/swarmwatch-overview.json`.
    - Invariant: Zero-configuration single-command orchestration (`docker compose -f docker-compose.monitoring.yml up -d`); automatic Prometheus datasource registration and dashboard provisioning; dark-mode Grafana panels styled strictly to SwarmWatch design tokens (#080B11 canvas, tabular numerals, instrument cyan, emerald green, amber, crimson); zero external cloud dependencies.
18. **Automated Alert Route Webhook Testing & Synthetic Simulation Pipeline Seam (ADR 0029)**:
    - Primitives: `SyntheticAlertReceiver` (`synthetic_alert.py`), `AlertSimulationPipeline`, `AlertSimulationReport`, `RouteSimulationResult`, CLI `swarmwatch test-alerts`.
    - Invariant: Zero-dependency local mock HTTP server on ephemeral port; deterministic schema validation for Slack Block Kit, Discord Embeds, and PagerDuty Events v2; programmable fault injection (503, timeouts) and zero-loss Dead-Letter Queue (DLQ) recovery; live swarm circuit trip linkage with counterfactual ROI verification; zero remote cloud tokens or external network calls.
19. **High-Frequency Telemetry Ingestion Micro-Benchmark & Flamegraph Profile Suite Seam (ADR 0030)**:
    - Primitives: `TelemetryProfiler` (`profiler.py`), `IngestionBenchmarkConfig`, `LatencyPercentiles`, `BenchmarkMemoryStats`, `IngestionBenchmarkReport`, CLI `swarmwatch benchmark`.
    - Invariant: Empirical proof of $>10,000$ spans/sec throughput and sub-microsecond enqueue latency ($<1.0\mu s$ median); zero memory leaks verified via bounded RSS heap analysis; zero-dependency standard collapsed stack flamegraph export; 100% parity across Python and TypeScript runtimes.
20. **Cloud-Native Kubernetes & Helm Topology Seam (ADR 0031)**:
    - Primitives: Helm 3 package bundle `deploy/helm/swarmwatch/` (`Chart.yaml`, `values.yaml`, `deployment.yaml`, `service.yaml`, `ingress.yaml`, `pvc.yaml`, `hpa.yaml`, `configmap.yaml`, `secret.yaml`, `servicemonitor.yaml`).
    - Invariant: Zero-cloud deterministic template rendering; dual-storage topology (Standalone SQLite WAL vs Distributed ClickHouse); non-root security context (UID 10001) with capability drops; automated HPA autoscaling and Prometheus ServiceMonitor metrics scraping at 5s intervals.
21. **Live Terminal User Interface (TUI) & Headless Cluster Observability Console Seam (ADR 0032)**:
    - Primitives: `TerminalMonitor` (`tui.py`), `AnsiFrameBuffer`, `SwarmSummaryView`, `SpanStreamView`, `CircuitStatusView`, CLI `swarmwatch top`.
    - Invariant: Pure standard library implementation with zero third-party terminal dependencies; dual data sourcing via HTTP REST or local SQLite storage fallback; dual operational modes (interactive alternate screen buffer vs snapshot `--once` mode); sub-second refresh latency; Impeccable monospace console aesthetics (#080B11 canvas, tabular numerals, zero emojis); zero external LLM tokens.
22. **Distributed Redis Circuit Breaker & Fleet State Synchronization Seam (ADR 0033 Dual-SDK Parity)**:
    - Primitives (Python): `RedisCircuitCoordinator` (`redis_circuit.py`), `RedisFleetBackend`, `RESPRedisClient`, `MockRedisClient`, REST endpoints `GET /api/v1/fleet/redis/metrics`, `POST /api/v1/fleet/redis/trip`, `POST /api/v1/fleet/redis/reset`, CLI `swarmwatch fleet`.
    - Primitives (TypeScript): `RedisCircuitCoordinator` (`redis_circuit.ts`), `RedisFleetBackend`, `MockRedisClient`, `RedisClientInterface`.
    - Invariant: Ultra-low-latency distributed coordination backbone; atomic quota ledgers (`HINCRBY`, `HINCRBYFLOAT`) preventing multi-worker race conditions; sub-millisecond ($<1\text{ms}$) trip broadcast propagation over Redis Pub/Sub; pure standard library RESP2 client eliminating mandatory third-party pip dependencies; full parity across Python and TypeScript SDKs; 100% deterministic zero-token guarantee.
23. **Distributed Redis Fleet & Cluster Monitor Widget Seam (ADR 0034)**:
    - Primitives (Console): `RedisFleetWidget` (`dashboard.html`), `#fleetBar`, `#btnFleetToggle`, `#fleetWorkersList`, `#fleetTurnsMeter`, `#fleetTokensMeter`, `#fleetCostMeter`, `#fleetTripAlert`, `#btnFleetTrip`, `#btnFleetReset`.
    - Primitives (Server): WebSocket broadcast integration (`ws_manager`) emitting `fleet_metric_update`, `fleet_trip_broadcast`, and `fleet_state_reset` events from `RedisCircuitCoordinator`.
    - Invariant: Live visualization of cluster backend connection, active worker roster, and cumulative atomic quota meters; immediate full-console trip alert banner on broadcast receipt; remote emergency trip and state reset control plane actions; strict adherence to Impeccable Craft Floor (#080B11 palette, tabular-nums, zero emojis, zero neon glows); zero-token cost guarantee.
24. **Distributed Redis Lock & Leader Election Seam (ADR 0035 Dual-SDK Parity)**:
    - Primitives (Python): `RedisDistributedLock` (`redis_lock.py`), `RedisLeaderElector`, `ServerNodeRole`, REST endpoints `GET /api/v1/cluster/leader`, `POST /api/v1/cluster/leader/step-down`, CLI `swarmwatch cluster`.
    - Primitives (TypeScript): `RedisDistributedLock` (`redis_lock.ts`), `RedisLeaderElector`, `ServerNodeRole`.
    - Invariant: Atomic mutual exclusion and lease renewals; split-brain prevention for multi-server background jobs; sub-second automatic standby failover; active cluster node roster tracking in Redis; dual-SDK parity; 100% deterministic local execution.
25. **S3 / Google Cloud Storage Streaming Telemetry Archival & Cold Storage Tier Seam (ADR 0036 Dual-SDK Parity)**:
    - Primitives (Python): `ObjectStorageClient` (`archival.py`), `MockObjectStorageClient`, `S3ObjectStorageClient`, `ColdStorageArchiver`, `ArchiveManifest`, `ArchiveRecord`, REST endpoints `/api/v1/storage/archive/{id}`, `/api/v1/storage/restore/{id}`, `/api/v1/storage/archives`, CLI `swarmwatch archive`.
    - Primitives (TypeScript): `ObjectStorageClient` (`archival.ts`), `MockObjectStorageClient`, `ColdStorageArchiver`, `ArchiveManifest`, `ArchiveRecord`.
    - Invariant: Zero-dependency streaming gzip compression; cryptographic SHA-256 manifest verification; standard time-partitioned cloud data lake hierarchy; bi-directional archive and restoration round-trip; leader-coordinated singleton background execution; 100% local deterministic offline verification.
26. **Enterprise Multi-Factor Authentication (MFA / TOTP) & Strict RBAC Engine Seam (ADR 0037 Dual-SDK Parity)**:
    - Primitives (Python): `Role`, `Permission`, `ROLE_PERMISSIONS`, `has_permission`, `UserAccount`, `TotpEngine` (`rbac.py`), `TenantContext` extensions (`tenant.py`), SQLite storage tables `users` & `audit_events` (`storage.py`), server dependencies `require_permission` and endpoints `/api/v1/auth/users`, `/api/v1/auth/mfa/enroll`, `/api/v1/auth/mfa/verify`, `/api/v1/auth/mfa/challenge`, `/api/v1/auth/audit` (`server.py`), CLI subcommands `swarmwatch mfa` and `swarmwatch rbac` (`cli.py`).
    - Primitives (TypeScript): `Role`, `Permission`, `ROLE_PERMISSIONS`, `hasPermission`, `base32Encode`, `base32Decode`, `TotpEngine` (`rbac.ts`), exported via `index.ts`.
    - Primitives (Console): Operator identity & MFA widget (`#fleetRoleBadge`, `#fleetMfaBadge`, `#btnMfaModal`, `#mfaModal`), step-up authentication challenge modal, dynamic `X-MFA-Code` header attachment for destructive control plane operations (`dashboard.html`).
    - Invariant: Zero external cryptographic library dependencies (pure Python `hashlib`/`hmac` & Node.js `node:crypto`); standard RFC 6238 TOTP (30s time-step, $\pm 1$ drift window, RFC 3548 Base32); single-use SHA-256 hashed recovery backup codes; 4-tier hierarchical RBAC matrix (`ADMIN`, `OPERATOR`, `AUDITOR`, `VIEWER`); Step-Up challenge enforcement for destructive endpoints; zero remote LLM token usage; 100% dual-SDK parity.
27. **OpenTelemetry OTLP Ingestion Gateway Seam (HTTP / Protobuf & JSON / ADR 0038 Dual-SDK Parity)**:
    - Primitives (Python): `OtelTraceParser`, `ProtobufWireDecoder`, `extract_otlp_attributes`, `otlp_span_to_swarm_span`, `OtlpIngestionStats` (`otlp.py`), server endpoints `POST /v1/traces`, `POST /api/v1/otlp/traces`, `GET /v1/traces`, `GET /api/v1/otlp/stats` (`server.py`), CLI subcommands `swarmwatch otlp ingest` and `swarmwatch otlp validate` (`cli.py`).
    - Primitives (TypeScript): `toOtelGenAiSpan`, `formatOtlpTracesPayload`, `parseOtlpTracesPayload`, `OtlpTraceExporter` (`otlp.ts`), exported via `index.ts`.
    - Primitives (Console): `#otlpGatewayBadge` and `#otlpSpansCount` in `#fleetBar` (`dashboard.html`).
    - Invariant: Official OpenTelemetry 1.0+ HTTP trace specification conformance (`POST /v1/traces`); zero-dependency pure standard library protobuf wire format decoding; transparent translation of OpenTelemetry GenAI Semantic Conventions (`gen_ai.*`); real-time interception via `EntropyCircuitBreaker` and `RedisCircuitCoordinator`; Prometheus metrics emission (`record_span_ingested`); WebSocket broadcast; zero remote LLM token usage; 100% dual-SDK parity.
28. **Enterprise WebPush & Server-Sent Events (SSE) Real-Time Alerts Seam (ADR 0039 Dual-SDK Parity)**:
    - Primitives (Python): `SseEventManager`, `SseEvent` (`sse.py`), `WebPushEngine`, `VapidKeyManager`, `PushSubscription`, `PushMessage`, `WebPushDispatcher` (`webpush.py`), SQLite storage table `push_subscriptions` (`storage.py`), server endpoints `GET /api/v1/events/stream`, `GET /api/v1/swarms/{id}/events/stream`, `GET /api/v1/push/vapid-public-key`, `POST /api/v1/push/subscribe`, `POST /api/v1/push/unsubscribe`, `POST /api/v1/push/test`, `GET /api/v1/push/subscriptions` (`server.py`), CLI subcommands `swarmwatch events listen` and `swarmwatch push keys|test|subscriptions` (`cli.py`).
    - Primitives (TypeScript): `SwarmEventSource`, `SseEvent` (`sse.ts`), `VapidSigner`, `PushClient` (`webpush.ts`), `client.listenEvents()` (`client.ts`), exported via `index.ts`.
    - Primitives (Console): `#sseStatusBadge` in `#fleetBar`, `#btnPushNotify` in header action group (`dashboard.html`).
    - Invariant: W3C standard Server-Sent Events (`text/event-stream`) streaming with automatic 15s keep-alive pings and `Last-Event-ID` ring-buffer replay; zero-dependency pure Python NIST P-256 (secp256r1) integer arithmetic and ES256 VAPID JWT signing; asynchronous non-blocking push dispatch with automatic cleanup of expired subscriptions (410 Gone); zero external LLM token usage; 100% dual-SDK parity.
29. **Kernel-Level Network Socket LLM Interception Gateway Seam (ADR 0040 Dual-SDK Parity)**:
    - Primitives (Python): `EbpfSocketGateway` (`ebpf.py`), `SocketRule`, `SocketStreamDecoder`, `SocketIntervention`, `TcpRstEmitter`, `MockEbpfEngine`, server endpoints `GET /api/v1/ebpf/status`, `GET/POST/DELETE /api/v1/ebpf/rules`, `POST /api/v1/ebpf/simulate` (`server.py`), CLI subcommand `swarmwatch ebpf` (`cli.py`).
    - Primitives (TypeScript): `SocketRule`, `SocketStreamDecoder`, `EbpfSocketGateway`, `MockEbpfEngine` (`ebpf.ts`), exported via `index.ts`.
    - Primitives (Console): `#ebpfGatewayBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Un-bypassable zero-trust network boundary enforcement; zero-dependency stream protocol dissection extracting HTTP/1.1 and streaming SSE payloads from raw TCP streams; physical connection severance (`RST` / `shutdown`) upon loop or budget breaches; cross-platform architecture with standard library socket proxy fallback and in-memory mock engine; 100% deterministic local offline testing; zero remote LLM token usage; dual-SDK parity.
30. **Multi-Cluster High-Availability Raft Consensus Engine Seam (ADR 0041 Dual-SDK Parity)**:
    - Primitives (Python): `RaftConsensusEngine` (`raft.py`), `RaftNodeRole`, `RaftLogEntry`, `RaftStateMachine`, `RequestVoteRpc`, `AppendEntriesRpc`, `MockRaftCluster`, server endpoints `GET /api/v1/raft/status`, `POST /api/v1/raft/propose`, `POST /api/v1/raft/rpc/request-vote`, `POST /api/v1/raft/rpc/append-entries`, `POST /api/v1/raft/step-down` (`server.py`), CLI subcommand `swarmwatch raft` (`cli.py`).
    - Primitives (TypeScript): `RaftNodeRole`, `RaftLogEntry`, `RaftStateMachine`, `RaftConsensusEngine`, `MockRaftCluster` (`raft.ts`), exported via `index.ts`.
    - Primitives (Console): `#raftConsensusBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Pure standard-library Raft consensus; monotonic terms, randomized election timeouts (150-300ms), and 50ms periodic heartbeats; replicated state machine coordinating atomic fleet quotas and cluster circuit trips without external Redis; strict quorum safety ($N/2 + 1$); dual-transport (in-memory bus & REST RPC); 100% deterministic local offline testing; zero remote LLM token usage; dual-SDK parity.
31. **Autonomous Counterfactual Swarm Optimization & Self-Healing Policy Controller Seam (ADR 0042 Dual-SDK Parity)**:
    - Primitives (Python): `SwarmSelfHealingController` (`selfhealing.py`), `AnomalyType`, `SelfHealingActionType`, `SelfHealingAction`, `SelfHealingPolicyConfig`, `CounterfactualEvaluation`, server endpoints `GET /api/v1/self-healing/status`, `GET /api/v1/self-healing/actions`, `POST /api/v1/self-healing/remediate`, `POST /api/v1/self-healing/rollback`, `POST /api/v1/self-healing/toggle` (`server.py`), CLI subcommand `swarmwatch heal` (`cli.py`).
    - Primitives (TypeScript): `AnomalyType`, `SelfHealingActionType`, `SelfHealingAction`, `SwarmSelfHealingController` (`selfhealing.ts`), exported via `index.ts`.
    - Primitives (Console): `#selfHealingBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: In-process real-time telemetry observation; zero-token mathematical anomaly detection (`REPEATED_TOOL_FAILURES`, `ENTROPY_VOLATILITY`, `BUDGET_RUNAWAY`, `CASCADE_ERROR`, `LATENCY_SPIKE`); surgical reversible remediation actions (`BLACKLIST_TOOL`, `ROUTE_MODEL_FALLBACK`, `CLAMP_QUOTA`, `AUTO_RESET_ON_STABILIZATION`, `INJECT_BACKOFF`); 100% state restoration on rollback; dual-SDK parity; 100% local deterministic offline testing; zero remote LLM token usage.
32. **Distributed Multi-Agent Root-Cause Causal Inference Engine Seam (ADR 0043 Dual-SDK Parity)**:
    - Primitives (Python): `CausalInferenceEngine` (`causal_inference.py`), `CausalPathNode`, `ContextPoisoningMetric`, `RootCauseAttribution`, `CausalInferenceResult`, server endpoints `GET /api/v1/swarms/{id}/root-cause`, `GET /api/v1/swarms/{id}/causal-path`, `GET /api/v1/swarms/{id}/context-poisoning` (`server.py`), CLI subcommand `swarmwatch rootcause` (`cli.py`).
    - Primitives (TypeScript): `CausalPathNode`, `ContextPoisoningMetric`, `RootCauseAttribution`, `CausalInferenceResult`, `CausalInferenceEngine` (`causal_inference.ts`), exported via `index.ts`.
    - Primitives (Console): `#rootCauseBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Structural Causal Model (SCM) analysis; backward ancestral DAG graph traversal ($Anc(S_{\text{symptom}})$); Judea Pearl Do-Calculus counterfactual reasoning; context poisoning scoring ($CPS \in [0.0, 1.0]$); automated Time-Machine fork turn determination (`recommended_fork_turn`); 100% deterministic local offline testing; zero remote LLM token usage; dual-SDK parity.
33. **Ultra-High-Throughput gRPC Streaming Ingestion Gateway Seam (ADR 0044 Dual-SDK Parity)**:
    - Primitives (Python): `GrpcStreamingGateway` (`grpc_gateway.py`), `GrpcMessageFramer`, `GrpcStatusCode`, `GrpcStreamServer`, server endpoints `POST /swarmwatch.v1.TelemetryService/StreamSpans`, `POST /swarmwatch.v1.TelemetryService/IngestBatch`, `POST /opentelemetry.proto.collector.trace.v1.TraceService/Export`, `GET /api/v1/grpc/status`, `POST /api/v1/grpc/simulate` (`server.py`), CLI subcommand `swarmwatch grpc` (`cli.py`).
    - Primitives (TypeScript): `GrpcMessageFramer`, `GrpcStatusCode`, `GrpcStreamClient` (`grpc.ts`), exported via `index.ts`.
    - Primitives (Console): `#grpcGatewayBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: 5-byte binary gRPC wire framing (`[compression(1B)][length(4B BE)][payload]`); zero external C-extensions or `grpcio` requirements; in-flight circuit breaking directly on binary stream chunks returning `RESOURCE_EXHAUSTED` (8); dual gateway topology (ASGI endpoint & pure asyncio TCP server on port 50051); >50,000 spans/sec throughput; 100% local deterministic offline testing; zero remote LLM token usage; dual-SDK parity.
34. **Embedding-Free Vector Semantic Drift & Hallucination Divergence Detector Seam (ADR 0045 Dual-SDK Parity)**:
    - Primitives (Python): `SemanticDriftDetector` (`drift.py`), `MinHashSigner`, `SemanticDriftProfile`, `DivergenceStatus`, server endpoints `GET /api/v1/swarms/{id}/drift`, `POST /api/v1/swarms/{id}/drift/evaluate` (`server.py`), CLI subcommand `swarmwatch drift` (`cli.py`).
    - Primitives (TypeScript): `MinHashSigner`, `SemanticDriftProfile`, `SemanticDriftDetector` (`drift.ts`), exported via `index.ts`.
    - Primitives (Console): `#driftBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: 64-bit universal MinHash LSH and lexical n-gram shingling; pairwise Jaccard similarity estimation in $<15\mu\text{s}$; vocabulary compression ratio ($VCR = |V_{\text{unique}}| / |V_{\text{total}}|$) tracking echo-chamber collapse; composite semantic drift ($\Delta_{\text{drift}} = 1.0 - (0.6 \cdot J(t, t_0) + 0.4 \cdot J(t, t-1))$); circuit breaker trip on sustained drift ($\Delta_{\text{drift}} > 0.72$); 100% deterministic local offline testing; zero external LLM token usage; dual-SDK parity.
35. **Adaptive Dynamic Quota Re-balancing across Distributed Fleets Seam (ADR 0046 Dual-SDK Parity)**:
    - Primitives (Python): `AdaptiveQuotaRebalancer` (`rebalancer.py`), `AgentTier`, `AgentAllocation`, `RebalancePlan`, `RebalancePolicy`, server endpoints `GET /api/v1/swarms/{id}/rebalancer`, `POST /api/v1/swarms/{id}/rebalancer/rebalance`, `POST /api/v1/swarms/{id}/rebalancer/policy` (`server.py`), CLI subcommand `swarmwatch rebalance` (`cli.py`).
    - Primitives (TypeScript): `AgentTier`, `AgentAllocation`, `RebalancePlan`, `RebalancePolicy`, `AdaptiveQuotaRebalancer` (`rebalancer.ts`), exported via `index.ts`.
    - Primitives (Console): `#rebalancerBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Composite agent utility calculation ($U_a \in [0.0, 1.0]$) unifying semantic convergence, lexical diversity (VCR), Shannon entropy stability, and fault resilience; dynamic 4-tier classification (`BOOSTED`, `NOMINAL`, `THROTTLED`, `QUARANTINED`); non-linear sensitivity exponent re-allocation ($\kappa = 1.5$); reclaimed reserve buffer pooling with surge slice grants to converging workers; non-blocking in-process execution ($<50\mu\text{s}$ per turn); zero external LLM token expenditure; dual-SDK parity.
36. **Real-Time Topological Graph Clustering & Dynamic Agent Coalitions Seam (ADR 0047 Dual-SDK Parity)**:
    - Primitives (Python): `TopologicalCoalitionEngine` (`coalition.py`), `AgentCoalition`, `TopologicalClusterResult`, `NodeCentrality`, server endpoints `GET /api/v1/swarms/{id}/coalitions`, `POST /api/v1/swarms/{id}/coalitions/cluster` (`server.py`), CLI subcommand `swarmwatch coalition` (`cli.py`).
    - Primitives (TypeScript): `AgentCoalition`, `TopologicalClusterResult`, `NodeCentrality`, `TopologicalCoalitionEngine` (`coalition.ts`), exported via `index.ts`.
    - Primitives (Console): `#coalitionBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Directed causal interaction graph analysis; weighted interaction strength ($W(u, v) = \sum (1 + \ln(1 + \text{tokens}))$); degree centrality and bottleneck transit detection ($B(v)$); Newman-Girvan modularity community clustering ($Q \ge 0.30$); autonomous coalition formation ($\ge 0.50$ density) with designated Coalition Leaders; in-flight conversational deadlock decoupling; non-blocking execution ($<50\mu\text{s}$ per turn); zero external LLM token expenditure; dual-SDK parity.
37. **Zero-Copy Shared-Memory Ring Buffer IPC Gateway Seam (ADR 0048 Dual-SDK Parity)**:
    - Primitives (Python): `ShmRingBuffer`, `ShmRingBufferWriter`, `ShmRingBufferReader`, `ShmIpcGateway`, `ShmRingHeader` (`shm_ring.py`), server endpoints `GET /api/v1/shm/status`, `POST /api/v1/shm/flush`, `POST /api/v1/shm/reset` (`server.py`), CLI subcommand `swarmwatch shm` (`cli.py`).
    - Primitives (TypeScript): `ShmRingBufferWriter`, `ShmRingBufferReader`, `ShmIpcGateway` (`shm_ring.ts`), exported via `index.ts`.
    - Primitives (Console): `#shmBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Zero-copy cross-process telemetry ingestion using memory-mapped ring buffer (`mmap` / POSIX `SharedMemory`); fixed 64-byte binary header (`SWRB`), atomic write/read sequence pointers, and fixed slot framing; sub-microsecond write latency ($<1.0\mu\text{s}$) with zero kernel socket context switches; sustained throughput $>500,000$ spans/sec; ring overflow tracking without blocking caller execution; zero external dependencies; dual-SDK parity.
38. **Differential Privacy & Cryptographic State Anonymization Seam (ADR 0049 Dual-SDK Parity)**:
    - Primitives (Python): `DifferentialPrivacyEngine`, `LaplaceMechanism`, `DifferentialPrivacyPolicy`, `AnonymizedSpanRecord` (`diff_privacy.py`), server endpoints `GET /api/v1/privacy/status`, `POST /api/v1/privacy/anonymize`, `POST /api/v1/privacy/policy` (`server.py`), CLI subcommand `swarmwatch privacy` (`cli.py`).
    - Primitives (TypeScript): `DifferentialPrivacyEngine`, `LaplaceMechanism`, `DifferentialPrivacyPolicy` (`diff_privacy.ts`), exported via `index.ts`.
    - Primitives (Console): `#dpBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Calibrated Laplace noise injection ($\eta \sim \text{Laplace}(0, \Delta f / \epsilon)$) on numeric telemetry (tokens, duration, cost, Shannon entropy) with bounded clamping; deterministic cryptographic HMAC-SHA256 pseudonymization for agent identities and roles preserving causal topology; privacy budget accounting with exhaustion protection; non-blocking execution ($<50\mu\text{s}$ per turn); zero external LLM token expenditure; dual-SDK parity.
39. **Predictive Multi-Agent Failure Forecasting & Early-Warning Entropy Martingales Seam (ADR 0050 Dual-SDK Parity)**:
    - Primitives (Python): `EntropyMartingaleForecaster`, `MartingaleDriftSequence`, `EarlyWarningHorizon`, `ForecastRiskLevel`, `SwarmForecastReport` (`forecast.py`), server endpoints `GET /api/v1/swarms/{id}/forecast`, `POST /api/v1/swarms/{id}/forecast/evaluate`, `POST /api/v1/swarms/{id}/forecast/reset` (`server.py`), CLI subcommand `swarmwatch forecast` (`cli.py`).
    - Primitives (TypeScript): `EntropyMartingaleForecaster`, `ForecastRiskLevel`, `SwarmForecastReport` (`forecast.ts`), exported via `index.ts`.
    - Primitives (Console): `#forecastBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Discrete-time supermartingale convergence testing on turn-to-turn entropy delta sequences ($\Delta H_t$); centered martingale difference tracking ($M_n = \sum (D_t - \mu)$); Azuma-Hoeffding concentration inequality computing exact failure probability ($P_{\text{risk}} \in [0.0, 1.0]$) and early-warning horizon ($T_{\text{forecast}} \in [1, 5]$ turns) 2 to 4 turns before circuit breaker halts; in-process execution ($<30\mu\text{s}$ per turn); zero external LLM token expenditure; dual-SDK parity.
40. **Multi-Agent Byzantine Attack & Prompt Injection Stress Harness Seam (ADR 0051 Dual-SDK Parity)**:
    - Primitives (Python): `ByzantineStressHarness`, `ByzantineAttackType`, `ByzantineFaultProfile`, `ByzantineResilienceReport`, `AgentQuarantinePolicy` (`byzantine.py`), server endpoints `GET /api/v1/swarms/{id}/byzantine/status`, `POST /api/v1/swarms/{id}/byzantine/inject`, `POST /api/v1/swarms/{id}/byzantine/clear` (`server.py`), CLI subcommand `swarmwatch byzantine` (`cli.py`).
    - Primitives (TypeScript): `ByzantineStressHarness`, `ByzantineAttackType`, `ByzantineResilienceReport` (`byzantine.ts`), exported via `index.ts`.
    - Primitives (Console): `#byzantineBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Synthetic adversarial attack injection (`PROMPT_INJECTION`, `STATE_POISONING`, `ENTROPY_JAMMING`, `SYBIL_CONTAGION`); quantitative Byzantine Fault Tolerance (BFT) resilience scoring ($B_{\text{resilience}} \in [0.0, 1.0]$); autonomous agent quarantine isolation removing compromised nodes from causal interaction graphs; in-process execution ($<40\mu\text{s}$ per turn); zero external LLM API calls; dual-SDK parity.
41. **Multi-Agent Dynamic Honeypot & Deception Sandboxing Seam (ADR 0052 Dual-SDK Parity)**:
    - Primitives (Python): `HoneypotManager`, `HoneytokenGenerator`, `DeceptionSandbox`, `HoneypotDiversionRouter`, `ShadowGraph`, `HoneypotReport` (`honeypot.py`), server endpoints `GET /api/v1/swarms/{id}/honeypot/status`, `POST /api/v1/swarms/{id}/honeypot/divert`, `POST /api/v1/swarms/{id}/honeypot/reset` (`server.py`), CLI subcommand `swarmwatch honeypot` (`cli.py`).
    - Primitives (TypeScript): `HoneypotManager`, `DeceptionSandbox`, `HoneypotReport` (`honeypot.ts`), exported via `index.ts`.
    - Primitives (Console): `#honeypotBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Dynamic canary honeytoken generation (`sw_canary_...`, `sw_honey_...`) for credential exfiltration tripwire detection; seamless in-flight diversion routing of compromised or adversarial agents into an isolated in-process shadow interaction graph (`ShadowGraph`); synthetic, realistic mock response synthesis preventing real production state mutation; in-process execution ($<50\mu\text{s}$ per turn); zero external LLM API calls; dual-SDK parity.
42. **Bounded Model Checking & Multi-Agent Invariant Verification Seam (ADR 0053 Dual-SDK Parity)**:
    - Primitives (Python): `BoundedModelChecker`, `AgentTransitionModel`, `VerificationInvariant`, `CounterexampleTrace`, `StateSpaceExplorer`, `VerificationVerdict` (`model_checker.py`), server endpoints `GET /api/v1/swarms/{id}/model-check/status`, `POST /api/v1/swarms/{id}/model-check/verify` (`server.py`), CLI subcommand `swarmwatch verify` (`cli.py`).
    - Primitives (TypeScript): `BoundedModelChecker`, `VerificationInvariant`, `AgentTransitionModel` (`model_checker.ts`), exported via `index.ts`.
    - Primitives (Console): `#modelCheckBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Discrete-state transition modeling ($S = (\text{agent}, \text{turn}, \text{flags}, \text{tokens})$); exhaustive depth-bounded state space graph exploration up to bound $k \in [1, 25]$; formal mathematical verification of 4 canonical invariants (`NO_DEADLOCK`, `NO_UNBOUNDED_CYCLES`, `BOUNDED_BUDGET`, `MUTUAL_EXCLUSION`); deterministic counterexample trace synthesis; sub-millisecond in-process execution ($<1.0\text{ms}$); zero external solver or LLM dependencies; dual-SDK parity.
43. **Autonomous Dynamic Topology Healing & Auto-Patching Seam (ADR 0054 Dual-SDK Parity)**:
    - Primitives (Python): `TopologyAutoHealer`, `TopologyPatch`, `PatchAction`, `HealedModelReport`, `global_topology_healer` (`topology_healer.py`), server endpoints `GET /api/v1/swarms/{id}/topology-heal/status`, `POST /api/v1/swarms/{id}/topology-heal/apply`, `POST /api/v1/swarms/{id}/topology-heal/revert` (`server.py`), CLI subcommand `swarmwatch patch` (`cli.py`).
    - Primitives (TypeScript): `TopologyAutoHealer`, `PatchAction`, `TopologyPatch`, `HealedModelReport`, `globalTopologyHealer` (`topology_healer.ts`), exported via `index.ts`.
    - Primitives (Console): `#topologyHealBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Graph-theoretic automated patch synthesis (`FALLBACK_REDIRECT`, `CYCLE_GATE_BREAK`, `BUDGET_REWIRE`, `MUTEX_MEDIATE`) in response to BMC invariant violations; non-destructive graph rewiring; mandatory immediate secondary BMC reverification proving the patched model satisfies all invariants (`VERIFIED`); fully reversible patch history; in-process execution ($<2.0\text{ms}$); zero remote LLM API calls; dual-SDK parity.
44. **Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking Seam (ADR 0055 Dual-SDK Parity)**:
    - Primitives (Python): `MultiModalInspector`, `MediaModality`, `MediaMetadata`, `ArtifactWatermark`, `WatermarkedArtifact`, `global_multimodal_inspector` (`multimodal.py`), server endpoints `GET /api/v1/swarms/{id}/artifacts`, `POST /api/v1/swarms/{id}/artifacts/inspect`, `POST /api/v1/swarms/{id}/artifacts/watermark`, `POST /api/v1/swarms/{id}/artifacts/verify` (`server.py`), CLI subcommand `swarmwatch artifact` (`cli.py`).
    - Primitives (TypeScript): `MultiModalInspector`, `MediaModality`, `MediaMetadata`, `ArtifactWatermark`, `WatermarkedArtifact`, `globalMultiModalInspector` (`multimodal.ts`), exported via `index.ts`.
    - Primitives (Console): `#artifactBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Pure in-process binary header inspection (<0.1ms per asset) for PNG, JPEG, GIF, WEBP, WAV, MP3, MP4, and PDF; lightweight `MediaMetadata` extraction; deterministic 16-hex character perceptual hash (pHash) for visual media; cryptographic provenance watermarking (`ArtifactWatermark`) with HMAC-SHA256 signatures; sub-millisecond tamper verification ($<20\mu\text{s}$) flagging `tamper_detected = True`; zero remote multimodal LLM calls; dual-SDK parity.

45. **Decentralized Epidemic Gossip Telemetry Mesh Sync Seam (ADR 0056 Dual-SDK Parity)**:
    - Primitives (Python): `MeshNodeStatus`, `MeshNode`, `VectorClock`, `GossipDigest`, `GossipDelta`, `MeshStatusReport`, `GossipMeshManager`, `global_gossip_manager` (`gossip.py`), server endpoints `GET /api/v1/mesh/status`, `GET /api/v1/mesh/peers`, `POST /api/v1/mesh/peers/register`, `POST /api/v1/mesh/gossip/round`, `POST /api/v1/mesh/gossip/sync` (`server.py`), CLI subcommand `swarmwatch mesh` (`cli.py`).
    - Primitives (TypeScript): `MeshNodeStatus`, `MeshNode`, `VectorClock`, `GossipDigest`, `GossipDelta`, `MeshStatusReport`, `GossipMeshManager`, `globalGossipManager` (`gossip.ts`), exported via `index.ts`.
    - Primitives (Console): `#meshBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Decentralized peer-to-peer telemetry propagation across distributed, edge, and air-gapped clusters without central brokers; causal vector clocks (`VectorClock`) tracking event precedence, partial orders, and concurrent branching; dual epidemic gossip protocols (eager rumor mongering with $k=3$ fanout and pairwise anti-entropy digest exchange); deterministic $<50\mu\text{s}$ execution per turn; zero remote LLM API calls; dual-SDK parity.

46. **Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer Seam (ADR 0057 Dual-SDK Parity)**:
    - Primitives (Python): `NodeHealthTier`, `NodeComputeMetrics`, `ComputeCapacityScore`, `VirtualNode`, `ConsistentHashRing`, `ShardRouteResult`, `ShardingStatusReport`, `ComputeAwareBalancer`, `global_compute_balancer` (`sharding.py`), server endpoints `GET /api/v1/sharding/status`, `GET /api/v1/sharding/ring`, `POST /api/v1/sharding/route`, `POST /api/v1/sharding/node-capacity`, `POST /api/v1/sharding/rebalance` (`server.py`), CLI subcommand `swarmwatch shard` (`cli.py`).
    - Primitives (TypeScript): `NodeHealthTier`, `NodeComputeMetrics`, `ComputeCapacityScore`, `VirtualNode`, `ConsistentHashRing`, `ShardRouteResult`, `ShardingStatusReport`, `ComputeAwareBalancer`, `globalComputeBalancer` (`sharding.ts`), exported via `index.ts`.
    - Primitives (Console): `#shardBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Deterministic consistent hash ring with virtual nodes weighted by real-time compute capacity scores ($C_i \in [0.0, 1.0]$); causal session affinity preserving in-memory DAG cache hits; graceful spillover routing to successor replicas when primary nodes congest; sub-millisecond execution ($<25\mu\text{s}$ per routing decision); zero external LLM calls; dual-SDK parity.

47. **Autonomous Semantic Schema Migration & Backward-Compatible State Translation Engine Seam (ADR 0058 Dual-SDK Parity)**:
    - Primitives (Python): `SchemaVersion`, `FieldTransformAction`, `FieldTransformRule`, `MigrationStep`, `MigrationPlan`, `MigrationResult`, `SchemaMigrationEngine`, `global_schema_migration_engine` (`schema_migration.py`), server endpoints `GET /api/v1/schemas/status`, `GET /api/v1/schemas/versions`, `POST /api/v1/schemas/register-migration`, `POST /api/v1/schemas/migrate`, `POST /api/v1/schemas/validate` (`server.py`), CLI subcommand `swarmwatch schema` (`cli.py`).
    - Primitives (TypeScript): `SchemaVersion`, `FieldTransformAction`, `FieldTransformRule`, `MigrationStep`, `MigrationPlan`, `MigrationResult`, `SchemaMigrationEngine`, `globalSchemaMigrationEngine` (`schema_migration.ts`), exported via `index.ts`.
    - Primitives (Console): `#schemaBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: In-process shortest-path migration DAG resolution; deterministic directional field transformations (`RENAME`, `CAST_TYPE`, `ADD_DEFAULT`, `REMOVE_DEPRECATED`, `NEST_FIELDS`, `FLATTEN_FIELDS`, `COMPUTE_DERIVED`); bidirectional reversibility with automatic rollback; guaranteed backward compatibility across Time-Machine step-back and cross-cluster gossip sync; sub-30 microsecond execution ($<30\mu\text{s}$); zero remote LLM API calls; dual-SDK parity.

48. **Dynamic Multi-Agent Epistemic Logic & Knowledge Base State Synchronization Seam (ADR 0059 Dual-SDK Parity)**:
    - Primitives (Python): `EpistemicTruthValue`, `EpistemicStatus`, `PropositionalClaim`, `EpistemicConflict`, `EpistemicKnowledgeBase`, `EpistemicEngine`, `global_epistemic_engine` (`epistemic.py`), server endpoints `GET /api/v1/swarms/{id}/epistemic/status`, `GET /api/v1/swarms/{id}/epistemic/propositions`, `GET /api/v1/swarms/{id}/epistemic/conflicts`, `POST /api/v1/swarms/{id}/epistemic/assert`, `POST /api/v1/swarms/{id}/epistemic/reconcile` (`server.py`), CLI subcommand `swarmwatch epistemic` (`cli.py`).
    - Primitives (TypeScript): `EpistemicTruthValue`, `EpistemicStatus`, `PropositionalClaim`, `EpistemicConflict`, `EpistemicKnowledgeBase`, `EpistemicEngine`, `globalEpistemicEngine` (`epistemic.ts`), exported via `index.ts`.
    - Primitives (Console): `#epistemicBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Formal modal epistemic logic ($S5/KD45$) tracking individual knowledge assertions ($K_i \phi$), mutual knowledge ($E_G \phi$), and common knowledge ($C_G \phi$); in-process contradiction detection ($<50\mu\text{s}$) identifying conflicting premises ($K_i \phi \land K_j \neg\phi$) and calculating Epistemic Dissonance Score ($D \in [0.0, 1.0]$); deterministic AGM-style belief revision primitives (expansion, contraction, revision) resolving conflicts without external LLM judges; sub-50 microsecond execution; zero remote LLM API calls; dual-SDK parity.

49. **Zero-Knowledge Causal Proof Generation & Cryptographic Audit Engine Seam (ADR 0060 Dual-SDK Parity)**:
    - Primitives (Python): `ProofCommitmentType`, `SpanCommitmentLeaf`, `MerkleAuditProof`, `CausalAuditReceipt`, `CausalProofAccumulator`, `ZkProofEngine`, `global_zk_proof_engine` (`zk_proof.py`), server endpoints `GET /api/v1/swarms/{id}/proof/root`, `POST /api/v1/swarms/{id}/proof/generate`, `POST /api/v1/swarms/{id}/proof/verify`, `GET /api/v1/swarms/{id}/proof/manifest` (`server.py`), CLI subcommand `swarmwatch proof` (`cli.py`).
    - Primitives (TypeScript): `ProofCommitmentType`, `SpanCommitmentLeaf`, `MerkleAuditProof`, `CausalAuditReceipt`, `CausalProofAccumulator`, `ZkProofEngine`, `globalZkProofEngine` (`zk_proof.ts`), exported via `index.ts`.
    - Primitives (Console): `#zkProofBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Balanced binary Merkle tree accumulator computing session root anchors over span commitment leaves; zero-knowledge audit proofs verifying turn inclusion, safety circuit breaker trips, and causal DAG lineage without disclosing private prompt or message payloads; sub-millisecond proof generation ($<500\mu\text{s}$) and verification ($<20\mu\text{s}$); zero external LLM API calls; dual-SDK parity.

50. **Canonical Causal Encoding & Byte-Deterministic Contract Engine Seam (ADR 0061 Dual-SDK Parity)**:
    - Primitives (Python): `CanonicalCodec`, `CanonicalDomainTag`, `CanonicalEncodingError` (`canonical.py`), server endpoints `POST /api/v1/codec/canonicalize`, `POST /api/v1/codec/hash`, CLI subcommand `swarmwatch codec canonicalize|hash` (`cli.py`).
    - Primitives (TypeScript): `CanonicalCodec`, `CanonicalDomainTag`, `CanonicalEncodingError` (`canonical.ts`), exported via `index.ts`.
    - Primitives (Console): `#codecBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: RFC 8785 (JCS) aligned canonical serialization guaranteeing byte-for-byte cross-runtime equality between Python and TypeScript; deterministic lexicographical key sorting, strict float and negative-zero normalization, whitespace stripping, and unescaped UTF-8 preservation; domain-separated cryptographic hashing ($H_v(\text{domain\_tag}, x) = \text{SHA256}(\text{domain\_tag} \parallel \text{0x00} \parallel C_v(x))$); fail-closed validation rejecting circular references and non-finite numbers; sub-5 microsecond execution ($<5\mu\text{s}$); zero remote LLM API calls; dual-SDK parity.

51. **Unified Safety Authority & Linearizable Execution Permit Engine Seam (ADR 0062 Dual-SDK Parity)**:
    - Primitives (Python): `PermitStatus`, `AuthorityState`, `SafetyPermit`, `SafetyAuthority`, `global_safety_authority` (`safety_authority.py`), server endpoints `POST /api/v1/safety/reserve`, `POST /api/v1/safety/commit`, `POST /api/v1/safety/release`, `POST /api/v1/safety/trip`, `POST /api/v1/safety/rearm`, `GET /api/v1/safety/status` (`server.py`), CLI subcommand `swarmwatch safety` (`cli.py`).
    - Primitives (TypeScript): `PermitStatus`, `AuthorityState`, `SafetyPermit`, `SafetyAuthority`, `globalSafetyAuthority` (`safety_authority.ts`), exported via `index.ts`.
    - Primitives (Console): `#safetyBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Single authoritative execution permit engine enforcing linearizable `reserve → commit/release → trip` transitions across all distributed workers and subsystems; guaranteed fail-closed execution where no agent turn or tool invocation starts without an unexpired tenant-scoped permit from the current epoch; monotonic fencing epoch counter invalidating previous permits immediately upon trip; canonical permit hashing (`sw:pmt:v1`); sub-10 microsecond execution ($<10\mu\text{s}$); zero external LLM calls; dual-SDK parity.

52. **Fenced Effect Ledger & Monotonic Idempotency Barrier Seam (ADR 0063 Dual-SDK Parity)**:
    - Primitives (Python): `ClaimStatus`, `EffectClaim`, `EffectRecord`, `EffectReceipt`, `EffectLedger`, `global_effect_ledger` (`effect_ledger.py`), server endpoints `POST /api/v1/effects/claim`, `POST /api/v1/effects/authorize`, `POST /api/v1/effects/complete`, `POST /api/v1/effects/abort`, `GET /api/v1/effects/reconcile`, `GET /api/v1/effects/status` (`server.py`), CLI subcommand `swarmwatch effects` (`cli.py`).
    - Primitives (TypeScript): `ClaimStatus`, `EffectClaim`, `EffectRecord`, `EffectReceipt`, `EffectLedger`, `globalEffectLedger` (`effect_ledger.ts`), exported via `index.ts`.
    - Primitives (Console): `#ledgerBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Compare-and-swap (CAS) transition system with monotonic fencing epochs and idempotency barrier guaranteeing each external side-effect executes and commits at most once; stale leaders holding older epochs are rejected fail-closed (`STALE_EPOCH`); canonical effect receipt signing with domain tag `sw:eff:v1`; sub-15 microsecond execution ($<15\mu\text{s}$); zero external LLM API calls; dual-SDK parity.

53. **Loss-Aware Causal Ingestion & Evidentiary Completeness Rule Seam (ADR 0064 Dual-SDK Parity)**:
    - Primitives (Python): `IngressPriority`, `CompletenessStatus`, `SequenceGap`, `CompletenessReport`, `IngressResult`, `CausalIngress`, `global_causal_ingress` (`causal_ingress.py`), server endpoints `POST /api/v1/ingress/submit`, `POST /api/v1/ingress/drain`, `GET /api/v1/ingress/completeness/{swarm_id}`, `GET /api/v1/ingress/status` (`server.py`), CLI subcommand `swarmwatch ingress` (`cli.py`).
    - Primitives (TypeScript): `IngressPriority`, `CompletenessStatus`, `SequenceGap`, `CompletenessReport`, `IngressResult`, `CausalIngress`, `globalCausalIngress` (`causal_ingress.ts`), exported via `index.ts`.
    - Primitives (Console): `#ingressBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Prioritized, bounded ingestion queue with monotonic per-stream sequence numbering and explicit gap accounting; guarantees 100% preservation of `CRITICAL` safety trips and security interventions under queue saturation; enforces the evidentiary completeness rule where missing sequences or unanchored parent spans permanently degrade trace status to `INCOMPLETE`; sub-microsecond enqueue latency ($<1\mu\text{s}$); zero external LLM API calls; dual-SDK parity.

54. **Durable Audit Anchor & Merkle Audit Receipt Engine Seam (ADR 0065 Dual-SDK Parity)**:
    - Primitives (Python): `RootEpoch`, `MerkleAuditReceipt`, `VerificationResult`, `AuditAnchor`, `global_audit_anchor` (`audit_anchor.py`), server endpoints `POST /api/v1/audit/append-root`, `POST /api/v1/audit/issue-receipt`, `POST /api/v1/audit/verify-receipt`, `GET /api/v1/audit/chain`, `GET /api/v1/audit/status` (`server.py`), CLI subcommand `swarmwatch audit` (`cli.py`).
    - Primitives (TypeScript): `RootEpoch`, `MerkleAuditReceipt`, `VerificationResult`, `AuditAnchor`, `globalAuditAnchor` (`audit_anchor.ts`), exported via `index.ts`.
    - Primitives (Console): `#auditBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Append-only cryptographic hash chain linking root epochs from Genesis ($H_{chain}^{(e)} = \text{SHA256}(H_{chain}^{(e-1)} \parallel e \parallel R_e \parallel \text{tenant\_id})$); historical epochs persist across system crashes and restarts with automatic post-crash chain of custody verification; mathematical Merkle inclusion receipts independently verifiable without revealing adjacent payloads; strictly designated as Merkle audit receipts (not ZK proofs); sub-25 microsecond proof verification ($<25\mu\text{s}$); zero external LLM API calls; dual-SDK parity.

55. **Bounded Verification Semantics & Explicit Counterexample Engine Seam (ADR 0066 Dual-SDK Parity)**:
    - Primitives (Python): `BoundedVerdict`, `VerificationProperty`, `VerificationLimits`, `TransitionStep`, `VerificationReport`, `SafetyVerifier`, `global_safety_verifier` (`safety_verifier.py`), server endpoints `POST /api/v1/verifier/verify`, `POST /api/v1/verifier/check-trace`, `GET /api/v1/verifier/status` (`server.py`), CLI subcommand `swarmwatch verifier` (`cli.py`).
    - Primitives (TypeScript): `BoundedVerdict`, `VerificationProperty`, `VerificationLimits`, `TransitionStep`, `VerificationReport`, `SafetyVerifier`, `globalSafetyVerifier` (`safety_verifier.ts`), exported via `index.ts`.
    - Primitives (Console): `#verifierBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Strict bounded verification semantics eliminating overbroad `VERIFIED` assertions; exploration within bound $k$ returns `BOUNDED_SAFE` with explicit depth, assumptions, and state coverage ratio; early termination under resource limits returns `INDETERMINATE`; complete state-space closure returns `UNBOUNDED_PROVEN`; detected invariant violations return `VIOLATED` with minimal counterexample trace independently sound under `verify_trace`; sub-millisecond execution ($<1.0\text{ms}$); zero external LLM API calls or SAT solver services; dual-SDK parity.

56. **Governed Remediation & Fenced Action State Machine Seam (ADR 0067 Dual-SDK Parity)**:
    - Primitives (Python): `ApprovalTier`, `ProposalStatus`, `BlastRadius`, `RemediationProposal`, `RemediationDecision`, `RemediationResult`, `CompensationResult`, `RemediationGovernor`, `global_remediation_governor` (`remediation_governor.py`), server endpoints `POST /api/v1/remediation/propose`, `POST /api/v1/remediation/authorize`, `POST /api/v1/remediation/apply`, `POST /api/v1/remediation/compensate`, `GET /api/v1/remediation/status` (`server.py`), CLI subcommand `swarmwatch remediation` (`cli.py`).
    - Primitives (TypeScript): `ApprovalTier`, `ProposalStatus`, `BlastRadius`, `RemediationProposal`, `RemediationDecision`, `RemediationResult`, `CompensationResult`, `RemediationGovernor`, `globalRemediationGovernor` (`remediation_governor.ts`), exported via `index.ts`.
    - Primitives (Console): `#remediationBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Strict multi-tiered approval gating and blast radius constraints governing all self-healing mutations; proposals must pass precondition contracts before execution and postcondition invariants after mutation; commits must hold an authoritative monotonic fencing epoch; stale leaders presenting obsolete epochs are rejected fail-closed (`STALE_EPOCH`); every applied remediation possesses an idempotent compensation routine (`compensate`) guaranteeing safe rollback; sub-20 microsecond execution ($<20\mu\text{s}$); zero external LLM API calls; dual-SDK parity.

57. **Partition Reconciliation & Conflict Lattice Seam (ADR 0068 Dual-SDK Parity)**:
    - Primitives (Python): `LatticeVerdict`, `ReconciliationPlan`, `MergeResult`, `AuthorityReconciliationResult`, `MeshReconciler`, `global_mesh_reconciler` (`mesh_reconciler.py`), server endpoints `POST /api/v1/reconciler/diff`, `POST /api/v1/reconciler/merge`, `POST /api/v1/reconciler/reconcile-authority`, `GET /api/v1/reconciler/status` (`server.py`), CLI subcommand `swarmwatch reconciler` (`cli.py`).
    - Primitives (TypeScript): `LatticeVerdict`, `ReconciliationPlan`, `MergeResult`, `AuthorityReconciliationResult`, `MeshReconciler`, `globalMeshReconciler` (`mesh_reconciler.ts`), exported via `index.ts`.
    - Primitives (Console): `#reconcilerBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Deterministic conflict lattice and vector-clock partial ordering safely rejoining partitioned gossip nodes and shard replicas; safety circuit breaker trips are strictly monotonic and can never silently clear upon rejoining (`TRIPPED` dominates `NOMINAL`); concurrent span claims retain provenance metadata (`PROVENANCE_RETAINED`) eliminating silent telemetry drops; linearizable safety authority remains strictly decoupled from eventual-consistency gossip; sub-25 microsecond execution ($<25\mu\text{s}$); zero external LLM API calls; dual-SDK parity.

58. **Privacy Release Ledger & Differential Privacy Accountant Seam (ADR 0069 Dual-SDK Parity)**:
    - Primitives (Python): `PrivacyReservation`, `PrivacyReleaseResult`, `CompositionAccountant`, `PrivacyReleaseLedger`, `global_privacy_release` (`privacy_ledger.py`), server endpoints `POST /api/v1/privacy-ledger/reserve`, `POST /api/v1/privacy-ledger/release`, `GET /api/v1/privacy-ledger/remaining`, `GET /api/v1/privacy-ledger/status` (`server.py`), CLI subcommand `swarmwatch privacy-ledger` (`cli.py`).
    - Primitives (TypeScript): `PrivacyReservation`, `PrivacyReleaseResult`, `CompositionAccountant`, `PrivacyReleaseLedger`, `globalPrivacyRelease` (`privacy_ledger.ts`), exported via `index.ts`.
    - Primitives (Console): `#privacyLedgerBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Bounding repeated-query telemetry leakage across multi-tenant boundaries; telemetry exports occur only when tenant's remaining privacy budget permits; inputs clipped strictly within configured sensitivity bounds $[L_{\text{min}}, L_{\text{max}}]$ guaranteeing bounded $L_1$ sensitivity $\Delta f \le L_{\text{max}} - L_{\text{min}}$; calibrated Laplace noise perturbation; dual composition accounting (linear $\epsilon$-DP and advanced $(\epsilon, \delta)$-DP); CSPRNG in production, deterministic seeded reproducibility for audit verification; sub-25 microsecond execution ($<25\mu\text{s}$); zero external LLM API calls; dual-SDK parity.

59. **Migration Preservation & Partial Isomorphisms Seam (ADR 0070 Dual-SDK Parity)**:
    - Primitives (Python): `LossClassification`, `PreservedDelta`, `EvolutionPlan`, `EvolutionStep`, `MigrationResult`, `LossyMigrationError`, `UnsupportedReverseMigrationError`, `SchemaEvolution`, `global_schema_evolution` (`schema_evolution.py`), server endpoints `POST /api/v1/schema-evolution/plan`, `POST /api/v1/schema-evolution/migrate`, `POST /api/v1/schema-evolution/reverse`, `POST /api/v1/schema-evolution/classify-loss`, `GET /api/v1/schema-evolution/status` (`server.py`), CLI subcommand `swarmwatch schema-evolution` (`cli.py`).
    - Primitives (TypeScript): `LossClassification`, `PreservedDelta`, `EvolutionPlan`, `EvolutionStep`, `MigrationResult`, `LossyMigrationError`, `UnsupportedReverseMigrationError`, `SchemaEvolution`, `globalSchemaEvolution` (`schema_evolution.ts`), exported via `index.ts`.
    - Primitives (Console): `#evolutionBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Replaces blanket reversible claims with versioned partial isomorphisms $(f, g, \Delta)$; migrations explicitly declare `LOSSLESS`, `LOSSY_PRESERVED`, or `IRREVERSIBLE`; forward migrations capture deleted, truncated, or shadowed fields into canonically hashed `PreservedDelta` (`sw:evo:v1`); reverse transformations faithfully restore original state when delta is provided; reverse migrations fail closed (`LossyMigrationError`) when delta is omitted on lossy paths; sub-25 microsecond execution ($<25\mu\text{s}$); zero external LLM API calls; dual-SDK parity.

60. **Enterprise Acceptance Harness & Multi-Dimensional Release Verification Gate Seam (ADR 0071 Dual-SDK Parity)**:
    - Primitives (Python): `AcceptanceGate`, `GateResult`, `AcceptanceReport`, `AcceptanceAttestation`, `AcceptanceHarness`, `global_acceptance_harness` (`acceptance.py`), server endpoints `POST /api/v1/acceptance/run`, `POST /api/v1/acceptance/attest`, `POST /api/v1/acceptance/verify-attestation`, `GET /api/v1/acceptance/audit-ui`, `GET /api/v1/acceptance/status` (`server.py`), CLI subcommand `swarmwatch acceptance` (`cli.py`).
    - Primitives (TypeScript): `AcceptanceGate`, `GateResult`, `AcceptanceReport`, `AcceptanceAttestation`, `AcceptanceHarness`, `globalAcceptanceHarness` (`acceptance.ts`), exported via `index.ts`.
    - Primitives (Console): `#acceptanceBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Multi-dimensional evaluation across Security, Interoperability, Loss, Latency, Dual-SDK Parity, and UI Craft; any gate regression triggers immediate fail-closed rejection (`ACCEPTANCE_REJECTED`); programmatic AST inspection of raw `dashboard.html` asserts palette `#080B11`, zero emojis, no neon glows, tabular numbers, and badge text length $\le 30$ chars uppercase; canonically hashed attestation receipts (`sw:acc:v1`); zero external LLM API calls; dual-SDK parity.

61. **Consensus Authority Binding & Linearizable Quorum Fencing Seam (ADR 0072 Dual-SDK Parity)**:
    - Primitives (Python): `ConsensusPermitStatus`, `ConsensusLogEntry`, `ConsensusPermitResult`, `CommittedTenantState`, `ConsensusTripResult`, `ConsensusRearmResult`, `ConsensusAuthorityBinding`, `global_consensus_authority` (`consensus_authority.py`), server endpoints `POST /api/v1/consensus/reserve`, `POST /api/v1/consensus/commit`, `POST /api/v1/consensus/release`, `POST /api/v1/consensus/trip`, `POST /api/v1/consensus/rearm`, `GET /api/v1/consensus/state/{tenant_id}`, `GET /api/v1/consensus/status` (`server.py`), CLI subcommand `swarmwatch consensus` (`cli.py`).
    - Primitives (TypeScript): `ConsensusPermitStatus`, `ConsensusLogEntry`, `ConsensusPermitResult`, `CommittedTenantState`, `ConsensusTripResult`, `ConsensusRearmResult`, `ConsensusAuthorityBinding`, `globalConsensusAuthority` (`consensus_authority.ts`), exported via `index.ts`.
    - Primitives (Console): `#consensusBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Formal Replicated State Machine (RSM) quorum model $Q \ge \lfloor N / 2 \rfloor + 1$ ensuring consensus-backed linearizable safety permit reservation, commit, release, trip, and rearm; minority partitions strictly fail-closed (`MINORITY_PARTITION`); split-brain leaders holding obsolete epochs are fenced out fail-closed (`STALE_EPOCH`); reads verified linearizable across quorum ($Q_{\text{read}} \ge \lfloor N / 2 \rfloor + 1$); sub-50 microsecond execution ($<50\mu\text{s}$); zero external LLM API calls; dual-SDK parity.

62. **Downstream Effect Protocol & 2PC Outcome-Unknown Reconciliation Seam (ADR 0073 Dual-SDK Parity)**:
    - Primitives (Python): `EffectPhase`, `DownstreamIdempotencyContract`, `DownstreamReceipt`, `EffectPreparation`, `DispatchResult`, `AcknowledgeResult`, `ReconciliationResult`, `DownstreamEffectBoundary`, `global_downstream_effect_boundary` (`downstream_effect.py`), server endpoints `POST /api/v1/downstream-effects/prepare`, `POST /api/v1/downstream-effects/dispatch`, `POST /api/v1/downstream-effects/acknowledge`, `POST /api/v1/downstream-effects/reconcile`, `GET /api/v1/downstream-effects/{effect_id}`, `GET /api/v1/downstream-effects/status` (`server.py`), CLI subcommand `swarmwatch downstream` (`cli.py`).
    - Primitives (TypeScript): `EffectPhase`, `DownstreamIdempotencyContract`, `DownstreamReceipt`, `EffectPreparation`, `DispatchResult`, `AcknowledgeResult`, `ReconciliationResult`, `DownstreamEffectBoundary`, `globalDownstreamEffectBoundary` (`downstream_effect.ts`), exported via `index.ts`.
    - Primitives (Console): `#downstreamBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Explicit 2-phase commit semantics across downstream boundaries eliminating false exactly-once claims; network timeouts and transport errors fail-closed into `OUTCOME_UNKNOWN`; uncoordinated downstream APIs strictly reject blind retries (`UncoordinatedRetryBlockedError`); downstream-fenced APIs retry under identical idempotency key and epoch fence; active reconciliation probes determine final commit status; in-memory transitions $<25\mu\text{s}$ (audit SLO $<250\mu\text{s}$); zero external LLM API calls; dual-SDK parity.

63. **Critical Evidence Spool & NIST SP 800-53 Write-Ahead Journal Seam (ADR 0074 Dual-SDK Parity)**:
    - Primitives (Python): `EvidenceRecordType`, `EvidenceSpoolFullException`, `EvidenceCorruptionException`, `SequenceGapException`, `JournalReceipt`, `FlushResult`, `RecoveryResult`, `CompletenessReceipt`, `EvidenceSpoolStatus`, `EvidenceRecord`, `EvidenceSpool`, `global_evidence_spool` (`evidence_spool.py`), server endpoints `POST /api/v1/evidence/offer`, `POST /api/v1/evidence/flush`, `POST /api/v1/evidence/recover`, `GET /api/v1/evidence/completeness`, `GET /api/v1/evidence/status` (`server.py`), CLI subcommand `swarmwatch evidence` (`cli.py`).
    - Primitives (TypeScript): `EvidenceRecordType`, `EvidenceSpoolFullException`, `EvidenceCorruptionException`, `SequenceGapException`, `computeCrc32`, `EvidenceRecord`, `EvidenceSpool`, `globalEvidenceSpool` (`evidence_spool.ts`), exported via `index.ts`.
    - Primitives (Console): `#evidenceBadge` in `#fleetBar` (`dashboard.html`).
    - Invariant: Dual-tier durability separating Tier 0 (fail-closed, zero-drop safety-critical evidence) from Tier 1 (loss-aware, bounded operational telemetry); write-ahead journal (`EvidenceJournal`) with fixed framing, strictly monotonic uint64 sequence numbers, CRC32 checksums, and continuous SHA-256 hash chains (`sw:evid:v1`); capacity saturation or disk backpressure strictly halts fail-closed (`EVIDENCE_JOURNAL_FULL`); crash recovery replay verifies sequential continuity (`SEQUENCE_GAP_DETECTED`) and hash chain integrity (`CORRUPT_RECORD_HASH`); audit completeness receipts certify zero dropped critical records under NIST SP 800-53 Rev 5 (AU-4, AU-9, SC-7); in-memory execution $<150\mu\text{s}$ (audit SLO $<250\mu\text{s}$); zero external LLM API calls; dual-SDK parity.












