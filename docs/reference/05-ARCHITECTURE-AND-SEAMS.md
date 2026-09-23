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
25. **S3 / Google Cloud Storage Streaming Log Archival & Cold Storage Tier Seam (ADR 0036 Dual-SDK Parity)**:
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
