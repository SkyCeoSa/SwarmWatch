# 06. Domain Glossary & Ubiquitous Language

This document reflects the authoritative ubiquitous language defined in `CONTEXT.md`. All team communications, variable names, APIs, and tests must adhere strictly to these terms.

---

## Core Entities

### Observability & Telemetry

**SwarmSpan**:
An immutable atomic record of an agent action, tool invocation, or causal message exchange within a multi-agent swarm.
_Avoid_: Log, trace event, message object

**Causal Stream**:
The chronologically ordered and parent-linked sequence of agent interactions forming a directed acyclic graph of swarm reasoning.
_Avoid_: Activity log, execution history, chat transcript

**StateSnapshot**:
A sanitized, compressed, and SHA-256 hashed capture of an agent's memory and working context at a specific turn.
_Avoid_: Memory dump, checkpoint, state backup

---

### Circuit Breaking & Safety

**Shannon Entropy**:
A normalized statistical score measuring the lexical diversity and information content of an agent's token stream.
_Avoid_: Repetition score, randomness metric, diversity index

**Lexical Similarity**:
A zero-token token-level Jaccard overlap measure computed across a sliding window of recent agent responses.
_Avoid_: Semantic similarity, embedding distance, LLM evaluation

**Circuit Breaker**:
An in-process mathematical governor that halts runaway agent execution when entropy collapses or repetitive loops are detected.
_Avoid_: Rate limiter, watchdog, abort controller

**Intervention**:
An automated action taken when a loop is identified, progressing from a context warning hint to an execution halt.
_Avoid_: Kill switch, exception handler, interrupt

**Counterfactual Savings**:
The estimated tokens and compute expenditure prevented by terminating an autonomous agent before reaching its configured iteration or budget ceiling.
_Avoid_: Predicted savings, future tokens, imaginary cost

---

### State & Time Machine

**Branch**:
An independent, isolated trajectory of swarm execution states originating from a root session or an explicit fork.
_Avoid_: Fork name, thread, session variant

**Time-Machine Fork**:
A non-destructive operation that clones historical state at a target span into a new branch for parallel exploration or bug recovery.
_Avoid_: Rewind, roll back, restore point

---

### Packaging & Distribution

**DistributionArtifact**:
An immutable compiled binary or source package built for production developer distribution.
_Avoid_: Build output, package file, tar, zip

**WheelPackage**:
A Python binary wheel distribution artifact containing bytecode, package metadata, entrypoints, and embedded console assets.
_Avoid_: Egg, setup file, python bundle

**NpmTarball**:
A gzipped tar archive distribution artifact containing TypeScript ESM modules, type declarations, and package manifests.
_Avoid_: Node package, tar archive, tgz file

**ArtifactIntegrityReport**:
A structured evaluation verifying archive checksums, member completeness, metadata conformance, and internal asset health.
_Avoid_: Lint report, check result, build status

**ForbiddenPatternViolation**:
The detection of unmasked secrets, test caches, or internal dotfiles within a distribution artifact.
_Avoid_: Leak, security bug, dirty file

---

### Operational Metrics & Prometheus

**PrometheusMetric**:
A standardized OpenMetrics time-series sample (Counter, Gauge, or Histogram) measuring runtime swarm throughput, circuit safety events, and operational latency.
_Avoid_: Log statistic, telemetry stat, monitoring value

**ScrapeEndpoint**:
The dedicated `/metrics` HTTP handler serving OpenMetrics formatted exposition payload to cluster Prometheus or Datadog agents.
_Avoid_: Stats URL, monitoring page, ping route

---

### Ecosystem & Framework Adapters

**FrameworkAdapter**:
A non-invasive bridge translating third-party agent framework lifecycle events into SwarmSpans and circuit interventions without monkey-patching.
_Avoid_: Plugin, wrapper, interceptor, monkey patch

**ConversationalTurn**:
A discrete request-response message exchange between two conversational agents in a multi-agent dialogue.
_Avoid_: Chat item, utterance, message step

**TeleprompterIteration**:
A compilation candidate generation cycle executed by a prompt optimizer (e.g. BootstrapFewShot, MIPRO) evaluated for Shannon entropy stability and budget limits.
_Avoid_: Tuning step, prompt retry, optimization loop

**FunctionFilterInvocation**:
A discrete tool or function execution intercepted within a Semantic Kernel multi-agent workflow before tool dispatch.
_Avoid_: Hook call, function callback, middleware run

---

### Interactive Chaos & Loop Simulator

**ChaosInjection**:
The deterministic execution of a synthetic multi-agent failure scenario within the control plane to validate circuit breaker reactions, dashboard metric gauges, and alert dispatchers.
_Avoid_: Chaos test, mock run, dummy test, fake trace

**ChaosScenario**:
A reproducible specification of agent roles, synthetic turn payloads, and token consumption patterns designed to trigger a specific circuit breaking failure mode.
_Avoid_: Test case, stress test, simulated script

**EntropyCollapseScenario**:
A chaos scenario in which repetitive agent code generation collapses lexical Shannon entropy below the safety floor, triggering an entropy trip.
_Avoid_: Loop test, repetitive bug test

**InfiniteRecursionScenario**:
A chaos scenario simulating recursive, unresolvable tool execution failures with identical parameters exceeding repetition limits.
_Avoid_: Tool loop, retry spam test

**BudgetQuotaExhaustionScenario**:
A chaos scenario simulating runaway token consumption that breaches configured token or dollar budget ceilings.
_Avoid_: Spend test, cost blowup test

**ConversationalDeadlockScenario**:
A chaos scenario simulating polite ping-pong dialogue between autonomous conversational agents with high token similarity.
_Avoid_: Ping-pong test, chatter deadlock test

---

### Visual Causal Topology

**CausalTopology**:
Spatial, directed network representation of swarm agent interactions, mapping sender-receiver flows, edge metrics, and cycle boundaries.
_Avoid_: Call graph, flowchart, architecture diagram

**AgentNode**:
Visual representation of an autonomous agent in topological space, encapsulating role, message count, and health state.
_Avoid_: Box, circle, component

**CausalEdge**:
Directed vector connecting a source agent to a target agent, weighted by turn count, mean latency, and token volume.
_Avoid_: Arrow, link, connector

**CyclicLoopPath**:
A closed directed cycle ($A \to B \to \dots \to A$) detected in agent message traffic indicating an active or impending runaway loop.
_Avoid_: Recursion line, bad link, loop circle

**TopologyMatrix**:
Adjacency representation used to analyze message density and detect cycles across distributed agents.
_Avoid_: Route table, map

---

### Enterprise Security & Secret Redaction Engine

**SensitiveDataMasker**:
A deterministic, in-process engine executing pre-compiled regex patterns to redact cloud keys, database DSNs, JWTs, PEM private keys, credit cards, and PII before telemetry persistence.
_Avoid_: Cleaner, scrubber, filter script

**SecretPattern**:
A formalized regular expression targeting a specific credential or authentication token format within arbitrary payload structures.
_Avoid_: Search regex, match rule, pattern check

**RedactedToken**:
The safe placeholder string (e.g. `[REDACTED_API_KEY]`, `[REDACTED_DB_DSN]`) substituting sensitive values while preserving overall payload schema.
_Avoid_: Starred password, blank text, hidden string

---

### Incident Post-Mortem & Financial ROI Engine

**IncidentPostMortem**:
A structured forensic and financial audit document generated upon swarm completion or circuit trip, detailing prevented waste, root cause diagnosis, and entropy trajectory.
_Avoid_: Incident ticket, bug log, crash dump, error trace

**FinancialRoiMetrics**:
The quantitative calculation of compute dollars and token expenditure prevented by circuit breaker intervention.
_Avoid_: Guess, profit stat, cost estimation

**RootCauseDiagnosis**:
The deterministic attribution of the failure mode, causal agent pair, and trigger turn responsible for circuit intervention.
_Avoid_: Blame, error guess, bug attribution

**EntropyTrajectory**:
The chronological time-series of normalized Shannon entropy ($H_{norm}$) and Jaccard similarity across agent conversational turns.
_Avoid_: Diversity graph, randomness history

**SecurityRedactionSummary**:
The categorized tally of sensitive authentication tokens and PII neutralized during swarm execution.
_Avoid_: Censored list, scrub count

---

### Declarative Guardrail Policy Engine & Policy DSL

**GuardrailPolicy**:
A declarative specification document (YAML or JSON) defining global baseline thresholds, role-based agent overrides, and event-condition-action guardrail rules.
_Avoid_: Config file, settings script, security ruleset

**PolicyEngine**:
The deterministic, sub-microsecond in-memory evaluation engine that resolves agent rule inheritance and enforces declarative guardrail policies at runtime.
_Avoid_: Filter, rule checker, validator script

**RuleConfig**:
The quantitative constraints applied to an agent or swarm session, including entropy floors, similarity ceilings, token budgets, turn limits, and prohibited tools.
_Avoid_: Agent limits, thresholds, budget dict

**DeclarativeRule**:
An event-condition-action statement matching agent patterns and predicates to execute circuit trips, warnings, or redactions.
_Avoid_: Trigger, alert condition, check rule

**PolicyEvaluationResult**:
The structured verdict (`pass`, `warn`, or `trip`) emitted by the PolicyEngine for an agent turn or causal span.
_Avoid_: Check output, decision result, validation status

---

### Live Fork Resumption Runner

**ForkRunner**:
A deep module and runtime execution coordinator that programmatically resumes autonomous agent execution from an antecedent state snapshot on a designated branch.
_Avoid_: Resume script, restart handler, runner bot, execution manager

**ForkResumptionContext**:
The isolated, scoped execution context providing the resumed agent handler with sanitized antecedent state, applied state overrides, active branch binding, and causal span linkage.
_Avoid_: Resume info, state bag, context dict

**ResumptionSpan**:
An immutable atomic boundary span linking the antecedent target span of a forked branch to newly resumed agent execution turns.
_Avoid_: Resume marker, fork log, restart event

---

### State Snapshot Diff Inspector

**StateDiffReport**:
The structured, deterministic comparison output between two state snapshots encapsulating added, modified, and removed entries alongside cryptographic fingerprints.
_Avoid_: Diff output, state comparison, delta json

**DiffEntry**:
An atomic property-level delta record specifying property path, mutation kind (`added`, `modified`, `removed`, `unchanged`), antecedent value, subsequent value, and secret redaction status.
_Avoid_: Change line, diff item, mutation log

**FingerprintDelta**:
Cryptographic state transition indicator tracking SHA-256 hash mutations across discrete execution turns.
_Avoid_: Hash change, checksum diff

---

### Industrial Monitoring Stack & Grafana Topology

**MonitoringTopology**:
The multi-service Docker Compose architecture orchestrating SwarmWatch, Prometheus, and Grafana in an isolated network with zero external cloud dependencies.
_Avoid_: Docker cluster, server group, compose bundle

**DashboardPreset**:
The pre-built, production-grade Grafana dashboard JSON specification mapping SwarmWatch OpenMetrics to executive ROI, throughput, and safety panels.
_Avoid_: Grafana template, board json, chart layout

**DatasourceProvisioning**:
The automated filesystem-based registration of the local Prometheus service within Grafana without manual UI interaction.
---

### Automated Alert Route Webhook Testing & Synthetic Simulation Pipeline

**SyntheticAlertReceiver**:
An ephemeral local mock HTTP server that receives and schema-validates outgoing webhook notifications for Slack, Discord, and PagerDuty without third-party cloud dependencies.
_Avoid_: Fake webhook server, dummy endpoint, mock server

**AlertSimulationPipeline**:
An automated test harness executing multi-channel notification verification, fault injection, Dead-Letter Queue (DLQ) buffering, and live circuit breaker trip linkage.
_Avoid_: Alert test script, webhook test suite

**FaultInjectionScenario**:
A programmed simulation of upstream HTTP network errors (503 Service Unavailable, connection timeout) used to verify caller non-blocking behavior and DLQ preservation.
_Avoid_: Chaos network test, simulated downtime

**RouteSimulationReport**:
A structured summary of delivery latencies, schema validity, retry counts, and dead-letter replay status across all tested alert routes.
_Avoid_: Webhook result, test output
---

### High-Frequency Telemetry Ingestion Micro-Benchmark & Flamegraph Profile Suite

**TelemetryProfiler**:
An in-process, zero-dependency benchmarking and memory profiling engine measuring span ingestion throughput, microsecond enqueue latency distributions, and heap retention under concurrency.
_Avoid_: Benchmark script, load tester, speed test

**IngestionBenchmarkReport**:
The diagnostic evaluation output containing throughput (spans/sec), latency percentiles (P50, P90, P99), RSS memory deltas, and zero memory leak verification.
_Avoid_: Benchmark log, test metrics, profile dump

**LatencyPercentiles**:
The granular distribution breakdown (P50, P90, P99, Min, Max, Mean) of non-blocking span submission durations measured in microseconds ($\mu s$).
_Avoid_: Timers, speed stats, delay metrics

**CollapsedStackFlamegraph**:
A text-based trace representation formatting internal method call stacks and execution counts into `frame1;frame2 count` syntax compatible with standard flamegraph viewers.
_Avoid_: Profile graph, flame chart, stack trace dump
---

### Cloud-Native Kubernetes & Helm Topology

**CloudNativeTopology**:
The container orchestration architecture and Kubernetes resource definitions governing high-availability, horizontally-scalable SwarmWatch deployments.
_Avoid_: K8s cluster setup, pod layout, server deployment

**HelmChartPackage**:
The versioned, reproducible package bundle (`Chart.yaml`, `values.yaml`, templates) automating deployment, ingress routing, volume persistence, and autoscaling across cloud Kubernetes clusters.
_Avoid_: Helm bundle, chart archive, deploy script

**IngressRouting**:
The HTTP and WebSocket reverse-proxy ingress definition with TLS termination and long-lived streaming timeouts for SwarmWatch APIs and console.
_Avoid_: Reverse proxy route, url rewrite, ingress path

**ServiceMonitorBinding**:
The Prometheus Operator custom resource definition enabling automated Prometheus discovery and scraping of `/metrics` at high frequency.
_Avoid_: Prometheus config, scrape rule, monitor CRD

---

### Terminal User Interface (TUI) & Headless Cluster Observability

**TerminalMonitor**:
The in-process terminal controller orchestrating periodic data polling, ANSI viewport formatting, and keyboard interaction loop for SwarmWatch sessions.
_Avoid_: Terminal watcher, CLI poller, console daemon

**AnsiFrameBuffer**:
The pure-function terminal layout engine rendering structured, responsive ASCII tables, health banners, and causal span streams using ANSI color escapes.
_Avoid_: Screen buffer, console renderer, terminal output string

**HeadlessTriage**:
The operational workflow enabling developers and SREs to monitor, diagnose, and inspect active swarms and circuit halts inside terminal shells without a web browser.
_Avoid_: CLI debugging, terminal checking, log tailing

---

### Distributed Redis Circuit Breaker & Fleet State Synchronization

**RedisCircuitCoordinator**:
The deep module coordinating distributed swarm sessions across multiple worker processes, container pods, and Kubernetes replicas using Redis as the high-throughput synchronization backbone.
_Avoid_: Distributed lock manager, cache controller, redis cluster boss

**RedisFleetBackend**:
The storage and messaging abstraction managing Redis key formatting, atomic quota evaluation, and pub/sub transport.
_Avoid_: Redis adapter, db connector, redis wrapper

**RedisTripBroadcast**:
The real-time event payload dispatched over Redis Pub/Sub channels when a worker halts a swarm session, achieving sub-millisecond fleet-wide intervention.
_Avoid_: Halt message, redis pubsub alert, kill ping

**RedisQuotaLedger**:
The atomic hash data structure in Redis storing cumulative turns, tokens, costs, and trip status per swarm session.
_Avoid_: Redis budget hash, quota table, cache counter

**RESPRedisClient**:
The pure Python standard library socket implementation of the Redis Serialization Protocol (RESP2) providing zero-external-dependency communication with Redis.
_Avoid_: Socket script, redis connector, mini client

**MockRedisClient**:
The thread-safe in-memory Redis client supporting hash operations, atomic increments, and pub/sub dispatch for offline and local testing.
_Avoid_: Fake redis, test cache, memory db

---

### Distributed Redis Fleet & Cluster Monitor Widget

**RedisFleetWidget**:
The developer console interface component visualizing distributed cluster synchronization state, active worker rosters, and real-time atomic quota meters.
_Avoid_: Redis graph, cluster viewer, fleet window

**FleetClusterStatus**:
The visual indicator representing whether the Redis backend is synchronized (`CONNECTED`), degraded, or running in local in-memory mock mode.
_Avoid_: Redis state tag, cluster LED, connection pill

**FleetTripBanner**:
The urgent, non-decorative alert surface rendering when a distributed trip broadcast is received from any cluster worker pod.
_Avoid_: Error box, red toast, halt popup

**FleetQuotaMeter**:
The tabular numeral progress indicator displaying current consumption against session ceiling limits (turns, tokens, dollar cost).
_Avoid_: Speedometer, gauge bar, progress slider

**EmergencyHaltModal**:
The confirmation modal allowing operators to dispatch an immediate manual circuit trip across all nodes in the distributed cluster.
_Avoid_: Kill popup, abort dialog, halt window

---

### Distributed Redis Lock & Leader Election (Seam 24 / ADR 0035)

**RedisDistributedLock**:
The atomic mutual exclusion primitive implementing safe distributed locking via Redis `SET NX PX` and CAS release/extend Lua scripts.
_Avoid_: Process lock, mutex file, redlock clone

**RedisLeaderElector**:
The coordination manager running heartbeat leases, automatic standby failover, and voluntary step-down routines across multi-server SwarmWatch instances.
_Avoid_: Master slave script, election loop, vote daemon

**ServerNodeRole**:
The explicit state of a SwarmWatch server node within the cluster (`LEADER`, `STANDBY`, or `STEPPED_DOWN`).
_Avoid_: Master node, slave worker, backup server

**LeaderHeartbeat**:
The periodic lease renewal dispatched by the active Leader every renewal interval to prevent lease expiration.
_Avoid_: Keepalive ping, leader tick, lease refresher

**ClusterNodeRoster**:
The cluster-wide registry of active SwarmWatch server instances, roles, and epoch timestamps stored in Redis.
_Avoid_: Server list, node catalog, host directory

---

### S3 / Google Cloud Storage Streaming Log Archival & Cold Storage Tier (Seam 25 / ADR 0036)

**ObjectStorageClient**:
The pluggable object storage interface abstracting Amazon S3, MinIO, Ceph, Cloudflare R2, and Google Cloud Storage operations (upload_object, download_object, list_objects, delete_object, head_object).
_Avoid_: S3 wrapper, bucket adapter, cloud connector

**MockObjectStorageClient**:
The thread-safe in-memory object store providing deterministic byte storage, checksum validation, and prefix listing for offline tests.
_Avoid_: Fake S3, dummy bucket, test blob

**S3ObjectStorageClient**:
The pure standard-library HTTP/SigV4 REST client communicating with S3-compatible cloud object storage without external SDK dependencies.
_Avoid_: Boto clone, S3 script, aws-cli runner

**ColdStorageArchiver**:
The deep module coordinating the serialization, gzip compression, partitioning, and cryptographic manifest generation for cold swarm telemetry offloading.
_Avoid_: Log tarballer, dump maker, zip exporter

**ArchiveManifest**:
The immutable cryptographic metadata envelope recording bit-level SHA-256 hashes, uncompressed/compressed sizes, compression ratio, telemetry tallies, and multi-tenant context.
_Avoid_: Index file, info json, header tag

**ArchiveRecord**:
The high-level descriptor of an archived swarm session stored in object storage.
_Avoid_: Cold row, archive summary, backup entry

**PartitionScheme**:
The standard time-partitioned object key layout: swarmwatch/v1/org={org_id}/proj={project_id}/year={YYYY}/month={MM}/day={DD}/swarm={swarm_id}.jsonl.gz.
_Avoid_: Folder structure, key path, bucket directory

---

### Enterprise Multi-Factor Authentication (MFA / TOTP) & Strict RBAC Engine (Seam 26 / ADR 0037)

**Role**:
The standardized categorical authorization tier (`ADMIN`, `OPERATOR`, `AUDITOR`, `VIEWER`) defining an actor's privilege envelope.
_Avoid_: User type, access tier, permission group

**Permission**:
The fine-grained operation identifier (e.g. `VIEW_ANALYTICS`, `EXPORT_AUDIT`, `DISPATCH_ACTIONS`, `EXECUTE_CIRCUIT_TRIP`, `MODIFY_POLICIES`, `MANAGE_KEYS`, `MANAGE_USERS`) checked via the RBAC matrix.
_Avoid_: Action right, privilege flag, capability check

**TotpEngine**:
The zero-dependency in-process cryptographic authenticator generating and validating RFC 6238 time-based one-time passwords using HMAC-SHA1 and RFC 3548 Base32 encoding.
_Avoid_: 2FA script, authenticator helper, code generator

**StepUpChallenge**:
The mandatory, high-friction multi-factor verification required before executing destructive or high-impact control plane mutations (fleet trip/reset, policy changes, API key revocation, data purges).
_Avoid_: Extra login, confirmation dialog, 2FA popup

**BackupRecoveryCode**:
A single-use, cryptographically generated hex recovery token stored exclusively as a SHA-256 hash and burned upon redemption.
_Avoid_: Emergency code, recovery password, fallback key

**SecurityAuditEvent**:
An immutable record capturing actor identity, role, timestamp, target resource, operation kind, and outcome status for enterprise compliance inspection.
_Avoid_: Auth log, access entry, audit line

---

### OpenTelemetry OTLP Ingestion Gateway (Seam 27 / ADR 0038)

**OtlpIngestionGateway**:
The high-throughput HTTP ingestion control plane exposing standard `/v1/traces` endpoints accepting standard OpenTelemetry JSON and Protobuf payloads.
_Avoid_: OTel proxy, trace listener, OTLP collector clone

**OtelTraceParser**:
The deep parsing engine translating nested OpenTelemetry `resourceSpans`, `scopeSpans`, attributes, and events into SwarmWatch internal `SwarmSpan` entities.
_Avoid_: Trace converter, span scraper, otel translator

**ProtobufWireDecoder**:
The pure Python standard library binary parser decoding tag-length-value (TLV) wire protocol records without external Google protobuf pip dependencies.
_Avoid_: Proto unpacker, binary deserializer, pb reader

**GenAiSemanticTranslator**:
The specialized convention extractor mapping standard `gen_ai.*` attributes (systems, operations, model IDs, token tallies, prompts, completions) into SwarmWatch schemas.
_Avoid_: LLM field mapper, prompt parser, ai tag reader

**OtlpTraceExportResponse**:
The official response envelope returned to upstream OTel SDKs and collectors (`partialSuccess`, `rejectedSpans`, `errorMessage`) conforming to OTel 1.0 specifications.
_Avoid_: Trace ack, ingest result, upload response

---

### Enterprise WebPush & Server-Sent Events (SSE) Real-Time Alerts (Seam 28 / ADR 0039)

**SseEventManager**:
The asynchronous broadcast manager orchestrating persistent HTTP 	ext/event-stream client subscriptions, automatic 15-second keep-alive heartbeats, and Last-Event-ID ring-buffer replay.
_Avoid_: SSE broadcaster, event pusher, stream loop

**SseEvent**:
A structured Server-Sent Event conforming to W3C specifications containing event identifier, typed event category, retry interval, and JSON payload.
_Avoid_: Stream frame, event packet, line message

**WebPushEngine**:
The asynchronous notification engine managing VAPID keys, JWT token minting, and RFC 8292 standard push notification dispatch with exponential backoff and dead-letter queue buffering.
_Avoid_: Push sender, browser notifier, webpush service

**VapidKeyManager**:
The cryptographic key manager generating and validating NIST P-256 (secp256r1) keypairs and ES256 signatures via pure Python standard library arithmetic without external C-extensions.
_Avoid_: Key handler, vapid cert tool, crypto helper

**PushSubscription**:
A persistent browser or mobile client push subscription registration containing endpoint URI, client ephemeral public key (p256dh), and authentication secret (uth).
_Avoid_: Push token, browser hook, notification registration

**PushMessage**:
The notification payload delivered to client Service Workers containing title, body description, icon URI, and contextual metadata.
_Avoid_: Push body, alert text, notification dict

---

### Kernel-Level Network Socket LLM Interception Gateway (Seam 29 / ADR 0040)

**EbpfSocketGateway**:
The deep module managing transparent socket interception, packet sniffing/forwarding, protocol decoding, and kernel-level circuit enforcement.
_Avoid_: Socket proxy, network sniffer, wiretap daemon

**SocketRule**:
Declarative filter criteria for network traffic inspection specifying destination ports, domain patterns, target action (`ALLOW`, `INSPECT`, `TERMINATE_RST`), and token/cost thresholds.
_Avoid_: Firewall rule, port mapping, filter config

**SocketStreamDecoder**:
Zero-dependency stream protocol parser extracting HTTP/1.1 headers, JSON payloads, and Server-Sent Events (SSE) token chunks from raw TCP byte streams without third-party HTTP decoders.
_Avoid_: Stream unpacker, http reader, payload extractor

**SocketIntervention**:
Physical disruption and termination of an offending TCP session via TCP RST packet emulation or immediate socket shutdown (`SHUT_RDWR`), physically cutting off runaway agent loops at the network layer.
_Avoid_: Kill connection, socket reset, packet drop

**TcpRstEmitter**:
Low-level kernel or socket abstraction executing immediate TCP RST / abort operations on violating connections.
_Avoid_: Resetter, socket dropper, reset tool

---

### Multi-Cluster High-Availability Raft Consensus Engine (Seam 30 / ADR 0041)

**RaftConsensusEngine**:
The deep module managing distributed cluster consensus, term transitions, leader elections, and replicated state machine logs without external dependencies.
_Avoid_: Master node daemon, cluster sync tool, leader loop

**RaftNodeRole**:
The operational role of a node within the Raft consensus group (`LEADER`, `CANDIDATE`, `FOLLOWER`).
_Avoid_: Worker type, cluster rank, server state

**RaftTerm**:
Monotonically increasing epoch number coordinating leadership terms and preventing stale leader operations.
_Avoid_: Cluster age, election round, epoch tag

**RaftLogEntry**:
An immutable replicated state machine entry recording term, log index, and domain command.
_Avoid_: Replicated log line, journal item, commit record

**RaftStateMachine**:
The deterministic state machine applying committed Raft entries (circuit trips, quota increments, worker states).
_Avoid_: State handler, sync runner, local cache

**RequestVoteRpc**:
Candidate RPC soliciting votes from cluster peers during leader elections.
_Avoid_: Vote ping, ballot message, candidate request

**AppendEntriesRpc**:
Leader RPC replicating log entries and emitting periodic heartbeats to maintain authority.
_Avoid_: Heartbeat ping, log push, sync packet

---

### Autonomous Counterfactual Swarm Optimization & Self-Healing Policy Controller (Seam 31 / ADR 0042)

**SwarmSelfHealingController**:
The autonomous control plane module continuously monitoring causal span streams, detecting multi-agent operational anomalies, and enacting surgical, reversible policy remediations.
_Avoid_: Auto fixer, health daemon, repair bot

**SelfHealingAction**:
An atomic remediation intervention recording action type, target entity, counterfactual savings, original state, and rollback lifecycle.
_Avoid_: Patch record, fix event, repair command

**AnomalyType**:
Formal classification of detectable runtime failure modes (`REPEATED_TOOL_FAILURES`, `ENTROPY_VOLATILITY`, `BUDGET_RUNAWAY`, `CASCADE_ERROR`, `LATENCY_SPIKE`).
_Avoid_: Bug type, error kind, defect category

**SelfHealingActionType**:
Surgical intervention strategies (`BLACKLIST_TOOL`, `ROUTE_MODEL_FALLBACK`, `CLAMP_QUOTA`, `AUTO_RESET_ON_STABILIZATION`, `INJECT_BACKOFF`).
_Avoid_: Fix action, repair type, mitigation step

**CounterfactualEvaluation**:
Deterministic calculus quantifying token and financial dollar savings achieved by comparing the alternative remediation trajectory against the unmitigated failure path.
_Avoid_: What-if math, savings estimate, cost check

**ModelFallbackRouter**:
Dynamic routing mechanism switching erratic or costly LLM models to stable, efficient alternatives under autonomous policy direction.
_Avoid_: Model swapper, alternate selector, backup route

---

### Distributed Multi-Agent Root-Cause Causal Inference Engine (Seam 32 / ADR 0043)

**CausalInferenceEngine**:
The deep module executing Structural Causal Model (SCM) analysis, backward ancestral DAG graph traversal, and Judea Pearl Do-Calculus counterfactual reasoning across swarm executions.
_Avoid_: Debug tracer, fault finder, RCA bot

**RootCauseAttribution**:
Deterministic forensic diagnosis attributing the true origin agent, trigger span, turn index, propagation depth, and confidence score of an execution anomaly.
_Avoid_: Culprit report, blame tag, fault attribution

**CausalPathNode**:
An atomic node along the reconstructed critical failure propagation path from origin to symptom manifestation.
_Avoid_: Chain step, link item, trace hop

**ContextPoisoningMetric**:
Quantitative measure of contaminated prompt context ($CPS \in [0.0, 1.0]$) injected by an upstream agent and spread to downstream peers.
_Avoid_: Taint score, pollution level, dirty context

**CausalInferenceResult**:
Comprehensive forensic assessment document uniting root cause attribution, critical propagation path, context poisoning metrics, and Time-Machine fork turn recommendations.
_Avoid_: RCA output, cause summary, analysis result

**StructuralCausalModel**:
The directed causal dependency graph representing causal relationships, intervening operations, and counterfactual potential outcomes across multi-agent interactions.
_Avoid_: Causal diagram, dependency tree, flow schema

---

### Ultra-High-Throughput gRPC Streaming Ingestion Gateway (Seam 33 / ADR 0044)

**GrpcStreamingGateway**:
The deep module executing binary gRPC/HTTP2 stream demultiplexing, in-flight circuit breaking, and telemetry span ingestion at high frequency (>50,000 spans/sec).
_Avoid_: RPC proxy, gRPC receiver, streaming listener

**GrpcMessageFramer**:
The pure standard-library byte framing engine encoding and decoding 5-byte prefixed gRPC protocol envelopes (`[compression(1B)][length(4B BE)][payload]`).
_Avoid_: Packet framer, chunk wrapper, byte slicer

**GrpcFrame**:
An atomic framed binary message carrying a serialized protobuf payload across a gRPC stream.
_Avoid_: RPC packet, message chunk, payload block

**GrpcStatusCode**:
The canonical gRPC integer status code (`OK=0`, `CANCELLED=1`, `INVALID_ARGUMENT=3`, `RESOURCE_EXHAUSTED=8`, etc.) signaling stream disposition and circuit trips.
_Avoid_: Return code, error number, HTTP status

**GrpcStreamSession**:
An active, multiplexed bidirectional or client-streaming connection delivering telemetry spans from an agent worker pod.
_Avoid_: Stream connection, socket channel, worker link

---

### Embedding-Free Vector Semantic Drift & Hallucination Divergence Detector (Seam 34 / ADR 0045)

**SemanticDriftDetector**:
The deep module executing multi-order lexical n-gram shingling, 64-bit universal MinHash LSH, and Type-Token Ratio dispersion calculus to detect conversational topic deviation and hallucinatory echo chambers in $<50\mu\text{s}$ per turn with zero external token expenditure.
_Avoid_: Drift checker, semantic monitor, hallucination judge

**MinHashSigner**:
The pure mathematical hashing engine generating 64-element universal integer signature vectors for rapid pairwise Jaccard similarity estimation without dense neural embeddings.
_Avoid_: Vector embedder, signature builder, hash generator

**SemanticDriftProfile**:
An immutable turn record capturing cumulative drift score ($\Delta_{\text{drift}}$), root task anchor similarity, predecessor turn similarity, vocabulary compression ratio, divergence status, and shifting n-gram clusters.
_Avoid_: Drift report, deviation score, turn analysis

**VocabularyCompressionRatio**:
The mathematical ratio of unique tokens to total token volume ($VCR = |V_{\text{unique}}| / |V_{\text{total}}|$) measuring vocabulary shrinkage and detecting echo-chamber hallucination collapse.
_Avoid_: Diversity ratio, word count, token fraction

**AnchorSimilarity**:
The pairwise Jaccard similarity score between the current agent turn and the foundational task anchor prompt ($t_0$), preventing gradual mission creep.
_Avoid_: Baseline match, prompt score, initial similarity

**DivergenceStatus**:
The categorical operational state of agent trajectory semantic coherence (`nominal`, `warning`, `divergent`, `tripped`).
_Avoid_: Drift level, warning grade, status flag

---

### Adaptive Dynamic Quota Re-balancing across Distributed Fleets (Seam 35 / ADR 0046)

**AdaptiveQuotaRebalancer**:
The deep module calculating real-time composite agent utility, managing quota tier transitions, and executing non-blocking dynamic quota re-allocation across multi-agent fleets in $<50\mu\text{s}$ per turn without remote LLM judge calls.
_Avoid_: Quota manager, rate limiter, load balancer, token allocator

**AgentUtilityScore**:
The normalized composite score $U_a \in [0.0, 1.0]$ quantifying real-time agent convergence, lexical richness (VCR), Shannon entropy stability, and fault resilience.
_Avoid_: Performance score, agent rank, quality grade

**AgentTier**:
The categorical operational allocation tier governing quota eligibility (`BOOSTED`, `NOMINAL`, `THROTTLED`, `QUARANTINED`).
_Avoid_: Agent status, priority level, quota band

**RebalancePlan**:
An immutable allocation specification detailing assigned agent quotas, surge slices, reclaimed reserve pool, and mathematical justification per rebalancing epoch.
_Avoid_: Quota split, budget plan, distribution log

**RebalancePolicy**:
The configurable parameter set defining utility weights, sensitivity exponent ($\kappa$), minimum safety floor, and epoch trigger conditions.
_Avoid_: Quota rules, limiter settings, allocation config

**SurgeQuotaGrant**:
A dynamic quota slice granted from the reclaimed buffer to a converging agent nearing its budget limit without expanding the global fleet ceiling.
_Avoid_: Extra tokens, bonus budget, overdraft

---

### Real-Time Topological Graph Clustering & Dynamic Agent Coalitions (Seam 36 / ADR 0047)

**TopologicalCoalitionEngine**:
The deep module analyzing causal span interaction graphs, calculating degree and bottleneck centrality, performing modularity-based community clustering, and forming dynamic agent coalitions in $<50\mu\text{s}$ per turn without remote LLM calls.
_Avoid_: Graph analyzer, agent grouper, cluster bot, network visualizer

**AgentCoalition**:
An autonomous, tightly-coupled sub-collective of swarm agents grouped by topological interaction density, possessing an elected Coalition Leader and isolated conversational context.
_Avoid_: Agent team, sub-group, swarm squad, cluster cell

**ClusterModularity**:
The Newman-Girvan modularity metric $Q \in [-0.5, 1.0]$ quantifying the structural strength of community partitioning in the multi-agent interaction graph.
_Avoid_: Graph score, grouping quality, cluster index

**BottleneckCentrality**:
Quantitative score measuring whether an agent acts as a congested bridge or transit bottleneck between disparate agent clusters.
_Avoid_: Traffic choke, slow agent, bridge metric

**CoalitionLeader**:
The agent within an `AgentCoalition` exhibiting the highest internal degree centrality, designated to orchestrate sub-task completion and external communication.
_Avoid_: Team captain, head agent, group manager

**InteractionDensity**:
The ratio of actual weighted interactions among a coalition's member agents relative to the total possible edge capacity.
_Avoid_: Chat volume, traffic ratio, link count

---

### Zero-Copy Shared-Memory Ring Buffer IPC Gateway (Seam 37 / ADR 0048)

**ShmRingBuffer**:
The low-level memory-mapped circular ring buffer with fixed 64-byte binary header (`SWRB`), atomic write/read sequence pointers, and fixed slot framing for zero-copy IPC telemetry exchange.
_Avoid_: Shared array, memory queue, IPC pipe, temp buffer

**ShmRingBufferWriter**:
The high-throughput producer writing structured causal spans into shared memory slots in $<1.0\mu\text{s}$ per record without socket or kernel transitions.
_Avoid_: SHM client, span sender, memory writer

**ShmRingBufferReader**:
The consumer component draining completed slots from the shared memory ring buffer into SwarmWatch storage.
_Avoid_: SHM reader, queue pop, buffer consumer

**ShmIpcGateway**:
The background supervisory daemon managing the lifecycle, slot ingestion, and health monitoring of shared-memory IPC telemetry channels.
_Avoid_: IPC daemon, socket gateway, memory server

**RingSlotStatus**:
The state machine flag governing individual slot lifecycle (`EMPTY`, `WRITING`, `READY`, `READING`).
_Avoid_: Slot flag, lock byte, item status

**DroppedRecordsCounter**:
The monotonically increasing 64-bit sequence tracking discarded spans during memory buffer saturation without blocking worker execution.
_Avoid_: Lost count, drop rate, overflow number

---

### Differential Privacy & Cryptographic State Anonymization (Seam 38 / ADR 0049)

**DifferentialPrivacyEngine**:
The deep module applying formal Laplace perturbation to numeric telemetry and HMAC pseudonymization to agent identifiers in $<50\mu\text{s}$ per turn without remote LLM calls.
_Avoid_: Noise maker, data scrambler, obfuscator script, anonymizer bot

**LaplaceMechanism**:
The mathematical perturbation mechanism injecting calibrated noise $\eta \sim \text{Laplace}(0, \Delta f / \epsilon)$ to achieve $\epsilon$-differential privacy.
_Avoid_: Random noise, gaussian blur, jitter generator

**PrivacyBudget**:
The cumulative $\epsilon$ allocation tracking total allowed information leakage across federated queries and export pipelines.
_Avoid_: Privacy limit, query count, token budget

**CryptographicPseudonym**:
The deterministic, irreversible token generated via HMAC-SHA256 replacing raw agent identifiers while preserving topology linkage.
_Avoid_: Fake name, random ID, alias, hashed string

**SensitivityBound**:
The maximum possible change in a numeric metric between adjacent database states ($\Delta f = U - L$).
_Avoid_: Max range, variance scale, bound delta

**DifferentialPrivacyPolicy**:
The configuration defining $\epsilon$, sensitivity bounds, target fields, and tenant salt keys for privacy enforcement.
_Avoid_: Privacy rules, mask settings, DP config

---

### Predictive Multi-Agent Failure Forecasting & Early-Warning Entropy Martingales (Seam 39 / ADR 0050)

**EntropyMartingaleForecaster**:
The deep module calculating turn-to-turn conditional entropy drift, centered supermartingale stochastic processes, and Azuma-Hoeffding risk bounds to predict multi-agent conversational failure 2 to 4 turns in advance without remote LLM calls.
_Avoid_: Failure predictor, crash watcher, forecast bot, error guesser

**MartingaleDriftSequence**:
The centered stochastic process $M_n = \sum (D_t - \mu)$ tracking persistent structural entropy degradation distinct from random conversational variance.
_Avoid_: Trend line, drift array, diff sequence

**EarlyWarningHorizon**:
The estimated number of discrete execution turns remaining before an agent swarm crosses critical circuit breaker halt thresholds ($T_{\text{forecast}}$).
_Avoid_: Countdown, time to crash, turn estimate

**AzumaHoeffdingBound**:
The mathematical concentration inequality computing the probability $P_{\text{risk}}$ of critical collapse within $h$ turns given bounded filtration differences.
_Avoid_: Risk math, failure odds, chance calculation

**ForecastRiskLevel**:
The categorical classification of impending swarm risk (`NOMINAL`, `EARLY_WARNING`, `CRITICAL_COLLAPSE`).
_Avoid_: Danger state, threat level, alert color

**PreemptiveIntervention**:
The proactive automated adjustment (e.g. self-healing prompt perturbation, fallback routing) triggered prior to a hard circuit breaker trip.
_Avoid_: Early kill, proactive stop, pre-halt

---

### Multi-Agent Byzantine Attack & Prompt Injection Stress Harness (Seam 40 / ADR 0051)

**ByzantineStressHarness**:
The deep module coordinating synthetic adversarial injections, measuring multi-agent defense posture, and computing quantitative Byzantine Fault Tolerance (BFT) resilience scores.
_Avoid_: Hack script, attack bot, fuzzer tool, chaos injector

**ByzantineAttackType**:
The categorical attack vectors (`PROMPT_INJECTION`, `STATE_POISONING`, `ENTROPY_JAMMING`, `SYBIL_CONTAGION`) defining adversarial payload patterns and intent.
_Avoid_: Hack type, exploit kind, attack name

**BftResilienceScore**:
The quantitative metric $B_{\text{resilience}} \in [0.0, 1.0]$ quantifying the proportion of successfully intercepted attacks, isolated rogue agents, and contained context contagion.
_Avoid_: Safety grade, health rating, defense percent

**ByzantineFaultProfile**:
The configuration specifying attack vector, injection probability, payload pattern, and target agent for stress testing.
_Avoid_: Exploit profile, attack recipe, test config

**AgentQuarantinePolicy**:
The automated isolation rule blacklisting adversarial or compromised agents from active causal interaction topologies to prevent contagion.
_Avoid_: Ban list, blacklist rule, agent kick

**ContagionIsolationRatio**:
The proportion of downstream peer agents successfully shielded from poisoned context during an adversarial injection ($1 - C_{\text{contagion}}$).
_Avoid_: Shield percent, infection rate, safe ratio

---

### Multi-Agent Dynamic Honeypot & Deception Sandboxing (Seam 41 / ADR 0052)

**HoneypotManager**:
The deep module managing deception sandboxes, canary honeytoken generation, and diversion routing for adversarial agents without remote LLM calls.
_Avoid_: Trap bot, fake agent, honey script, trick manager

**Honeytoken**:
The synthetic, cryptographically identifiable credentials (`sw_canary_...`, `sw_honey_...`) embedded into swarm memory to detect unauthorized access or exfiltration attempts.
_Avoid_: Fake key, dummy secret, bait password, trap token

**DeceptionSandbox**:
The in-process shadow interaction environment returning plausible synthetic responses to trapped adversarial agents without executing real operations or mutating production state.
_Avoid_: Fake room, mock engine, fake runner, dummy sandbox

**DiversionRouting**:
The seamless redirection of in-flight causal spans from compromised agents into simulated sandboxes without alerting the attacker.
_Avoid_: Reroute hack, traffic detour, span ditching

**ShadowGraph**:
The isolated synthetic execution graph tracking diverted turns, trapped attack payloads, and adversary behavioral telemetry.
_Avoid_: Ghost DAG, fake graph, dummy trace

**HoneypotStatus**:
The high-level operational state (`ARMED`, `DECEIVING`, `DEFUSED`) tracking active deception engagements and sandboxed adversaries.
_Avoid_: Trap flag, decoy state, trick status

---

### Bounded Model Checking & Multi-Agent Invariant Verification (Seam 42 / ADR 0053)

**BoundedModelChecker**:
The deep module exhaustively verifying multi-agent interaction state spaces up to depth bound $k$ to mathematically prove safety and deadlock-freedom.
_Avoid_: Test script, checker bot, graph solver, math bot

**AgentTransitionModel**:
The formal state machine specification defining agent states, transitions, delegation actions, and terminal conditions.
_Avoid_: Agent graph, workflow diagram, step plan

**VerificationInvariant**:
The mathematical safety or liveness condition (`NO_DEADLOCK`, `NO_UNBOUNDED_CYCLES`, `BOUNDED_BUDGET`, `MUTUAL_EXCLUSION`) verified across all paths.
_Avoid_: Test rule, safety assert, check condition

**CounterexampleTrace**:
The deterministic chronological sequence of states and actions proving that an invariant is violated.
_Avoid_: Error log, fail path, bug trace

**StateSpaceExplorer**:
The algorithmic bounded traversal engine exhaustively exploring reachable swarm states without external solver dependencies.
_Avoid_: Tree searcher, path walker, state crawler

**VerificationVerdict**:
The formal verification outcome (`VERIFIED`, `VIOLATED`, `VACUOUS`) certifying multi-agent topology safety.
_Avoid_: Test result, check grade, pass status

---

### Autonomous Dynamic Topology Healing & Auto-Patching (Seam 43 / ADR 0054)

**TopologyAutoHealer**:
The deep module synthesizing, applying, and reverifying graph-theoretic patches to heal multi-agent topology failures discovered by BMC or runtime telemetry.
_Avoid_: Fixer script, patch bot, graph doctor, auto-repairer

**TopologyPatch**:
The formal graph mutation specification defining target agent, repair action (`FALLBACK_REDIRECT`, `CYCLE_GATE_BREAK`, `BUDGET_REWIRE`, `MUTEX_MEDIATE`), and synthetic transitions.
_Avoid_: Graph fix, edge hack, hot patch

**PatchAction**:
The canonical repair mechanism (`FALLBACK_REDIRECT`, `CYCLE_GATE_BREAK`, `BUDGET_REWIRE`, `MUTEX_MEDIATE`) resolving specific invariant breaches.
_Avoid_: Fix type, repair mode, patch kind

**HealedModelReport**:
The audit record containing original violation, synthesized patch, resulting transition model, and mathematical reverification certification via BMC.
_Avoid_: Fix log, patch report, repair summary

**FallbackRedirection**:
The automated insertion of recovery transitions from deadlocked non-terminal agents to authorized supervisor or fallback agents.
_Avoid_: Detour hack, error jump, skip route

**CycleGateBreak**:
The injection of progress counters or conditional guard flags to guarantee loop termination in cyclic agent delegations.
_Avoid_: Loop break, cycle cut, exit hook

---

### Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking (Seam 44 / ADR 0055)

**MultiModalInspector**:
The deep module parsing binary headers, extracting multi-modal metadata, computing perceptual hashes, and verifying cryptographic provenance watermarks in $<0.1\text{ms}$ with zero LLM tokens.
_Avoid_: Media parser, file checker, image validator, asset bot

**MediaModality**:
The categorical media classification (`IMAGE`, `AUDIO`, `VIDEO`, `DOCUMENT`, `EMBEDDING`, `UNKNOWN`) identifying multi-modal artifact types.
_Avoid_: File type, asset kind, media category

**MediaMetadata**:
The lightweight technical descriptor capturing MIME type, dimensions (width, height), duration seconds, sample rate, channels, byte size, SHA-256 checksum, and perceptual hash.
_Avoid_: File info, asset stats, tag bundle

**PerceptualFingerprint**:
The deterministic 16-character hexadecimal perceptual hash (pHash) capturing spatial/frequency byte distributions for instantaneous visual asset integrity matching without neural models.
_Avoid_: Image hash, visual tag, photo fingerprint

**ArtifactWatermark**:
The cryptographic metadata signature binding artifact identifier, producer agent ID, swarm session ID, tenant ID, byte checksum, and nanosecond timestamp via HMAC-SHA256.
_Avoid_: File tag, digital stamp, signature label

**WatermarkedArtifact**:
The immutable multi-modal asset envelope combining extracted technical metadata with cryptographic provenance watermarks for tamper-evident delegation across agent DAGs.
_Avoid_: Signed file, tagged asset, wrapped blob

---

### Decentralized Epidemic Gossip Telemetry Mesh Sync (Seam 45 / ADR 0056)

**GossipMeshManager**:
The deep module orchestrating decentralized peer-to-peer telemetry propagation, Vector Clock causality tracking, and anti-entropy synchronization in $<50\mu\text{s}$ per turn without central brokers or LLM calls.
_Avoid_: P2P sync bot, cluster gossip daemon, mesh router, broadcast service

**MeshNode**:
The autonomous peer node participating in the decentralized SwarmWatch telemetry mesh, characterized by `node_id`, network endpoint, lifecycle status (`ACTIVE`, `SUSPECT`, `DEAD`), and heartbeat epoch.
_Avoid_: Server node, cluster peer, host entry

**VectorClock**:
The monotonically advancing mapping of `{node_id: sequence_counter}` capturing causal event ordering, partial precedence, and concurrent branching without physical clock synchronization.
_Avoid_: Lamport timestamp, clock dict, tick map

**GossipDigest**:
The compact summary of active vector clocks and known event checksums exchanged during periodic Anti-Entropy rounds to identify missing spans or circuit events.
_Avoid_: Hash list, sync packet, peer digest

**GossipDelta**:
The targeted reconciliation payload containing causal spans, circuit trip interventions, and artifact watermarks transmitted to bring a lagging peer to convergence.
_Avoid_: Sync delta, diff batch, update message

**MeshConvergenceHealth**:
The categorical evaluation of mesh state coherence (`CONVERGED`, `SYNCING`, `PARTITIONED`) measuring cluster-wide telemetry consistency.
_Avoid_: Mesh grade, sync status, network health

---

### Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer (Seam 46 / ADR 0057)

**ComputeAwareBalancer**:
The deep module orchestrating decentralized, compute-weighted consistent hashing, real-time node capacity scoring, causal session affinity, and automated spillover routing in $<25\mu\text{s}$ per decision without external LLM calls.
_Avoid_: Task router, load balancer daemon, shard proxy, traffic splitter

**ComputeCapacityScore**:
The normalized scalar $C_i \in [0.0, 1.0]$ quantifying a node's available compute headroom computed via weighted CPU headroom, memory availability, and queue headroom.
_Avoid_: Node score, server load rating, capacity ratio, health weight

**ConsistentHashRing**:
The deterministic 32-bit circular hash space with dynamic virtual node allocation ($V_i = \max(1, \lfloor V_{\text{base}} \times C_i \rfloor)$) and $O(\log N)$ binary search lookup ensuring minimal key remapping during topology changes.
_Avoid_: Hash circle, shard lookup table, token map

**ShardRoutingKey**:
The structured routing identifier encapsulating `tenant_id`, `project_id`, `swarm_id`, and optional `session_affinity_key` used to guarantee causal locality.
_Avoid_: Shard key, hash input, routing string

**NodeHealthTier**:
The categorical operational health classification (`OPTIMAL`, `NOMINAL`, `CONGESTED`, `OVERLOADED`) dynamically determined by compute headroom thresholds.
_Avoid_: Server status, node tier, load grade

**ShardRouteResult**:
The immutable routing resolution envelope containing selected primary node ID, endpoint URI, spillover node ID, execution mode, and lookup latency nanoseconds.
_Avoid_: Route response, dispatch envelope, shard pick

---

### Autonomous Semantic Schema Migration & Backward-Compatible State Translation Engine (Seam 47 / ADR 0058)

**SchemaMigrationEngine**:
The deep module orchestrating automated shortest-path migration graph planning, directional field transformations, and backward-compatible state dictionary translations in $<30\mu\text{s}$ with zero LLM calls.
_Avoid_: Schema fixer, version converter, json updater, data mutator

**SchemaVersion**:
The structured semantic version identifier (`major.minor.patch`) supporting deterministic precedence comparisons, ordering, and semver parsing.
_Avoid_: Version tag, release string, schema level

**FieldTransformAction**:
The categorical primitive transformation action (`RENAME`, `CAST_TYPE`, `ADD_DEFAULT`, `REMOVE_DEPRECATED`, `NEST_FIELDS`, `FLATTEN_FIELDS`, `COMPUTE_DERIVED`) executed on payload fields.
_Avoid_: Change type, mutator action, edit command

**FieldTransformRule**:
The atomic, deterministic transformation rule binding target field path, transform action, parameters, and default fallback values.
_Avoid_: Rule item, transform config, schema patch

**MigrationStep**:
The registered bidirectional transformation link between two discrete schema versions encapsulating forward and backward rule sequences.
_Avoid_: Version hop, upgrade step, transition record

**MigrationPlan**:
The calculated shortest path of sequential migration steps traversing the schema version DAG from source to target version.
_Avoid_: Migration route, step sequence, version path

**MigrationResult**:
The immutable output envelope containing transformed payload, applied version hops, schema validation status, and execution latency nanoseconds.
_Avoid_: Migration response, output json, translated data

---

### Dynamic Multi-Agent Epistemic Logic & Knowledge Base State Synchronization Engine (Seam 48 / ADR 0059)

**EpistemicEngine**:
The deep module providing in-process modal epistemic logic ($S5/KD45$) tracking, fast contradiction detection ($<50\mu\text{s}$), and AGM-style belief revision with zero external LLM calls.
_Avoid_: Fact checker, logic bot, truth validator, debate judge

**EpistemicTruthValue**:
The discrete truth assignment (`TRUE`, `FALSE`, `UNKNOWN`, `CONTRADICTORY`) bound to a propositional claim.
_Avoid_: Boolean flag, truth rating, validity stat

**EpistemicStatus**:
The categorical coherence state (`CONSISTENT`, `DISSONANT`, `STALE`, `UNVERIFIED`) representing the convergence of knowledge assertions across the agent swarm.
_Avoid_: Agreement score, belief flag, sync health

**PropositionalClaim**:
The immutable modal knowledge assertion $K_i \phi$ capturing source agent, topic, predicate, truth value, confidence score, and causal evidence linkage.
_Avoid_: Fact record, assertion statement, agent claim, state sentence

**EpistemicConflict**:
The direct formal contradiction between two or more propositional claims asserted by different agents regarding the same predicate ($K_i \phi \land K_j \neg\phi$).
_Avoid_: Disagreement, argument, collision, logic error

**EpistemicKnowledgeBase**:
The isolated, thread-safe in-memory store indexing agent belief spaces, mutual knowledge ($E_G \phi$), and active contradiction pairs for a swarm.
_Avoid_: Fact DB, belief map, truth store, agent memory

**EpistemicDissonanceScore**:
The normalized scalar metric $D \in [0.0, 1.0]$ quantifying the total cognitive disagreement and contested propositions across an agent swarm.
_Avoid_: Conflict rate, error percent, divergence index

---

### Zero-Knowledge Causal Proof Generation & Cryptographic Audit Engine (Seam 49 / ADR 0060)

**ZkProofEngine**:
The deep module generating zero-knowledge causal execution proofs, Merkle tree commitments, and verifiable audit receipts in $<500\mu\text{s}$ with zero external LLM calls.
_Avoid_: Crypto logger, snark service, proof bot, notary

**SpanCommitmentLeaf**:
The cryptographic commitment leaf $L_i$ binding span ID, turn index, agent ID, payload hash, causal parent hashes, and circuit breaker trip state into a SHA-256 digest.
_Avoid_: Leaf record, hash row, proof item, span digest

**MerkleAuditProof**:
The $O(\log n)$ sequence of sibling hashes proving mathematical inclusion of a commitment leaf within the immutable session Merkle root without disclosing prompt or message content.
_Avoid_: Hash list, tree proof, path array, audit trail

**CausalAuditReceipt**:
The complete tamper-evident audit package containing commitment metadata, Merkle proof, session root, and cryptographic verification signature.
_Avoid_: Audit log, receipt json, compliance dump, proof packet

**ProofCommitmentType**:
The categorical audit assertion type (`TURN_INCLUSION`, `CIRCUIT_TRIP`, `CAUSAL_LINEAGE`, `POLICY_COMPLIANCE`) specifying the verified regulatory invariant.
_Avoid_: Proof kind, audit mode, check category

**CausalProofAccumulator**:
The in-process binary Merkle tree engine building session commitment trees, computing root anchors, and verifying audit receipts in $<20\mu\text{s}$.
_Avoid_: Merkle calc, tree builder, hash chain, accumulator service

---

### Canonical Causal Encoding & Byte-Deterministic Contract Engine (Seam 50 / ADR 0061)

**CanonicalCodec**:
The deep module providing byte-deterministic canonical serialization conforming to RFC 8785 (JCS) principles, guaranteeing identical SHA-256 state hashes across Python 3.12+ and TypeScript/Node.js 22+.
_Avoid_: JSON dumper, serializer utility, stringifier, object flattener

**CanonicalDomainTag**:
The cryptographic prefix string (e.g. `sw:leaf:v1`, `sw:snap:v1`, `sw:rcpt:v1`, `sw:pmt:v1`) separating hashing contexts to prevent cross-domain proof or state substitution attacks.
_Avoid_: Hash salt, prefix label, tag type, domain name

**CanonicalEncodingError**:
The fail-closed exception raised when an input object contains non-finite numbers (`NaN`, `Infinity`), circular references, or unrepresentable types.
_Avoid_: JSON error, serialize exception, dump failure

---

### Unified Safety Authority & Linearizable Execution Permit Engine (Seam 51 / ADR 0062)

**SafetyAuthority**:
The deep module and linearizable state machine acting as the single authoritative decision plane across distributed workers, issuing, committing, and tripping execution permits.
_Avoid_: Guard manager, trip service, auth controller, budget master

**SafetyPermit**:
A cryptographically verifiable, unexpired token authorizing an agent execution turn or tool invocation under a tenant budget and authoritative fencing epoch.
_Avoid_: Token ticket, pass json, lease dict, access permit

**PermitStatus**:
The explicit categorical decision status (`ACCEPTED`, `REJECTED`, `INDETERMINATE`) returned by the authority for any execution request.
_Avoid_: Check code, result string, verdict flag

**AuthorityState**:
The global or tenant operating condition of the SafetyAuthority (`NOMINAL`, `TRIPPED`, `DRAINING`).
_Avoid_: Status mode, health state, circuit condition

**FencingEpoch**:
A monotonically increasing integer counter incremented upon every circuit trip or re-arm, permanently and atomically obsoleting all execution permits issued under previous epochs.
_Avoid_: Version number, lease counter, timestamp tag

---

### Fenced Effect Ledger & Monotonic Idempotency Barrier

**EffectLedger**:
The deep module and compare-and-swap (CAS) transition system enforcing that each external side-effect executes at most once under a monotonic fencing epoch.
_Avoid_: Action log, tool tracker, effect database, call history

**EffectClaim**:
A time-bounded, epoch-fenced reservation placed on an effect key prior to executing an external tool action.
_Avoid_: Lock ticket, action reservation, claim ticket

**EffectRecord**:
The internal durable state tracking an effect key, its idempotency key, epoch, tenant ID, and claim status.
_Avoid_: Log entry, row dict, history record

**EffectReceipt**:
A cryptographically verifiable certificate signed with canonical domain tag `sw:eff:v1` proving that an external effect completed and committed exactly once.
_Avoid_: Proof receipt, done receipt, result token

**ClaimStatus**:
The discrete lifecycle state of an effect claim (`CLAIMED`, `AUTHORIZED`, `COMMITTED`, `ABORTED`, `EXPIRED`).
_Avoid_: Action state, progress flag, step code

**IdempotencyKey**:
A unique client-provided string deduplicating concurrent or repeated requests for the same side-effectual action.
_Avoid_: Nonce, request tag, repeat key

---

### Loss-Aware Causal Ingestion & Evidentiary Completeness Rule

**CausalIngress**:
The prioritized, bounded ingestion coordinator that assigns monotonic sequence numbers and detects dropped spans to prevent forensic blindspots.
_Avoid_: Event collector, stream queue, log consumer, ingestion pump

**IngressPriority**:
The priority tier of an incoming telemetry span (`NORMAL`, `HIGH`, `CRITICAL`), ensuring that safety circuit trips and honeypot alerts are preserved under queue saturation.
_Avoid_: Urgency level, tag weight, priority int

**CompletenessStatus**:
The evidentiary state of a swarm trace (`COMPLETE`, `INCOMPLETE`, `SATURATED`).
_Avoid_: Integrity flag, check status, valid state

**SequenceGap**:
An explicit, recorded discontinuity ($s_i - s_{i-1} > 1$) in the monotonic sequence stream proving telemetry loss.
_Avoid_: Missing event, drop mark, queue skip

**CompletenessReport**:
The diagnostic summary detailing submitted, accepted, and dropped span counts, sequence gaps, and the evidentiary completeness verdict.
_Avoid_: Loss stats, audit log, queue dump

**EvidentiaryCompleteness**:
The invariant requiring that missing causal ancestors or dropped sequence ranges permanently disqualify an execution trace from being certified as an intact causal DAG.
_Avoid_: Fullness guarantee, unbroken check, proof intactness

---

### Durable Audit Anchor & Merkle Audit Receipt Engine

**AuditAnchor**:
The append-only durable root ledger and verification authority that prevents mutable roots and process restarts from rewriting historical execution telemetry.
_Avoid_: Proof keeper, root manager, tree database, log anchor

**RootEpoch**:
An immutable record of an epoch's Merkle root, monotonically increasing epoch index, previous chain digest, new chain digest, and cryptographic signature.
_Avoid_: Epoch block, root snapshot, state commit

**MerkleAuditReceipt**:
A cryptographically verifiable inclusion package containing leaf commitment, $O(\log n)$ sibling hashes, epoch root, chain digest, and cryptographic signature.
_Avoid_: ZK proof, zero knowledge certificate, inclusion token

**ChainDigest**:
The rolling cryptographic hash link ($H_{chain}^{(e)} = \text{SHA256}(H_{chain}^{(e-1)} \parallel e \parallel R_e \parallel \text{tenant\_id})$) binding all historical epoch roots to Genesis.
_Avoid_: Ledger hash, block chain link, root checksum

**PostCrashChainOfCustody**:
The invariant that an AuditAnchor recovering from a crash or restart must reconstruct and cryptographically verify the continuous hash chain from Genesis before accepting new root commitments.
_Avoid_: Recovery boot, restart scan, integrity replay

---

### Bounded Verification Semantics & Explicit Counterexample Engine

**SafetyVerifier**:
The deterministic verification engine providing truthful bounded verification semantics and minimal counterexample generation without SAT solvers or external LLM API calls.
_Avoid_: Model checker bot, proof prover, theorem solver, validator script

**BoundedVerdict**:
The discrete formal verdict (`BOUNDED_SAFE`, `VIOLATED`, `INDETERMINATE`, `UNBOUNDED_PROVEN`, `VACUOUS`) certified for a transition model within exploration bound $k$.
_Avoid_: Check result, pass flag, verification status, proof code

**BOUNDED_SAFE**:
Verification verdict indicating the invariant holds strictly for all states reachable within depth $k$, while the exploration frontier remains open beyond $k$.
_Avoid_: Verified, completely safe, bug-free, unconditionally safe

**UNBOUNDED_PROVEN**:
Verification verdict certifying that the entire reachable state space is exhaustively closed ($V_{\text{frontier}} = \emptyset$) at depth $d \le k$, proving the invariant for all possible executions.
_Avoid_: Totally verified, infinite proof, absolute guarantee

**StateSpaceCoverage**:
The quantitative ratio ($|V_{\text{explored}}| / (|V_{\text{explored}}| + |V_{\text{frontier}}|)$) measuring the explored proportion of discovered reachable states.
_Avoid_: Exploration percent, scan ratio, test coverage

**CounterexampleTrace**:
An ordered, minimal sequence of agent transitions leading directly from the initial configuration to an invariant violation state.
_Avoid_: Error log, failure path, bug trace, crash stack

**TraceValidation**:
The sub-millisecond, independent replay verification (`verify_trace`) validating that each step of a counterexample trace is admissible in the transition system and terminates in a genuine invariant violation.
_Avoid_: Trace check, path replay, bug confirmation

---

### Governed Remediation & Fenced Action State Machine

**RemediationGovernor**:
The deterministic state machine and execution coordinator governing self-healing interventions, approval gating, and monotonic epoch fencing.
_Avoid_: Auto-healer, patch bot, fixer script, remediation manager

**ApprovalTier**:
The authorization tier (`TIER_0_AUTOMATIC`, `TIER_1_SUPERVISED`, `TIER_2_MANUAL`) defining blast radius ceilings and safety permit requirements.
_Avoid_: Auth level, permission grade, approval rank

**ProposalStatus**:
The discrete lifecycle state (`PROPOSED`, `AUTHORIZED`, `APPLIED`, `COMPENSATED`, `REJECTED`, `FAILED`) of a remediation action.
_Avoid_: Patch state, action step, progress flag

**BlastRadius**:
The quantitative bounding contract specifying affected agent IDs, tool names, cost deltas, token limits, and cross-tenant restrictions.
_Avoid_: Impact score, danger zone, blast area

**PreconditionContract**:
The set of deterministic predicates that must strictly hold before a remediation mutation is committed.
_Avoid_: Pre-check, input check, prerequisite list

**PostconditionContract**:
The set of deterministic invariant predicates that must hold immediately following remediation execution, triggering automated compensation on violation.
_Avoid_: Post-check, sanity check, output verify

**CompensationAction**:
The idempotent, deterministic rollback routine restoring system invariants and neutralizing downstream side-effects of an applied remediation.
_Avoid_: Undo script, rollback handler, revert hack

---

### Partition Reconciliation & Conflict Lattice Engine

**MeshReconciler**:
The deterministic reconciliation coordinator resolving divergence between partitioned gossip clusters and shard replicas.
_Avoid_: Rejoin bot, sync script, partition fixer, mesh merger

**ConflictLattice**:
The deterministic partial order resolution lattice preserving provenance and causal integrity across concurrent partitioned spans.
_Avoid_: Conflict resolver, tie breaker, merge tree

**ReconciliationPlan**:
The structured delta specification enumerating missing sequence intervals, diverging vector clocks, and detected state conflicts between peers.
_Avoid_: Diff list, sync instructions, update manifest

**MergeResult**:
The deduplicated, causally ordered telemetry stream result binding converged spans, provenance forks, and sequence gap updates.
_Avoid_: Merged log, combined events, joined stream

**AuthorityReconciliationResult**:
The authoritative safety state outcome strictly enforcing trip monotonicity and monotonic fencing epoch advancement across rejoined partitions.
_Avoid_: Safety status, authority verdict, sync decision

**ProvenanceRetainedSpan**:
A causal telemetry span preserved alongside concurrent counterpart spans with explicit origin peer metadata during partition convergence.
_Avoid_: Conflict clone, fork span, duplicate copy

**TripMonotonicity**:
The foundational safety invariant mandating that a circuit breaker trip recorded in any partition can never be silently cleared by a rejoining nominal peer.
_Avoid_: Permanent halt, sticky trip, latching check

---

### Privacy Release Ledger & Differential Privacy Accountant

**PrivacyRelease**:
The deep module coordinating tenant privacy budget reservations, sensitivity clipping, and composition accounting.
_Avoid_: Privacy proxy, anonymizer wrapper, noise injector, DP service

**PrivacyReservation**:
An ephemeral cryptographically bound reservation permit reserving $\epsilon$ and $\delta$ privacy budget prior to query evaluation.
_Avoid_: Budget hold, query ticket, privacy permit

**PrivacyReleaseResult**:
The release output binding clipped and perturbed query results, injected Laplace noise, and remaining tenant privacy budget.
_Avoid_: Anonymized output, noisy response, sanitized data

**CompositionAccountant**:
The mathematical tracking engine computing cumulative privacy loss over multiple queries under linear and advanced $(\epsilon, \delta)$ composition theorems.
_Avoid_: Budget counter, spend tracker, epsilon tally

**ClippedSensitivity**:
The strict input bounding contract clamping numerical values to $[L_{\text{min}}, L_{\text{max}}]$ to mathematically bound global $L_1$ query sensitivity.
_Avoid_: Min-max clamp, value truncation, range check

---

### Migration Preservation & Partial Isomorphisms

**SchemaEvolution**:
The deep module planning and executing versioned partial isomorphisms across schema definitions, guaranteeing faithful round-trips.
_Avoid_: Schema manager, data converter, migration runner, field transformer

**LossClassification**:
The formal categorical fidelity of a schema migration step or plan: `LOSSLESS` (true isomorphism), `LOSSY_PRESERVED` (partial isomorphism with preservation delta), or `IRREVERSIBLE`.
_Avoid_: Loss type, fidelity grade, migration status

**PreservedDelta**:
The cryptographic record of removed, truncated, downcasted, or shadowed fields required to execute faithful inverse state reconstruction.
_Avoid_: Undo buffer, backup delta, dropped fields cache, shadow tombstone

**EvolutionPlan**:
A deterministic, shortest-path sequence of migration steps between two discrete schema versions with cumulative loss classification.
_Avoid_: Migration route, conversion pipeline, transform path

**PartialIsomorphism**:
The mathematical mapping pair $(f, g, \Delta)$ where forward transformation $f$ produces $(y, \Delta)$ and inverse $g(y, \Delta)$ recovers original state $x$ identically.
_Avoid_: Reversible converter, two-way mapping, bidirectional adapter

---

### Enterprise Acceptance Harness & Multi-Dimensional Release Verification Gate

**AcceptanceHarness**:
The deep module coordinating multi-dimensional release verification across Security, Interoperability, Loss, Latency, Parity, and UI Craft gates.
_Avoid_: Release checker, test runner, validator, gatekeeper

**AcceptanceGate**:
A discrete evaluation dimension (e.g. `SECURITY`, `INTEROPERABILITY`, `LOSS`, `LATENCY`, `PARITY`, `UI_CRAFT`) enforcing binary pass/fail criteria and quantitative thresholds.
_Avoid_: Test phase, quality check, criterion

**AcceptanceReport**:
The comprehensive, structured diagnostic evaluation output enumerating scenario outcomes, gate scores, and detailed violation manifests.
_Avoid_: Test summary, run output, audit dump

**AcceptanceAttestation**:
An immutable, canonically signed cryptographic receipt (`sw:acc:v1`) certifying that a specific software release candidate passed all acceptance gates.
_Avoid_: Signature file, stamp, approval token, certificate

**AcceptanceProfile**:
A named configuration preset (`quick`, `standard`, `production`, `enterprise`) defining the scenario matrix and strictness levels for acceptance gates.
_Avoid_: Run mode, test env, suite config

---

### Consensus Authority Binding & Linearizable Quorum Fencing (Seam 61 / ADR 0072)

**ConsensusAuthorityBinding**:
The deep module coordinating quorum-based replication of `SafetyAuthority` transitions across a cluster of $N$ nodes using a replicated state machine (RSM) log and monotonic fencing epochs.
_Avoid_: Raft wrapper, consensus manager, cluster coordinator, quorum proxy

**QuorumCommitment**:
The state where an operation (reserve, commit, release, trip, rearm) has been acknowledged by a strict majority of nodes $Q \ge \lfloor N / 2 \rfloor + 1$ before state transition is exposed as committed.
_Avoid_: Majority write, cluster sync, node approval

**MinorityFailClosed**:
The fundamental safety invariant whereby any node or partition possessing fewer than a strict quorum of nodes immediately fails closed with `MINORITY_PARTITION` rather than issuing permits or mutations.
_Avoid_: Partition error, split-brain drop, cluster disconnect

**LinearizableReadState**:
State read verified against a quorum of nodes ($Q_{\text{read}} \ge \lfloor N / 2 \rfloor + 1$) verifying that the reading node's epoch is current and no subsequent epoch has been established.
_Avoid_: Fresh read, cluster get, live query

---

### Downstream Effect Protocol & 2PC Outcome-Unknown Reconciliation (Seam 62 / ADR 0073)

**DownstreamEffectBoundary**:
The deep module coordinating downstream effect dispatch, timeout fail-closed outcome-unknown isolation, and active reconciliation probes across network boundaries.
_Avoid_: Effect dispatcher, RPC wrapper, API proxy, downstream gateway

**OutcomeUnknown**:
The explicit fail-closed operational state assumed when a network timeout, socket reset, or server error occurs during in-flight mutation before receiving downstream acknowledgment.
_Avoid_: Timeout error, unknown failure, aborted action, pending request

**DownstreamIdempotencyContract**:
The explicit contract declaration specifying whether a target downstream endpoint enforces atomic deduplication (`DOWNSTREAM_FENCED`) or is uncoordinated (`UNCOORDINATED`).
_Avoid_: API type, retry mode, deduplication flag

**ReconciliationProbe**:
An active idempotent query to downstream services determining whether an in-flight mutation was executed or never arrived, transitioning state out of `OUTCOME_UNKNOWN`.
_Avoid_: Status poller, health check, retry probe, lookup query

**UncoordinatedRetryBlocked**:
The safety invariant preventing blind retries against uncoordinated downstream APIs when in `OUTCOME_UNKNOWN` state.
_Avoid_: Retry limit, blocked call, duplicate error

---

### Critical Evidence Spool & NIST SP 800-53 Write-Ahead Journal (Seam 63 / ADR 0074)

**EvidenceSpool**:
The dedicated, write-ahead journal and buffer isolating safety-critical evidence (circuit trips, Byzantine quarantines, emergency halts, fence revocations, audit anchors) from general operational telemetry.
_Avoid_: Audit logger, trip recorder, log buffer, event sink

**DualTierDurability**:
The formal architectural separation between Tier 0 (fail-closed, zero-drop safety evidence persisted via write-ahead journal) and Tier 1 (loss-aware, bounded operational telemetry with explicit drop accounting).
_Avoid_: Two-level logging, split telemetry, priority queue

**WriteAheadJournal**:
An append-only durable journal using fixed framing, monotonic uint64 sequence numbers, CRC32 checksums, and continuous SHA-256 hash chains.
_Avoid_: Log file, transaction log, disk file, audit trace

**FailClosedAdmission**:
The invariant halting execution with `EVIDENCE_JOURNAL_FULL` when safety journal storage capacity is exhausted, preventing unmonitored agent operation.
_Avoid_: Drop policy, rate limit, buffer overflow drop, silent discard

**CompletenessReceipt**:
An immutable audit certificate proving contiguous, non-repudiable presence of all safety events in range $[1, N]$ with zero dropped critical records under NIST SP 800-53 Rev 5.
_Avoid_: Audit report, check result, verification log

---

### Independent Root Witness & Public-Key Chains (Seam 64 / ADR 0075)

**RootWitness**:
The independent, append-only cryptographic witness verifying tree growth, maintaining asymmetric ECDSA (NIST P-256 / ES256) public-key chains, and producing RFC 6962 consistency proofs.
_Avoid_: Tree signer, root anchor, notary, certification authority

**SignedCheckpoint**:
An immutable root epoch record binding monotonic epoch, non-decreasing tree size, Merkle root hash, previous checkpoint hash, and timestamp under an asymmetric IEEE P1363 digital signature.
_Avoid_: Signed root, epoch ticket, certificate, root block

**LogarithmicConsistencyProof**:
An $\mathcal{O}(\log N)$ Certificate Transparency (RFC 6962) proof demonstrating that an older tree of size $M$ is an unaltered prefix of a newer tree of size $N$.
_Avoid_: Tree diff, incremental proof, history check, tree path

**RollbackAttackProtection**:
The formal verification invariant detecting and rejecting backward epoch proposals or tree size truncation attempts with `RollbackAttackError`.
_Avoid_: Time-travel check, rewind prevention, downgrade guard

**SplitBrainForkProtection**:
The cryptographic chain integrity check detecting divergent parent hashes or broken hash links with `SplitBrainForkError`.
_Avoid_: Fork checker, conflict resolver, split guard

**KeyRotationLog**:
A NIST SP 800-57 compliant audit log verifying key transitions with mutual challenge-response signatures (proof-of-possession by the new key and authorization by the old key).
_Avoid_: Key history, certificate chain, key list

---

### Tenant Identity Boundary & Attenuated Capability Tokens (Seam 65 / ADR 0076)

**TenantCapabilityManager**:
The deep module governing cryptographic capability tokens, enforcing tenant identity isolation, monotonic delegation attenuation, and cascading subtree revocation across multi-agent swarms.
_Avoid_: Access controller, permission manager, token issuer, auth guard

**CapabilityToken**:
An immutable, cryptographically signed token binding tenant identity, issuer principal, subject principal, action scopes, resource pattern, epoch, and time validity windows under an asymmetric ECDSA signature.
_Avoid_: Access ticket, bearer token, API key, auth certificate

**MonotonicAttenuation**:
The invariant requiring delegated child tokens to possess a strict subset of scopes, a narrower resource pattern, and an equal or shorter validity duration than their parent token.
_Avoid_: Permission downscaling, sub-delegation, token narrowing

**SubtreeRevocationCascade**:
The recursive validation rule ensuring that revoking any ancestor token immediately invalidates its entire downward delegation tree in $\mathcal{O}(d)$ time.
_Avoid_: Token cascade, child invalidation, batch revocation

**CapabilityVerdict**:
The fail-closed evaluation outcome returned in $<250\,\mu\text{s}$ asserting cryptographic authenticity, tenant isolation, scope match, resource match, temporal validity, and revocation status before action dispatch.
_Avoid_: Check result, auth verdict, allow/deny response

---

### Effect-Aware Compensation Algebra & Monotone Approval Lattices (Seam 66 / ADR 0077)

**CompensationEngine**:
The deep module executing formal action classification, reversibility lattice join composition, deterministic reverse-ordered compensation planning, and fail-closed human escalation.
_Avoid_: Rollback manager, undo engine, failure recovery service, revert daemon

**ReversibilityClass**:
A bounded total order (lattice) classifying agent actions: $\text{REVERSIBLE} (0) \prec \text{COMPENSATABLE} (1) \prec \text{IRREVERSIBLE} (2)$.
_Avoid_: Action category, rollback type, side-effect level

**MonotoneApprovalLattice**:
The mathematical join operation $\bigsqcup_{i=1}^k \text{Class}(\alpha_i) = \max_i \text{Class}(\alpha_i)$ guaranteeing that an action sequence containing even a single irreversible action yields an irreversible composite classification.
_Avoid_: Lattice check, category merger, risk level accumulator

**CompensationPlan**:
A deterministic sequence of inverse actions ($f^{-1}$) and semantic forward compensations ($c(s)$) executed in strict reverse chronological order $(A_k, \dots, A_1)$.
_Avoid_: Undo script, rollback list, recovery workflow

**HumanEscalationReceipt**:
An immutable cryptographic audit record generated when an irreversible action is encountered, halting automated rollback with `IrreversibleActionError` until human operator authorization.
_Avoid_: Approval ticket, incident ticket, manual review request

**EscalationResolution**:
A non-repudiable authorization record signed by an authorized human principal resolving an escalation with `APPROVED_BYPASS`, `MANUAL_REMEDIATED`, or `ABORTED`.
_Avoid_: Override decision, admin approve, signoff record

---

### Durable Dataset-Level Privacy Accountant & Quorum (ε,δ) Ledger (Seam 67 / ADR 0078)

**DatasetPrivacyLedger**:
The deep module governing durable dataset-level and entity-level differential privacy bounds, atomic reservation admission, calibrated Laplace perturbation, and monotone partition reconciliation.
_Avoid_: Privacy counter, budget tracker, DP manager, noise daemon

**EntityAdjacencyBound**:
The mathematical constraint $\|D \oplus D'\|_{\text{entity}} \le 1$ defining neighboring datasets by adding or removing all contributions of a single entity, ensuring cumulative protection against linkage attacks across disparate queries.
_Avoid_: Record neighbor, row diff, user filter, tenant margin

**HierarchicalBudgetEvaluation**:
The atomic dual-tier check enforcing both global dataset capacity $(\mathcal{E}_{\text{dataset}}, \Delta_{\text{dataset}})$ and per-entity contribution caps $(\epsilon_{\text{entity\_max}}, \delta_{\text{entity\_max}})$ prior to granting query evaluation permits.
_Avoid_: Dual limit, two-stage budget, double check

**DatasetPrivacyReservation**:
An atomic cryptographic permit reserving $(\epsilon, \delta)$ budget prior to query evaluation, preventing over-allocation race conditions and failing closed with `DatasetPrivacyBudgetExhaustedError` or `EntityPrivacyBudgetExhaustedError`.
_Avoid_: Budget lock, reservation ticket, query permit

**MonotoneSupremaReconciliation**:
The distributed join operation $\max(\text{Ledger}_A(e), \text{Ledger}_B(e))$ merging divergent partition expenditures upon network heal, mathematically guaranteeing that spent privacy budget never regresses.
_Avoid_: Ledger sync, budget merge, partition join
















