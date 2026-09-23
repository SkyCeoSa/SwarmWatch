# SwarmWatch

A lightweight, zero-token observability runtime, entropy circuit breaker, and state time-machine for multi-agent LLM systems.

## Language

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

### State & Time Machine

**Branch**:
An independent, isolated trajectory of swarm execution states originating from a root session or an explicit fork.
_Avoid_: Fork name, thread, session variant

**Time-Machine Fork**:
A non-destructive operation that clones historical state at a target span into a new branch for parallel exploration or bug recovery.
_Avoid_: Rewind, roll back, restore point

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

### Operational Metrics & Prometheus

**PrometheusMetric**:
A standardized OpenMetrics time-series sample (Counter, Gauge, or Histogram) measuring runtime swarm throughput, circuit safety events, and operational latency.
_Avoid_: Log statistic, telemetry stat, monitoring value

**ScrapeEndpoint**:
The dedicated `/metrics` HTTP handler serving OpenMetrics formatted exposition payload to cluster Prometheus or Datadog agents.
_Avoid_: Stats URL, monitoring page, ping route

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
- **ChaosInjection**: The deterministic execution of a synthetic failure scenario within the control plane to validate circuit breaker reactions, dashboard metric gauges, and alert dispatchers.
- **ChaosScenario**: A reproducible specification of agent roles, synthetic turn payloads, and token consumption patterns designed to trigger a specific circuit breaking failure mode.
- **EntropyCollapseScenario**: Repetitive agent code generation collapsing lexical Shannon entropy below the safety floor.
- **InfiniteRecursionScenario**: Recursive tool execution failures with identical parameters exceeding repetition limits.
- **BudgetQuotaExhaustionScenario**: Runaway token consumption breaching configured token or dollar budget ceilings.
- **ConversationalDeadlockScenario**: Polite ping-pong dialogue between autonomous conversational agents with high token similarity.

---

### Visual Causal Topology
- **CausalTopology**: Spatial, directed network representation of swarm agent interactions, mapping sender-receiver flows and cycle boundaries.
  _Avoid_: Call graph, flowchart, diagram
- **AgentNode**: Visual representation of an autonomous agent in topological space, encapsulating role, message count, and health state.
  _Avoid_: Box, circle, component
- **CausalEdge**: Directed vector connecting a source agent to a target agent, weighted by turn count, mean latency, and token volume.
  _Avoid_: Arrow, link, connector
- **CyclicLoopPath**: A closed directed cycle ($A \to B \to \dots \to A$) detected in agent message traffic indicating an active or impending runaway loop.
  _Avoid_: Recursion line, bad link, loop circle
- **TopologyMatrix**: Adjacency representation used to analyze message density and detect cycles across distributed agents.
  _Avoid_: Route table, map

---

### Enterprise Security & Secret Redaction Engine
- **SensitiveDataMasker**: A deterministic, in-process engine executing pre-compiled regex patterns to redact cloud keys, database DSNs, JWTs, PEM private keys, credit cards, and PII before telemetry persistence.
  _Avoid_: Cleaner, scrubber, filter script
- **SecretPattern**: A formalized regular expression targeting a specific credential or authentication token format within arbitrary payload structures.
  _Avoid_: Search regex, match rule, pattern check
- **RedactedToken**: The safe placeholder string (e.g. `[REDACTED_API_KEY]`, `[REDACTED_DB_DSN]`) substituting sensitive values while preserving overall payload schema.
  _Avoid_: Starred password, blank text, hidden string

---

### Incident Post-Mortem & Financial ROI Engine
- **IncidentPostMortem**: A structured forensic and financial audit document generated upon swarm completion or circuit trip, detailing prevented waste, root cause diagnosis, and entropy trajectory.
  _Avoid_: Incident ticket, bug log, crash dump, error trace
- **FinancialRoiMetrics**: The quantitative calculation of compute dollars and token expenditure prevented by circuit breaker intervention.
  _Avoid_: Guess, profit stat, cost estimation
- **RootCauseDiagnosis**: The deterministic attribution of the failure mode, causal agent pair, and trigger turn responsible for circuit intervention.
  _Avoid_: Blame, error guess, bug attribution
- **EntropyTrajectory**: The chronological time-series of normalized Shannon entropy ($H_{norm}$) and Jaccard similarity across agent conversational turns.
  _Avoid_: Diversity graph, randomness history
- **SecurityRedactionSummary**: The categorized tally of sensitive authentication tokens and PII neutralized during swarm execution.
  _Avoid_: Censored list, scrub count

---

### Declarative Guardrail Policy Engine & Policy DSL
- **GuardrailPolicy**: A declarative specification document (YAML or JSON) defining global baseline thresholds, role-based agent overrides, and event-condition-action guardrail rules.
  _Avoid_: Config file, settings script, security ruleset
- **PolicyEngine**: The deterministic, sub-microsecond in-memory evaluation engine that resolves agent rule inheritance and enforces declarative guardrail policies at runtime.
  _Avoid_: Filter, rule checker, validator script
- **RuleConfig**: The quantitative constraints applied to an agent or swarm session, including entropy floors, similarity ceilings, token budgets, turn limits, and prohibited tools.
  _Avoid_: Agent limits, thresholds, budget dict
- **DeclarativeRule**: An event-condition-action statement matching agent patterns and predicates to execute circuit trips, warnings, or redactions.
  _Avoid_: Trigger, alert condition, check rule
- **PolicyEvaluationResult**: The structured verdict (`pass`, `warn`, or `trip`) emitted by the PolicyEngine for an agent turn or causal span.
  _Avoid_: Check output, decision result, validation status

---

### Visual Time-Machine & Branch Forking Controls
- **TimeMachineScrubber**: The bi-directional interactive control bar enabling temporal traversal across multi-agent execution turns with instantaneous state replay.
  _Avoid_: Playhead, video bar, seek widget
- **BranchFork**: An isolated, divergent execution timeline created from an antecedent state snapshot, replicating historical spans and enabling counterfactual continuation.
  _Avoid_: Split run, copy branch, new test run
- **TurnTimeline**: The ordered sequence of discrete agent execution steps and state snapshots indexed chronologically along an execution branch.
  _Avoid_: Turn list, history feed, message thread

---

### Live Fork Resumption Runner
- **ForkRunner**: A deep module and runtime execution coordinator that programmatically resumes autonomous agent execution from an antecedent state snapshot on a designated branch.
  _Avoid_: Resume script, restart handler, runner bot, execution manager
- **ForkResumptionContext**: The isolated, scoped execution context providing the resumed agent handler with sanitized antecedent state, applied state overrides, active branch binding, and causal span linkage.
  _Avoid_: Resume info, state bag, context dict
- **ResumptionSpan**: An immutable atomic boundary span linking the antecedent target span of a forked branch to newly resumed agent execution turns.
  _Avoid_: Resume marker, fork log, restart event

---

### State Snapshot Diff Inspector
- **StateDiffReport**: The structured, deterministic comparison output between two state snapshots encapsulating added, modified, and removed entries alongside cryptographic fingerprints.
  _Avoid_: Diff output, state comparison, delta json
- **DiffEntry**: An atomic property-level delta record specifying property path, mutation kind (`added`, `modified`, `removed`, `unchanged`), antecedent value, subsequent value, and secret redaction status.
  _Avoid_: Change line, diff item, mutation log
- **FingerprintDelta**: Cryptographic state transition indicator tracking SHA-256 hash mutations across discrete execution turns.
  _Avoid_: Hash change, checksum diff

---

### Industrial Monitoring Stack & Grafana Topology
- **MonitoringTopology**: The multi-service Docker Compose architecture orchestrating SwarmWatch, Prometheus, and Grafana in an isolated network with zero external cloud dependencies.
  _Avoid_: Docker cluster, server group, compose bundle
- **DashboardPreset**: The pre-built, production-grade Grafana dashboard JSON specification mapping SwarmWatch OpenMetrics to executive ROI, throughput, and safety panels.
  _Avoid_: Grafana template, board json, chart layout
- **DatasourceProvisioning**: The automated filesystem-based registration of the local Prometheus service within Grafana without manual UI interaction.
  _Avoid_: DB setup, connection config, manual datasource
---

### Automated Alert Route Webhook Testing & Synthetic Simulation Pipeline
- **SyntheticAlertReceiver**: An ephemeral local mock HTTP server that receives and schema-validates outgoing webhook notifications for Slack, Discord, and PagerDuty without third-party cloud dependencies.
  _Avoid_: Fake webhook server, dummy endpoint, mock server
- **AlertSimulationPipeline**: An automated test harness executing multi-channel notification verification, fault injection, Dead-Letter Queue (DLQ) buffering, and live circuit breaker trip linkage.
  _Avoid_: Alert test script, webhook test suite
- **FaultInjectionScenario**: A programmed simulation of upstream HTTP network errors (503 Service Unavailable, connection timeout) used to verify caller non-blocking behavior and DLQ preservation.
  _Avoid_: Chaos network test, simulated downtime
- **RouteSimulationReport**: A structured summary of delivery latencies, schema validity, retry counts, and dead-letter replay status across all tested alert routes.
  _Avoid_: Webhook result, test output
---

### High-Frequency Telemetry Ingestion Micro-Benchmark & Flamegraph Profile Suite
- **TelemetryProfiler**: An in-process, zero-dependency benchmarking and memory profiling engine measuring span ingestion throughput, microsecond enqueue latency distributions, and heap retention under concurrency.
  _Avoid_: Benchmark script, load tester, speed test
- **IngestionBenchmarkReport**: The diagnostic evaluation output containing throughput (spans/sec), latency percentiles (P50, P90, P99), RSS memory deltas, and zero memory leak verification.
  _Avoid_: Benchmark log, test metrics, profile dump
- **LatencyPercentiles**: The granular distribution breakdown (P50, P90, P99, Min, Max, Mean) of non-blocking span submission durations measured in microseconds ($\mu s$).
  _Avoid_: Timers, speed stats, delay metrics
- **CollapsedStackFlamegraph**: A text-based trace representation formatting internal method call stacks and execution counts into `frame1;frame2 count` syntax compatible with standard flamegraph viewers.
  _Avoid_: Profile graph, flame chart, stack trace dump
---

### Cloud-Native Kubernetes & Helm Topology
- **CloudNativeTopology**: The container orchestration architecture and Kubernetes resource definitions governing high-availability, horizontally-scalable SwarmWatch deployments.
  _Avoid_: K8s cluster setup, pod layout, server deployment
- **HelmChartPackage**: The versioned, reproducible package bundle (`Chart.yaml`, `values.yaml`, templates) automating deployment, ingress routing, volume persistence, and autoscaling across cloud Kubernetes clusters.
  _Avoid_: Helm bundle, chart archive, deploy script
- **IngressRouting**: The HTTP and WebSocket reverse-proxy ingress definition with TLS termination and long-lived streaming timeouts for SwarmWatch APIs and console.
  _Avoid_: Reverse proxy route, url rewrite, ingress path
- **ServiceMonitorBinding**: The Prometheus Operator custom resource definition enabling automated Prometheus discovery and scraping of `/metrics` at high frequency.
  _Avoid_: Prometheus config, scrape rule, monitor CRD
---

### Terminal User Interface (TUI) & Headless Cluster Observability
- **TerminalMonitor**: The in-process terminal controller orchestrating periodic data polling, ANSI viewport formatting, and keyboard interaction loop for SwarmWatch sessions.
  _Avoid_: Terminal watcher, CLI poller, console daemon
- **AnsiFrameBuffer**: The pure-function terminal layout engine rendering structured, responsive ASCII tables, health banners, and causal span streams using ANSI color escapes.
  _Avoid_: Screen buffer, console renderer, terminal output string
- **HeadlessTriage**: The operational workflow enabling developers and SREs to monitor, diagnose, and inspect active swarms and circuit halts inside terminal shells without a web browser.
  _Avoid_: CLI debugging, terminal checking, log tailing
---

### Distributed Redis Circuit Breaker & Fleet State Synchronization
- **RedisCircuitCoordinator**: The deep module coordinating distributed swarm sessions across multiple worker processes, container pods, and Kubernetes replicas using Redis as the high-throughput synchronization backbone.
  _Avoid_: Distributed lock manager, cache controller, redis cluster boss
- **RedisFleetBackend**: The storage and messaging abstraction managing Redis key formatting, atomic quota evaluation, and pub/sub transport.
  _Avoid_: Redis adapter, db connector, redis wrapper
- **RedisTripBroadcast**: The real-time event payload dispatched over Redis Pub/Sub channels when a worker halts a swarm session, achieving sub-millisecond fleet-wide intervention.
  _Avoid_: Halt message, redis pubsub alert, kill ping
- **RedisQuotaLedger**: The atomic hash data structure in Redis storing cumulative turns, tokens, costs, and trip status per swarm session.
  _Avoid_: Redis budget hash, quota table, cache counter
- **RESPRedisClient**: The pure Python standard library socket implementation of the Redis Serialization Protocol (RESP2) providing zero-external-dependency communication with Redis.
  _Avoid_: Socket script, redis connector, mini client
- **MockRedisClient**: The thread-safe in-memory Redis client supporting hash operations, atomic increments, and pub/sub dispatch for offline and local testing.
  _Avoid_: Fake redis, test cache, memory db
---

### Distributed Redis Fleet & Cluster Monitor Widget
- **RedisFleetWidget**: The developer console interface component visualizing distributed cluster synchronization state, active worker rosters, and real-time atomic quota meters.
  _Avoid_: Redis graph, cluster viewer, fleet window
- **FleetClusterStatus**: The visual indicator representing whether the Redis backend is synchronized (`CONNECTED`), degraded, or running in local in-memory mock mode.
  _Avoid_: Redis state tag, cluster LED, connection pill
- **FleetTripBanner**: The urgent, non-decorative alert surface rendering when a distributed trip broadcast is received from any cluster worker pod.
  _Avoid_: Error box, red toast, halt popup
- **FleetQuotaMeter**: The tabular numeral progress indicator displaying current consumption against session ceiling limits (turns, tokens, dollar cost).
  _Avoid_: Speedometer, gauge bar, progress slider
- **EmergencyHaltModal**: The confirmation modal allowing operators to dispatch an immediate manual circuit trip across all nodes in the distributed cluster.
  _Avoid_: Kill popup, abort dialog, halt window
---

### Distributed Redis Lock & Leader Election (Seam 24 / ADR 0035)
- **RedisDistributedLock**: The atomic mutual exclusion primitive implementing safe distributed locking via Redis `SET NX PX` and CAS release/extend Lua scripts.
  _Avoid_: Process lock, mutex file, redlock clone
- **RedisLeaderElector**: The coordination manager running heartbeat leases, automatic standby failover, and voluntary step-down routines across multi-server SwarmWatch instances.
  _Avoid_: Master slave script, election loop, vote daemon
- **ServerNodeRole**: The explicit state of a SwarmWatch server node within the cluster (`LEADER`, `STANDBY`, or `STEPPED_DOWN`).
  _Avoid_: Master node, slave worker, backup server
- **LeaderHeartbeat**: The periodic lease renewal dispatched by the active Leader every renewal interval to prevent lease expiration.
  _Avoid_: Keepalive ping, leader tick, lease refresher
- **ClusterNodeRoster**: The cluster-wide registry of active SwarmWatch server instances, roles, and epoch timestamps stored in Redis.
  _Avoid_: Server list, node catalog, host directory
---

### Cold Storage Streaming Log Archival & Cold Storage Tier (Seam 25 / ADR 0036)
- **ObjectStorageClient**: The pluggable object storage interface abstracting Amazon S3, MinIO, Ceph, Cloudflare R2, and Google Cloud Storage operations (`upload_object`, `download_object`, `list_objects`, `delete_object`, `head_object`).
  _Avoid_: S3 wrapper, bucket adapter, cloud connector
- **MockObjectStorageClient**: The thread-safe in-memory object store providing deterministic byte storage, checksum validation, and prefix listing for offline tests.
  _Avoid_: Fake S3, dummy bucket, test blob
- **S3ObjectStorageClient**: The pure standard-library HTTP/SigV4 REST client communicating with S3-compatible cloud object storage without external SDK dependencies.
  _Avoid_: Boto clone, S3 script, aws-cli runner
- **ColdStorageArchiver**: The deep module coordinating the serialization, gzip compression, partitioning, and cryptographic manifest generation for cold swarm telemetry offloading.
  _Avoid_: Log tarballer, dump maker, zip exporter
- **ArchiveManifest**: The immutable cryptographic metadata envelope recording bit-level SHA-256 hashes, uncompressed/compressed sizes, compression ratio, telemetry tallies, and multi-tenant context.
  _Avoid_: Index file, info json, header tag
- **ArchiveRecord**: The high-level descriptor of an archived swarm session stored in object storage.
  _Avoid_: Cold row, archive summary, backup entry
- **PartitionScheme**: The standard time-partitioned object key layout: `swarmwatch/v1/org={org_id}/proj={project_id}/year={YYYY}/month={MM}/day={DD}/swarm={swarm_id}.jsonl.gz`.
  _Avoid_: Folder structure, key path, bucket directory
---

### Enterprise MFA / TOTP & Role-Based Access Control (Seam 26 / ADR 0037)
- **Role**: Canonical authorization tier within the SwarmWatch enterprise control plane (`ADMIN`, `OPERATOR`, `AUDITOR`, `VIEWER`).
  _Avoid_: User level, access rank, account group
- **Permission**: Granular action entitlement defining authorized operations (e.g. `swarms:trip`, `fleet:manage`, `policy:write`, `keys:revoke`).
  _Avoid_: Priv, capability, access flag
- **TotpEngine**: Zero-dependency RFC 6238 Time-Based One-Time Password generator and validator implementing HMAC-SHA1 dynamic truncation across 30-second steps.
  _Avoid_: Authenticator plugin, 2FA library, OTP tool
- **StepUpChallenge**: Mandatory secondary authentication verification required prior to executing sensitive or destructive operations via `X-MFA-Code`.
  _Avoid_: Sudo mode, reauth check, secondary password
- **BackupRecoveryCode**: Single-use formatted emergency recovery code (`XXXX-XXXX`) stored as a cryptographic SHA-256 hash and burned upon verification.
  _Avoid_: Backup key, rescue string, static password
- **SecurityAuditEvent**: Structured and immutable audit trail event recording security identity actions, enrollment steps, and access decisions.
  _Avoid_: Security log, auth record, event trace


