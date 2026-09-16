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



