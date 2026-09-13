# SwarmWatch Domain Model & Ubiquitous Language

This document defines the formal domain model, ubiquitous vocabulary, and core invariants of **SwarmWatch**. All contributors, modules, and tests must adhere strictly to these definitions.

---

## 1. Glossary (Ubiquitous Language)

### Core Observability Entities
- **SwarmSpan**: An immutable atomic unit of agent activity representing a causal computation, tool call, chain execution, or inter-agent message transfer. A span contains `source_agent`, `target_agent`, `start_time`, `end_time`, `duration_ms`, `token_count`, `entropy_score`, and optional `parent_span_id`.
- **Causality Flow**: The directed acyclic graph (DAG) formed by `parent_span_id` relationships linking parent tasks to downstream child steps across different collaborating agents.
- **StateSnapshot**: An immutable capture of an agent's memory or working context at a specific span index, compressed via GZIP with sensitive keys (`sk-...`, passwords, secrets) redacted before persistence.

### Zero-Token Circuit Breaker
- **Normalized Shannon Entropy ($H_{norm}$)**: A statistical measure of lexical diversity in an agent's token sequence:
  $$H(X) = -\sum_{i=1}^{N} p(x_i) \log_2 p(x_i), \quad H_{norm} = \frac{H(X)}{\log_2 N}$$
  Bounded strictly in $[0.0, 1.0]$. A score below the threshold signifies repetitive babble or low-information loops.
- **Sliding-Window Lexical Similarity**: Token-level Jaccard similarity across recent turns:
  $$J(A, B) = \frac{|A \cap B|}{|A \cup B|}$$
  Computed in sub-millisecond time with zero LLM API calls.
- **Intervention Engine**: The two-tier safety enforcement model:
  1. *Tier 1 (Soft Warning)*: Injection of `[SWARMWATCH CIRCUIT WARNING]` system hint into the agent context window to stimulate self-correction.
  2. *Tier 2 (Hard Halt)*: Halting runaway execution by raising `SwarmCircuitBreakException` if repetitive behavior persists across consecutive turns.

### Time Machine & State Branching
- **Branch**: An isolated line of execution state. By default, agents run on `main`.
- **Branch Forking**: Non-destructive creation of a divergent state line (`fork-01`, `fork-02`) from a historical snapshot, allowing speculative re-execution or bug patching without corrupting the canonical run.

---

## 2. Core Seams & Architectural Boundaries

According to Deep Module principles:
1. **Telemetry Ingestion Seam**:
   - *Interface*: `client.span(...)`, `@swarmwatch.agent()`, `client.sync_state(...)`.
   - *Behind the seam*: Lock-free bounded ring buffer, atomic monotonic sequence numbering, daemon worker thread, transactional SQLite batch flushes.
2. **Storage Adapter Seam**:
   - *Interface*: `StorageAdapter` abstract base class (`save_spans`, `get_spans`, `save_snapshot`, `get_snapshots`, `fork_branch`).
   - *Implementation*: `SQLiteStorageAdapter` (thread-local connections, WAL mode), pluggable into ClickHouse or PostgreSQL.
3. **Ecosystem Adapter Seam**:
   - *Interface*: `SwarmWatchCallbackHandler` (LangChain/LangGraph) and `SwarmWatchCrewListener` (CrewAI).
   - *Behind the seam*: Translation of disparate framework events into canonical `SwarmSpan` and automatic loop interception without monkey-patching.

---

## 3. Invariants & Guarantees

1. **Zero Telemetry Latency Impact**: Span submission must NEVER perform synchronous network I/O on the agent execution thread. Overhead must remain $< 1\mu s$.
2. **Deterministic Process Drain**: On process exit (`atexit` or `shutdown()`), the queue worker must flush all pending spans and snapshots using a flush sentinel before closing storage connections.
3. **Thread Safety**: SQLite connections must never be shared across OS threads. Each thread accesses an independent connection configured with `PRAGMA journal_mode=WAL` and `PRAGMA busy_timeout=5000`.
4. **Secret Leakage Prevention**: Payloads must be sanitized before persistence; any string matching regex patterns for API keys (`sk-...`, Bearer tokens) must be permanently redacted.
