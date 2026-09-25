# ADR 0064: Loss-Aware Causal Ingestion & Evidentiary Completeness Rule

## Status
Accepted (Seam 53)

## Context
In high-throughput multi-agent swarms, telemetry ingestion pipelines face non-blocking queue overflow and shared-memory backpressure. Prior to this seam, high-volume span submission satisfied caller latency constraints by silently dropping spans.

However, silent span drops undermine causal auditability and forensic root-cause analysis:
1. If an intermediate turn or tool output is dropped, downstream causal DAG reconstruction treats the missing parent as a root or ignores it, masking causal loops and state poisoning.
2. Safety circuit trips, policy interventions, and security honeypot alerts could be dropped alongside mundane conversational tokens during load spikes.
3. Auditors cannot verify whether an execution trace represents a complete, intact causal history or a fragmented subset.

## Decision
We implement a deep, prioritized, and loss-aware causal ingestion engine (**`CausalIngress`**) with an explicit **Evidentiary Completeness Rule**:

1. **Monotonic Sequence Allocation & Gap Accounting**:
   Every submitted span is assigned a monotonically increasing sequence number $s \in \mathbb{N}$ per `(swarm_id, stream_id)`. Any missing sequence ($s_i - s_{i-1} > 1$) is recorded as an immutable `SequenceGap`.

2. **Evidentiary Completeness Status**:
   - `COMPLETE`: The trace has zero gaps and all referenced causal parents are accounted for.
   - `INCOMPLETE`: Any gap or dangling parent reference immediately degrades the forensic verdict to `INCOMPLETE`. An incomplete trace is never certified as an intact causal DAG.
   - `SATURATED`: The ingestion buffer has experienced queue overflow.

3. **Prioritized Headroom & Safety Preservation**:
   - Spans are classified by `IngressPriority`: `NORMAL`, `HIGH`, `CRITICAL`.
   - Normal conversational turns may be shed under queue saturation, but a dedicated reserve headroom guarantees that `CRITICAL` spans (circuit breaker trips, honeypot alarms, security redactions) are never dropped (100% delivery ratio).

4. **Zero-Token Local Execution**:
   Operates strictly in-memory with sub-microsecond atomic sequencing, bounded FIFO queues, and zero external LLM dependencies.

## Consequences
- Queue overflows become visible and measurable rather than silent data corruption.
- Guarantees 100% preservation of critical security interventions and circuit breaker trips even under catastrophic telemetry saturation.
- Delivers an explicit evidentiary completeness contract across both Python and TypeScript runtimes.
