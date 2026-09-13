# ADR 0001: In-Process Sub-Microsecond Telemetry Queue

## Status
Accepted

## Context
Multi-agent AI systems execute fast iterative loops across multiple cooperating agents. Conventional APM tools send telemetry synchronously over HTTP at each span creation. In a 5-agent swarm doing 30 reasoning rounds, synchronous network round-trips add 150–400ms per step, resulting in 10+ seconds of dead latency.

## Decision
We implement an in-process, non-blocking producer-consumer architecture:
- Spans are pushed to an in-memory thread-safe `queue.Queue` with a bounded maximum capacity (default 10,000).
- If the queue is saturated, incoming spans are dropped non-blockingly with an atomic counter increment, preventing backpressure on agent threads.
- A background daemon worker thread batches flushes (100 items or 500ms timeout) to the storage adapter.
- A deterministic sentinel protocol (`("__SWARMWATCH_FLUSH__", event)`) guarantees that `flush()` and `shutdown()` completely drain in-flight batches before termination.

## Consequences
- **Positive**: Telemetry submission takes $<1\mu s$, imposing zero perceptible latency on agent loops.
- **Positive**: Network failures or slow disks cannot freeze agent reasoning.
- **Negative**: If an unhandled `SIGKILL` abruptly kills the OS process, at most 500ms of unpersisted spans could be lost.
