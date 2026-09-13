# 02: In-Process SDK Transport & Bounded Queue Worker

**What to build:**
The non-blocking background telemetry pipeline inside the Python SDK. Calling `swarmwatch.init()` spins up a background worker thread that batches spans from an in-memory bounded queue without adding latency to the host agent process, with graceful `atexit` flushing.

**Blocked by:** 01 (Core Event Model & SQLite WAL Ingestion Seam)

**Status:** resolved

## Acceptance Criteria
- [x] Span submission (`queue.put_nowait()`) executes in <1 microsecond.
- [x] Background worker flushes batches to storage adapter every 500ms or 100 spans.
- [x] Bounded queue (10,000 items) drops oldest items gracefully if downstream is congested.
- [x] `atexit` hook flushes remaining spans cleanly on process exit.
