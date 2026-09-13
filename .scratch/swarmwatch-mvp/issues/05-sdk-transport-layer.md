Type: grilling
Status: resolved

# Ticket 05: SDK Telemetry Transport & Background Flushing
How do telemetry spans move from the host agent process to the storage engine without introducing execution latency?

## Answer
Adopted Option C: In-process bounded queue (queue.Queue(maxsize=10000)) with a dedicated non-daemon Worker Thread.
- Span capture in put_nowait() takes <1 microsecond, non-blocking for host agent.
- Background worker flushes in batches every 500ms or upon 100 spans.
- Resilient retry logic and texit hook guarantee zero data loss on graceful process termination.
