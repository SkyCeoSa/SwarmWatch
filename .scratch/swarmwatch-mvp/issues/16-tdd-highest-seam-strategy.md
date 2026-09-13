Type: grilling
Status: resolved

# Ticket 16: TDD Test Strategy & Highest Architectural Seam
What is the highest architectural seam at which the system will be tested test-first according to Matt Pocock's TDD principles?

## Answer
Adopted Option B: Highest observable architectural seam (End-to-End lifecycle test).
- Test fixture boots in-memory SQLite storage + FastAPI background collector.
- Runs mock multi-agent swarm with `@swarmwatch.agent`.
- Asserts: span persistence, causal DAG graph linkage, circuit breaker trip on loop, non-destructive branch forking on `step_back()`, and WebSocket graph delta delivery.
