# 05: Safe State Serializer & Time-Machine Branching

**What to build:**
Safe memory snapshotting with regex secret redaction and GZIP compression, enabling non-destructive Git-like DAG execution branching.

**Blocked by:** 03 (Public Developer Surface)

**Status:** ready-for-agent

## Acceptance Criteria
- [ ] `SafeStateSerializer` safely handles non-serializable objects via `__repr__` and masks API keys (`sk-...`, etc.).
- [ ] State snapshots over 2MB are GZIP compressed.
- [ ] `step_back()` creates an isolated `fork-1` branch without corrupting original `main` branch.
- [ ] `swarmwatch.sync_state()` yields mutated state on forked branch.
