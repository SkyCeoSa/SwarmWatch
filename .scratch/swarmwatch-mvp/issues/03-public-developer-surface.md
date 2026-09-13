# 03: Public Developer Surface (Decorators & Context Managers)

**What to build:**
The clean, ergonomic developer API surface. Developers can annotate agents with `@swarmwatch.agent(...)` and wrap interaction blocks with `with swarmwatch.span(...)`, automatically capturing execution time, inputs, outputs, and errors.

**Blocked by:** 02 (In-Process SDK Transport & Bounded Queue Worker)

**Status:** ready-for-agent

## Acceptance Criteria
- [ ] `@swarmwatch.agent(role="...", swarm="...")` wraps sync and async functions cleanly.
- [ ] `with swarmwatch.span(...)` correctly resolves and tracks parent-child span hierarchy.
- [ ] Exceptions in user agent code are captured in span metadata and re-raised without masking.
- [ ] Native `swarmwatch.init()` establishes the singleton client cleanly.
