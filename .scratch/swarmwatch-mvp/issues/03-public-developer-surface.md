# 03: Public Developer Surface (Decorators & Context Managers)

**What to build:**
The clean, ergonomic developer API surface. Developers can annotate agents with `@swarmwatch.agent(...)` and wrap interaction blocks with `with swarmwatch.span(...)`, automatically capturing execution time, inputs, outputs, and errors.

**Blocked by:** 02 (In-Process SDK Transport & Bounded Queue Worker)

**Status:** resolved

## Acceptance Criteria
- [x] `@swarmwatch.agent(role="...", swarm="...")` wraps sync and async functions cleanly.
- [x] `with swarmwatch.span(...)` correctly resolves and tracks parent-child span hierarchy.
- [x] Exceptions in user agent code are captured in span metadata and re-raised without masking.
- [x] Native `swarmwatch.init()` establishes the singleton client cleanly.
