# 04: Zero-Token Shannon Entropy Circuit Breaker

**What to build:**
The mathematical loop detector and 2-step smart intervention engine. When consecutive agent messages show high lexical similarity or low entropy across a sliding window, inject a warning hint, and if uncorrected, safely halt execution with `SwarmCircuitBreakException`.

**Blocked by:** 03 (Public Developer Surface)

**Status:** resolved

## Acceptance Criteria
- [x] Mathematical evaluator computes Shannon entropy and sliding-window similarity in <1ms without LLM calls.
- [x] Presets (`strict`, `balanced`, `lenient`) and `CustomPolicy` configure thresholds properly.
- [x] First trigger injects `[SWARMWATCH CIRCUIT WARNING]` system hint into context.
- [x] Second consecutive trigger halts execution and raises `SwarmCircuitBreakException`.
