# ADR 0002: Zero-Token Shannon Entropy Circuit Breaker

## Status
Accepted

## Context
Multi-agent swarms frequently deadlock or enter infinite conversational loops (e.g. arguing about tool parameters or repeating stale hypotheses). Standard industry recommendations rely on an "LLM as a judge" (such as calling GPT-4 to verify if agents are stuck). This approach burns thousands of expensive tokens, costs money, and adds 2–3 seconds of latency per check while the system is already failing.

## Decision
We implement a zero-token mathematical circuit breaker:
1. **Normalized Shannon Entropy**:
   $$H_{norm}(X) = \frac{-\sum p(x_i) \log_2 p(x_i)}{\log_2 N}$$
2. **Sliding-Window Lexical Jaccard Similarity**:
   Evaluates overlap across the last $N$ agent responses in $<0.5ms$ locally.
3. **2-Step Intervention**:
   - *Step 1*: Inject a warning hint (`[SWARMWATCH CIRCUIT WARNING]`) into agent context to prompt self-correction.
   - *Step 2*: If the agent continues repeating patterns beyond threshold, immediately raise `SwarmCircuitBreakException`.

## Consequences
- **Positive**: Loop detection costs \$0.00 and runs in $<0.5ms$ with zero external API dependencies.
- **Positive**: Halts token burn before runaway loops consume account budgets.
- **Negative**: High repetition with slight syntactic variation could theoretically evade basic n-gram analysis; mitigated by configurable preset thresholds (`STRICT`, `BALANCED`, `LENIENT`).
