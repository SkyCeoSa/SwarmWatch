Type: grilling
Status: resolved

# Ticket 13: Circuit Breaker Threshold Tuning & Dynamic Sensitivity
How should entropy and similarity thresholds be configured and tuned for different agent workflows?

## Answer
Adopted Option B: Configurable sensitivity presets + `CustomPolicy`.
- Presets: "strict" (creative/exploratory swarms), "balanced" (default multi-agent tasks), "lenient" (debugging, iterative code editing).
- Custom policy: `CustomPolicy(max_similarity=0.90, window_size=3, max_repeated_tools=4)`.
- Accessible via `swarmwatch.init(circuit_breaker=...)`.
