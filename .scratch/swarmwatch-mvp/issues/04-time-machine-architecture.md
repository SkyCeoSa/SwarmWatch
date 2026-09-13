Type: grilling
Status: resolved

# Ticket 04: Time-Machine Replay & State Branching Architecture
How do we design a reliable time-machine debugger for stochastic, non-deterministic LLM agent systems?

## Answer
Adopted Option B: State Snapshotting & Tool-Call Memoization.
Past steps and tool calls are cached and frozen. When stepping back to step K, historical steps 1..K-1 are read from immutable cache.
The user mutates state at step K, and only the new divergent execution branch runs forward.
