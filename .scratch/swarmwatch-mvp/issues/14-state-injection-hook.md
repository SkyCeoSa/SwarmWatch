Type: grilling
Status: resolved

# Ticket 14: Time-Machine State Re-injection Protocol
When a user forks from Step K with mutated state, how does SwarmWatch inject the modified state back into the active agent runtime?

## Answer
Adopted Option B: Zero-friction `swarmwatch.sync_state(default=...)` hook.
- Introspects whether current execution is a new root run or a Time-Machine forked branch.
- If forked, yields the mutated snapshot state; otherwise preserves default state.
- Natively interoperates with stateful engines (e.g. LangGraph `update_state()`).
