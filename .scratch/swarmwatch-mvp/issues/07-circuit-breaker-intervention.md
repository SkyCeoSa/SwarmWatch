Type: grilling
Status: resolved

# Ticket 07: Circuit Breaker Intervention Policy & Escalation Protocol
When a loop is mathematically detected, what exact programmatic action does SwarmWatch take to stop token burn?

## Answer
Adopted Option C: 2-Step Smart Intervention protocol.
1. Warning threshold: Injects high-priority [SWARMWATCH CIRCUIT WARNING] system hint into the agent context urging loop termination.
2. Trip threshold: If loop persists on the subsequent turn, immediately halts execution, snapshots current state into Time Machine, raises rich SwarmCircuitBreakException, and fires real-time WebSocket alert to UI.
