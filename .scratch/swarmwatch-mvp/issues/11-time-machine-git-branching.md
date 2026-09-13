Type: grilling
Status: resolved

# Ticket 11: Time-Machine State Forking & Git-Like Execution Tree
When a user forks execution from an earlier step, how does the system preserve historical divergence vs active execution?

## Answer
Adopted Option B: Git-like DAG Execution Tree.
- Sessions modeled as a directed tree with `branch_id`, `parent_span_id`, and `snapshot_hash`.
- Forking from Step K creates an isolated branch (`fork-1`), preserving original run (`main`) for A/B token and hallucination comparison.
