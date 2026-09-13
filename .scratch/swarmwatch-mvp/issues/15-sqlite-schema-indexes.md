Type: grilling
Status: resolved

# Ticket 15: Storage Schema, Relational DDL & Indexing Strategy
What is the exact SQLite relational table structure and indexes guaranteeing sub-millisecond query performance?

## Answer
Adopted Option B: 4 Relational SQLite tables with `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL`.
- `swarms`: id, name, created_at
- `spans`: id, swarm_id, branch_id, parent_span_id, source_agent, target_agent, span_type, start_time, end_time, duration_ms, input_payload, output_payload, token_count, entropy_score, created_at. Compound index on (swarm_id, branch_id, start_time) and index on parent_span_id.
- `snapshots`: id, span_id, branch_id, state_hash, state_data_compressed (GZIP). Index on (branch_id, span_id).
- `circuit_events`: id, swarm_id, span_id, event_type, reason, metric_val, created_at.
