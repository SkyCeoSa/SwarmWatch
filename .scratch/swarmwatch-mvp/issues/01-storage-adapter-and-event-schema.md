# 01: Core Event Model & SQLite WAL Ingestion Seam

**What to build:**
The foundational storage adapter and canonical `SwarmSpan` schema. When an event is captured, it is persisted to normalized SQLite tables running in WAL mode with compound indexes, queryable by swarm and branch ID.

**Blocked by:** None (can start immediately).

**Status:** claimed

## Acceptance Criteria
- [ ] Pydantic `SwarmSpan` schema with multi-agent fields (`swarm_id`, `source_agent`, `target_agent`, `parent_span_id`, `entropy_score`).
- [ ] `StorageAdapter` abstract base class defining `save_spans`, `save_snapshot`, and `get_swarm_spans`.
- [ ] `SQLiteStorageAdapter` initializes WAL mode (`PRAGMA journal_mode=WAL`) and compound indexes.
- [ ] Roundtrip tests pass: saving spans and querying by `swarm_id` and `branch_id`.
