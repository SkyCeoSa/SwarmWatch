Type: grilling
Status: resolved

# Ticket 02: Storage Engine & Local Developer Experience
What is the underlying storage and ingestion architecture for local zero-docker development vs production scale?

## Answer
Adopted Option C: Dual-engine architecture with a clean StorageAdapter interface (ADR-0001).
- Local default: Embedded SQLite in WAL mode with async batching. Zero external dependencies / zero Docker.
- Enterprise/Team scale: Pluggable ClickHouse driver selectable via environment variable.
