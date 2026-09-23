# ADR 0036: S3 / Google Cloud Storage Streaming Log Archival & Cold Storage Tier

## Status
Accepted

## Context
As agent swarms execute across enterprise environments, they generate extensive telemetry: causal spans, parent-child DAG lineages, turn payloads, state snapshots, and circuit breaker events.
While hot persistent storage (SQLite and ClickHouse) is optimized for live dashboard visualization, sub-microsecond non-blocking streaming, and instant circuit breaker evaluation, long-term retention of completed swarms in the hot tier introduces severe operational challenges:
1. **Unbounded Storage Growth**: Historical telemetry accumulates rapidly, consuming expensive hot SSD/NVMe storage.
2. **Regulatory & Compliance Mandates**: Enterprise compliance (SOC 2, ISO 27001, HIPAA, GDPR) requires retaining raw audit logs and state snapshots for 1 to 7 years in tamper-evident, durable cold object storage.
3. **Multi-Tenant Cost Optimization**: Organizations need the ability to archive inactive swarms to ultra-low-cost cloud object stores (Amazon S3, S3-compatible like MinIO/Ceph, Cloudflare R2, Google Cloud Storage) while retaining the ability to restore and inspect historical runs on demand.

## Decision
We introduce **Seam 25**: **Cold Storage Streaming Log Archival & Cold Storage Tier** (`ColdStorageArchiver` and `ObjectStorageClient`) to SwarmWatch.

### Architectural Invariants:
1. **Pluggable Object Storage Abstraction (`ObjectStorageClient`)**:
   - High-level interface abstracting underlying object stores: `upload_object`, `download_object`, `list_objects`, `delete_object`, `head_object`.
   - Built-in zero-dependency implementations:
     - `MockObjectStorageClient`: In-process, thread-safe memory/filesystem store guaranteeing 100% offline, deterministic testing with zero external cloud credentials.
     - `S3ObjectStorageClient`: Pure standard-library HTTP/SigV4 client supporting AWS S3, MinIO, Ceph, Cloudflare R2, and LocalStack without external SDK bloat.
     - `GCSObjectStorageClient`: REST JSON API client for Google Cloud Storage.

2. **Deterministic Compression & Cryptographic Manifest (`ArchiveManifest`)**:
   - Swarm telemetry (spans, snapshots, circuit events, incident reports) is serialized into standard JSON Lines (JSONL).
   - Payloads are compressed using streaming `gzip` with configurable compression levels.
   - Each archive is accompanied by an immutable cryptographic manifest (`ArchiveManifest`) containing:
     - Bit-level SHA-256 checksum of the compressed bundle.
     - Uncompressed vs. compressed byte sizes and compression ratio.
     - Total spans, snapshots, and circuit events counts.
     - Multi-tenant identifiers (`organization_id`, `project_id`).
     - Timestamp and archiving node/operator identifier.

3. **Standard Time-Partitioned Storage Hierarchy**:
   Objects are structured using standard cloud data lake partitioning conventions:
   ```
   swarmwatch/v1/org={org_id}/proj={project_id}/year={YYYY}/month={MM}/day={DD}/swarm={swarm_id}.jsonl.gz
   swarmwatch/v1/org={org_id}/proj={project_id}/year={YYYY}/month={MM}/day={DD}/swarm={swarm_id}.manifest.json
   ```

4. **Bi-Directional Tiering (Archive & Restore)**:
   - `archive_swarm(swarm_id, delete_local=False)`: Offloads swarm telemetry to cold storage, optionally purging local hot storage records.
   - `restore_swarm(swarm_id, target_storage)`: Downloads the archive bundle from object storage, validates the SHA-256 checksum against the manifest, decompresses the payload, and re-hydrates hot storage atomically.

5. **Synergy with Leader Election (Seam 24 / ADR 0035)**:
   - In distributed deployments, automated retention workers (e.g. archiving swarms inactive for $>30\text{ days}$) are registered with `RedisLeaderElector.on_elected()`, ensuring only the active cluster leader coordinates archival jobs without race conditions or duplicate S3 writes.

6. **Dual-SDK Parity (TypeScript)**:
   - Full parity implementation in `@swarmwatch/sdk` (`packages/sdk-typescript/src/archival.ts`).

7. **Control Plane REST & CLI Subcommands**:
   - REST: `POST /api/v1/storage/archive/{swarm_id}`, `POST /api/v1/storage/restore/{swarm_id}`, `GET /api/v1/storage/archives`, `GET /api/v1/storage/archives/{swarm_id}/manifest`.
   - CLI: `swarmwatch archive list`, `swarmwatch archive export`, `swarmwatch archive restore`, `swarmwatch archive manifest`.

8. **Zero-Token Cost Guarantee**:
   - 100% deterministic local serialization, compression, and hashing. Zero remote LLM API calls.

## Consequences
- Enterprises can achieve multi-year compliance retention at minimal cloud object storage costs.
- Hot storage databases remain lightweight and performant.
- Historical swarms can be restored seamlessly on demand for Root Cause Analysis or Time-Machine resumption.
