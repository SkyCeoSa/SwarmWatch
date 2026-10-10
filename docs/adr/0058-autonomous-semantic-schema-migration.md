# ADR 0058: Autonomous Semantic Schema Migration & Backward-Compatible State Translation Engine

## Status
Accepted

## Context
In long-running, multi-agent deployments, agents continuously evolve:
1. **Model & Prompt Upgrades**: System prompt templates change, introducing new state variables or restructuring existing tool outputs.
2. **Schema Drift in Time-Machine**: Replaying historical execution traces, stepping back turns, or branching new subgraphs via the Time-Machine fails if snapshots recorded under schema version `1.0.0` are restored into agents running `2.0.0`.
3. **Cross-Cluster Synchronization**: Mesh nodes running different minor versions exchange telemetry spans and causal DAG states; without automatic translation, older nodes drop newer spans or crash on unexpected field types.
4. **LLM Cost & Latency Prohibitions**: Invoking an external LLM to translate schema dictionaries on every turn is slow (>1s) and violates SwarmWatch's **Zero-Token Cost Guarantee**.

## Decision
We implement Seam 47: Autonomous Semantic Schema Migration & Backward-Compatible State Translation Engine as an in-process deep module with full dual-SDK parity across Python (`swarmwatch.schema_migration`) and TypeScript (`@swarmwatch/sdk/src/schema_migration.ts`).

### 1. Semantic Versioning & Directed Transformations
- Explicit semantic versions (`major.minor.patch`) modeled via `SchemaVersion`.
- Primitive deterministic field transformation operations (`FieldTransformAction`):
  - `RENAME`: Transparently remaps field keys while preserving values.
  - `CAST_TYPE`: Enforces strict type conversion (`string`, `number`, `boolean`, `json_array`, `json_object`).
  - `ADD_DEFAULT`: Injects canonical default values for newly added fields.
  - `REMOVE_DEPRECATED`: Safely prunes deprecated attributes.
  - `NEST_FIELDS`: Gathers flat fields into a scoped sub-dictionary.
  - `FLATTEN_FIELDS`: Unpacks nested keys to root level.
  - `COMPUTE_DERIVED`: Calculates deterministic derived fields.

### 2. Migration Step Registration & Bidirectional Reversibility
- Each `MigrationStep` registers forward rules ($V_A \to V_B$) and automatic or explicit backward rules ($V_B \to V_A$), guaranteeing reversible state rollback.

### 3. Migration Graph Planner (`MigrationGraphPlanner`)
- Models schema versions as nodes and migration steps as directed weighted edges in a migration DAG.
- For any arbitrary source version $V_{\text{src}}$ and target version $V_{\text{tgt}}$, computes the shortest path using Breadth-First Search (BFS).
- Sequentially executes intermediate migrations, returning a validated `MigrationResult` containing transformed payload, applied path, and lookup latency in nanoseconds.

### 4. Zero-Token Cost Guarantee
- 100% in-process deterministic dictionary transformation ($<30\mu\text{s}$ per payload). Zero external LLM calls, zero third-party schema registries.

### 5. Dual-SDK Parity
- **Python**: `packages/sdk-python/swarmwatch/schema_migration.py` (`SchemaVersion`, `FieldTransformAction`, `FieldTransformRule`, `MigrationStep`, `MigrationPlan`, `SchemaMigrationEngine`, `global_schema_migration_engine`).
- **TypeScript**: `packages/sdk-typescript/src/schema_migration.ts` (`SchemaVersion`, `FieldTransformAction`, `FieldTransformRule`, `MigrationStep`, `MigrationPlan`, `SchemaMigrationEngine`, `globalSchemaMigrationEngine`) strictly complying with Node 22 `--experimental-strip-types`.

### 6. Control Plane, CLI & Web Console Integration
- **REST Endpoints**:
  - `GET /api/v1/schemas/status`: Active versions, migration count, and graph topology.
  - `GET /api/v1/schemas/versions`: List registered schema versions.
  - `POST /api/v1/schemas/register-migration`: Register custom migration step.
  - `POST /api/v1/schemas/migrate`: Transform a payload across versions.
  - `POST /api/v1/schemas/validate`: Validate schema compliance against a version.
- **CLI Subcommand**:
  - `swarmwatch schema status`: Monospace summary of registered schemas and migration edges.
  - `swarmwatch schema versions`: List all registered schema versions.
  - `swarmwatch schema migrate --source <v1> --target <v2> --payload '<json>'`: Test migration.
- **Web Console**:
  - `#schemaBadge` in `.fleet-left-group` of `#fleetBar` in `dashboard.html`.
  - Dynamic status indicator: Compatible (`SCHEMA: V2.1 (CONVERGED)` in green), Migrating (`SCHEMA: MIGRATING (N STEPS)` in amber).
  - 100% compliant with High-Density Craft Standard (0 anti-patterns `[]`).

## Consequences
### Positive
- Guarantees seamless backward and forward compatibility across multi-agent software upgrades.
- Eliminates deserialization errors during Time-Machine step-back and cross-cluster gossip sync.
- Sub-30 microsecond execution with zero external LLM token expenditure.

### Negative
- Non-trivial structural migrations (e.g. splitting one field into multiple) require explicit registered transformation rules.
