# ADR 0010: Multi-Tenant Architecture, Row-Level Isolation & Cryptographic API Keys

## Status
Accepted

## Context
As enterprise multi-agent deployments expand, a single SwarmWatch instance must securely ingest, partition, and query telemetry across multiple teams, departments, and applications without cross-tenant data leakage. Additionally, ingestion and query access must be secured with high-entropy cryptographic API keys (sw_live_... / sw_test_...) without incurring token costs or relying on paid third-party authentication services.

## Decision
We implement a lightweight, cloud-agnostic multi-tenant hierarchy and cryptographic key management engine:

1. **Tenant Hierarchy**:
   - Organization: Top-level enterprise tenant boundary (e.g. default-org).
   - Project: Scoped workspace belonging to an organization (e.g. default-project).
   - ApiKey: Cryptographic credential tied to a project with fine-grained capability scopes (	elemetry:write, 	elemetry:read).

2. **Cryptographic Invariant & Zero-Knowledge Storage**:
   - Raw secret keys are generated using 24 random bytes with sw_live_ or sw_test_ prefix (e.g. sw_live_4b8f...).
   - Raw keys are displayed **only once** at creation time.
   - Only the SHA-256 hash (key_hash) and an 8-character prefix (key_prefix) are stored in the database.
   - Verification uses constant-time comparison (hmac.compare_digest) backed by a thread-safe in-memory cache for sub-microsecond authentication checks.

3. **Row-Level Tenant Isolation**:
   - project_id column indexed across spans, snapshots, and circuit_events.
   - Ingestion automatically injects authenticated project_id.
   - Query endpoints (/api/v1/swarms/{swarm_id}/spans, /api/v1/swarms/{swarm_id}/branches) enforce strict row-level isolation based on the verified caller's project_id.
   - Backward compatibility: Unauthenticated local executions seamlessly default to default-project.

## Consequences
- **Positive**: Complete data segregation between teams and environments on local SQLite as well as enterprise ClickHouse.
- **Positive**: Zero external auth provider dependencies (Auth0, Clerk, Cognito) required; 100% self-contained and local.
- **Positive**: Constant-time verification prevents timing attacks on key validation.
