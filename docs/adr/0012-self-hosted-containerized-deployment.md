# ADR 0012: Zero-Cloud Self-Hosted Containerized Deployment

## Status
Accepted

## Context
Enterprise organizations deploying autonomous AI swarms face stringent data residency, privacy, and budget constraints. Relying on proprietary cloud platforms or third-party hosted SaaS introduces external token leakage, recurring subscription overhead, and security compliance hurdles. SwarmWatch requires an industrial-grade, single-command containerized deployment that operates entirely on-premise or on local workstations with zero external cloud dependencies.

## Decision
We introduce the Self-Hosted Container Topology via Docker and Docker Compose:

1. **Single-Container Architecture**:
   - Built on `python:3.12-slim` to minimize attack surface and image footprint (< 150MB compressed).
   - Packages the FastAPI observability control plane, the SQLite/ClickHouse persistence engine, and the static Impeccable console (`dashboard.html`) into a single container image.

2. **Zero-Cloud & Zero-Token Guarantee**:
   - Requires zero remote network connections, zero external telemetry relays, and zero remote LLM evaluation APIs.
   - All cryptographic hashing, entropy calculations, and time-machine snapshots execute within the isolated container boundary.

3. **Persistent Volume Isolation**:
   - Mounts `/data` as an external Docker volume (`swarmwatch_data`), preserving SQLite databases, state snapshots, and cryptographic keys across container restarts.

4. **Liveness & Readiness Probing**:
   - Integrates `/api/v1/health` endpoint into native Docker `HEALTHCHECK` with configurable interval, timeout, and retry thresholds.

## Consequences
- **Positive**: Single-command startup (`docker compose up -d`) for on-premise, air-gapped, or local workstation environments.
- **Positive**: Zero external cloud costs, data egress concerns, or external token fees.
- **Positive**: Full operational parity with local CLI and development modes.
