# ADR 0074: Critical Evidence Spool & NIST SP 800-53 Write-Ahead Journal

## Status
Accepted

## Context
In distributed multi-agent systems and enterprise autonomous swarms, agents emit two fundamentally disparate classes of observability and safety data:

1. **Tier 1: General Operational Telemetry**:
   - Routine spans, agent turn events, token consumption metrics, latency measurements, and heartbeat pings.
   - Under peak burst throughput or storage saturation, shedding or downsampling operational telemetry with loss-aware accounting (`dropped_telemetry_count`) is acceptable and bounded.
2. **Tier 0: Safety-Critical Evidence**:
   - Circuit breaker trips, Byzantine quarantine actions, security policy breaches, emergency halt directives, execution fence revocations, and cryptographic audit anchors.
   - Losing or dropping even a single safety-critical record violates enterprise regulatory compliance frameworks (specifically **NIST SP 800-53 Rev 5**: AU-4 Audit Storage Capacity, AU-9 Protection of Audit Information, AU-10 Non-Repudiation, and SC-7 Boundary Protection) and cripples post-incident forensic root-cause analysis.

Prior to Seam 63, operational telemetry and critical safety trip events shared ingress buffers and unreserved memory queues. Under extreme backpressure or storage saturation, critical safety evidence risked being dropped alongside general telemetry.

## Decision
We implement **Seam 63: Critical Evidence Spool (`EvidenceSpool`)** across both Python 3.12+ and TypeScript Node 22 SDKs, providing a dedicated, isolated, fail-closed write-ahead journal for critical safety evidence:

1. **Dual-Tier Durability & Bounded Admission**:
   - **Tier 0 (Critical Evidence)**: Dedicated reserved write-ahead journal (`EvidenceJournal`). Bounded admission strictly halts execution fail-closed (`EVIDENCE_JOURNAL_FULL`) if the durable spool capacity is exhausted or cannot guarantee persistence. Safety mutations are never silently dropped.
   - **Tier 1 (General Telemetry)**: Isolated telemetry transport with loss-aware drop counters. Saturation in Tier 1 never degrades or consumes Tier 0 reserved capacity.

2. **Immutable Append-Only Hash Chain**:
   - Each critical evidence record contains:
     - `record_id`: Unique identifier (UUIDv4).
     - `sequence_number`: Strictly monotonic 64-bit integer, starting at 1.
     - `timestamp_ns`: High-resolution nanosecond timestamp.
     - `record_type`: Categorical discriminator (`SAFETY_TRIP`, `POLICY_BREACH`, `BYZANTINE_QUARANTINE`, `EMERGENCY_HALT`, `FENCE_REVOCATION`, `AUDIT_ANCHOR`).
     - `tenant_id`: Cryptographic tenant identifier.
     - `agent_id`: Agent principal identifier.
     - `payload`: Canonicalized event payload.
     - `prev_record_hash`: SHA-256 digest of previous record (forming an immutable chain).
     - `record_hash`: SHA-256 digest over sequence, timestamp, type, tenant, payload, and previous hash.
     - `checksum`: CRC32 checksum over serialized record payload for corruption detection.

3. **Crash Recovery & Gap Detection (`recover()`)**:
   - Upon process restart or failure recovery, `recover()` replays the write-ahead journal from disk.
   - Verifies CRC32 checksum on every line/frame.
   - Verifies sequential continuity: raises `SEQUENCE_GAP_DETECTED` if any sequence number is missing.
   - Verifies hash-chain continuity: raises `CORRUPT_RECORD_HASH` if any record hash or parent link is altered.
   - Returns a verified `RecoveryResult` with start/end sequence, total records, and head digest.

4. **Audit Completeness Receipt (`completeness()`)**:
   - Provides a cryptographic proof receipt certifying that all safety events in range $[1, N]$ are durably present, contiguous, tamper-free, and that zero critical records were dropped.

5. **Sub-250µs In-Memory Hot Path & Zero-Token Guarantee**:
   - Offering critical evidence to the in-memory journal executes in $<250\,\mu\text{s}$ (audit SLO).
   - Zero LLM tokens consumed; 100% deterministic local computation.

6. **Dual-SDK Parity**:
   - Fully isomorphic implementations in Python (`evidence_spool.py`) and TypeScript (`evidence_spool.ts`).

## Consequences
### Positive
- Strict compliance with NIST SP 800-53 (AU-4, AU-9, SC-7) and enterprise audit standards.
- Fail-closed safety guarantee: systems cannot run unmonitored if safety journal persistence fails.
- Complete cryptographic verification of critical safety incidents and quarantine history.

### Negative
- Applications running in resource-starved environments must allocate sufficient disk/storage capacity for the safety journal to avoid fail-closed execution halts.
