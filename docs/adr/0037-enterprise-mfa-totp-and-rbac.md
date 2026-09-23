# ADR 0037: Enterprise Multi-Factor Authentication (MFA / TOTP) & Role-Based Access Control (RBAC)

## Status
Accepted

## Context
As SwarmWatch transitions into mission-critical, enterprise multi-agent production clusters, observability and circuit-breaking platforms become targets for privilege escalation and lateral movement. A compromised API key or credential with unrestricted access could:
1. Maliciously trip or reset distributed circuit breakers across an entire swarm fleet.
2. Weaken or disable entropy collapse and token budget policies.
3. Permanently purge telemetry and causal state snapshots from active storage.
4. Tamper with audit logs or exfiltrate sensitive agent state traces.

To satisfy SOC 2 Type II, ISO 27001, and HIPAA compliance mandates, SwarmWatch requires:
- **Zero-Dependency RFC 6238 TOTP Engine**: Native implementation of Time-Based One-Time Passwords using HMAC-SHA1 with standard 30-second intervals, $\pm 1$ time-step drift tolerance, and RFC 3548 Base32 secret generation without external libraries.
- **Single-Use Cryptographic Backup Codes**: Generation of human-readable recovery backup codes (`XXXX-XXXX`) persisted solely as one-way SHA-256 hashes, burned upon consumption.
- **Granular 4-Tier RBAC Matrix**: Separation of duties across `ADMIN`, `OPERATOR`, `AUDITOR`, and `VIEWER` roles.
- **Step-Up Verification on Destructive Actions**: Requiring real-time TOTP/backup code validation via `X-MFA-Code` header before executing high-impact mutating operations (such as emergency fleet trips, policy changes, and local storage purges).
- **Dual-SDK Parity**: Zero-overhead native implementations in both Python and TypeScript runtimes.

---

## Decision

### 1. Zero-Dependency RFC 6238 Time-Based One-Time Password (TOTP)
SwarmWatch implements the RFC 6238 standard directly using language standard libraries (`hashlib`, `hmac`, `struct`, `base64` in Python; `node:crypto` in TypeScript):
- **Time Step ($T_X$)**: 30 seconds.
- **Epoch ($T_0$)**: 0 (Unix epoch).
- **Counter ($C$)**: $C = \lfloor \frac{\text{current\_time} - T_0}{T_X} \rfloor$ encoded as an 8-byte big-endian unsigned integer.
- **HMAC Hash Function**: SHA-1, yielding a 20-byte digest.
- **Dynamic Truncation**:
  ```python
  offset = hmac_digest[-1] & 0x0F
  code = ((hmac_digest[offset] & 0x7F) << 24 |
          (hmac_digest[offset + 1] & 0xFF) << 16 |
          (hmac_digest[offset + 2] & 0xFF) << 8 |
          (hmac_digest[offset + 3] & 0xFF)) % 1_000_000
  return f"{code:06d}"
  ```
- **Drift Window Tolerance**: $\pm 1$ step (testing $[C-1, C, C+1]$, a 90-second total validity envelope) to absorb network latency and client clock skew.
- **Constant-Time Verification**: Verification utilizes `hmac.compare_digest` in Python and `crypto.timingSafeEqual` in TypeScript to eliminate side-channel timing attacks.
- **Standard URI Scheme**: `otpauth://totp/SwarmWatch:{username}?secret={secret}&issuer=SwarmWatch` for universal compatibility with authenticator apps.

### 2. Cryptographic Single-Use Recovery Backup Codes
- During MFA enrollment, SwarmWatch issues 8 recovery codes formatted as `XXXX-XXXX`.
- Only the SHA-256 hashes of these codes are stored in the database (`users.backup_codes_json`).
- Upon successful authentication with a backup code, its hash is immediately purged from the active record, rendering it permanently consumed.

### 3. Role-Based Access Control (RBAC) Architecture
SwarmWatch establishes four canonical roles:
- **`ADMIN`**: Complete administrative dominion over organizations, projects, API keys, policies, users, storage, and cluster leaders.
- **`OPERATOR`**: Authority to launch swarms, execute branch forks, pause/resume execution, trip/reset circuit breakers, archive/restore swarms, and inspect metrics. Cannot delete projects, manage users, or revoke arbitrary API keys.
- **`AUDITOR`**: Read-only compliance and forensic oversight. Has access to all spans, snapshots, diffs, incident post-mortem reports, security audit events, and cold storage manifests. Cannot mutate state, trip circuits, or alter policies.
- **`VIEWER`**: Read-only observational visibility into active swarms, real-time metrics, and DAG topologies.

#### Permission Mapping Matrix:
| Permission | Admin | Operator | Auditor | Viewer |
| :--- | :---: | :---: | :---: | :---: |
| `swarms:read` | Yes | Yes | Yes | Yes |
| `swarms:run` | Yes | Yes | No | No |
| `swarms:trip` | Yes | Yes | No | No |
| `swarms:resume` | Yes | Yes | No | No |
| `swarms:fork` | Yes | Yes | No | No |
| `policy:read` | Yes | Yes | Yes | Yes |
| `policy:write` | Yes | No | No | No |
| `storage:archive` | Yes | Yes | No | No |
| `storage:restore` | Yes | Yes | No | No |
| `keys:read` | Yes | Yes | Yes | No |
| `keys:write` | Yes | No | No | No |
| `keys:revoke` | Yes | No | No | No |
| `mfa:manage` | Yes | No | No | No |
| `audit:read` | Yes | No | Yes | No |
| `fleet:manage` | Yes | Yes | No | No |

### 4. Step-Up Verification for Sensitive Operations
When an authenticated user has MFA enabled (`mfa_enabled = True`), destructive endpoints enforce a Step-Up Challenge:
- Target endpoints:
  - `POST /api/v1/fleet/redis/trip`
  - `POST /api/v1/fleet/redis/reset`
  - `POST /api/v1/policy`
  - `DELETE /api/v1/keys/{id}`
  - `POST /api/v1/storage/archive/{id}` with `delete_local=True`
- The client must pass `X-MFA-Code: <code>` in HTTP headers. If missing or invalid, the API returns HTTP 403 Forbidden with:
  ```json
  {
    "detail": "MFA verification required for this sensitive operation",
    "mfa_required": true
  }
  ```

### 5. Local-First & Developer Ergonomics Invariant
In unauthenticated local development mode (e.g. `swarmwatch ui` run locally without configuring user accounts or API keys), requests default to `ADMIN` permissions and bypass MFA requirements, ensuring zero friction for local prototyping.

---

## Consequences

### Positive
- **Enterprise Compliance**: Delivers immediate alignment with SOC 2, ISO 27001, and Zero Trust security architectures.
- **Zero-Dependency Footprint**: Zero external libraries needed for TOTP, QR URIs, or RBAC evaluation.
- **Impeccable Safety**: Prevents accidental or unauthorized cluster-wide halts, policy tampering, or local data purges.
- **Dual-SDK Parity**: TypeScript and Python SDKs share identical permission models and TOTP cryptographic engines.

### Neutral / Trade-Offs
- Operators executing destructive operations via curl or automated pipelines must pass automated TOTP step-up tokens or utilize an authorized Service Account API key exempt from interactive TOTP challenges.
