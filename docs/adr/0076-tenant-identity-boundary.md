# ADR 0076: Tenant Identity Boundary & Attenuated Capability Tokens

## Status
Accepted

## Context
In enterprise multi-tenant agent systems, autonomous swarms execute tasks on behalf of distinct tenant organizations, project partitions, and human principals. As agents collaborate, sub-delegate, and spawn helper workers, they must pass authorization across trust boundaries.

Prior to Seam 65, SwarmWatch relied on static API keys and unattenuated tenant identifiers (`tenant_id`, `TenantContext`). This model presents critical security risks in distributed agent workflows:
1. **Privilege Escalation via Sub-Delegation**: If a root agent possessing broad permissions (`telemetry:write`, `effect:dispatch`, `circuit:trip`) delegates a specialized sub-task (e.g. data scraping or summarization) to a subagent, sharing the root API key or tenant credential grants the subagent full unconstrained access.
2. **Cross-Tenant Confused Deputy & Forgery**: Without cryptographic binding between tenant identity, agent principal, and permitted resources, a compromised or misbehaving worker can claim arbitrary tenant IDs or forge effects across tenants.
3. **Absence of Monotonic Attenuation**: In standard capability systems (e.g. object capabilities, Macaroons, biscuits), sub-delegations must be strictly monotonic: an agent can only grant a subset of its own permissions, narrow the resource URI pattern, and shorten the validity window.
4. **Irrevocable Delegation Branches**: If a worker or sub-swarm is detected behaving anomalously or becomes compromised, revoking the entire tenant API key causes cluster-wide denial of service, whereas lacking granular revocation leaves Rogue sub-agents uncontainable.

## Decision
We implement **Seam 65: Tenant Identity Boundary & Attenuated Capability Tokens (`TenantCapabilityManager`)** across both Python 3.12+ and TypeScript Node 22 SDKs, providing a cryptographically enforced capability delegation and verification engine:

1. **Cryptographically Bound Capability Tokens (`CapabilityToken`)**:
   - Each token encapsulates:
     - `token_id`: Unique identifier (UUIDv4).
     - `parent_token_id`: Identifier of the issuing parent token (or `None` for root tenant capabilities).
     - `tenant_id`: Immutable tenant boundary identifier.
     - `issuer_principal`: Identity of the agent or authority issuing/attenuating the token.
     - `subject_principal`: Identity of the agent authorized to exercise the token.
     - `scopes`: Granted permission set (e.g. `{"telemetry:write", "effect:dispatch"}`).
     - `resource_pattern`: Glob/URI pattern defining permitted resource namespace (e.g. `sw://acme-corp/swarms/finance/*`).
     - `epoch`: Monotonic authority epoch bound to consensus state.
     - `not_before_ns`: Start of token validity (nanoseconds).
     - `expires_at_ns`: End of token validity (nanoseconds).
     - `depth`: Delegation tree depth ($0$ for root, $+1$ per delegation).
     - `max_depth`: Maximum allowed delegation depth.
     - `attenuation_digest`: SHA-256 digest canonically binding all token fields and parent link (`sw:cap:v1`).
     - `signature`: Asymmetric ECDSA (NIST P-256 / IEEE P1363) signature by `issuer_principal`.

2. **Monotonic Attenuation Invariant (`attenuate()`)**:
   - A delegated child token $C_C$ derived from a parent token $C_P$ must satisfy strict mathematical subset constraints:
     - **Tenant Invariance**: $\text{tenant\_id}(C_C) == \text{tenant\_id}(C_P)$ (strict prohibition of cross-tenant delegation).
     - **Scope Monotonicity**: $\text{scopes}(C_C) \subseteq \text{scopes}(C_P)$ (cannot grant unheld permissions).
     - **Resource Narrowing**: $\text{resource}(C_C)$ must be a sub-pattern of $\text{resource}(C_P)$ (cannot widen target namespace).
     - **Temporal Monotonicity**: $\text{expires\_at}(C_C) \le \text{expires\_at}(C_P)$ and $\text{not\_before}(C_C) \ge \text{not\_before}(C_P)$.
     - **Epoch Monotonicity**: $\text{epoch}(C_C) \ge \text{epoch}(C_P)$.
     - **Depth Boundedness**: $\text{depth}(C_C) = \text{depth}(C_P) + 1 \le \text{max\_depth}(C_P)$.
   - Any attempt to violate these monotonic rules immediately aborts with `PrivilegeEscalationError` or `AttenuationViolationError`.

3. **Revocable Delegation Trees (`revoke()`)**:
   - Revoking a token by `token_id` immediately invalidates that token and all descendant tokens derived from it down the delegation tree.
   - An in-memory and durable revocation ledger tracks revoked tokens with monotonic timestamps and reason codes.

4. **Fail-Closed Execution Verification (`verify()`)**:
   - The execution boundary verifies all constraints before permitting agent actions:
     - Valid cryptographic signature and unbroken parent attenuation hash chain.
     - Matching `tenant_id` and authorized `subject_principal`.
     - Valid time window ($\text{not\_before} \le \text{now} \le \text{expires\_at}$).
     - Action scope present in token scopes.
     - Target resource matches `resource_pattern`.
     - Non-revocation check across the entire ancestor path.
   - Any failure returns `CapabilityVerdict(valid=False, reason=...)` or raises `CapabilityVerificationError`.

5. **Sub-250µs Verification SLO & Zero-Token Guarantee**:
   - Token attenuation and verification execute locally in $< 50\,\mu\text{s}$ (audit SLO $<250\,\mu\text{s}$).
   - 100% deterministic local cryptography; zero external LLM API calls and zero cloud provider egress.

6. **Dual-SDK Parity**:
   - Fully isomorphic implementations in Python (`tenant_capability.py`) and TypeScript (`tenant_capability.ts`).

## Consequences
### Positive
- Enforces strict least-privilege delegation across multi-agent hierarchies.
- Guarantees complete tenant isolation; prevents cross-tenant forged requests and replay attacks.
- Provides instantaneous subtree revocation for rogue or compromised subagents.
- Pure local verification with zero token overhead and microsecond latency.

### Negative
- Delegating agents must compute monotonic attenuation signatures when spawning subagents, requiring key management for agent principals.
