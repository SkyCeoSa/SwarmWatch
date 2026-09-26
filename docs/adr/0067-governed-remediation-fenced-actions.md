# ADR 0067: Governed Remediation & Fenced Action State Machine (Seam 56)

## Status
Accepted

## Context
In Seam 31 (ADR 0042) and Seam 43 (ADR 0054), SwarmWatch introduced autonomous self-healing and dynamic topology patching. However, the Senior Code & Architecture Audit identified an unacceptable operational and safety risk:
1. **Unconstrained Blast Radius**: Autonomous self-healing agents or auto-healers could propose and apply mutations (e.g. rewiring agent topologies, blacklisting tools, truncating budgets) across tenant boundaries or exceeding operator authority without explicit approval gates.
2. **Missing Precondition and Postcondition Verification**: Remediation actions were applied unconditionally without proving preconditions (e.g. valid failure state, matching configuration hash, non-exhausted compensation budget) or validating postconditions (e.g. invariant satisfaction, bounded side-effects).
3. **Stale Workers & Unfenced Actions**: A former cluster leader or lagging worker process could apply an obsolete remediation or duplicate an intervention after losing its lease or falling behind a fencing epoch.
4. **Non-Compensatable Mutations**: Reversible in-memory state snapshots do not equal safe rollback of downstream side-effects. Every remediation must provide an explicit, deterministic compensation contract (`compensate()`).

## Decision
We implement **Seam 56: Governed Remediation (`RemediationGovernor`)**:
1. **Fenced Action State Machine (`ProposalStatus`)**:
   - `PROPOSED`: A self-healing or topology patch is submitted with explicit blast radius, preconditions, postconditions, and resource ceilings.
   - `AUTHORIZED`: The proposal has satisfied required approval tiers, tenant quotas, and holds an unexpired `SafetyPermit` under the current authoritative epoch.
   - `APPLIED`: The mutation is committed under a monotonic fencing epoch; stale epochs are rejected fail-closed (`STALE_EPOCH`).
   - `COMPENSATED`: A previously applied remediation is safely rolled back via an idempotent compensation routine.
   - `REJECTED`: The proposal failed preconditions, exceeded blast radius ceilings, or lacked necessary approval.
   - `FAILED`: Execution failed during application, triggering automated compensation.
2. **Tiered Approval Matrix (`ApprovalTier`)**:
   - `TIER_0_AUTOMATIC`: Low-impact, bounded interventions (e.g. transient backoff, probation clamping) authorized automatically if within tenant quotas.
   - `TIER_1_SUPERVISED`: Topology mutations and model down-routing requiring a verified `SafetyPermit` from the authoritative safety epoch.
   - `TIER_2_MANUAL`: High blast radius modifications (e.g. global tool blacklisting, architectural rewiring, branch terminations) requiring operator/admin signature or step-up MFA.
3. **Precondition / Postcondition Contracts**:
   - Every proposal defines computable predicates (`preconditions`, `postconditions`). Before application, preconditions are strictly evaluated; after application, postconditions are evaluated. If postconditions fail, the action is automatically compensated.
4. **Deep Module Interface**:
   - `RemediationGovernor.propose(tenant_id, action_type, target_entity, parameters, blast_radius, approval_tier, preconditions, postconditions) -> RemediationProposal`
   - `RemediationGovernor.authorize(proposal_id, approver_role, permit_hash, signature) -> RemediationDecision`
   - `RemediationGovernor.apply(proposal_id, current_epoch, context) -> RemediationResult`
   - `RemediationGovernor.compensate(proposal_id, reason) -> CompensationResult`
   - `RemediationGovernor.status(tenant_id) -> Dict[str, Any]`
   - `RemediationGovernor.reset() -> None`
5. **Zero-Token & Dual-SDK Guarantee**:
   - 100% deterministic local in-memory execution. Zero remote LLM API calls.
   - Full dual-SDK parity across Python (`packages/sdk-python/swarmwatch/remediation_governor.py`) and TypeScript (`packages/sdk-typescript/src/remediation_governor.ts`).

## Consequences
- **Positive**:
  - Limits autonomous blast radius to defined resource ceilings and approval tiers.
  - Prevents stale workers from applying outdated patches through monotonic fencing epochs.
  - Guarantees deterministic, idempotent compensation rollbacks.
- **Negative**:
  - High blast radius interventions (Tier 2) require explicit administrative authorization.
