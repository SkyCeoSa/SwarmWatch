# ADR 0077: Effect-Aware Compensation Algebra & Monotone Approval Lattices (Seam 66)

## Status
Accepted

## Context
In multi-agent systems and distributed swarm environments, agents autonomously trigger real-world effects, including database mutations, financial transactions, cloud resource provisioning, email/webhook dispatches, and file modifications. When a circuit breaker trips, an entropy collapse occurs, or a Byzantine failure is detected, naive systems attempt blanket "undo" or "rollback" operations.

However, in the real world:
1. **Not all actions are invertible**: Overwriting a temporary scratch cache is strictly reversible; refunding a credit card charge or deleting an S3 object is compensatable; sending a physical letter or publishing a public tweet is strictly irreversible.
2. **False equivalence causes catastrophic damage**: Blindly running automated compensators on irreversible actions either silently fails, corrupts upstream state, or causes double-spend / duplicate side-effects.
3. **Sequential compositions propagate irreversibility**: If a workflow executes nine reversible steps and one irreversible step, the composite transaction cannot be automatically rolled back; it must fail closed and escalate to human governance.

Prior to Seam 66, SwarmWatch tracked effect ledgers and 2PC downstream boundaries, but lacked a formal mathematical algebra for action classification, composite reversibility lattice joins, and fail-closed human escalation for irreversible effects.

## Decision
We implement **Effect-Aware Compensation Algebra & Monotone Approval Lattices** in both Python 3.12+ and TypeScript (Node 22 native type-stripping).

### 1. Mathematical Formalism: Action Classification & Reversibility Lattice
Every atomic action $\alpha \in \mathcal{A}$ is mapped to a class in the bounded total order (lattice) $(\mathcal{L}, \prec)$:
$$\text{REVERSIBLE} \prec \text{COMPENSATABLE} \prec \text{IRREVERSIBLE}$$

where:
- $\text{REVERSIBLE}$ ($0$): Actions with an exact mathematical inverse $f^{-1}(s)$ that restores the exact prior state without side-effects (e.g., rolling back an uncommitted transaction, unlinking an in-memory scratch key, decrementing an in-memory counter).
- $\text{COMPENSATABLE}$ ($1$): Actions that cannot be physically undone, but whose semantic effects can be neutralized via a registered forward compensating transaction $c(s)$ (e.g., voiding an authorization, dispatching a refund, issuing a credit note, releasing an allocated cloud lease).
- $\text{IRREVERSIBLE}$ ($2$): Actions whose external effects are final, observable by external parties, or cannot be neutralized via automated software execution (e.g., emitting an external webhook, dispatching a public email, permanently purging raw audit logs).

### 2. Composition Algebra
For any sequence of actions $W = (\alpha_1, \alpha_2, \dots, \alpha_k)$, the composite reversibility classification is the lattice join (least upper bound):
$$\text{Class}(W) = \bigsqcup_{i=1}^k \text{Class}(\alpha_i) = \max_{1 \le i \le k} \text{Class}(\alpha_i)$$

- If $\text{Class}(W) = \text{REVERSIBLE}$: All steps have deterministic inverses. Automated recovery runs the reverse sequence: $(\alpha_k^{-1}, \alpha_{k-1}^{-1}, \dots, \alpha_1^{-1})$.
- If $\text{Class}(W) = \text{COMPENSATABLE}$: Automated recovery executes the compensating transactions in reverse order for all completed steps.
- If $\text{Class}(W) = \text{IRREVERSIBLE}$: Automated rollback is **strictly prohibited**. The compensation engine halts fail-closed with `IrreversibleActionError`, preserves all execution evidence, and generates a non-repudiable `HumanEscalationReceipt`.

### 3. Monotone Approval Lattices for Escalation
When an irreversible action is encountered during recovery:
1. Automated execution is blocked.
2. A `HumanEscalationRequest` is registered detailing:
   - `action_id`, `action_type`, `tenant_id`, `principal`
   - `irreversibility_reason`
   - `state_snapshot_hash` and `evidence_refs`
3. Only an authenticated human operator or cryptographic quorum with sufficient role clearance (`ADMIN` or `OPERATOR`) can sign an `EscalationAuthorization`, transitioning the state to `APPROVED_BYPASS`, `MANUAL_REMEDIATED`, or `ABORTED`.

### 4. Deep Module Architecture
The core engine `CompensationPlanner` / `CompensationEngine` provides:
- `register_action_contract(contract: ActionContract)`: Binds action type to reversibility class and handler.
- `classify(actions: List[ActionExecution]) -> CompositeClassification`: Evaluates lattice join and identifies irreversible bottlenecks.
- `build_compensation_plan(history: List[ActionExecution]) -> CompensationPlan`: Generates ordered step-by-step compensation instructions.
- `execute_compensation(plan: CompensationPlan) -> CompensationResult`: Executes reversible/compensatable steps; halts fail-closed on unapproved irreversible steps.
- `escalate(action: ActionExecution, reason: str) -> HumanEscalationReceipt`: Generates immutable audit receipt for human review.
- `authorize_escalation(escalation_id: str, authorizer_principal: str, decision: str) -> EscalationResolution`: Records cryptographic human sign-off.

### 5. Performance & Resource Constraints
- **Sub-250µs Execution Target**: Action classification and lattice joins must complete in $<50\,\mu\text{s}$.
- **Zero-Token Cost Guarantee**: All algebraic evaluations, lattice joins, and compensation planning are 100% local mathematical operations without external LLM calls.
- **Fail-Closed Default**: If an action type is unregistered, its classification defaults to `IRREVERSIBLE` (fail-closed principle).

## Consequences

### Positive
- Prevents catastrophic automated execution loops on irreversible external actions.
- Provides mathematically sound composite reversibility evaluation for arbitrary agent execution traces.
- Guarantees complete audit traceability under NIST SP 800-53 via signed human escalation receipts.
- 100% dual-SDK symmetry between Python 3.12+ and Node 22 native type-stripping.

### Negative / Trade-Offs
- Irreversible workflows require human interaction to resolve if tripped, pausing automated agent pipelines until human review.
