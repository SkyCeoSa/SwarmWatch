# ADR 0087: Multi-Cluster Epistemic Proof Verification & Automated Cross-Swarm Dispute Resolution

## Status
Accepted

## Context
As autonomous agent swarms scale into multi-cluster, cross-tenant, and heterogeneous federations (established in ADRs 0083, 0084, 0085, and 0086), swarms frequently assert critical propositions and knowledge claims concerning shared environments—including task completion, data migration integrity, capability authorizations, and safety-critical state mutations.

However, when independent swarms operating under differing causal contexts reach contradictory conclusions regarding a shared proposition $\phi$ (e.g., Swarm $A$ asserts "Cloud resource provisioned safely" while Swarm $B$ asserts "Resource deployment failed with quota violation"), critical distributed failure modes emerge:
1. **Epistemic Deadlock and Thrashing**: Without a formal arbitration mechanism, contradictory beliefs ($K_A \phi \land K_B \neg\phi$) cause swarms to enter recursive argument loops, repetitive tool retries, and conversational deadlocks, consuming token budgets without making operational progress.
2. **Unverifiable Epistemic Deductions**: Prior epistemic frameworks (such as ADR 0048) tracked internal belief states within a single process. External swarms cannot audit or verify another swarm's multi-step deductive reasoning without inspecting massive, private prompt histories or proprietary reasoning scratchpads.
3. **Absence of Deterministic Evidentiary Dominance**: Relying on external LLM judges to resolve disputes is non-deterministic, introduces multi-second network latencies, risks bias or hallucination, and violates SwarmWatch's **Zero-Token Cost Guarantee**.
4. **Decoupling from Swarm Safety Circuits**: Irreconcilable epistemic contradictions frequently signal underlying data corruption, network partitioning (ADR 0081), or adversarial hallucination. Traditional distributed systems lack an automated mechanism to latch the global safety circuit and halt execution when epistemic consensus fails.

Seam 76 resolves these challenges by introducing an in-process, zero-token **Multi-Cluster Epistemic Proof Verification & Cross-Swarm Dispute Resolution Engine** (`EpistemicArbitrationEngine`).

## Decision
We implement a deterministic, in-process **Multi-Cluster Epistemic Proof Verification & Cross-Swarm Dispute Resolution Engine** with complete dual-SDK parity across Python 3.12+ (`swarmwatch.epistemic_arbitration`) and TypeScript Node 22 native type-stripping (`@swarmwatch/sdk/src/epistemic_arbitration.ts`).

### 1. Formal Epistemic Claims & Proof DAGs
An agent or swarm asserts an authenticated proposition via an `EpistemicClaim`:
$$\mathcal{C} = \langle \text{claim\_id}, \text{swarm\_id}, \text{subject}, \text{predicate}, \text{polarity}, \mathcal{M}, \Sigma_{\text{evidence}}, c, t \rangle$$
where:
- $\text{polarity} \in \{ \text{TRUE}, \text{FALSE} \}$ designates $\phi$ versus $\neg\phi$.
- $\mathcal{M} \in \{ \text{KNOWS}, \text{BELIEVES}, \text{JUSTIFIED} \}$ defines epistemic modality ($K_a \phi \implies B_a \phi$).
- $\Sigma_{\text{evidence}}$ is an ordered list of cryptographic evidence hashes linking to ADR 0084 vector evidence, ADR 0082 execution receipts, or ADR 0086 ZK rollup receipts.
- $c \in [0.0, 1.0]$ is the claimant's asserted confidence score.

The justification for $\mathcal{C}$ is structured as an immutable directed acyclic graph (`EpistemicProofDAG`):
$$\mathcal{G} = (\mathcal{V}, \mathcal{E})$$
- Leaf nodes $\mathcal{V}_{\text{leaf}}$ represent ground-truth empirical evidence.
- Intermediate nodes $\mathcal{V}_{\text{rule}}$ represent formal deductive inference steps:
  - `MODUS_PONENS`: $P, P \implies Q \vdash Q$
  - `CONJUNCTION`: $P, Q \vdash P \land Q$
  - `EPISTEMIC_NECESSITATION`: $\vdash \phi \implies K_a \phi$
  - `EVIDENTIARY_GROUNDING`: Linking empirical evidence to atomic premise.
- The engine verifies proof DAGs via topological traversal ($O(|\mathcal{V}| + |\mathcal{E}|)$), enforcing maximum proof depth, checking leaf authenticity, and rejecting any circular reasoning with `EpistemicCircularityError`.

### 2. Cross-Swarm Dispute Cases
When two conflicting claims $\mathcal{C}_1$ and $\mathcal{C}_2$ concerning the same subject and predicate have opposite polarities ($\phi \text{ vs } \neg\phi$), the engine registers a formal `DisputeCase`:
$$\mathcal{D} = \langle \text{dispute\_id}, \text{subject}, \text{predicate}, \mathcal{C}_{\text{claimant}}, \mathcal{C}_{\text{respondent}}, \text{status}, t_{\text{opened}} \rangle$$
Dispute states advance through:
`OPEN` $\to$ `EVIDENCE_SUBMITTED` $\to$ `EVALUATING` $\to$ `RESOLVED` / `DEADLOCKED` / `ESCALATED_CIRCUIT`.

### 3. Deterministic Evidentiary Dominance Algebra
Arbitration is performed without external LLM judges using a deterministic scoring lattice:
$$\mathcal{S}(\mathcal{C}, \mathcal{G}) = w_e \cdot \overline{\text{weight}}(\mathcal{V}_{\text{leaf}}) + w_d \cdot \text{validity}(\mathcal{V}_{\text{rule}}) + w_c \cdot c - \lambda \cdot \text{depth}(\mathcal{G})$$
where:
- $w_e = 0.50$: Grounded empirical evidence weight.
- $w_d = 0.30$: Formal deductive step validity ratio.
- $w_c = 0.15$: Claimant confidence score.
- $\lambda = 0.05$: Occam's razor penalty for unnecessarily complex or deep proof chains.

**Binding Verdict Rules**:
- If $|\mathcal{S}_1 - \mathcal{S}_2| \ge \Delta_{\text{margin}}$ (default $\Delta = 0.10$):
  The mathematically dominating claim prevails (`CLAIMANT_PREVAILS` or `RESPONDENT_PREVAILS`).
- If both proofs fail validity checks or rely on invalid leaves: `MUTUAL_INVALIDATION`.
- If both proofs are equally matched ($|\mathcal{S}_1 - \mathcal{S}_2| < \Delta$): `DEADLOCKED`.
- If a deadlock involves a safety-critical proposition or persists beyond timeout: `EPISTEMIC_DIVERGENCE_CIRCUIT_TRIP`.

### 4. Tamper-Evident Dispute Resolution Receipts
Upon resolving a dispute, the arbitrator seals an immutable `DisputeResolutionReceipt`:
$$\text{Receipt Hash} = \text{"sw:dispute:v1:"} \,\|\, \text{SHA256}(\text{canonical\_json}(\text{DisputeResolutionReceipt}))$$
recording the dispute ID, competing claim IDs, dominating score, winning swarm, and nanosecond timestamp.

### 5. Fail-Closed Safety Circuit Coupling
The arbitration engine couples directly with SwarmWatch circuit breakers:
- **Dispute Lockout**: While a dispute is in active status, downstream tool executions dependent on proposition $\phi$ are gated.
- **Divergence Trip**: If arbitration yields `DEADLOCKED` or `EPISTEMIC_DIVERGENCE_CIRCUIT_TRIP`, the arbitrator autonomously trips the global safety circuit, halting runaway swarms.

## Invariants
1. **Zero-Token Cost Guarantee**: All proof DAG evaluations, dominance algebra calculations, and dispute resolutions execute locally with zero external LLM API calls ($0.00 USD token cost).
2. **Sub-250µs Execution SLO**: Complete dispute evaluation and receipt minting execute in $<250\,\mu\text{s}$.
3. **Strict Acyclicity**: Any circular reasoning in a proof DAG is deterministically detected and rejected with `EpistemicCircularityError`.
4. **Deterministic Evidentiary Dominance**: Identical competing proofs produce bit-exact identical arbitration verdicts across all runtimes.
5. **Fail-Closed Safety Latching**: Unresolvable epistemic contradictions trip the safety circuit to prevent split-brain execution.
6. **Dual-SDK Parity**: 100% identical types, proof scoring, and receipt hashes across Python 3.12+ and TypeScript Node 22 native type-stripping.

## Consequences
- **Positive**: Eliminates multi-agent argumentative thrashing and conversational deadlocks.
- **Positive**: Enables cross-cluster verification of epistemic claims without transmitting heavy private scratchpads.
- **Positive**: Provides mathematical dispute arbitration with zero LLM operational cost.
- **Negative**: Proof DAGs require upstream agents to model deductive steps using supported inference rules.
