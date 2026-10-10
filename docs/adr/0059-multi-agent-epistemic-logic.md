# ADR 0059: Dynamic Multi-Agent Epistemic Logic & Knowledge Base State Synchronization Engine

## Status
Accepted

## Context
In decentralized, heterogeneous multi-agent swarms (e.g. quantitative trading, distributed medical diagnosis, security incident response, and legal reasoning):
1. **Divergent Agent Belief States**: Each autonomous agent $i$ constructs and maintains its own local knowledge base / belief state $\mathcal{B}_i$ based on observed turns, tool invocations, and incoming messages.
2. **Epistemic Drift & Hallucination Cascades**: When agents exchange messages across long conversational turns, their asserted facts can drift or conflict ($K_i \phi \land K_j \neg\phi$). If unmitigated, conflicting premises propagate through the causal delegation DAG, resulting in split-brain decisions, invalid conclusions, and catastrophic execution failures.
3. **Lack of Formal Epistemic Modal Tracking**: Standard observability frameworks only record string logs without tracking what individual agents *know* ($K_i \phi$), what a group *mutually knows* ($E_G \phi = \bigwedge_{i \in G} K_i \phi$), or what has achieved *common knowledge* status ($C_G \phi$).
4. **Zero-Token Cost Constraint**: Invoking external LLM judges to reconcile every proposition or cross-examine every turn is cost-prohibitive and introduces second-scale latencies, violating SwarmWatch's sub-millisecond local execution standards.

## Decision
We implement Seam 48: Dynamic Multi-Agent Epistemic Logic & Knowledge Base State Synchronization Engine as an in-process, zero-external-dependency, zero-token deep module with full dual-SDK parity across Python (`swarmwatch.epistemic`) and TypeScript (`@swarmwatch/sdk/src/epistemic.ts`).

### 1. Modal Epistemic Logic Formalism ($S5 / KD45$)
- **Truth Values (`EpistemicTruthValue`)**: `TRUE`, `FALSE`, `UNKNOWN`, `CONTRADICTORY`.
- **Propositional Claims (`PropositionalClaim`)**:
  - `claim_id`: Unique identifier (UUID).
  - `agent_id`: Source agent asserting knowledge ($K_i$).
  - `topic`: Domain entity or context namespace (e.g. `order_book`, `patient_vitals`, `policy_compliance`).
  - `predicate`: Specific logical predicate $\phi$ (e.g. `market_status`, `heart_rate_elevated`, `encryption_enabled`).
  - `truth_value`: Assigned truth state.
  - `confidence`: Calibrated certainty score $[0.0, 1.0]$.
  - `timestamp`: UTC ISO timestamp when proposition was asserted.
  - `evidence_span_id`: Optional causal span ID validating provenance.
- **Epistemic Operators**:
  - Individual Knowledge: $K_i \phi$ ("Agent $i$ knows $\phi$").
  - Mutual Knowledge: $E_G \phi = \bigwedge_{i \in G} K_i \phi$ ("Every agent in group $G$ knows $\phi$ with identical truth value").
  - Epistemic Conflict: Detected when $K_i \phi = \text{TRUE}$ and $K_j \phi = \text{FALSE}$ (or vice versa).

### 2. Fast Contradiction Detection & Dissonance Scoring
- Evaluates propositions in $O(1)$ per assertion using indexed bipartite claim mappings `(topic, predicate) -> {agent_id: claim}`.
- Computes **Epistemic Dissonance Score** $D \in [0.0, 1.0]$:
  $$D = \frac{\sum_{c \in \text{conflicts}} (w_i + w_j)}{2 \cdot \text{Total Evaluated Predicates}}$$
- Emits real-time contradiction alerts when dissonance exceeds zero or configurable tolerance threshold.

### 3. AGM-Style Belief Revision Primitives
- Implements deterministic belief revision based on Alchourrón-Gärdenfors-Makinson (AGM) postulates:
  - **Expansion**: Adds non-conflicting proposition to $\mathcal{B}_i$.
  - **Contraction**: Retracts or demotes superseded / unverified claims.
  - **Revision**: Resolves conflicts via epistemic entrenchment rules (higher confidence score + newer temporal provenance wins, or flags as `CONTRADICTORY` if evenly matched).

### 4. Zero-Token Cost Guarantee
- 100% in-process deterministic dictionary and graph evaluations ($<50\mu\text{s}$ per assertion). Zero external LLM calls, zero remote database overhead.

### 5. Dual-SDK Parity
- **Python**: `packages/sdk-python/swarmwatch/epistemic.py` (`EpistemicTruthValue`, `EpistemicStatus`, `PropositionalClaim`, `EpistemicConflict`, `EpistemicKnowledgeBase`, `EpistemicEngine`, `global_epistemic_engine`).
- **TypeScript**: `packages/sdk-typescript/src/epistemic.ts` (`EpistemicTruthValue`, `EpistemicStatus`, `PropositionalClaim`, `EpistemicConflict`, `EpistemicKnowledgeBase`, `EpistemicEngine`, `globalEpistemicEngine`) strictly conforming to Node 22 `--experimental-strip-types`.

### 6. Control Plane, CLI & Web Console Integration
- **REST Endpoints**:
  - `GET /api/v1/swarms/{swarm_id}/epistemic/status`: Consolidated report of knowledge base health, proposition counts, and dissonance score.
  - `GET /api/v1/swarms/{swarm_id}/epistemic/propositions`: Active propositional claims filtered by topic or agent.
  - `GET /api/v1/swarms/{swarm_id}/epistemic/conflicts`: Detailed active contradiction pairs and contested predicates.
  - `POST /api/v1/swarms/{swarm_id}/epistemic/assert`: Ingest and assert a proposition $K_i \phi$.
  - `POST /api/v1/swarms/{swarm_id}/epistemic/reconcile`: Trigger AGM-style belief revision and conflict resolution.
- **CLI Subcommand**:
  - `swarmwatch epistemic status`: Monospace summary of swarm epistemic health, dissonance score, and active conflicts.
  - `swarmwatch epistemic propositions`: Tabular output of asserted knowledge claims.
  - `swarmwatch epistemic assert --agent <id> --topic <topic> --predicate <pred> --val <true|false>`: Assert proposition.
  - `swarmwatch epistemic reconcile`: Reconcile conflicting claims.
- **Web Console**:
  - `#epistemicBadge` in `.fleet-left-group` of `#fleetBar` in `dashboard.html`.
  - Polling interval every 10500ms updating badge text: Consistent (`EPISTEMIC: CONSISTENT (N PROPS)` in green), Dissonant (`EPISTEMIC: DISSONANT (N CONFLICTS)` in amber/red).
  - 100% compliant with High-Density Craft Standard (0 anti-patterns `[]`).

## Consequences
### Positive
- Instantly detects logical and factual contradictions between agents before erroneous decisions are committed.
- Enables autonomous self-reconciliation and epistemic consensus without expensive LLM cross-examination prompts.
- Provides cryptographic traceability linking knowledge assertions directly to causal telemetry spans.

### Negative / Trade-offs
- Requires structured proposition extraction or programmatic assertion by agents/adapters.
