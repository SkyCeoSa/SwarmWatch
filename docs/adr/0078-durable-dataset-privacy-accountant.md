# ADR 0078: Durable Dataset-Level Privacy Accountant & Quorum (ε,δ) Ledger (Seam 67)

## Status
Accepted

## Context
In multi-agent telemetry export, external metric sharing, and causal graph inspection, differential privacy must protect sensitive individual data against reconstruction attacks.
Seam 58 (ADR 0069) established initial query-level $(\epsilon, \delta)$ differential privacy accounting. However, as identified in Foundational Gap 6 of the external architectural audit:

1. **Per-Query vs. Dataset Scope**: Query-level accounting bounds individual queries independently, but does not constrain the cumulative privacy loss of a specific dataset or underlying entity across multi-agent workflows and prolonged export campaigns.
2. **Entity-Level Contribution Leakage**: When multiple agents query distinct slices of a dataset containing data for the same entity $e \in \mathcal{E}$, without entity-specific contribution caps, the privacy loss accumulates linearly or under advanced composition to arbitrarily high values ($\epsilon_{\text{global}} \gg \epsilon_{\text{target}}$), enabling entity re-identification.
3. **Double-Spending Under Network Partitions**: In distributed multi-worker architectures, concurrent workers could double-spend an entity's privacy budget across network splits if budget reservations are not tracked via partition-resistant, monotone ledgers with fail-closed pending charges.

## Decision
We implement the **Durable Dataset-Level Privacy Accountant & Quorum (ε,δ) Ledger** in both Python 3.12+ and TypeScript (Node 22 native type-stripping).

### 1. Mathematical Formalism: Entity-Level Adjacency & Hierarchical Budgets
We model privacy at the dataset and entity granularity:
- **Entity Adjacency**: Two datasets $D, D' \in \mathcal{D}$ are adjacent under entity substitution ($D \sim D'$) if they differ by at most one entity's records: $\|D \oplus D'\|_{\text{entity}} \le 1$.
- **Hierarchical Privacy Caps**:
  - **Dataset Cap**: $(\mathcal{E}_{\text{dataset}}, \Delta_{\text{dataset}})$ represents the global privacy ceiling for dataset $D$.
  - **Entity Cap**: $(\epsilon_{\text{entity\_max}}, \delta_{\text{entity\_max}})$ represents the maximum allowable privacy expenditure for any single entity $e \in \mathcal{E}$ across all queries against $D$.
- **Dual Verification Invariant**: Any proposed query $q$ impacting entity $e$ with requested privacy loss $(\epsilon_q, \delta_q)$ is admitted if and only if both conditions hold simultaneously:
  $$\sum_{i \in \text{committed} \cup \text{pending}} \epsilon_i + \epsilon_q \le \mathcal{E}_{\text{dataset}}$$
  $$\sum_{j \in \text{committed}_e \cup \text{pending}_e} \epsilon_j + \epsilon_q \le \epsilon_{\text{entity\_max}}$$
  If either inequality is violated, the accountant halts fail-closed with `DatasetPrivacyBudgetExhaustedError` or `EntityPrivacyBudgetExhaustedError`.

### 2. Monotone Suprema Reconciliation & Double-Spending Prevention
To prevent double-spending across distributed workers and partition splits:
1. **Fail-Closed Pending Charge**: When a query reservation is requested, the budget is immediately charged against both dataset and entity balances in `PENDING` state. Unconfirmed or timed-out reservations remain charged until explicitly cancelled or reconciled.
2. **Monotone Suprema Join**: When merging ledger states from replica $A$ and replica $B$, the reconciliation operator computes the point-wise supremum over cumulative expenditures:
   $$\text{Ledger}_{A \sqcup B}(e) = \max(\text{Ledger}_A(e), \text{Ledger}_B(e))$$
   This ensures that concurrent spending during a network partition never resets or underestimates spent budget.
3. **Cryptographic Reservation Receipts**: Every reservation and release emits a canonical SHA-256 receipt:
   $$\text{ReceiptHash} = \text{SHA256}(\text{"sw:dpl:v1:"} \parallel \text{reservation\_id} \parallel \text{dataset\_id} \parallel \text{tenant\_id} \parallel \text{entity\_id} \parallel \epsilon \parallel \delta \parallel \text{status})$$

### 3. Deep Module Architecture
The deep module `DatasetPrivacyLedger` / `PrivacyAccountant` provides:
- `register_dataset(dataset_id, tenant_id, max_epsilon, max_delta, entity_epsilon_limit, entity_delta_limit)`: Initializes a dataset privacy boundary.
- `reserve(tenant_id, dataset_id, entity_id, epsilon, delta, sensitivity, query_id)`: Atomically reserves budget against both dataset and entity caps.
- `commit_release(reservation_id, raw_value, noise_seed, min_bound, max_bound)`: Perturbs the raw measurement using calibrated Laplace noise ($b = \Delta f / \epsilon$) and transitions reservation to `COMMITTED`.
- `cancel_reservation(reservation_id)`: Releases a pending reservation back to available balances if a query is aborted before execution.
- `reconcile(peer_snapshot)`: Merges peer expenditures via monotone join $\max(E_1, E_2)$.
- `remaining(tenant_id, dataset_id, entity_id)`: Returns residual dataset and entity privacy allowances.
- `status(tenant_id, dataset_id)`: Diagnostic observability of registered datasets, active reservations, and entity budgets.

### 4. Performance & Operational SLOs
- **Sub-250µs Execution Target**: Reservation admission, entity lookup, and Laplace perturbation complete in $<50\,\mu\text{s}$.
- **Zero-Token Cost Guarantee**: All accounting, bound clipping, and noise sampling use local CPU arithmetic without external LLM calls.
- **CSPRNG Noise Generation**: Uses system cryptographically secure pseudorandom number generators in production (`secrets` in Python, `crypto.randomBytes` in Node.js) with deterministic seeding support for reproducible test verification.

## Consequences

### Positive
- Strict mathematical protection against entity re-identification across repeated swarm telemetry queries.
- Partition-tolerant, double-spend resistant budget accounting.
- Deterministic, non-repudiable audit trails for compliance with GDPR, HIPAA, and NIST SP 800-53 privacy controls.
- Full dual-SDK parity across Python 3.12+ and Node 22 native type-stripping.

### Negative / Trade-offs
- Setting overly conservative entity caps may cause high-frequency queries touching the same entity to be blocked early.
- Reconciling partition states via monotone suprema can reduce available budget if concurrent queries were permitted during a split.
