# ADR 0082: Autonomous Action Gateway & Governed Downstream Agent Execution (Seam 71)

## Status
Accepted

## Context
Multi-agent LLM systems in production are entrusted with increasingly autonomous capabilities, including issuing database mutations, initiating financial transactions, interacting with third-party webhooks, invoking expensive foundation model reasoning endpoints, and provisioning cloud infrastructure. 

Without a centralized, mathematically verified execution gateway:
1. **Unmediated Tool Side-Effects**: Agents can execute destructive or irreversible external operations without cryptographic authorization or tenant boundary enforcement.
2. **Ambient Authority & Privilege Escalation**: Sub-agents inherit unrestricted ambient permissions rather than attenuated capability tokens (violating ADR 0076 / Seam 65).
3. **Blind Irreversible Actions**: Actions with catastrophic side-effects execute automatically without reversibility classification or compensation planning (violating ADR 0077 / Seam 66).
4. **Lack of Human Quorum**: Critical operations (e.g., transfers over \$1,000, schema drops, cluster resets) fail to require multi-party human operator sign-off before dispatch.
5. **Zero Control-Plane Metering**: Execution costs and external model token consumptions are not tracked or bounded, allowing runaway agent loops to burn computational and financial budgets.
6. **Absence of Tamper-Evident Receipts**: Downstream execution outcomes lack non-repudiable cryptographic receipts linking agent intent, capability token, approval signature, execution duration, and outcome status.

## Decision
We implement the **Autonomous Action Gateway & Governed Downstream Agent Execution (`ActionGateway`)** deep module across Python 3.12+ and TypeScript (Node 22 native type-stripping).

### 1. Formal Execution Architecture & Lifecycle
Every downstream agent action passes through a deterministic 4-stage lifecycle:
$$\text{ActionRequest} \xrightarrow[\text{Capability Check}]{\text{Admission}} \text{Risk Evaluation} \xrightarrow[\text{Quorum Gate}]{\text{Authorization}} \text{Execution Dispatch} \xrightarrow[\text{Audit Ledger}]{\text{Receipt Minting}}$$

1. **Request Submission (`request_action`)**:
   An agent or tool client submits an `ActionRequest` specifying:
   - `request_id`: Unique identifier (`act-req-...`).
   - `tenant_id`: Mandatory tenant isolation context.
   - `agent_id`: Calling agent identity.
   - `action_name`: Qualified tool or action name (e.g., `db:execute_query`, `stripe:refund`, `llm:reasoning`).
   - `target_resource`: Resource URI or pattern (e.g., `pg://main/users`, `acct_10928`).
   - `parameters`: Execution payload dictionary.
   - `capability_token`: Attenuated `CapabilityToken` string (NIST P-256 signed per ADR 0076).
   - `estimated_cost_usd`: Upper bound cost in USD.
   - `reversibility_class`: Declared reversibility (`REVERSIBLE`, `COMPENSATABLE`, `IRREVERSIBLE`).

2. **Admission & Attenuation Verification**:
   The gateway verifies:
   - `capability_token` is cryptographically valid, unexpired, and matches `tenant_id`.
   - The token contains a matching scope granting `action_name` on `target_resource`.
   - The cluster is not in an active safety trip or minority network partition (ADR 0081).
   Any failure halts immediately fail-closed with `UnauthorizedActionError` or `ActionCircuitBlockedError`.

3. **Risk Tier & Multi-Party Quorum Evaluation**:
   The gateway determines the `ActionRiskLevel`:
   - **LOW**: `REVERSIBLE` action, non-privileged scope, estimated cost $\le \$1.00$. Automatically approved and admitted for immediate dispatch.
   - **MEDIUM**: `COMPENSATABLE` action or cost between $\$1.00$ and $\$50.00$. Requires single operator approval or policy allowance.
   - **HIGH / CRITICAL**: `IRREVERSIBLE` action, destructive scope (e.g., `delete:*`, `drop:*`), or cost $> \$50.00$. Requires $M$-of-$N$ human operator quorum approval (`ActionApprovalStatus.PENDING_APPROVAL`).

4. **Execution & Receipt Generation (`execute_action`)**:
   Upon authorization, the registered action executor is called with execution timing. Regardless of success or error:
   - A non-repudiable `ActionExecutionReceipt` is minted with canonical SHA-256 hash:
     $$\text{ReceiptHash} = \text{SHA256}(\text{"sw:act:v1:"} \parallel \text{request\_id} \parallel \text{status} \parallel \text{duration\_us} \parallel \text{outcome})$$
   - Results are committed to the internal audit ledger and evidence spool.

### 2. Quorum Governance & Approval Rules
For High/Critical risk actions requiring human quorum:
- Approval records (`ActionApproval`) must be signed by authorized operator principals.
- Required quorum threshold $M \le N$ is evaluated (default: 1-of-1 for standard operator, 2-of-3 for critical enterprise tier).
- Attempting to execute an action in `PENDING_APPROVAL` status raises `ActionQuorumPendingError`.
- Rejection by any quorum member sets status to `REJECTED` and halts execution.

### 3. Deep Module Interface
- `ActionGateway`:
  - `register_executor(action_name, handler, default_risk, default_reversibility)`: Registers deterministic action handlers.
  - `request_action(tenant_id, agent_id, action_name, target_resource, parameters, ...)`: Submits and evaluates an action request.
  - `approve_action(request_id, approver_principal, decision, comment)`: Records human operator approval/rejection.
  - `execute_action(request_id)`: Dispatches the authorized action and produces execution receipt.
  - `get_pending_approvals(tenant_id)`: Lists pending requests awaiting quorum sign-off.
  - `get_receipt(receipt_id)`: Retrieves immutable cryptographic execution receipt.
  - `status()`: Returns operational metrics (throughput, approval latency, pending count, failure rates).
  - `reset()`: Resets transient gateway state for testing.

### 4. Zero-Token Cost Guarantee & Sub-250µs Execution SLO
All capability verification, reversibility classification, cost metering, and quorum evaluation execute 100% locally using in-memory cryptographic routines (SHA-256, ECDSA verification, dictionary lookups) with zero external LLM API calls and execution latency $< 250\mu\text{s}$.

### 5. Dual-SDK Parity
Both Python 3.12+ (`swarmwatch.action_gateway`) and TypeScript (`@swarmwatch/sdk/src/action_gateway.ts`) provide identical models, error classes, state transitions, and canonical hashing adhering to Node 22 native type-stripping rules.
