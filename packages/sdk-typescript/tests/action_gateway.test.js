/**
 * Unit and integration test suite for Autonomous Action Gateway (ADR 0082 / Seam 71).
 * Plain ES module for Node.js native execution.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  ActionGatewayError,
  UnauthorizedActionError,
  ActionQuorumPendingError,
  ActionCircuitBlockedError,
  ActionExecutionFailedError,
  ActionRiskLevel,
  ActionApprovalStatus,
  ActionRequest,
  ActionApproval,
  ActionExecutionReceipt,
  ActionGateway,
  globalActionGateway,
} from "../src/action_gateway.ts";

test("Low-risk auto-approved execution", () => {
  const gateway = new ActionGateway();
  const req = gateway.requestAction({
    tenant_id: "tenant-alpha",
    agent_id: "agent-analyst",
    action_name: "db:query",
    target_resource: "pg://analytics/metrics",
    parameters: { sql: "SELECT 1" },
    estimated_cost_usd: 0.05,
  });

  assert.equal(req.risk_level, ActionRiskLevel.LOW);
  assert.equal(req.status, ActionApprovalStatus.APPROVED);
  assert.equal(req.required_quorum_count, 0);

  const receipt = gateway.executeAction(req.request_id);
  assert.equal(receipt.status, "SUCCESS");
  assert.equal(receipt.result_summary.rows_returned, 42);
  assert.equal(receipt.receipt_hash.length, 64);
  assert.ok(gateway.getReceipt(receipt.receipt_id) !== undefined);
});

test("Medium-risk single approval quorum", () => {
  const gateway = new ActionGateway();
  const req = gateway.requestAction({
    tenant_id: "tenant-beta",
    agent_id: "agent-worker",
    action_name: "api:webhook_call",
    target_resource: "https://api.partner.com/notify",
    parameters: { url: "https://api.partner.com/notify" },
    estimated_cost_usd: 5.0,
  });

  assert.equal(req.risk_level, ActionRiskLevel.MEDIUM);
  assert.equal(req.status, ActionApprovalStatus.PENDING_APPROVAL);
  assert.equal(req.required_quorum_count, 1);

  // Cannot execute while pending approval
  assert.throws(
    () => {
      gateway.executeAction(req.request_id);
    },
    (err) => err instanceof ActionQuorumPendingError
  );

  // Approve by operator
  gateway.approveAction(
    req.request_id,
    "operator-alice",
    "APPROVED",
    "Verified webhook destination"
  );

  const updatedReq = gateway.getRequest(req.request_id);
  assert.ok(updatedReq !== undefined);
  assert.equal(updatedReq.status, ActionApprovalStatus.APPROVED);

  const receipt = gateway.executeAction(req.request_id);
  assert.equal(receipt.status, "SUCCESS");
  assert.equal(receipt.result_summary.delivered, true);
});

test("Critical-risk multi-party quorum", () => {
  const gateway = new ActionGateway();
  const req = gateway.requestAction({
    tenant_id: "tenant-corp",
    agent_id: "agent-dba",
    action_name: "db:drop_table",
    target_resource: "pg://prod/legacy_users",
    parameters: { table: "legacy_users" },
    estimated_cost_usd: 0.0,
    reversibility_class: "IRREVERSIBLE",
  });

  assert.equal(req.risk_level, ActionRiskLevel.CRITICAL);
  assert.equal(req.status, ActionApprovalStatus.PENDING_APPROVAL);
  assert.ok(req.required_quorum_count >= 2);

  // First approval (1/2)
  gateway.approveAction(req.request_id, "secops-lead", "APPROVED");
  assert.throws(
    () => {
      gateway.executeAction(req.request_id);
    },
    (err) => err instanceof ActionQuorumPendingError
  );

  // Second approval (reaches 2/2 quorum)
  gateway.approveAction(req.request_id, "infrastructure-vp", "APPROVED");

  const receipt = gateway.executeAction(req.request_id);
  assert.equal(receipt.status, "SUCCESS");
  assert.equal(receipt.result_summary.dropped, true);
});

test("Quorum rejection halts execution", () => {
  const gateway = new ActionGateway();
  const req = gateway.requestAction({
    tenant_id: "tenant-corp",
    agent_id: "agent-worker",
    action_name: "finance:refund",
    target_resource: "stripe://acct_01",
    parameters: { amount: 500 },
    estimated_cost_usd: 500.0,
  });

  // Reject request
  gateway.approveAction(
    req.request_id,
    "risk-officer",
    "REJECTED",
    "Fraud alert flagged on account"
  );

  assert.equal(req.status, ActionApprovalStatus.REJECTED);

  assert.throws(
    () => {
      gateway.executeAction(req.request_id);
    },
    /was rejected/
  );
});

test("Circuit tripped halts gateway fail-closed", () => {
  const gateway = new ActionGateway();
  gateway.setCircuitTripped(true);

  assert.throws(
    () => {
      gateway.requestAction({
        tenant_id: "tenant-test",
        agent_id: "agent-test",
        action_name: "db:query",
        target_resource: "pg://metrics",
      });
    },
    (err) => err instanceof ActionCircuitBlockedError
  );
});

test("Capability token enforcement", () => {
  const gateway = new ActionGateway();

  function mockValidator(token, action, resource, tenant_id) {
    return token === "valid-token-alpha" && tenant_id === "tenant-alpha";
  }

  gateway.setCapabilityValidator(mockValidator, true);

  // 1. Missing token raises UnauthorizedActionError
  assert.throws(
    () => {
      gateway.requestAction({
        tenant_id: "tenant-alpha",
        agent_id: "agent-01",
        action_name: "db:query",
        target_resource: "pg://analytics",
      });
    },
    (err) => err instanceof UnauthorizedActionError && err.message.includes("requires capability token")
  );

  // 2. Invalid token raises UnauthorizedActionError
  assert.throws(
    () => {
      gateway.requestAction({
        tenant_id: "tenant-alpha",
        agent_id: "agent-01",
        action_name: "db:query",
        target_resource: "pg://analytics",
        capability_token: "invalid-token",
      });
    },
    (err) => err instanceof UnauthorizedActionError && err.message.includes("rejected")
  );

  // 3. Valid token succeeds
  const req = gateway.requestAction({
    tenant_id: "tenant-alpha",
    agent_id: "agent-01",
    action_name: "db:query",
    target_resource: "pg://analytics",
    capability_token: "valid-token-alpha",
  });
  assert.equal(req.status, ActionApprovalStatus.APPROVED);
});

test("Downstream handler failure produces failed receipt", () => {
  const gateway = new ActionGateway();
  gateway.registerExecutor(
    "faulty:action",
    () => {
      throw new Error("Simulated downstream database timeout");
    },
    ActionRiskLevel.LOW,
    "REVERSIBLE",
    0
  );

  const req = gateway.requestAction({
    tenant_id: "tenant-err",
    agent_id: "agent-err",
    action_name: "faulty:action",
    target_resource: "test://err",
  });

  assert.throws(
    () => {
      gateway.executeAction(req.request_id);
    },
    (err) => err instanceof ActionExecutionFailedError
  );

  const receipts = gateway.getReceipts("tenant-err");
  assert.equal(receipts.length, 1);
  assert.equal(receipts[0].status, "FAILED");
  assert.match(String(receipts[0].result_summary.error), /Simulated downstream database timeout/);
});

test("Receipt canonical hash integrity", () => {
  const gateway = new ActionGateway();
  const req = gateway.requestAction({
    tenant_id: "tenant-hash",
    agent_id: "agent-hash",
    action_name: "db:query",
    target_resource: "pg://test",
    parameters: { limit: 10 },
  });
  const receipt = gateway.executeAction(req.request_id);

  assert.equal(receipt.receipt_hash.includes("sw:act:v1:"), false); // raw hex
  assert.equal(receipt.receipt_hash.length, 64);

  const d = receipt.toDict();
  const restored = ActionExecutionReceipt.fromDict(d);
  assert.equal(restored.receipt_hash, receipt.receipt_hash);
  assert.equal(restored.duration_us, receipt.duration_us);
});

test("Pending approvals filtering", () => {
  const gateway = new ActionGateway();
  gateway.requestAction({ tenant_id: "t1", agent_id: "a1", action_name: "api:webhook_call", target_resource: "res1" });
  gateway.requestAction({ tenant_id: "t2", agent_id: "a2", action_name: "api:webhook_call", target_resource: "res2" });

  const allPending = gateway.getPendingApprovals();
  assert.equal(allPending.length, 2);

  const t1Pending = gateway.getPendingApprovals("t1");
  assert.equal(t1Pending.length, 1);
  assert.equal(t1Pending[0].tenant_id, "t1");
});

test("Sub-250us Action Gateway SLO", () => {
  const gateway = new ActionGateway();

  // Warmup
  for (let i = 0; i < 10; i++) {
    const req = gateway.requestAction({
      tenant_id: "tenant-bench",
      agent_id: "agent-bench",
      action_name: "db:query",
      target_resource: "pg://bench",
    });
    gateway.executeAction(req.request_id);
  }

  // Benchmark: 500 complete request -> execute cycles
  const iterations = 500;
  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    const req = gateway.requestAction({
      tenant_id: "tenant-bench",
      agent_id: "agent-bench",
      action_name: "db:query",
      target_resource: "pg://bench",
    });
    gateway.executeAction(req.request_id);
  }
  const t1 = performance.now();

  const avgUs = ((t1 - t0) / iterations) * 1000;
  assert.ok(avgUs < 250.0, `Action Gateway roundtrip took ${avgUs.toFixed(2)}µs, exceeding 250µs SLO`);
});

test("Global singleton status and reset", () => {
  const st = globalActionGateway.status();
  assert.ok("registered_executors_count" in st);
  assert.ok(Number(st.registered_executors_count) >= 4);

  globalActionGateway.requestAction({
    tenant_id: "t-singleton",
    agent_id: "agent-s",
    action_name: "db:query",
    target_resource: "pg://s",
  });
  assert.ok(Number(globalActionGateway.status().total_requests) >= 1);

  globalActionGateway.reset();
  assert.equal(globalActionGateway.status().total_requests, 0);
  assert.equal(globalActionGateway.status().circuit_tripped, false);
});
