import test from "node:test";
import assert from "node:assert/strict";

import {
  TenantEngineRegistry,
} from "../src/engine_registry.ts";
import { CircuitBusTrippedError } from "../src/circuit_bus.ts";

test("Seam 89: Tenant non-interference differential (TypeScript)", async () => {
  const registry = new TenantEngineRegistry();

  const busA = registry.get("tenant-alpha", "circuit_bus");
  const busB = registry.get("tenant-beta", "circuit_bus");

  assert.notEqual(busA, busB);
  assert.equal(busA.isTripped(), false);
  assert.equal(busB.isTripped(), false);

  // Invariant T1: Mutate and trip Tenant A's circuit
  busA.trip("Critical failure in Alpha", "GLOBAL");
  assert.equal(busA.isTripped(), true);

  // Tenant B remains completely unperturbed!
  assert.equal(busB.isTripped(), false);
  let executedB = false;
  await busB.guard("GLOBAL", () => {
    executedB = true;
  });
  assert.equal(executedB, true);

  // Guard on A throws fail-closed
  await assert.rejects(
    async () => {
      await busA.guard("GLOBAL", () => {});
    },
    CircuitBusTrippedError
  );
});

test("Seam 89: Tenant CRDT state isolation (TypeScript)", () => {
  const registry = new TenantEngineRegistry();

  const crdtA = registry.get("tenant-a", "crdt_kernel");
  const crdtB = registry.get("tenant-b", "crdt_kernel");

  crdtA.putVertex("user-1", { role: "ADMIN" });

  // User-1 exists in Tenant A, but DOES NOT exist in Tenant B
  assert.notEqual(crdtA.getVertex("user-1"), null);
  assert.equal(crdtB.getVertex("user-1"), null);
  assert.notEqual(crdtA.getStateDigest(), crdtB.getStateDigest());
});

test("Seam 89: Zero-bleed tenant eviction (TypeScript)", () => {
  const registry = new TenantEngineRegistry();

  registry.get("tenant-temp", "circuit_bus");
  registry.get("tenant-keep", "circuit_bus");

  assert.ok(registry.listTenants().includes("tenant-temp"));
  assert.ok(registry.listTenants().includes("tenant-keep"));

  // Invariant T3: Evict temporary tenant
  registry.evictTenant("tenant-temp");
  assert.ok(!registry.listTenants().includes("tenant-temp"));
  assert.ok(registry.listTenants().includes("tenant-keep"));
});

test("Seam 89: Sub-250us execution latency SLO (TypeScript)", () => {
  const registry = new TenantEngineRegistry();

  // Warmup
  for (let i = 0; i < 50; i++) registry.get("perf-tenant", "circuit_bus");

  const startNs = process.hrtime.bigint();
  const count = 1000;
  for (let i = 0; i < count; i++) {
    registry.get("perf-tenant", "circuit_bus");
  }
  const avgUs = Number(process.hrtime.bigint() - startNs) / (count * 1000);
  assert.ok(avgUs < 10, `Average registry lookup latency ${avgUs}us must be within SLO`);
});
