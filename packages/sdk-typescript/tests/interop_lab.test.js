/**
 * Tests for Protocol Interoperability Lab & OTLP/gRPC Conformance Matrix (Seam 68 / ADR 0079).
 */

import test from "node:test";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

import {
  InteropLab,
  ProtocolStandard,
  ConformanceFault,
  ClientVariant,
  ProtocolState,
  DeviationSeverity,
  ProtocolDeviation,
  ExerciseResult,
  ConformanceAttestationReceipt,
  ProtocolConformanceStateMachine,
  WireFaultInjector,
  globalInteropLab,
} from "../src/interop_lab.ts";

test("Protocol state machine valid transitions", () => {
  const sm = new ProtocolConformanceStateMachine();
  assert.equal(sm.current_state, ProtocolState.IDLE);

  assert.equal(sm.transition(ProtocolState.CONNECTING), true);
  assert.equal(sm.current_state, ProtocolState.CONNECTING);

  assert.equal(sm.transition(ProtocolState.STREAMING), true);
  assert.equal(sm.current_state, ProtocolState.STREAMING);

  assert.equal(sm.transition(ProtocolState.FRAME_ACCUMULATION), true);
  assert.equal(sm.current_state, ProtocolState.FRAME_ACCUMULATION);

  assert.equal(sm.transition(ProtocolState.DISPATCHING), true);
  assert.equal(sm.current_state, ProtocolState.DISPATCHING);

  assert.equal(sm.transition(ProtocolState.COMPLETED), true);
  assert.equal(sm.current_state, ProtocolState.COMPLETED);

  assert.equal(sm.history.length, 6);
});

test("Protocol state machine invalid transitions rejection", () => {
  const sm = new ProtocolConformanceStateMachine();
  assert.equal(sm.transition(ProtocolState.DISPATCHING), false);
  assert.equal(sm.transition(ProtocolState.COMPLETED), false);
  assert.equal(sm.current_state, ProtocolState.IDLE);
});

test("Wire fault injector chunking across 1-byte and 7-byte slices", () => {
  const data = Buffer.from("SWARMWATCH_OTLP_STREAM_PAYLOAD_TEST_12345", "utf8");
  const chunks1b = WireFaultInjector.chunkBytes(data, 1);
  assert.equal(chunks1b.length, data.length);
  assert.deepEqual(Buffer.concat(chunks1b), data);

  const chunks7b = WireFaultInjector.chunkBytes(data, 7);
  assert.equal(chunks7b.length, Math.ceil(data.length / 7));
  assert.deepEqual(Buffer.concat(chunks7b), data);
});

test("Exercise standard healthy OTLP and gRPC protocols", () => {
  const lab = new InteropLab();
  const protocols = [
    ProtocolStandard.OTLP_HTTP_JSON,
    ProtocolStandard.OTLP_HTTP_PROTOBUF,
    ProtocolStandard.GRPC_STREAMING,
    ProtocolStandard.GRPC_UNARY,
  ];

  for (const proto of protocols) {
    const res = lab.exercise(
      proto,
      ClientVariant.NATIVE_OTEL_SDK,
      ConformanceFault.NONE
    );
    assert.equal(res.success, true);
    assert.equal(res.protocol, proto);
    assert.equal(res.reconstructed_spans_count, 1);
    assert.equal(res.state_history[res.state_history.length - 1], "COMPLETED");
    assert.equal(typeof res.receipt_hash, "string");
  }

  const st = lab.status();
  assert.equal(st.total_exercises_run, 4);
  assert.equal(st.passed_exercises, 4);
  assert.equal(st.failed_exercises, 0);
  assert.equal(st.conformance_certified, true);
});

test("Exercise fragmentation fault reassembly across small chunks", () => {
  const lab = new InteropLab();
  const customSpan = { trace_id: "t1", span_id: "s1", name: "frag_op" };

  const res1b = lab.exercise(
    ProtocolStandard.OTLP_HTTP_JSON,
    ClientVariant.MOCK_CLIENT,
    ConformanceFault.FRAGMENTATION,
    customSpan,
    1
  );
  assert.equal(res1b.success, true);
  assert.equal(res1b.reconstructed_spans_count, 1);
  assert.ok(res1b.state_history.includes("FRAME_ACCUMULATION"));
  assert.ok(res1b.state_history.includes("DISPATCHING"));

  const res7b = lab.exercise(
    ProtocolStandard.GRPC_STREAMING,
    ClientVariant.MOCK_CLIENT,
    ConformanceFault.FRAGMENTATION,
    customSpan,
    7
  );
  assert.equal(res7b.success, true);
  assert.equal(res7b.reconstructed_spans_count, 1);
});

test("Exercise early cancellation lifecycle handling", () => {
  const lab = new InteropLab();
  const res = lab.exercise(
    ProtocolStandard.GRPC_STREAMING,
    ClientVariant.GRPC_CORE,
    ConformanceFault.EARLY_CANCELLATION
  );
  assert.equal(res.success, true);
  assert.equal(res.state_history[res.state_history.length - 1], "TERMINATED_ABORTED");
});

test("Exercise backpressure flow control window saturation", () => {
  const lab = new InteropLab(100);
  const res = lab.exercise(
    ProtocolStandard.OTLP_HTTP_JSON,
    ClientVariant.MOCK_CLIENT,
    ConformanceFault.BACKPRESSURE_FLOW_CONTROL
  );
  assert.equal(res.success, true);
  assert.equal(res.deviations.length, 1);
  assert.equal(res.deviations[0].scenario, "flow_control_window_saturation");
  assert.equal(res.deviations[0].severity, DeviationSeverity.INFORMATIONAL);
});

test("Exercise malformed header and truncated body injection", () => {
  const lab = new InteropLab();

  // Malformed header
  const resHdr = lab.exercise(
    ProtocolStandard.GRPC_UNARY,
    ClientVariant.MOCK_CLIENT,
    ConformanceFault.MALFORMED_HEADER
  );
  assert.equal(resHdr.success, true);
  assert.equal(resHdr.state_history[resHdr.state_history.length - 1], "TERMINATED_ABORTED");
  assert.equal(resHdr.deviations.length, 1);

  // Truncated body
  const resTrunc = lab.exercise(
    ProtocolStandard.OTLP_HTTP_PROTOBUF,
    ClientVariant.MOCK_CLIENT,
    ConformanceFault.TRUNCATED_BODY
  );
  assert.equal(resTrunc.success, true);
  assert.equal(resTrunc.state_history[resTrunc.state_history.length - 1], "TERMINATED_ABORTED");
  assert.equal(resTrunc.deviations.length, 1);
});

test("Record deviation and matrix aggregation", () => {
  const lab = new InteropLab();
  lab.recordDeviation(
    ProtocolStandard.OTLP_HTTP_JSON,
    "missing_service_name_attribute",
    "service.name missing in resource attributes",
    "service.name must be set per OTel convention",
    DeviationSeverity.WARNING
  );

  const st = lab.status();
  assert.equal(st.warning_deviations, 1);
  assert.equal(st.fatal_deviations, 0);

  const matrix = lab.getMatrix();
  assert.equal(matrix.total_deviations, 1);
});

test("Conformance attestation matrix receipt generation", () => {
  const lab = new InteropLab();
  lab.exercise(ProtocolStandard.OTLP_HTTP_JSON, ClientVariant.MOCK_CLIENT, ConformanceFault.NONE);
  lab.exercise(ProtocolStandard.GRPC_STREAMING, ClientVariant.MOCK_CLIENT, ConformanceFault.NONE);
  lab.exercise(ProtocolStandard.OTLP_HTTP_PROTOBUF, ClientVariant.MOCK_CLIENT, ConformanceFault.FRAGMENTATION);

  const receipt = lab.attestMatrix("Production OTel Gateway Conformance Gate");
  assert.equal(receipt.suite_name, "Production OTel Gateway Conformance Gate");
  assert.equal(receipt.total_exercises, 3);
  assert.equal(receipt.pass_count, 3);
  assert.equal(receipt.fail_count, 0);
  assert.equal(receipt.protocols_covered.length, 3);
  assert.equal(receipt.attestation_hash.length, 64);

  const d = receipt.toDict();
  assert.equal(typeof d.attestation_hash, "string");
});

test("Sub-250 microsecond execution latency SLO benchmark", () => {
  const lab = new InteropLab();

  // Warmup
  for (let i = 0; i < 10; i++) {
    lab.exercise(ProtocolStandard.OTLP_HTTP_JSON, ClientVariant.MOCK_CLIENT, ConformanceFault.NONE);
  }

  // Benchmark: 500 iterations
  const iterations = 500;
  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    const fault = i % 2 === 0 ? ConformanceFault.NONE : ConformanceFault.FRAGMENTATION;
    lab.exercise(ProtocolStandard.OTLP_HTTP_JSON, ClientVariant.MOCK_CLIENT, fault, undefined, 7);
  }
  const t1 = performance.now();

  const avgUs = ((t1 - t0) / iterations) * 1000;
  assert.ok(avgUs < 250, `Average exercise time ${avgUs.toFixed(2)}µs exceeded 250µs SLO`);
});

test("Global singleton and status reporting", () => {
  const st = globalInteropLab.status();
  assert.ok("total_exercises_run" in st);
  assert.ok("passed_exercises" in st);
  assert.ok("conformance_certified" in st);
});
