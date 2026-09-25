import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";
import {
  IngressPriority,
  CompletenessStatus,
  SequenceGap,
  IngressResult,
  CompletenessReport,
  CausalIngress,
  globalCausalIngress,
} from "../src/causal_ingress.ts";

describe("Seam 53: Loss-Aware Causal Ingestion & Evidentiary Completeness Rule (ADR 0064)", () => {
  beforeEach(() => {
    globalCausalIngress.reset();
  });

  test("SequenceGap creation and round-trip verification", () => {
    const gap = new SequenceGap({
      swarmId: "sw-ts-01",
      streamId: "stream-main",
      expectedSeq: 5,
      receivedSeq: 8,
      missingCount: 3,
      detectedAt: 12345.67,
    });

    assert.equal(gap.swarmId, "sw-ts-01");
    assert.equal(gap.expectedSeq, 5);
    assert.equal(gap.receivedSeq, 8);
    assert.equal(gap.missingCount, 3);

    const dict = gap.toDict();
    assert.equal(dict.missing_count, 3);
    const reconstructed = SequenceGap.fromDict(dict);
    assert.equal(reconstructed.expectedSeq, 5);
    assert.equal(reconstructed.receivedSeq, 8);
  });

  test("Monotonic stream sequencing and independent stream isolation", () => {
    const ingress = new CausalIngress(100);

    const r1 = ingress.submit({ span_id: "s1", swarm_id: "sw1" }, IngressPriority.NORMAL, "streamA");
    const r2 = ingress.submit({ span_id: "s2", swarm_id: "sw1" }, IngressPriority.NORMAL, "streamA");
    const r3 = ingress.submit({ span_id: "s3", swarm_id: "sw1" }, IngressPriority.NORMAL, "streamA");

    assert.equal(r1.seqNum, 1);
    assert.equal(r2.seqNum, 2);
    assert.equal(r3.seqNum, 3);
    assert.equal(r1.gapDetected, false);
    assert.equal(r2.gapDetected, false);

    // Stream B independent
    const rb1 = ingress.submit({ span_id: "sb1", swarm_id: "sw1" }, IngressPriority.NORMAL, "streamB");
    assert.equal(rb1.seqNum, 1);
  });

  test("Normal priority headroom shedding at 80% threshold", () => {
    // maxCapacity = 10, reserveRatio = 0.2 => normalThreshold = 8
    const ingress = new CausalIngress(10, 0.2);

    for (let i = 0; i < 8; i++) {
      const res = ingress.submit({ span_id: `s_${i}`, swarm_id: "sw1" }, IngressPriority.NORMAL);
      assert.equal(res.accepted, true);
      assert.equal(res.status, "ACCEPTED");
    }

    // 9th normal span should be dropped
    const shed = ingress.submit({ span_id: "s_shed", swarm_id: "sw1" }, IngressPriority.NORMAL);
    assert.equal(shed.accepted, false);
    assert.equal(shed.status, "DROPPED_SATURATION");
    assert.ok(shed.reason?.includes("Normal priority shed"));

    const rep = ingress.completeness("sw1");
    assert.equal(rep.totalSubmitted, 9);
    assert.equal(rep.totalAccepted, 8);
    assert.equal(rep.totalDropped, 1);
    assert.ok(rep.lossRatio > 0);
    assert.equal(rep.status, CompletenessStatus.SATURATED);
  });

  test("Critical priority 100% delivery guarantee under queue saturation", () => {
    const ingress = new CausalIngress(10, 0.2);

    // Saturate normal threshold
    for (let i = 0; i < 8; i++) {
      ingress.submit({ span_id: `s_${i}`, swarm_id: "sw1" }, IngressPriority.NORMAL);
    }

    // 9th normal drops
    ingress.submit({ span_id: "s_drop", swarm_id: "sw1" }, IngressPriority.NORMAL);

    // Critical spans must be accepted
    const c1 = ingress.submit({ span_id: "crit_1", swarm_id: "sw1" }, IngressPriority.CRITICAL);
    const c2 = ingress.submit({ span_id: "crit_2", swarm_id: "sw1" }, IngressPriority.CRITICAL);

    assert.equal(c1.accepted, true);
    assert.equal(c2.accepted, true);

    const rep = ingress.completeness("sw1");
    assert.equal(rep.criticalSubmitted, 2);
    assert.equal(rep.criticalAccepted, 2);
    assert.equal(rep.criticalDeliveryRatio, 1.0);
  });

  test("Sequence gap detection on dropped normal spans", () => {
    const ingress = new CausalIngress(5, 0.2);
    // normalThreshold = 4

    for (let i = 0; i < 4; i++) {
      ingress.submit({ span_id: `s_${i}`, swarm_id: "sw_gap" }, IngressPriority.NORMAL, "main");
    }

    // 5th normal span dropped (seq 5 dropped)
    const d5 = ingress.submit({ span_id: "s_5", swarm_id: "sw_gap" }, IngressPriority.NORMAL, "main");
    assert.equal(d5.accepted, false);
    assert.equal(d5.seqNum, 5);

    // 6th span is CRITICAL (accepted with seq 6)
    const c6 = ingress.submit({ span_id: "s_6", swarm_id: "sw_gap" }, IngressPriority.CRITICAL, "main");
    assert.equal(c6.accepted, true);
    assert.equal(c6.seqNum, 6);
    assert.equal(c6.gapDetected, true);
    assert.ok(c6.gap !== null);
    assert.equal(c6.gap.expectedSeq, 5);
    assert.equal(c6.gap.receivedSeq, 6);
    assert.equal(c6.gap.missingCount, 1);

    const rep = ingress.completeness("sw_gap");
    assert.equal(rep.sequenceGaps.length, 1);
    assert.equal(rep.sequenceGaps[0].missing_count, 1);
  });

  test("Evidentiary completeness and unanchored causal parent detection", () => {
    const ingress = new CausalIngress(100);

    ingress.submit({ span_id: "root_ts_01", swarm_id: "sw_lineage" });
    ingress.submit({ span_id: "child_ts_01", swarm_id: "sw_lineage", parent_span_ids: ["root_ts_01"] });

    const rep1 = ingress.completeness("sw_lineage");
    assert.equal(rep1.status, CompletenessStatus.COMPLETE);
    assert.equal(rep1.missingCausalParents.length, 0);

    // Add unanchored child
    ingress.submit({ span_id: "orphan_01", swarm_id: "sw_lineage", parent_span_ids: ["missing_parent_xyz"] });
    const rep2 = ingress.completeness("sw_lineage");
    assert.equal(rep2.status, CompletenessStatus.INCOMPLETE);
    assert.ok(rep2.missingCausalParents.includes("missing_parent_xyz"));
  });

  test("FIFO buffer draining and diagnostic status metrics", () => {
    const ingress = new CausalIngress(50);
    for (let i = 0; i < 5; i++) {
      ingress.submit({ span_id: `spn_${i}`, swarm_id: "sw_drain" });
    }

    const drained = ingress.drain(3);
    assert.equal(drained.length, 3);
    assert.equal(drained[0].span_id, "spn_0");
    assert.equal(drained[1].span_id, "spn_1");
    assert.equal(drained[2].span_id, "spn_2");

    const st = ingress.status("sw_drain");
    assert.equal(st.queue_length, 2);
    assert.equal(st.completeness_status, CompletenessStatus.COMPLETE);

    const remaining = ingress.drain(10);
    assert.equal(remaining.length, 2);
    assert.equal(remaining[0].span_id, "spn_3");
    assert.equal(remaining[1].span_id, "spn_4");
  });
});
