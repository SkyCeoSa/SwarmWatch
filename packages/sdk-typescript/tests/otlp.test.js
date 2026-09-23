import { test } from "node:test";
import assert from "node:assert/strict";

import {
  extractOtlpValue,
  extractOtlpAttributes,
  formatOtlpTracesPayload,
  parseOtlpTracesPayload,
  OtlpTraceExporter,
} from "../src/otlp.ts";

test("OTLP - extractOtlpValue converts proto3 AnyValue types cleanly", () => {
  assert.equal(extractOtlpValue({ stringValue: "hello" }), "hello");
  assert.equal(extractOtlpValue({ intValue: "42" }), 42);
  assert.equal(extractOtlpValue({ doubleValue: 3.14 }), 3.14);
  assert.equal(extractOtlpValue({ boolValue: true }), true);
  assert.equal(extractOtlpValue({ bytesValue: "deadbeef" }), "deadbeef");

  const arr = extractOtlpValue({
    arrayValue: { values: [{ stringValue: "a" }, { intValue: 10 }] },
  });
  assert.deepEqual(arr, ["a", 10]);
});

test("OTLP - extractOtlpAttributes handles both proto3 list and flat dictionary", () => {
  const proto3List = [
    { key: "gen_ai.system", value: { stringValue: "openai" } },
    { key: "gen_ai.usage.total_tokens", value: { intValue: 250 } },
    { key: "temperature", value: { doubleValue: 0.7 } },
  ];
  const parsedFromList = extractOtlpAttributes(proto3List);
  assert.equal(parsedFromList["gen_ai.system"], "openai");
  assert.equal(parsedFromList["gen_ai.usage.total_tokens"], 250);
  assert.equal(parsedFromList["temperature"], 0.7);

  const flatDict = {
    "gen_ai.system": "anthropic",
    "gen_ai.usage.total_tokens": 500,
  };
  const parsedFromFlat = extractOtlpAttributes(flatDict);
  assert.equal(parsedFromFlat["gen_ai.system"], "anthropic");
  assert.equal(parsedFromFlat["gen_ai.usage.total_tokens"], 500);
});

test("OTLP - formatOtlpTracesPayload generates standard OTLP JSON structure", () => {
  const span = {
    id: "span-ts-1",
    swarm_id: "trace-ts-1",
    branch_id: "main",
    source_agent: "Architect",
    target_agent: "Engineer",
    span_type: "agent_call",
    token_count: 320,
    entropy_score: 0.88,
    cost_estimate: 0.0064,
    start_time: "2026-09-23T10:00:00.000Z",
    end_time: "2026-09-23T10:00:00.500Z",
    duration_ms: 500,
  };

  const payload = formatOtlpTracesPayload([span], "my-ts-swarm");
  assert.ok(payload.resourceSpans);
  assert.equal(payload.resourceSpans.length, 1);

  const rs = payload.resourceSpans[0];
  assert.equal(rs.scopeSpans.length, 1);
  const otelSpan = rs.scopeSpans[0].spans[0];

  assert.equal(otelSpan.traceId, "trace-ts-1");
  assert.equal(otelSpan.spanId, "span-ts-1");
  assert.equal(otelSpan.name, "Architect -> Engineer:agent_call");
});

test("OTLP - parseOtlpTracesPayload translates OTel GenAI conventions to SwarmSpan", () => {
  const samplePayload = {
    resourceSpans: [
      {
        resource: {
          attributes: [
            { key: "service.name", value: { stringValue: "crewai-production" } },
          ],
        },
        scopeSpans: [
          {
            scope: { name: "crewai.telemetry", version: "1.0.0" },
            spans: [
              {
                traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
                spanId: "00f067aa0ba902b7",
                parentSpanId: "",
                name: "Researcher -> Summarizer:chat",
                startTimeUnixNano: "1727085600000000000",
                endTimeUnixNano: "1727085600850000000",
                attributes: [
                  { key: "gen_ai.system", value: { stringValue: "crewai" } },
                  { key: "gen_ai.agent.name", value: { stringValue: "Researcher" } },
                  { key: "gen_ai.target.agent", value: { stringValue: "Summarizer" } },
                  { key: "gen_ai.operation.name", value: { stringValue: "chat" } },
                  { key: "gen_ai.usage.total_tokens", value: { intValue: 1250 } },
                  { key: "gen_ai.swarmwatch.entropy_score", value: { doubleValue: 0.91 } },
                ],
              },
            ],
          },
        ],
      },
    ],
  };

  const spans = parseOtlpTracesPayload(samplePayload, "proj-101");
  assert.equal(spans.length, 1);
  const s = spans[0];

  assert.equal(s.id, "00f067aa0ba902b7");
  assert.equal(s.swarm_id, "4bf92f3577b34da6a3ce929d0e0e4736");
  assert.equal(s.source_agent, "Researcher");
  assert.equal(s.target_agent, "Summarizer");
  assert.equal(s.span_type, "llm_completion"); // "chat" normalized to "llm_completion"
  assert.equal(s.token_count, 1250);
  assert.equal(s.entropy_score, 0.91);
  assert.equal(s.project_id, "proj-101");
  assert.equal(Math.round(s.duration_ms), 850);

});

test("OTLP - OtlpTraceExporter queues and flushes spans", async () => {
  const exporter = new OtlpTraceExporter({
    endpoint: "http://localhost:8765/v1/traces",
    batchSize: 2,
  });

  const span1 = {
    id: "s1",
    swarm_id: "sw1",
    source_agent: "A",
    target_agent: "B",
    span_type: "agent_call",
    token_count: 10,
    entropy_score: 0.8,
  };

  exporter.exportSpan(span1);
  const res = await exporter.flush();
  assert.equal(typeof res, "boolean");
});
