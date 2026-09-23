/**
 * SwarmWatch TypeScript OpenTelemetry OTLP Exporter, Parser & GenAI Semantic Convention Translator.
 * Fully compatible with Node 22 type-stripping rules (no enums, clean object literals).
 */

import type { SwarmSpan } from "./schema.ts";

export interface OtelAttributeValue {
  stringValue?: string;
  intValue?: number | string;
  doubleValue?: number;
  boolValue?: boolean;
  bytesValue?: string;
  arrayValue?: { values?: OtelAttributeValue[] };
  kvlistValue?: { values?: Array<{ key: string; value: OtelAttributeValue }> };
}

export interface OtelKeyValue {
  key: string;
  value: OtelAttributeValue;
}

export interface OtelSpanEvent {
  name: string;
  timeUnixNano?: number | string;
  attributes?: OtelKeyValue[] | Record<string, any>;
}

export interface OtelSpan {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  kind?: number;
  startTimeUnixNano: number | string;
  endTimeUnixNano: number | string;
  attributes: OtelKeyValue[] | Record<string, any>;
  events?: OtelSpanEvent[];
  status?: { code: number; message?: string };
}

export interface OtelScopeSpans {
  scope?: { name: string; version?: string };
  spans: OtelSpan[];
}

export interface OtelResourceSpans {
  resource?: { attributes: OtelKeyValue[] | Record<string, any> };
  scopeSpans: OtelScopeSpans[];
}

export interface OtlpExportTraceRequest {
  resourceSpans: OtelResourceSpans[];
}

/**
 * Extracts a normalized value from an OTLP proto3 AnyValue object.
 */
export function extractOtlpValue(valObj: any): any {
  if (valObj === null || valObj === undefined) return valObj;
  if (typeof valObj !== "object") return valObj;

  if ("stringValue" in valObj) return valObj.stringValue;
  if ("intValue" in valObj) return Number(valObj.intValue);
  if ("doubleValue" in valObj) return Number(valObj.doubleValue);
  if ("boolValue" in valObj) return Boolean(valObj.boolValue);
  if ("bytesValue" in valObj) return String(valObj.bytesValue);
  if ("arrayValue" in valObj && Array.isArray(valObj.arrayValue?.values)) {
    return valObj.arrayValue.values.map(extractOtlpValue);
  }
  if ("kvlistValue" in valObj && Array.isArray(valObj.kvlistValue?.values)) {
    return extractOtlpAttributes(valObj.kvlistValue.values);
  }
  return valObj;
}

/**
 * Normalizes OTLP attributes from either a proto3 key-value array or flat object.
 */
export function extractOtlpAttributes(attrs: any): Record<string, any> {
  if (!attrs) return {};
  if (typeof attrs === "object" && !Array.isArray(attrs)) {
    const res: Record<string, any> = {};
    for (const [k, v] of Object.entries(attrs)) {
      res[k] = extractOtlpValue(v);
    }
    return res;
  }

  const res: Record<string, any> = {};
  if (Array.isArray(attrs)) {
    for (const item of attrs) {
      if (item && typeof item === "object" && item.key) {
        res[String(item.key)] = extractOtlpValue(item.value);
      }
    }
  }
  return res;
}

/**
 * Maps a native SwarmSpan to standard OpenTelemetry GenAI Semantic Conventions.
 */
export function toOtelGenAiSpan(span: SwarmSpan): OtelSpan {
  const startNano = span.start_time ? Date.parse(span.start_time) * 1e6 : Date.now() * 1e6;
  const endNano = span.end_time ? Date.parse(span.end_time) * 1e6 : startNano + (span.duration_ms || 0) * 1e6;
  const opName = `${span.source_agent} -> ${span.target_agent}:${span.span_type}`;

  const kvAttrs: OtelKeyValue[] = [
    { key: "gen_ai.system", value: { stringValue: "swarmwatch" } },
    { key: "gen_ai.agent.name", value: { stringValue: span.source_agent } },
    { key: "gen_ai.target.agent", value: { stringValue: span.target_agent } },
    { key: "gen_ai.operation.name", value: { stringValue: opName } },
    { key: "gen_ai.usage.total_tokens", value: { intValue: span.token_count } },
    { key: "gen_ai.swarmwatch.entropy_score", value: { doubleValue: span.entropy_score } },
    { key: "gen_ai.swarmwatch.branch_id", value: { stringValue: span.branch_id || "main" } },
  ];

  if (span.cost_estimate !== undefined) {
    kvAttrs.push({ key: "gen_ai.usage.cost", value: { doubleValue: span.cost_estimate } });
  }

  return {
    traceId: span.swarm_id,
    spanId: span.id,
    parentSpanId: span.parent_span_id || "",
    name: opName,
    kind: 1, // INTERNAL
    startTimeUnixNano: startNano,
    endTimeUnixNano: endNano,
    attributes: kvAttrs,
    status: { code: 1 },
  };
}

/**
 * Formats a batch of SwarmSpans into an OTLP/HTTP JSON trace payload.
 */
export function formatOtlpTracesPayload(
  spans: SwarmSpan[],
  serviceName: string = "swarmwatch"
): OtlpExportTraceRequest {
  const otelSpans = spans.map(toOtelGenAiSpan);

  return {
    resourceSpans: [
      {
        resource: {
          attributes: [
            { key: "service.name", value: { stringValue: serviceName } },
            { key: "telemetry.sdk.name", value: { stringValue: "swarmwatch-sdk" } },
            { key: "telemetry.sdk.language", value: { stringValue: "typescript" } },
          ],
        },
        scopeSpans: [
          {
            scope: {
              name: "@swarmwatch/sdk.otlp",
              version: "0.1.0",
            },
            spans: otelSpans,
          },
        ],
      },
    ],
  };
}

/**
 * Parses an OTLP trace JSON payload into SwarmSpan items.
 */
export function parseOtlpTracesPayload(
  payload: any,
  defaultProjectId: string = "default-project"
): SwarmSpan[] {
  if (!payload || typeof payload !== "object") return [];
  const resourceSpans = payload.resourceSpans || payload.resource_spans || [];
  const spans: SwarmSpan[] = [];

  for (const rs of resourceSpans) {
    const resAttrs = extractOtlpAttributes(rs.resource?.attributes);
    const scopeSpans = rs.scopeSpans || rs.scope_spans || [];

    for (const ss of scopeSpans) {
      const rawSpans = ss.spans || [];
      for (const s of rawSpans) {
        const sAttrs = extractOtlpAttributes(s.attributes);
        const traceId = String(s.traceId || s.trace_id || "swarm-otlp-default");
        const spanId = String(s.spanId || s.span_id || "span-otlp-0");
        const parentSpanId = s.parentSpanId || s.parent_span_id || undefined;
        const name = String(s.name || "agent_execution");

        let source = sAttrs["gen_ai.agent.name"] || sAttrs["gen_ai.agent.id"] || resAttrs["service.name"];
        if (!source) {
          source = name.includes(" -> ") ? name.split(" -> ")[0].trim() : "external_agent";
        }

        let target = sAttrs["gen_ai.target.agent"] || sAttrs["gen_ai.request.model"] || sAttrs["gen_ai.response.model"];
        if (!target) {
          if (name.includes(" -> ")) {
            const part = name.split(" -> ")[1];
            target = part.includes(":") ? part.split(":")[0].trim() : part.trim();
          } else {
            target = "agent_target";
          }
        }

        let spanType = sAttrs["gen_ai.operation.name"] || sAttrs["gen_ai.span_type"] || "agent_call";
        if (name.includes(":") && !sAttrs["gen_ai.operation.name"]) {
          spanType = name.split(":").pop()?.trim() || "agent_call";
        }
        const opLower = spanType.toLowerCase();
        if (opLower === "chat" || opLower === "completion" || opLower === "generate_content") {
          spanType = "llm_completion";
        } else if (opLower === "tool" || opLower === "execute_tool" || opLower === "function_call") {
          spanType = "tool_call";
        } else if (opLower === "embeddings" || opLower === "embedding") {
          spanType = "embedding";
        }


        const tokenCount = Number(
          sAttrs["gen_ai.usage.total_tokens"] ||
          (Number(sAttrs["gen_ai.usage.input_tokens"] || 0) + Number(sAttrs["gen_ai.usage.output_tokens"] || 0)) ||
          0
        );

        const cost = sAttrs["gen_ai.usage.cost"] !== undefined
          ? Number(sAttrs["gen_ai.usage.cost"])
          : Number((tokenCount * 0.00002).toFixed(6));

        const rawStart = s.startTimeUnixNano ?? s.start_time_unix_nano;
        const rawEnd = s.endTimeUnixNano ?? s.end_time_unix_nano;

        let startTime = new Date().toISOString();
        let endTime = startTime;
        let durationMs = 0;

        if (rawStart) {
          try {
            const startNanoBig = BigInt(rawStart);
            startTime = new Date(Number(startNanoBig / 1000000n)).toISOString();
            if (rawEnd) {
              const endNanoBig = BigInt(rawEnd);
              endTime = new Date(Number(endNanoBig / 1000000n)).toISOString();
              durationMs = Number(endNanoBig - startNanoBig) / 1e6;
              if (durationMs < 0) durationMs = 0;
            } else {
              endTime = startTime;
            }
          } catch {
            const startNum = Number(rawStart);
            const endNum = Number(rawEnd || rawStart);
            startTime = new Date(startNum / 1e6).toISOString();
            endTime = new Date(endNum / 1e6).toISOString();
            durationMs = Math.max(0, (endNum - startNum) / 1e6);
          }
        }


        const entropy = sAttrs["gen_ai.swarmwatch.entropy_score"] !== undefined
          ? Number(sAttrs["gen_ai.swarmwatch.entropy_score"])
          : 0.85;

        spans.push({
          id: spanId,
          swarm_id: traceId,
          branch_id: sAttrs["gen_ai.swarmwatch.branch_id"] || "main",
          parent_span_id: parentSpanId && parentSpanId !== "0" ? parentSpanId : undefined,
          source_agent: String(source),
          target_agent: String(target),
          span_type: String(spanType),
          input_payload: { prompt: sAttrs["gen_ai.prompt"] || "" },
          output_payload: { completion: sAttrs["gen_ai.completion"] || "", status: "ok" },
          token_count: tokenCount,
          entropy_score: entropy,
          start_time: startTime,
          end_time: endTime,
          duration_ms: durationMs,
          cost_estimate: cost,
          project_id: sAttrs["swarmwatch.project_id"] || defaultProjectId,
        });
      }
    }
  }

  return spans;
}

/**
 * In-process asynchronous, non-blocking OpenTelemetry trace exporter.
 */
export class OtlpTraceExporter {
  public endpoint: string;
  public serviceName: string;
  public batchSize: number;
  public headers: Record<string, string>;
  private queue: SwarmSpan[] = [];
  private timer: any = null;

  constructor(options: {
    endpoint?: string;
    serviceName?: string;
    batchSize?: number;
    headers?: Record<string, string>;
  } = {}) {
    this.endpoint = options.endpoint || "http://localhost:8765/v1/traces";
    this.serviceName = options.serviceName || "swarmwatch-node";
    this.batchSize = options.batchSize || 50;
    this.headers = {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    };
  }

  public exportSpan(span: SwarmSpan): void {
    this.queue.push(span);
    if (this.queue.length >= this.batchSize) {
      this.flush();
    }
  }

  public async flush(): Promise<boolean> {
    if (this.queue.length === 0) return true;
    const batch = this.queue.splice(0, this.batchSize);
    const payload = formatOtlpTracesPayload(batch, this.serviceName);

    try {
      if (typeof fetch !== "undefined") {
        const resp = await fetch(this.endpoint, {
          method: "POST",
          headers: this.headers,
          body: JSON.stringify(payload),
        });
        return resp.ok;
      }
      return true;
    } catch {
      return false;
    }
  }
}
