/**
 * SwarmWatch TypeScript Schema & OpenTelemetry GenAI Semantic Conventions.
 * Strict parity with SwarmWatch Python SDK Core Entities.
 */

export interface SwarmSpan {
  id: string;
  swarm_id: string;
  branch_id?: string;
  parent_span_id?: string | null;
  source_agent: string;
  target_agent: string;
  span_type?: string;
  input_payload?: Record<string, any>;
  output_payload?: Record<string, any>;
  token_count?: number;
  entropy_score?: number;
  start_time: string;
  end_time: string;
  project_id?: string;
  duration_ms?: number;
}

export interface StateSnapshot {
  id: string;
  span_id: string;
  branch_id: string;
  state_hash: string;
  state_data: Record<string, any>;
  created_at: string;
}

export interface CounterfactualSavings {
  tokens_saved: number;
  dollars_saved: number;
  turns_averted: number;
  budget_consumed: number;
  budget_protected: number;
}

export interface CircuitEvent {
  id: string;
  swarm_id: string;
  span_id?: string | null;
  event_type: "warning" | "trip";
  reason: string;
  metric_val: number;
  counterfactual_savings?: CounterfactualSavings | null;
  created_at: string;
}

/**
 * Maps a SwarmSpan to OpenTelemetry GenAI Semantic Conventions.
 */
export function toOtelGenAiSpan(span: SwarmSpan): Record<string, any> {
  const startNano = Math.floor(new Date(span.start_time).getTime() * 1e6);
  const endNano = Math.floor(new Date(span.end_time).getTime() * 1e6);
  const opName = `${span.source_agent} -> ${span.target_agent}:${span.span_type || "agent_call"}`;

  return {
    name: opName,
    trace_id: span.swarm_id,
    span_id: span.id,
    parent_span_id: span.parent_span_id || null,
    start_time_unix_nano: startNano,
    end_time_unix_nano: endNano,
    attributes: {
      "gen_ai.system": "swarmwatch",
      "gen_ai.agent.name": span.source_agent,
      "gen_ai.target.agent": span.target_agent,
      "gen_ai.operation.name": opName,
      "gen_ai.usage.total_tokens": span.token_count || 0,
      "gen_ai.swarmwatch.entropy_score": span.entropy_score ?? 1.0,
      "gen_ai.swarmwatch.branch_id": span.branch_id || "main",
    },
  };
}
