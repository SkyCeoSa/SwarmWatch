/**
 * SwarmWatch Vercel AI SDK Adapter.
 * Integrates zero-overhead telemetry and Shannon entropy loop prevention
 * with Vercel AI SDK (`ai` package: generateText, streamText, etc.).
 */

import { getClient } from "../client.ts";
import type { SwarmClient } from "../client.ts";
import type { CircuitDecision } from "../circuit.ts";

export interface VercelOnFinishEvent {
  text?: string;
  usage?: {
    promptTokens?: number;
    completionTokens?: number;
    totalTokens?: number;
  };
  finishReason?: string;
  response?: {
    id?: string;
    modelId?: string;
    timestamp?: Date;
  };
  warnings?: any[];
}

export interface VercelAdapterOptions {
  agentName?: string;
  swarmClient?: SwarmClient;
  autoBreak?: boolean;
}

/**
 * Creates an onFinish callback compatible with Vercel AI SDK streamText / generateText.
 */
export function swarmWatchVercelOnFinish(options: VercelAdapterOptions = {}) {
  const agent = options.agentName || "vercel-agent";
  const client = options.swarmClient || getClient();
  const autoBreak = options.autoBreak ?? true;

  return async (event: VercelOnFinishEvent): Promise<CircuitDecision | undefined> => {
    const text = event.text || "";
    const totalTokens = event.usage?.totalTokens || (event.usage?.promptTokens || 0) + (event.usage?.completionTokens || 0);

    return client.withSpan(
      {
        name: `vercel:${agent}`,
        source: agent,
        target: event.response?.modelId || "llm",
        span_type: "llm_completion",
        input_payload: { finishReason: event.finishReason },
      },
      (span) => {
        span.token_count = totalTokens;
        span.output_payload = {
          text: text.slice(0, 2000),
          usage: event.usage,
          modelId: event.response?.modelId,
        };

        if (text) {
          const decision = client.evaluateOutput(text, agent, totalTokens);
          return decision;
        }
        return undefined;
      }
    );
  };
}
