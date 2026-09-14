/**
 * SwarmWatch LangChain.js Callback Handler.
 * Zero-dependency duck-typed callback handler for LangChain.js LLM, Tool, and Agent chains.
 * Captures execution spans and applies Shannon entropy circuit breaking without extra token usage.
 */

import { getClient } from "../client.ts";
import type { SwarmClient } from "../client.ts";
import type { SwarmSpan } from "../schema.ts";

export interface LangChainHandlerOptions {
  agentName?: string;
  swarmClient?: SwarmClient;
  autoBreak?: boolean;
}

export class SwarmWatchLangChainCallbackHandler {
  public name = "SwarmWatchLangChainCallbackHandler";
  private agentName: string;
  private client: SwarmClient;
  private activeRuns: Map<string, SwarmSpan> = new Map();

  constructor(options: LangChainHandlerOptions = {}) {
    this.agentName = options.agentName || "langchain-agent";
    this.client = options.swarmClient || getClient();
  }

  public handleLLMStart(
    llm: { name?: string },
    prompts: string[],
    runId: string,
    parentRunId?: string
  ): void {
    const span: SwarmSpan = {
      id: runId,
      swarm_id: this.client.swarmId,
      branch_id: "main",
      parent_span_id: parentRunId || null,
      source_agent: this.agentName,
      target_agent: llm.name || "llm",
      span_type: "llm_call",
      input_payload: { prompts: prompts.map((p) => p.slice(0, 1000)) },
      output_payload: {},
      token_count: 0,
      entropy_score: 1.0,
      start_time: new Date().toISOString(),
      end_time: new Date().toISOString(),
      duration_ms: 0.0,
    };
    this.activeRuns.set(runId, span);
  }

  public handleLLMEnd(
    output: { generations?: Array<Array<{ text?: string }>>; llmOutput?: any },
    runId: string
  ): void {
    const span = this.activeRuns.get(runId);
    if (!span) return;
    this.activeRuns.delete(runId);

    span.end_time = new Date().toISOString();
    const texts: string[] = [];
    if (output.generations) {
      for (const genList of output.generations) {
        for (const gen of genList) {
          if (gen.text) texts.push(gen.text);
        }
      }
    }
    const combinedText = texts.join("\n");
    span.output_payload = {
      generations: texts.slice(0, 5),
      token_usage: output.llmOutput?.tokenUsage,
    };

    const tokenCount = output.llmOutput?.tokenUsage?.totalTokens || combinedText.split(/\s+/).length;
    span.token_count = tokenCount;

    if (combinedText) {
      this.client.evaluateOutput(combinedText, this.agentName, tokenCount);
    }

    this.client.transport.emitSpan(span);
  }

  public handleToolStart(
    tool: { name?: string },
    input: string | Record<string, any>,
    runId: string,
    parentRunId?: string
  ): void {
    const span: SwarmSpan = {
      id: runId,
      swarm_id: this.client.swarmId,
      branch_id: "main",
      parent_span_id: parentRunId || null,
      source_agent: this.agentName,
      target_agent: `tool:${tool.name || "tool"}`,
      span_type: "tool_call",
      input_payload: typeof input === "object" ? input : { input: String(input).slice(0, 1000) },
      output_payload: {},
      token_count: 0,
      entropy_score: 1.0,
      start_time: new Date().toISOString(),
      end_time: new Date().toISOString(),
      duration_ms: 0.0,
    };
    this.activeRuns.set(runId, span);
  }

  public handleToolEnd(output: string | Record<string, any>, runId: string): void {
    const span = this.activeRuns.get(runId);
    if (!span) return;
    this.activeRuns.delete(runId);

    span.end_time = new Date().toISOString();
    span.output_payload = typeof output === "object" ? output : { result: String(output).slice(0, 2000) };
    this.client.transport.emitSpan(span);
  }

  public handleToolError(err: any, runId: string): void {
    const span = this.activeRuns.get(runId);
    if (!span) return;
    this.activeRuns.delete(runId);

    span.end_time = new Date().toISOString();
    span.output_payload = { error: err?.message || String(err) };
    this.client.transport.emitSpan(span);
  }

  public handleLLMError(err: any, runId: string): void {
    const span = this.activeRuns.get(runId);
    if (!span) return;
    this.activeRuns.delete(runId);

    span.end_time = new Date().toISOString();
    span.output_payload = { error: err?.message || String(err) };
    this.client.transport.emitSpan(span);
  }
}
