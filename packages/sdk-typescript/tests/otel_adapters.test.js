/**
 * Tests for OpenTelemetry GenAI Semantic Conventions Mapping,
 * Vercel AI SDK Adapter, and LangChain.js Callback Handler.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  toOtelGenAiSpan,
  init,
  InMemoryStorageSink,
  swarmWatchVercelOnFinish,
  SwarmWatchLangChainCallbackHandler,
} from "../src/index.ts";

test("OpenTelemetry GenAI Semantic Conventions Mapping", async (t) => {
  await t.test("maps SwarmSpan to standard OTel GenAI span format", () => {
    const span = {
      id: "span-123",
      swarm_id: "trace-456",
      branch_id: "feature-branch",
      parent_span_id: "parent-789",
      source_agent: "coder",
      target_agent: "reviewer",
      span_type: "code_review",
      input_payload: { diff: "+ code" },
      output_payload: { status: "approved" },
      token_count: 350,
      entropy_score: 0.88,
      start_time: "2026-09-14T10:00:00.000Z",
      end_time: "2026-09-14T10:00:02.500Z",
      duration_ms: 2500,
    };

    const otelSpan = toOtelGenAiSpan(span);

    assert.strictEqual(otelSpan.trace_id, "trace-456");
    assert.strictEqual(otelSpan.span_id, "span-123");
    assert.strictEqual(otelSpan.parent_span_id, "parent-789");
    assert.strictEqual(otelSpan.name, "coder -> reviewer:code_review");

    // Attributes verification
    const attrs = otelSpan.attributes;
    assert.strictEqual(attrs["gen_ai.system"], "swarmwatch");
    assert.strictEqual(attrs["gen_ai.agent.name"], "coder");
    assert.strictEqual(attrs["gen_ai.target.agent"], "reviewer");
    assert.strictEqual(attrs["gen_ai.usage.total_tokens"], 350);
    assert.strictEqual(attrs["gen_ai.swarmwatch.entropy_score"], 0.88);
    assert.strictEqual(attrs["gen_ai.swarmwatch.branch_id"], "feature-branch");
  });
});

test("Vercel AI SDK Adapter", async (t) => {
  await t.test("onFinish callback captures completion and evaluates entropy", async () => {
    const sink = new InMemoryStorageSink();
    const client = init({ swarm_id: "vercel-swarm", sink });

    const onFinish = swarmWatchVercelOnFinish({
      agentName: "writer-agent",
      swarmClient: client,
    });

    const finishEvent = {
      text: "The quick brown fox jumps over the lazy dog and explores the meadow.",
      finishReason: "stop",
      usage: { promptTokens: 50, completionTokens: 80, totalTokens: 130 },
      response: { id: "resp-1", modelId: "gpt-4o" },
    };

    const decision = await onFinish(finishEvent);
    assert.ok(decision);
    assert.strictEqual(decision.decision, "pass");

    await client.flush();
    assert.strictEqual(sink.spans.length, 1);
    const span = sink.spans[0];
    assert.strictEqual(span.source_agent, "writer-agent");
    assert.strictEqual(span.token_count, 130);
    assert.strictEqual(span.target_agent, "gpt-4o");

    await client.shutdown();
  });
});

test("LangChain.js Callback Handler", async (t) => {
  await t.test("traces LLM execution lifecycle and token metrics", async () => {
    const sink = new InMemoryStorageSink();
    const client = init({ swarm_id: "langchain-swarm", sink });

    const handler = new SwarmWatchLangChainCallbackHandler({
      agentName: "qa-agent",
      swarmClient: client,
    });

    const runId = "llm-run-1";
    handler.handleLLMStart({ name: "chat-gpt4" }, ["What is the capital of France?"], runId);
    handler.handleLLMEnd(
      {
        generations: [[{ text: "The capital of France is Paris." }]],
        llmOutput: { tokenUsage: { totalTokens: 28 } },
      },
      runId
    );

    await client.flush();
    assert.strictEqual(sink.spans.length, 1);
    const span = sink.spans[0];
    assert.strictEqual(span.id, runId);
    assert.strictEqual(span.source_agent, "qa-agent");
    assert.strictEqual(span.target_agent, "chat-gpt4");
    assert.strictEqual(span.token_count, 28);

    await client.shutdown();
  });

  await t.test("traces tool execution lifecycle", async () => {
    const sink = new InMemoryStorageSink();
    const client = init({ swarm_id: "langchain-tool-swarm", sink });

    const handler = new SwarmWatchLangChainCallbackHandler({
      agentName: "tool-caller",
      swarmClient: client,
    });

    const runId = "tool-run-1";
    handler.handleToolStart({ name: "calculator" }, { expression: "2 + 2" }, runId);
    handler.handleToolEnd("4", runId);

    await client.flush();
    assert.strictEqual(sink.spans.length, 1);
    const span = sink.spans[0];
    assert.strictEqual(span.id, runId);
    assert.strictEqual(span.target_agent, "tool:calculator");
    assert.strictEqual(span.output_payload.result, "4");

    await client.shutdown();
  });
});
