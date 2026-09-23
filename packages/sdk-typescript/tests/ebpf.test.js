import { test, describe } from 'node:test';
import assert from 'node:assert';

import {
  calculateShannonEntropy,
  SocketStreamDecoder,
  EbpfSocketGateway,
  MockEbpfEngine,
} from '../src/ebpf.ts';

describe('Kernel-Level Network Socket LLM Interception Gateway (TypeScript / ADR 0040)', () => {
  test('calculateShannonEntropy accurately distinguishes natural text from repetitive collapse', () => {
    const naturalText = 'The quick brown fox jumps over the lazy dog and explores deep causal graphs.';
    const naturalEntropy = calculateShannonEntropy(naturalText);
    assert.ok(naturalEntropy > 0.45, `Natural text entropy should be > 0.45, got ${naturalEntropy}`);

    const repetitiveText = 'loop loop loop loop loop loop loop loop loop loop loop loop loop loop loop loop loop';
    const repetitiveEntropy = calculateShannonEntropy(repetitiveText);
    assert.ok(repetitiveEntropy < naturalEntropy, 'Repetitive text entropy must be strictly less than natural text');

    const degenerateCollapse = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
    const degenerateEntropy = calculateShannonEntropy(degenerateCollapse);
    assert.ok(degenerateEntropy < 0.20, `Degenerate collapse entropy should be < 0.20, got ${degenerateEntropy}`);
  });

  test('SocketStreamDecoder parses HTTP/1.1 headers and extracts OpenAI messages', () => {
    const decoder = new SocketStreamDecoder();
    const reqStr =
      'POST /v1/chat/completions HTTP/1.1\r\n' +
      'Host: api.openai.com\r\n' +
      'Content-Type: application/json\r\n' +
      'Content-Length: 72\r\n\r\n' +
      '{"model":"gpt-4o","messages":[{"role":"user","content":"Explain entropy"}]}';

    decoder.feed(Buffer.from(reqStr, 'utf-8'));

    assert.strictEqual(decoder.method, 'POST');
    assert.strictEqual(decoder.path, '/v1/chat/completions');
    assert.strictEqual(decoder.headers['host'], 'api.openai.com');
    assert.strictEqual(decoder.extractedText, 'Explain entropy');
    assert.ok(decoder.estimatedTokenCount >= 1);
    assert.ok(decoder.entropy > 0.4);
  });

  test('SocketStreamDecoder parses Server-Sent Events (SSE) streaming token chunks', () => {
    const decoder = new SocketStreamDecoder();
    const sseHeader =
      'HTTP/1.1 200 OK\r\n' +
      'Content-Type: text/event-stream\r\n' +
      'Transfer-Encoding: chunked\r\n\r\n';

    decoder.feed(Buffer.from(sseHeader, 'utf-8'));

    const chunk1 = 'data: {"choices":[{"delta":{"content":"Hello "}}]}\n\n';
    const chunk2 = 'data: {"choices":[{"delta":{"content":"world!"}}]}\n\n';
    const chunkDone = 'data: [DONE]\n\n';

    decoder.feed(Buffer.from(chunk1 + chunk2 + chunkDone, 'utf-8'));

    assert.deepStrictEqual(decoder.streamedTokens, ['Hello ', 'world!']);
    assert.strictEqual(decoder.extractedText, 'Hello world!');
    assert.ok(decoder.estimatedTokenCount >= 2);
  });

  test('MockEbpfEngine forwards clean HTTP requests without TCP RST intervention', () => {
    const engine = new MockEbpfEngine();
    const { terminated, intervention, decoder } = engine.simulateHttpRequest(
      'POST',
      '/v1/chat/completions',
      'api.openai.com',
      443,
      {
        model: 'gpt-4o',
        messages: [{ role: 'user', content: 'What is the speed of light in vacuum?' }],
      }
    );

    assert.strictEqual(terminated, false);
    assert.strictEqual(intervention, null);
    assert.ok(decoder.estimatedTokenCount > 0);
  });

  test('MockEbpfEngine intercepts repetitive degenerate prompt and triggers physical TCP RST', () => {
    const engine = new MockEbpfEngine();
    const degeneratePrompt = 'repeat '.repeat(40); // very low entropy repetitive sequence
    const { terminated, intervention, decoder } = engine.simulateHttpRequest(
      'POST',
      '/v1/chat/completions',
      'api.openai.com',
      443,
      {
        model: 'gpt-4o',
        messages: [{ role: 'user', content: degeneratePrompt }],
      }
    );

    assert.strictEqual(terminated, true);
    assert.ok(intervention !== null);
    assert.strictEqual(intervention.action, 'TCP_RST');
    assert.ok(intervention.reason.includes('Shannon entropy collapse'));
    assert.ok(decoder.entropy < 0.35);
  });

  test('MockEbpfEngine terminates connection when token stream quota is breached', () => {
    const gateway = new EbpfSocketGateway();
    gateway.addRule({
      id: 'rule-strict-tokens',
      name: 'Strict Token Cap Rule',
      ports: [443],
      domains: ['api.openai.com'],
      action: 'inspect',
      maxTokensPerStream: 5,
    });

    const engine = new MockEbpfEngine(gateway);
    const largePrompt = 'The quick brown fox jumps over the lazy dog and wanders through the forest looking for food.';
    const { terminated, intervention } = engine.simulateHttpRequest(
      'POST',
      '/v1/chat/completions',
      'api.openai.com',
      443,
      {
        model: 'gpt-4o',
        messages: [{ role: 'user', content: largePrompt }],
      }
    );

    assert.strictEqual(terminated, true);
    assert.ok(intervention !== null);
    assert.ok(intervention.reason.includes('Token stream quota exceeded'));
  });

  test('MockEbpfEngine executes unconditional TCP RST rule', () => {
    const gateway = new EbpfSocketGateway();
    gateway.addRule({
      id: 'rule-block-untrusted',
      name: 'Block Untrusted Host',
      ports: [8080],
      domains: ['malicious-agent.internal'],
      action: 'terminate_rst',
    });

    const engine = new MockEbpfEngine(gateway);
    const { terminated, intervention } = engine.simulateHttpRequest(
      'POST',
      '/v1/call',
      'malicious-agent.internal',
      8080,
      { cmd: 'run' }
    );

    assert.strictEqual(terminated, true);
    assert.ok(intervention !== null);
    assert.strictEqual(intervention.action, 'TCP_RST');
    assert.ok(intervention.reason.includes('enforces unconditional TCP RST'));
  });

  test('EbpfSocketGateway rule lifecycle and diagnostic status', () => {
    const gateway = new EbpfSocketGateway();
    const initialCount = gateway.listRules().length;

    const newRule = {
      id: 'rule-test-01',
      name: 'Test Temporary Rule',
      ports: [11434],
      domains: ['localhost'],
      action: 'inspect',
    };
    gateway.addRule(newRule);
    assert.strictEqual(gateway.listRules().length, initialCount + 1);
    assert.strictEqual(gateway.getRule('rule-test-01')?.name, 'Test Temporary Rule');

    const deleted = gateway.deleteRule('rule-test-01');
    assert.strictEqual(deleted, true);
    assert.strictEqual(gateway.listRules().length, initialCount);

    const st = gateway.status();
    assert.strictEqual(st.status, 'attached');
    assert.strictEqual(st.mode, 'ebpf_kernel_socket_gateway');
  });
});
