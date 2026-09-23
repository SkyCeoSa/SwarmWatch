/**
 * SwarmWatch Kernel-Level Network Socket LLM Interception Gateway (Seam 29 / ADR 0040).
 *
 * TypeScript SDK parity implementation with Node 22 type-stripping compliance.
 * Provides transparent TCP stream inspection, Shannon entropy calculation,
 * and physical socket disconnection (TCP RST emulation) without external packages.
 */

import * as crypto from 'node:crypto';
import * as net from 'node:net';

export interface SocketRule {
  id: string;
  name: string;
  ports: number[];
  domains: string[];
  action: 'inspect' | 'allow' | 'terminate_rst';
  maxPayloadBytes?: number;
  maxTokensPerStream?: number;
  minShannonEntropy?: number;
  createdAt?: string;
}

export interface SocketIntervention {
  connectionId: string;
  srcAddr: string;
  dstAddr: string;
  dstPort: number;
  reason: string;
  action: 'TCP_RST';
  tokensSaved: number;
  costSaved: number;
  enactedAt: string;
}

export function calculateShannonEntropy(text: string): number {
  if (!text || text.length <= 1) return 1.0;
  const buf = Buffer.from(text, 'utf-8');
  const len = buf.length;
  if (len <= 1) return 1.0;

  const freqs: Record<number, number> = {};
  for (let i = 0; i < len; i++) {
    const b = buf[i];
    freqs[b] = (freqs[b] || 0) + 1;
  }

  let entropy = 0.0;
  for (const count of Object.values(freqs)) {
    const p = count / len;
    entropy -= p * Math.log2(p);
  }

  // Normalize by 8.0 (log2 of 256)
  return Math.max(0.0, Math.min(1.0, entropy / 8.0));
}

export class SocketStreamDecoder {
  buffer: Buffer = Buffer.alloc(0);
  headersParsed: boolean = false;
  headers: Record<string, string> = {};
  method: string | null = null;
  path: string | null = null;
  statusCode: number | null = null;
  extractedText: string = '';
  streamedTokens: string[] = [];
  estimatedTokenCount: number = 0;
  entropy: number = 1.0;
  jsonPayload: Record<string, unknown> | null = null;

  feed(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);

    if (!this.headersParsed) {
      const headerEnd = this.buffer.indexOf('\r\n\r\n');
      if (headerEnd !== -1) {
        const headerStr = this.buffer.subarray(0, headerEnd).toString('utf-8');
        this.parseHeaders(headerStr);
        this.headersParsed = true;
        this.buffer = this.buffer.subarray(headerEnd + 4);
      }
    }

    if (this.headersParsed) {
      const contentType = (this.headers['content-type'] || '').toLowerCase();
      if (contentType.includes('text/event-stream')) {
        this.parseSseStream();
      } else {
        this.parseJsonOrRawBody();
      }
    }
  }

  private parseHeaders(headerStr: string): void {
    const lines = headerStr.split('\r\n');
    if (lines.length > 0) {
      const parts = lines[0].split(' ');
      if (parts.length >= 2) {
        if (parts[0].startsWith('HTTP/')) {
          this.statusCode = parseInt(parts[1], 10) || 200;
        } else {
          this.method = parts[0];
          this.path = parts[1];
        }
      }
    }

    for (let i = 1; i < lines.length; i++) {
      const idx = lines[i].indexOf(':');
      if (idx !== -1) {
        const key = lines[i].substring(0, idx).trim().toLowerCase();
        const val = lines[i].substring(idx + 1).trim();
        this.headers[key] = val;
      }
    }
  }

  private parseJsonOrRawBody(): void {
    const text = this.buffer.toString('utf-8');
    try {
      const parsed = JSON.parse(text);
      this.jsonPayload = parsed;
      const extracted: string[] = [];
      if (parsed && typeof parsed === 'object') {
        if (Array.isArray(parsed.messages)) {
          for (const m of parsed.messages) {
            if (m && typeof m.content === 'string') {
              extracted.push(m.content);
            }
          }
        } else if (typeof parsed.prompt === 'string') {
          extracted.push(parsed.prompt);
        } else if (typeof parsed.input === 'string') {
          extracted.push(parsed.input);
        }
      }
      const combined = extracted.length > 0 ? extracted.join(' ') : text;
      this.extractedText = combined;
      this.estimatedTokenCount = Math.max(1, Math.floor(combined.length / 4));
      this.entropy = calculateShannonEntropy(combined);
    } catch {
      this.extractedText = text;
      this.estimatedTokenCount = Math.max(1, Math.floor(text.length / 4));
      this.entropy = calculateShannonEntropy(text);
    }
  }

  private parseSseStream(): void {
    const text = this.buffer.toString('utf-8');
    const lines = text.split('\n');
    const tokens: string[] = [];

    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed.startsWith('data:')) {
        const payload = trimmed.substring(5).trim();
        if (payload === '[DONE]') continue;
        try {
          const parsed = JSON.parse(payload);
          if (parsed && Array.isArray(parsed.choices) && parsed.choices.length > 0) {
            const content = parsed.choices[0]?.delta?.content;
            if (typeof content === 'string') {
              tokens.push(content);
            }
          }
        } catch {
          // ignore incomplete JSON chunks
        }
      }
    }

    if (tokens.length > 0) {
      this.streamedTokens = tokens;
      const combined = tokens.join('');
      this.extractedText = combined;
      this.estimatedTokenCount = Math.max(tokens.length, Math.floor(combined.length / 4));
      this.entropy = calculateShannonEntropy(combined);
    }
  }
}

export class EbpfSocketGateway {
  private rules: Map<string, SocketRule> = new Map();
  private interventions: SocketIntervention[] = [];
  private totalPackets: number = 0;
  private totalBytes: number = 0;
  private activeSockets: number = 0;
  private attached: boolean = true;

  constructor() {
    this.addRule({
      id: 'rule-default-ts',
      name: 'Default TypeScript LLM Inspection',
      ports: [80, 443, 11434, 8000, 8080],
      domains: ['*openai.com', '*anthropic.com', 'localhost', '127.0.0.1'],
      action: 'inspect',
      maxTokensPerStream: 4096,
      minShannonEntropy: 0.35,
    });
  }

  isAttached(): boolean {
    return this.attached;
  }

  setAttached(attached: boolean): void {
    this.attached = attached;
  }

  addRule(rule: SocketRule): SocketRule {
    this.rules.set(rule.id, rule);
    return rule;
  }

  getRule(id: string): SocketRule | undefined {
    return this.rules.get(id);
  }

  listRules(): SocketRule[] {
    return Array.from(this.rules.values());
  }

  deleteRule(id: string): boolean {
    return this.rules.delete(id);
  }

  getInterventions(): SocketIntervention[] {
    return [...this.interventions];
  }

  inspectStreamData(
    data: Buffer,
    host: string = 'api.openai.com',
    port: number = 443,
    srcAddr: string = '127.0.0.1',
    connectionId?: string
  ): { terminated: boolean; intervention: SocketIntervention | null; decoder: SocketStreamDecoder } {
    const connId = connectionId || `conn-${crypto.randomBytes(4).toString('hex')}`;
    this.totalPackets++;
    this.totalBytes += data.length;

    const decoder = new SocketStreamDecoder();
    decoder.feed(data);

    if (!this.attached) {
      return { terminated: false, intervention: null, decoder };
    }

    const hostLower = host.toLowerCase();
    const matchingRules = Array.from(this.rules.values()).filter((r) => {
      if (r.ports.length > 0 && !r.ports.includes(port)) return false;
      if (r.domains.length === 0) return true;
      return r.domains.some((d) => {
        const dLower = d.toLowerCase();
        if (dLower === '*' || dLower === hostLower) return true;
        if (dLower.startsWith('*')) {
          const suffix = dLower.replace(/^\*+\.?/, '');
          return hostLower === suffix || hostLower.endsWith('.' + suffix) || hostLower.endsWith(suffix);
        }
        return hostLower === dLower;
      });
    });

    for (const rule of matchingRules) {
      if (rule.action === 'allow') continue;

      if (rule.action === 'terminate_rst') {
        const intervention: SocketIntervention = {
          connectionId: connId,
          srcAddr,
          dstAddr: host,
          dstPort: port,
          reason: `Interception rule '${rule.name}' enforces unconditional TCP RST`,
          action: 'TCP_RST',
          tokensSaved: decoder.estimatedTokenCount,
          costSaved: decoder.estimatedTokenCount * 0.00002,
          enactedAt: new Date().toISOString(),
        };
        this.interventions.push(intervention);
        return { terminated: true, intervention, decoder };
      }

      // Check entropy collapse
      const minEntropy = rule.minShannonEntropy ?? 0.35;
      if (decoder.extractedText.length >= 30 && decoder.entropy < minEntropy) {
        const intervention: SocketIntervention = {
          connectionId: connId,
          srcAddr,
          dstAddr: host,
          dstPort: port,
          reason: `Shannon entropy collapse (${decoder.entropy.toFixed(4)} < threshold ${minEntropy})`,
          action: 'TCP_RST',
          tokensSaved: decoder.estimatedTokenCount,
          costSaved: decoder.estimatedTokenCount * 0.00002,
          enactedAt: new Date().toISOString(),
        };
        this.interventions.push(intervention);
        return { terminated: true, intervention, decoder };
      }

      // Check token ceiling
      const maxTokens = rule.maxTokensPerStream ?? 4096;
      if (decoder.estimatedTokenCount > maxTokens) {
        const intervention: SocketIntervention = {
          connectionId: connId,
          srcAddr,
          dstAddr: host,
          dstPort: port,
          reason: `Token stream quota exceeded (${decoder.estimatedTokenCount} > ${maxTokens})`,
          action: 'TCP_RST',
          tokensSaved: decoder.estimatedTokenCount,
          costSaved: decoder.estimatedTokenCount * 0.00002,
          enactedAt: new Date().toISOString(),
        };
        this.interventions.push(intervention);
        return { terminated: true, intervention, decoder };
      }
    }

    return { terminated: false, intervention: null, decoder };
  }

  status(): Record<string, unknown> {
    return {
      status: this.attached ? 'attached' : 'detached',
      active_sockets: this.activeSockets,
      packets_intercepted: this.totalPackets,
      bytes_intercepted: this.totalBytes,
      interventions_enacted: this.interventions.length,
      rules_count: this.rules.size,
      mode: 'ebpf_kernel_socket_gateway',
      version: '0.1.0',
    };
  }
}

export class MockEbpfEngine {
  gateway: EbpfSocketGateway;

  constructor(gateway?: EbpfSocketGateway) {
    this.gateway = gateway || new EbpfSocketGateway();
  }

  simulateHttpRequest(
    method: string = 'POST',
    path: string = '/v1/chat/completions',
    host: string = 'api.openai.com',
    port: number = 443,
    body: Record<string, unknown> | string = {}
  ): { terminated: boolean; intervention: SocketIntervention | null; decoder: SocketStreamDecoder } {
    const bodyStr = typeof body === 'string' ? body : JSON.stringify(body);
    const bodyBuf = Buffer.from(bodyStr, 'utf-8');
    const headerStr = `${method} ${path} HTTP/1.1\r\nHost: ${host}\r\nContent-Type: application/json\r\nContent-Length: ${bodyBuf.length}\r\n\r\n`;
    const payload = Buffer.concat([Buffer.from(headerStr, 'utf-8'), bodyBuf]);
    return this.gateway.inspectStreamData(payload, host, port);
  }

  simulateSseStream(
    tokens: string[],
    host: string = 'api.openai.com',
    port: number = 443
  ): { terminated: boolean; intervention: SocketIntervention | null; decoder: SocketStreamDecoder } {
    const lines = [
      'HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n',
    ];
    for (const tok of tokens) {
      lines.push(`data: ${JSON.stringify({ choices: [{ delta: { content: tok } }] })}\n\n`);
    }
    lines.push('data: [DONE]\n\n');
    const payload = Buffer.from(lines.join(''), 'utf-8');
    return this.gateway.inspectStreamData(payload, host, port);
  }
}
