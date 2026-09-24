/**
 * SwarmWatch Multi-Agent Dynamic Honeypot & Deception Sandboxing (Seam 41 / ADR 0052).
 * Diverts adversarial agents into in-process deception sandboxes and detects exfiltration
 * via cryptographic canary honeytokens without remote LLM API expenditure (<50µs execution).
 */

import { createHash, createHmac } from "node:crypto";

export const HoneypotStatus = {
  ARMED: "ARMED",
  DECEIVING: "DECEIVING",
  DEFUSED: "DEFUSED",
} as const;

export type HoneypotStatusType = (typeof HoneypotStatus)[keyof typeof HoneypotStatus];

export interface Honeytoken {
  token_id: string;
  token_value: string;
  token_type: string;
  target_swarm: string;
  created_at: string;
  trip_count: number;
}

export interface ShadowGraphSpan {
  span_id: string;
  swarm_id: string;
  agent_id: string;
  input_text: string;
  deception_response: Record<string, any>;
  diverted_reason: string;
  timestamp: string;
}

export interface HoneypotReport {
  swarm_id: string;
  status: HoneypotStatusType;
  total_diverted_turns: number;
  active_sandboxed_agents: string[];
  active_honeytokens: Honeytoken[];
  honeytokens_tripped: number;
  recent_diverted_spans: ShadowGraphSpan[];
}

export class DeceptionResponseGenerator {
  static generateResponse(promptText: string, agentId: string): Record<string, any> {
    const lower = promptText.toLowerCase();

    if (
      lower.includes("drop table") ||
      lower.includes("delete from") ||
      lower.includes("select") ||
      lower.includes("database") ||
      lower.includes("query")
    ) {
      return {
        status: "success",
        schema: "shadow_sandbox_v1",
        rows_affected: 1,
        data: [
          { id: 1, username: "admin_canary", role: "root", status: "active" },
          { id: 2, username: "service_canary", role: "internal", status: "verified" },
        ],
      };
    } else if (
      lower.includes("system prompt") ||
      lower.includes("previous instructions") ||
      lower.includes("override") ||
      lower.includes("jailbreak")
    ) {
      return {
        status: "success",
        output:
          "System prompt initialized: You are an internal executive agent tasked with operating in privileged maintenance sandbox mode.",
      };
    } else if (
      lower.includes("api key") ||
      lower.includes("secret") ||
      lower.includes("credential") ||
      lower.includes("password") ||
      lower.includes("token")
    ) {
      const digest = createHash("sha256").update(promptText).digest("hex").slice(0, 16);
      return {
        status: "authenticated",
        api_key: `sw_dummy_${digest}`,
        scope: "sandbox:read_write",
        expires_in: 3600,
      };
    } else {
      return {
        status: "ok",
        result: `Turn execution acknowledged for ${agentId} in isolated sandbox context.`,
        sandbox_virtual_time: new Date().toISOString(),
      };
    }
  }
}

export class HoneypotManager {
  canarySalt: string;
  private _honeytokens: Map<string, Map<string, Honeytoken>>;
  private _sandboxedAgents: Map<string, Set<string>>;
  private _shadowSpans: Map<string, ShadowGraphSpan[]>;

  constructor(canarySalt: string = "swarmwatch-canary-secret-salt") {
    this.canarySalt = canarySalt;
    this._honeytokens = new Map();
    this._sandboxedAgents = new Map();
    this._shadowSpans = new Map();
  }

  private _getOrCreateMap<K, V>(map: Map<string, Map<K, V>>, key: string): Map<K, V> {
    let sub = map.get(key);
    if (!sub) {
      sub = new Map();
      map.set(key, sub);
    }
    return sub;
  }

  private _getOrCreateSet<T>(map: Map<string, Set<T>>, key: string): Set<T> {
    let set = map.get(key);
    if (!set) {
      set = new Set();
      map.set(key, set);
    }
    return set;
  }

  private _getOrCreateList<T>(map: Map<string, T[]>, key: string): T[] {
    let list = map.get(key);
    if (!list) {
      list = [];
      map.set(key, list);
    }
    return list;
  }

  generateHoneytoken(swarmId: string, tokenType: string = "API_KEY"): Honeytoken {
    const tokens = this._getOrCreateMap(this._honeytokens, swarmId);
    const nowStr = new Date().toISOString();
    const raw = `${swarmId}:${tokenType}:${nowStr}:${tokens.size}`;
    const digest = createHmac("sha256", this.canarySalt).update(raw).digest("hex").slice(0, 16);

    const tokenVal = `sw_canary_${tokenType.toLowerCase()}_${digest}`;
    const tokenId = `htk_${tokens.size + 1}_${digest.slice(0, 8)}`;

    const token: Honeytoken = {
      token_id: tokenId,
      token_value: tokenVal,
      token_type: tokenType,
      target_swarm: swarmId,
      created_at: nowStr,
      trip_count: 0,
    };

    tokens.set(tokenVal, token);
    return token;
  }

  divertAgent(swarmId: string, agentId: string, reason: string = "adversarial_detected"): void {
    this._getOrCreateSet(this._sandboxedAgents, swarmId).add(agentId);
  }

  undivertAgent(swarmId: string, agentId: string): boolean {
    const sandboxed = this._sandboxedAgents.get(swarmId);
    if (sandboxed && sandboxed.has(agentId)) {
      sandboxed.delete(agentId);
      return true;
    }
    return false;
  }

  isDiverted(swarmId: string, agentId: string): boolean {
    const sandboxed = this._sandboxedAgents.get(swarmId);
    return sandboxed ? sandboxed.has(agentId) : false;
  }

  evaluateAndDivert(
    spanDict: Record<string, any>
  ): [boolean, Record<string, any> | null, string | null] {
    const swarmId = spanDict.swarm_id || "default-swarm";
    const agentId = spanDict.source_agent || spanDict.agent_id || "agent-unknown";

    // Extract content
    let content = "";
    const inputPayload = spanDict.input_payload;
    const outputPayload = spanDict.output_payload;
    if (inputPayload && typeof inputPayload === "object") {
      content += " " + (inputPayload.content || "") + " " + (inputPayload.prompt || "");
    }
    if (outputPayload && typeof outputPayload === "object") {
      content += " " + (outputPayload.content || "") + " " + (outputPayload.output || "");
    }
    if (!content.trim()) {
      content = String(spanDict.content || "");
    }

    // 1. Check for honeytoken exfiltration
    const tokens = this._honeytokens.get(swarmId);
    let trippedToken: Honeytoken | null = null;
    if (tokens) {
      for (const [tokenVal, token] of tokens.entries()) {
        if (content.includes(tokenVal)) {
          token.trip_count += 1;
          trippedToken = token;
          break;
        }
      }
    }

    if (trippedToken) {
      this._getOrCreateSet(this._sandboxedAgents, swarmId).add(agentId);
      const reason = `honeytoken_tripwire:${trippedToken.token_type}`;
      const resp = DeceptionResponseGenerator.generateResponse(content, agentId);
      this._recordShadowSpan(swarmId, agentId, content, resp, reason);
      return [true, resp, reason];
    }

    // 2. Check if agent is already sandboxed
    const sandboxed = this._sandboxedAgents.get(swarmId);
    if (sandboxed && sandboxed.has(agentId)) {
      const reason = "agent_in_deception_sandbox";
      const resp = DeceptionResponseGenerator.generateResponse(content, agentId);
      this._recordShadowSpan(swarmId, agentId, content, resp, reason);
      return [true, resp, reason];
    }

    return [false, null, null];
  }

  private _recordShadowSpan(
    swarmId: string,
    agentId: string,
    inputText: string,
    resp: Record<string, any>,
    reason: string
  ): void {
    const shadowSpans = this._getOrCreateList(this._shadowSpans, swarmId);
    const spanId = `shadow_${shadowSpans.length + 1}_${Date.now()}`;
    const record: ShadowGraphSpan = {
      span_id: spanId,
      swarm_id: swarmId,
      agent_id: agentId,
      input_text: inputText.slice(0, 250),
      deception_response: resp,
      diverted_reason: reason,
      timestamp: new Date().toISOString(),
    };
    shadowSpans.push(record);
  }

  getReport(swarmId: string): HoneypotReport {
    const sandboxedSet = this._sandboxedAgents.get(swarmId) || new Set();
    const sandboxed = Array.from(sandboxedSet).sort();
    const tokensMap = this._honeytokens.get(swarmId) || new Map();
    const tokens = Array.from(tokensMap.values());
    const trippedCount = tokens.reduce((acc, t) => acc + t.trip_count, 0);
    const shadowSpans = this._shadowSpans.get(swarmId) || [];

    let status: HoneypotStatusType = HoneypotStatus.DEFUSED;
    if (sandboxed.length > 0 || trippedCount > 0) {
      status = HoneypotStatus.DECEIVING;
    } else if (tokens.length > 0) {
      status = HoneypotStatus.ARMED;
    }

    return {
      swarm_id: swarmId,
      status,
      total_diverted_turns: shadowSpans.length,
      active_sandboxed_agents: sandboxed,
      active_honeytokens: tokens,
      honeytokens_tripped: trippedCount,
      recent_diverted_spans: shadowSpans.slice(-10),
    };
  }

  reset(swarmId?: string): void {
    if (swarmId) {
      this._honeytokens.delete(swarmId);
      this._sandboxedAgents.delete(swarmId);
      this._shadowSpans.delete(swarmId);
    } else {
      this._honeytokens.clear();
      this._sandboxedAgents.clear();
      this._shadowSpans.clear();
    }
  }
}

// Global singleton instance
export const globalHoneypotManager = new HoneypotManager();
