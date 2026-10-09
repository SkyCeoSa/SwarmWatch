/**
 * SwarmWatch Declarative Guardrail Policy Engine & Policy DSL (TypeScript Edition).
 * Conforms to ADR 0021 & ADR 0022 / Seam 12.
 *
 * Provides zero-dependency declarative YAML/JSON guardrail policies, role-specific
 * quota and entropy inheritance, prohibited tool enforcement, and sub-microsecond
 * in-memory evaluation with dynamic hot-reloading.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import type { SwarmSpan } from "./schema.ts";

export interface RuleConfig {
  min_entropy?: number | null;
  max_similarity?: number | null;
  window_size?: number | null;
  max_turns?: number | null;
  token_budget?: number | null;
  cost_budget?: number | null;
  cost_per_token?: number | null;
  max_tool_invocations?: number | null;
  prohibited_tools?: string[];
  action?: "trip" | "warn" | "redact";
}

export interface RuleCondition {
  min_entropy?: number | null;
  max_similarity?: number | null;
  token_limit?: number | null;
  tool_name_in?: string[] | null;
  tool_name_not_in?: string[] | null;
  prohibited_tools?: string[] | null;
  payload_contains_any?: string[] | null;
  payload_contains_all?: string[] | null;
}

export interface DeclarativeRule {
  id: string;
  description?: string | null;
  agent_pattern?: string;
  condition: RuleCondition;
  action?: "trip" | "warn" | "redact";
  message?: string | null;
}

export interface GuardrailPolicy {
  version?: string;
  name?: string;
  description?: string | null;
  defaults?: RuleConfig;
  agents?: Record<string, RuleConfig>;
  rules?: DeclarativeRule[];
}

export interface PolicyEvaluationResult {
  decision: "pass" | "warn" | "trip" | "redact";
  reason?: string | null;
  matched_rule_id?: string | null;
  effective_config?: RuleConfig | null;
}

/**
 * Fast zero-dependency wildcard matching supporting '*' and '?' patterns.
 */
export function globMatch(str: string, pattern: string): boolean {
  if (pattern === "*" || pattern === str) {
    return true;
  }
  if (!pattern.includes("*") && !pattern.includes("?")) {
    return str === pattern;
  }
  const escaped = pattern
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*/g, ".*")
    .replace(/\?/g, ".");
  return new RegExp(`^${escaped}$`).test(str);
}

/**
 * Merges an override RuleConfig over a base RuleConfig non-destructively.
 */
export function mergeRuleConfigs(base: RuleConfig, override: RuleConfig): RuleConfig {
  const result: RuleConfig = { ...base };

  for (const [key, val] of Object.entries(override)) {
    if (val !== undefined && val !== null) {
      if (key === "prohibited_tools" && Array.isArray(val)) {
        const existing = base.prohibited_tools || [];
        const combined = Array.from(new Set([...existing, ...val]));
        result.prohibited_tools = combined;
      } else {
        (result as any)[key] = val;
      }
    }
  }

  return result;
}

/**
 * Zero-dependency parser for standard JSON and structured YAML configuration documents.
 * Operates without requiring external third-party packages.
 */
export class SafePolicyParser {
  public static parseString(content: string): Record<string, any> {
    const stripped = content.trim();
    if (!stripped) {
      return {};
    }

    // 1. Fast path: JSON parse
    if (stripped.startsWith("{")) {
      try {
        return JSON.parse(stripped);
      } catch {
        // Fall back to YAML parser if not strict JSON
      }
    }

    // 2. Pure TypeScript indentation-aware YAML parser
    try {
      return this._parseYamlLines(content.split(/\r?\n/));
    } catch {
      return JSON.parse(stripped);
    }
  }

  private static _convertScalar(valStr: string): any {
    const s = valStr.trim();
    if (!s) return null;
    const lower = s.toLowerCase();
    if (lower === "true" || lower === "yes" || lower === "on") return true;
    if (lower === "false" || lower === "no" || lower === "off") return false;
    if (lower === "null" || lower === "none" || lower === "~") return null;

    if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
      return s.slice(1, -1);
    }

    if (!isNaN(Number(s))) {
      return Number(s);
    }

    return s;
  }

  private static _parseYamlLines(lines: string[]): Record<string, any> {
    const root: Record<string, any> = {};
    type StackItem = { indent: number; container: any; lastKey: string | null };
    const stack: StackItem[] = [{ indent: -1, container: root, lastKey: null }];

    for (const rawLine of lines) {
      // Strip comments
      const line = rawLine.replace(/#.*$/, "").trimEnd();
      if (!line.trim()) {
        continue;
      }

      const indent = line.length - line.trimStart().length;
      const trimmed = line.trim();

      // Adjust stack to current indentation
      while (stack.length > 1 && stack[stack.length - 1].indent >= indent) {
        stack.pop();
      }

      const current = stack[stack.length - 1];

      if (trimmed.startsWith("- ")) {
        // List item
        const itemContent = trimmed.slice(2).trim();

        // If parent was created as an empty object for a key, convert it to an array
        if (
          typeof current.container === "object" &&
          !Array.isArray(current.container) &&
          Object.keys(current.container).length === 0 &&
          stack.length > 1 &&
          current.lastKey
        ) {
          const parent = stack[stack.length - 2].container;
          if (parent && typeof parent === "object") {
            const listContainer: any[] = [];
            parent[current.lastKey] = listContainer;
            current.container = listContainer;
          }
        }

        if (itemContent.includes(":") && !itemContent.startsWith("{") && !itemContent.startsWith("[")) {
          // Dictionary inside a list (e.g. - id: rule-1)
          const colonIdx = itemContent.indexOf(":");
          const k = itemContent.slice(0, colonIdx).trim();
          const v = itemContent.slice(colonIdx + 1).trim();
          const dictItem: Record<string, any> = { [k]: this._convertScalar(v) };
          if (Array.isArray(current.container)) {
            current.container.push(dictItem);
          }
          stack.push({ indent, container: dictItem, lastKey: k });
        } else {
          const scalar = this._convertScalar(itemContent);
          if (Array.isArray(current.container)) {
            current.container.push(scalar);
          }
        }
      } else if (trimmed.includes(":")) {
        const colonIdx = trimmed.indexOf(":");
        const key = trimmed.slice(0, colonIdx).trim();
        const val = trimmed.slice(colonIdx + 1).trim();

        if (typeof current.container === "object" && !Array.isArray(current.container)) {
          if (!val) {
            // Nested object or start of list
            const nested: Record<string, any> = {};
            current.container[key] = nested;
            stack.push({ indent, container: nested, lastKey: key });
          } else {
            if (val.startsWith("[") && val.endsWith("]")) {
              const inner = val.slice(1, -1).trim();
              const items = inner
                ? inner.split(",").map((x) => this._convertScalar(x.trim())).filter((x) => x !== "")
                : [];
              current.container[key] = items;
            } else if (val.startsWith("{") && val.endsWith("}")) {
              try {
                current.container[key] = JSON.parse(val);
              } catch {
                current.container[key] = val;
              }
            } else {
              current.container[key] = this._convertScalar(val);
              stack.push({ indent, container: current.container, lastKey: key });
            }
          }
        }
      }
    }

    return root;
  }

  public static fromObject(data: Record<string, any>): GuardrailPolicy {
    return {
      version: data.version ?? "1.0",
      name: data.name ?? "default_policy",
      description: data.description ?? null,
      defaults: data.defaults ?? {},
      agents: data.agents ?? {},
      rules: data.rules ?? [],
    };
  }

  public static fromYaml(yamlContent: string): GuardrailPolicy {
    const parsed = this.parseString(yamlContent);
    return this.fromObject(parsed);
  }

  public static fromJson(jsonContent: string): GuardrailPolicy {
    const parsed = JSON.parse(jsonContent);
    return this.fromObject(parsed);
  }

  public static fromFile(filePath: string): GuardrailPolicy {
    const resolved = path.resolve(filePath);
    const content = fs.readFileSync(resolved, "utf-8");
    if (resolved.toLowerCase().endsWith(".json")) {
      return this.fromJson(content);
    }
    return this.fromYaml(content);
  }
}

/**
 * High-performance in-memory guardrail policy evaluation engine for TypeScript.
 * Resolves role-based configuration inheritance and enforces declarative rules in <1µs.
 */
export class PolicyEngine {
  public policy: GuardrailPolicy;
  private cachedEffective: Map<string, RuleConfig> = new Map();

  constructor(policy?: GuardrailPolicy | string | Record<string, any>) {
    if (!policy) {
      this.policy = SafePolicyParser.fromObject({});
    } else if (typeof policy === "string") {
      if (policy.includes("\n") || policy.includes(":")) {
        this.policy = SafePolicyParser.fromYaml(policy);
      } else {
        this.policy = SafePolicyParser.fromFile(policy);
      }
    } else if (typeof policy === "object") {
      this.policy = SafePolicyParser.fromObject(policy);
    } else {
      this.policy = SafePolicyParser.fromObject({});
    }
  }

  /**
   * Dynamically hot-reloads the active policy document with immediate in-memory effect.
   */
  public hotReload(policy: GuardrailPolicy | string | Record<string, any>): void {
    if (typeof policy === "string") {
      if (policy.includes("\n") || policy.includes(":")) {
        this.policy = SafePolicyParser.fromYaml(policy);
      } else {
        this.policy = SafePolicyParser.fromFile(policy);
      }
    } else if (typeof policy === "object") {
      this.policy = SafePolicyParser.fromObject(policy);
    } else {
      this.policy = SafePolicyParser.fromObject({});
    }
    this.cachedEffective.clear();
  }

  /**
   * Resolves effective RuleConfig for an agent role through hierarchical inheritance.
   * Applies agent-specific rules and pattern matches on top of global defaults.
   */
  public getEffectiveConfig(agentName: string): RuleConfig {
    if (this.cachedEffective.has(agentName)) {
      return this.cachedEffective.get(agentName)!;
    }

    let effective: RuleConfig = { ...(this.policy.defaults || {}) };

    if (this.policy.agents) {
      for (const [pattern, agentConfig] of Object.entries(this.policy.agents)) {
        if (globMatch(agentName, pattern)) {
          effective = mergeRuleConfigs(effective, agentConfig);
        }
      }
    }

    this.cachedEffective.set(agentName, effective);
    return effective;
  }

  /**
   * Evaluates an agent turn or tool invocation against declarative guardrails.
   * Execution runs in sub-microsecond time (<1µs) with zero external LLM token consumption.
   */
  public evaluateTurn(options: {
    agent: string;
    text?: string | null;
    token_count?: number | null;
    tool_name?: string | null;
    payload?: Record<string, any> | null;
    entropy_score?: number | null;
    similarity_score?: number | null;
  }): PolicyEvaluationResult {
    const { agent, text, token_count, tool_name, payload, entropy_score, similarity_score } = options;
    const config = this.getEffectiveConfig(agent);

    // 1. Prohibited tool check
    if (tool_name && config.prohibited_tools && config.prohibited_tools.length > 0) {
      if (config.prohibited_tools.some((p) => globMatch(tool_name, p))) {
        return {
          decision: config.action ?? "trip",
          reason: `Prohibited tool '${tool_name}' invoked by agent '${agent}'`,
          matched_rule_id: "config:prohibited_tools",
          effective_config: config,
        };
      }
    }

    // 2. Entropy safety floor check
    if (entropy_score !== undefined && entropy_score !== null && config.min_entropy !== undefined && config.min_entropy !== null) {
      if (entropy_score < config.min_entropy) {
        return {
          decision: config.action ?? "trip",
          reason: `Normalized Shannon entropy ${entropy_score.toFixed(3)} below floor ${config.min_entropy.toFixed(3)} for agent '${agent}'`,
          matched_rule_id: "config:min_entropy",
          effective_config: config,
        };
      }
    }

    // 3. Lexical similarity ceiling check
    if (similarity_score !== undefined && similarity_score !== null && config.max_similarity !== undefined && config.max_similarity !== null) {
      if (similarity_score > config.max_similarity) {
        return {
          decision: config.action ?? "trip",
          reason: `Lexical similarity ${similarity_score.toFixed(3)} exceeds ceiling ${config.max_similarity.toFixed(3)} for agent '${agent}'`,
          matched_rule_id: "config:max_similarity",
          effective_config: config,
        };
      }
    }

    // 4. Token budget ceiling check
    if (token_count !== undefined && token_count !== null && config.token_budget !== undefined && config.token_budget !== null) {
      if (token_count > config.token_budget) {
        return {
          decision: config.action ?? "trip",
          reason: `Token consumption ${token_count} exceeds budget ceiling ${config.token_budget} for agent '${agent}'`,
          matched_rule_id: "config:token_budget",
          effective_config: config,
        };
      }
    }

    // 5. Custom declarative event-condition-action rules
    let combinedPayloadText = "";
    if (payload || text) {
      combinedPayloadText = `${text || ""} ${JSON.stringify(payload || {})}`.toLowerCase();
    }

    if (this.policy.rules && this.policy.rules.length > 0) {
      for (const rule of this.policy.rules) {
        const agentPat = rule.agent_pattern || "*";
        if (!globMatch(agent, agentPat)) {
          continue;
        }

        const cond = rule.condition || {};
        let matched = true;

        // Tool name conditions
        if (cond.tool_name_in && cond.tool_name_in.length > 0) {
          if (!tool_name || !cond.tool_name_in.includes(tool_name)) {
            matched = false;
          }
        }

        if (matched && cond.tool_name_not_in && cond.tool_name_not_in.length > 0) {
          if (tool_name && cond.tool_name_not_in.includes(tool_name)) {
            matched = false;
          }
        }

        if (matched && cond.prohibited_tools && cond.prohibited_tools.length > 0) {
          if (!tool_name || !cond.prohibited_tools.some((p) => globMatch(tool_name, p))) {
            matched = false;
          }
        }

        // Keyword conditions
        if (matched && cond.payload_contains_any && cond.payload_contains_any.length > 0) {
          if (!cond.payload_contains_any.some((kw) => combinedPayloadText.includes(kw.toLowerCase()))) {
            matched = false;
          }
        }

        if (matched && cond.payload_contains_all && cond.payload_contains_all.length > 0) {
          if (!cond.payload_contains_all.every((kw) => combinedPayloadText.includes(kw.toLowerCase()))) {
            matched = false;
          }
        }

        // Metric conditions
        if (matched && cond.min_entropy !== undefined && cond.min_entropy !== null) {
          if (entropy_score === undefined || entropy_score === null || entropy_score >= cond.min_entropy) {
            matched = false;
          }
        }

        if (matched && cond.max_similarity !== undefined && cond.max_similarity !== null) {
          if (similarity_score === undefined || similarity_score === null || similarity_score <= cond.max_similarity) {
            matched = false;
          }
        }

        if (matched && cond.token_limit !== undefined && cond.token_limit !== null) {
          if (token_count === undefined || token_count === null || token_count <= cond.token_limit) {
            matched = false;
          }
        }

        if (matched) {
          return {
            decision: rule.action ?? "trip",
            reason: rule.message || `Declarative rule '${rule.id}' triggered by agent '${agent}'`,
            matched_rule_id: rule.id,
            effective_config: config,
          };
        }
      }
    }

    return {
      decision: "pass",
      effective_config: config,
    };
  }

  /**
   * Convenience method evaluating a causal span against the active guardrail policy.
   */
  public evaluateSpan(span: SwarmSpan): PolicyEvaluationResult {
    let tool_name: string | null = null;
    const spanType = (span as any).span_type || "";
    if (spanType === "tool" || spanType === "tool_call" || spanType === "function") {
      tool_name = (span as any).target_agent || null;
    }

    const payload: Record<string, any> = {};
    if ((span as any).input_payload) {
      Object.assign(payload, (span as any).input_payload);
    }
    if ((span as any).output_payload) {
      Object.assign(payload, (span as any).output_payload);
    }

    const text = String(payload.text || payload.message || "");

    return this.evaluateTurn({
      agent: span.source_agent,
      text,
      token_count: (span as any).token_count ?? null,
      tool_name,
      payload,
      entropy_score: (span as any).entropy_score ?? null,
      similarity_score: (span as any).similarity_score ?? null,
    });
  }
}
