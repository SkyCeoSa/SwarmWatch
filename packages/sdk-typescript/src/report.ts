/**
 * SwarmWatch Incident Post-Mortem & Financial ROI Report Generator (Seam 11 / ADR 0020).
 * TypeScript SDK implementation with exact domain and behavioral parity.
 *
 * Provides deterministic, zero-token forensic analysis over multi-agent swarms,
 * computing counterfactual dollar/token savings, root-cause attribution,
 * entropy trajectories, and credential neutralization audits.
 */

import type { CircuitEvent, SwarmSpan } from "./schema.ts";
import type { StorageSink } from "./transport.ts";

export interface FinancialRoiMetrics {
  dollars_saved: number;
  tokens_saved: number;
  turns_prevented: number;
  tokens_used: number;
}

export interface RootCauseDiagnosis {
  trigger_reason: string;
  source_agent: string;
  target_agent: string;
  trigger_span_id: string;
  trigger_entropy?: number | null;
  trigger_similarity?: number | null;
}

export interface EntropyTrajectoryPoint {
  step_index: number;
  source_agent: string;
  target_agent: string;
  entropy_score?: number | null;
  similarity_score?: number | null;
  tokens: number;
}

export interface SecurityRedactionSummary {
  total_redacted: number;
  redacted_categories: Record<string, number>;
}

export interface IncidentPostMortem {
  swarm_id: string;
  status: string;
  circuit_tripped: boolean;
  start_time?: string;
  end_time?: string;
  duration_seconds: number;
  total_spans: number;
  total_tokens: number;
  roi: FinancialRoiMetrics;
  root_cause?: RootCauseDiagnosis | null;
  entropy_trajectory: EntropyTrajectoryPoint[];
  security_summary: SecurityRedactionSummary;
  recommendations: string[];
}

export type ReportFormat = "markdown" | "json" | "terminal";

export type ReportStorageSource =
  | StorageSink
  | {
      spans?: SwarmSpan[];
      circuitEvents?: CircuitEvent[];
      getSpans?(swarmId: string): SwarmSpan[] | Promise<SwarmSpan[]>;
      getCircuitEvents?(swarmId: string): CircuitEvent[] | Promise<CircuitEvent[]>;
    }
  | SwarmSpan[];

export class IncidentReportGenerator {
  private static readonly REDACTION_PATTERN = /\[REDACTED(?:_([A-Z_]+))?\]/g;
  private source: ReportStorageSource;

  constructor(source: ReportStorageSource) {
    this.source = source;
  }

  public static fromSpans(spans: SwarmSpan[], circuitEvents: CircuitEvent[] = []): IncidentReportGenerator {
    return new IncidentReportGenerator({ spans, circuitEvents });
  }

  public static fromSink(sink: StorageSink): IncidentReportGenerator {
    return new IncidentReportGenerator(sink);
  }

  private resolveSpans(swarmId: string): SwarmSpan[] {
    if (Array.isArray(this.source)) {
      return this.source.filter((s) => s.swarm_id === swarmId);
    }
    if ("getSpans" in this.source && typeof this.source.getSpans === "function") {
      const res = this.source.getSpans(swarmId);
      return Array.isArray(res) ? res : [];
    }
    if ("spans" in this.source && Array.isArray(this.source.spans)) {
      return this.source.spans.filter((s) => s.swarm_id === swarmId);
    }
    return [];
  }

  private resolveCircuitEvents(swarmId: string): CircuitEvent[] {
    if (Array.isArray(this.source)) {
      return [];
    }
    if ("getCircuitEvents" in this.source && typeof this.source.getCircuitEvents === "function") {
      const res = this.source.getCircuitEvents(swarmId);
      return Array.isArray(res) ? res : [];
    }
    if ("circuitEvents" in this.source && Array.isArray(this.source.circuitEvents)) {
      return this.source.circuitEvents.filter((e) => e.swarm_id === swarmId);
    }
    return [];
  }

  public analyze(swarmId: string): IncidentPostMortem | null {
    const rawSpans = this.resolveSpans(swarmId);
    if (!rawSpans || rawSpans.length === 0) {
      return null;
    }

    // Sort chronologically
    const spans = [...rawSpans].sort((a, b) => {
      const tA = new Date(a.start_time).getTime();
      const tB = new Date(b.start_time).getTime();
      return tA - tB;
    });

    const circuitEvents = this.resolveCircuitEvents(swarmId);
    const trippedEvent = circuitEvents.find(
      (e) => (e.event_type || "").toUpperCase() === "TRIP" || (e.event_type || "").toUpperCase() === "HALT"
    );

    const getSpanTokens = (s: SwarmSpan): number => {
      return s.token_count || 0;
    };

    const totalTokens = spans.reduce((acc, s) => acc + getSpanTokens(s), 0);
    const t0 = new Date(spans[0].start_time).getTime();
    const t1 = new Date(spans[spans.length - 1].end_time || spans[spans.length - 1].start_time).getTime();
    const durationSeconds = Math.max(0.0, (t1 - t0) / 1000.0);

    // Build entropy trajectory
    const trajectory: EntropyTrajectoryPoint[] = spans.map((s, idx) => ({
      step_index: idx + 1,
      source_agent: s.source_agent,
      target_agent: s.target_agent,
      entropy_score: s.entropy_score ?? null,
      similarity_score: (s as any).similarity_score ?? null,
      tokens: getSpanTokens(s),
    }));

    // Tally neutralized security credentials
    let totalRedacted = 0;
    const redactedCategories: Record<string, number> = {};

    for (const s of spans) {
      const payloadStr = `${JSON.stringify(s.input_payload || {})} ${JSON.stringify(s.output_payload || {})}`;
      IncidentReportGenerator.REDACTION_PATTERN.lastIndex = 0;
      let match: RegExpExecArray | null;
      while ((match = IncidentReportGenerator.REDACTION_PATTERN.exec(payloadStr)) !== null) {
        totalRedacted += 1;
        const cat = match[1] || "CREDENTIAL";
        redactedCategories[cat] = (redactedCategories[cat] || 0) + 1;
      }
    }

    const securitySummary: SecurityRedactionSummary = {
      total_redacted: totalRedacted,
      redacted_categories: redactedCategories,
    };

    const circuitTripped = trippedEvent !== undefined;
    const status = circuitTripped ? "HALTED_BY_CIRCUIT_BREAKER" : "COMPLETED_SUCCESSFULLY";

    const roi: FinancialRoiMetrics = {
      dollars_saved: 0.0,
      tokens_saved: 0,
      turns_prevented: 0,
      tokens_used: totalTokens,
    };

    let rootCause: RootCauseDiagnosis | null = null;
    const recommendations: string[] = [];

    if (circuitTripped && trippedEvent) {
      const savings = trippedEvent.counterfactual_savings;
      if (savings) {
        roi.dollars_saved = savings.dollars_saved || 0.0;
        roi.tokens_saved = savings.tokens_saved || 0;
        roi.turns_prevented = savings.turns_averted || (savings as any).turns_prevented || 0;
      }

      // Find triggering span
      const triggerSpan =
        spans.find((s) => s.id === trippedEvent.span_id) || spans[spans.length - 1];

      rootCause = {
        trigger_reason: trippedEvent.reason,
        source_agent: triggerSpan.source_agent,
        target_agent: triggerSpan.target_agent,
        trigger_span_id: triggerSpan.id,
        trigger_entropy: triggerSpan.entropy_score ?? null,
        trigger_similarity: (triggerSpan as any).similarity_score ?? null,
      };

      const reasonLower = (trippedEvent.reason || "").toLowerCase();
      if (reasonLower.includes("entropy")) {
        const entStr = rootCause.trigger_entropy !== null && rootCause.trigger_entropy !== undefined
          ? rootCause.trigger_entropy.toFixed(2)
          : "0.00";
        recommendations.push(
          `Agent dialogue between '${rootCause.source_agent}' and '${rootCause.target_agent}' ` +
            `collapsed in vocabulary diversity (H_norm=${entStr}). ` +
            `Consider increasing temperature or applying system prompt diversity guardrails.`
        );
      } else if (reasonLower.includes("recursion") || reasonLower.includes("tool")) {
        recommendations.push(
          `Tool invocation loop detected for '${rootCause.target_agent}'. ` +
            `Add exponential backoff or validate tool error handling before re-trying.`
        );
      } else if (reasonLower.includes("budget") || reasonLower.includes("quota")) {
        recommendations.push(
          `Swarm reached session budget limits. ` +
            `Inspect sub-task decomposition to reduce turn count per swarm run.`
        );
      } else {
        recommendations.push(
          `Review conversational boundaries between '${rootCause.source_agent}' and '${rootCause.target_agent}'.`
        );
      }
    } else {
      recommendations.push("Swarm executed within healthy entropy and quota boundaries.");
    }

    return {
      swarm_id: swarmId,
      status,
      circuit_tripped: circuitTripped,
      start_time: spans[0].start_time,
      end_time: spans[spans.length - 1].end_time,
      duration_seconds: durationSeconds,
      total_spans: spans.length,
      total_tokens: totalTokens,
      roi,
      root_cause: rootCause,
      entropy_trajectory: trajectory,
      security_summary: securitySummary,
      recommendations,
    };
  }

  public generateReport(
    swarmId: string,
    format: ReportFormat = "markdown"
  ): string | Record<string, any> {
    const postMortem = this.analyze(swarmId);
    if (!postMortem) {
      if (format === "json") {
        return { error: `Swarm '${swarmId}' not found` };
      }
      return `Swarm '${swarmId}' not found in storage.`;
    }

    if (format === "json") {
      return postMortem;
    }

    if (format === "terminal") {
      return this.formatTerminal(postMortem);
    }

    return this.formatMarkdown(postMortem);
  }

  private formatMarkdown(pm: IncidentPostMortem): string {
    const statusBadge = pm.circuit_tripped ? "🔴 HALTED_BY_CIRCUIT_BREAKER" : "🟢 COMPLETED_SUCCESSFULLY";

    const lines: string[] = [
      `# SwarmWatch Incident Post-Mortem: ${pm.swarm_id}`,
      "",
      `**Execution Status**: \`${pm.status}\` (${statusBadge})  `,
      `**Total Spans**: \`${pm.total_spans}\` | **Duration**: \`${pm.duration_seconds.toFixed(2)}s\` | **Tokens Used**: \`${pm.total_tokens.toLocaleString()}\`  `,
      "",
      "---",
      "",
      "## 1. Financial ROI & Prevented Waste",
      "",
      "| Metric | Value |",
      "| :--- | :--- |",
      `| **Counterfactual Dollars Saved** | **\\$${pm.roi.dollars_saved.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}** |`,
      `| **Runaway Tokens Prevented** | **${pm.roi.tokens_saved.toLocaleString()} tokens** |`,
      `| **Runaway Turns Prevented** | **${pm.roi.turns_prevented} turns** |`,
      `| **Actual Tokens Consumed** | ${pm.roi.tokens_used.toLocaleString()} tokens |`,
      "",
    ];

    if (pm.circuit_tripped && pm.root_cause) {
      const rc = pm.root_cause;
      lines.push(
        "---",
        "",
        "## 2. Root Cause Analysis (RCA)",
        "",
        `- **Failure Mode**: \`${rc.trigger_reason}\``,
        `- **Primary Agent Vector**: \`${rc.source_agent}\` -> \`${rc.target_agent}\``,
        `- **Trigger Span ID**: \`${rc.trigger_span_id}\``
      );
      if (rc.trigger_entropy !== null && rc.trigger_entropy !== undefined) {
        lines.push(`- **Final Shannon Entropy ($H_{norm}$)**: \`${rc.trigger_entropy.toFixed(3)}\``);
      }
      if (rc.trigger_similarity !== null && rc.trigger_similarity !== undefined) {
        lines.push(`- **Final Lexical Similarity**: \`${rc.trigger_similarity.toFixed(3)}\``);
      }
      lines.push("");
    }

    lines.push(
      "---",
      "",
      "## 3. Entropy Trajectory & Diversity Timeline",
      "",
      "| Turn | Causal Vector | Shannon Entropy ($H_{norm}$) | Lexical Similarity | Tokens |",
      "| :--- | :--- | :--- | :--- | :--- |"
    );

    for (const pt of pm.entropy_trajectory) {
      const entStr = pt.entropy_score !== null && pt.entropy_score !== undefined ? pt.entropy_score.toFixed(3) : "N/A";
      const simStr = pt.similarity_score !== null && pt.similarity_score !== undefined ? pt.similarity_score.toFixed(3) : "N/A";
      lines.push(
        `| ${pt.step_index} | \`${pt.source_agent}\` -> \`${pt.target_agent}\` | \`${entStr}\` | \`${simStr}\` | ${pt.tokens.toLocaleString()} |`
      );
    }
    lines.push("");

    lines.push(
      "---",
      "",
      "## 4. Security & Credential Neutralization Audit",
      "",
      `- **Total Secrets Neutralized**: \`${pm.security_summary.total_redacted}\``
    );

    const catEntries = Object.entries(pm.security_summary.redacted_categories);
    if (catEntries.length > 0) {
      lines.push("| Category | Count |", "| :--- | :--- |");
      catEntries.sort(([a], [b]) => a.localeCompare(b));
      for (const [cat, count] of catEntries) {
        lines.push(`| \`${cat}\` | ${count} |`);
      }
    } else {
      lines.push("_No sensitive credentials or PII patterns detected in payloads._");
    }
    lines.push("");

    lines.push("---", "", "## 5. Actionable Recommendations", "");
    for (const rec of pm.recommendations) {
      lines.push(`- ${rec}`);
    }
    lines.push("");

    return lines.join("\n");
  }

  private formatTerminal(pm: IncidentPostMortem): string {
    const statusLine = pm.circuit_tripped ? "🔴 HALTED BY CIRCUIT BREAKER" : "🟢 COMPLETED SUCCESSFULLY";
    const bar = "=".repeat(76);
    const subBar = "-".repeat(76);

    const lines: string[] = [
      bar,
      ` SWARMWATCH INCIDENT POST-MORTEM: ${pm.swarm_id}`,
      bar,
      ` Status:        ${statusLine}`,
      ` Duration:      ${pm.duration_seconds.toFixed(2)}s | Spans: ${pm.total_spans} | Tokens Consumed: ${pm.total_tokens.toLocaleString()}`,
      subBar,
      " FINANCIAL ROI & PREVENTED WASTE:",
      `   Dollars Saved:       $${pm.roi.dollars_saved.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      `   Tokens Prevented:    ${pm.roi.tokens_saved.toLocaleString()} tokens`,
      `   Turns Prevented:     ${pm.roi.turns_prevented} turns`,
      subBar,
    ];

    if (pm.circuit_tripped && pm.root_cause) {
      const rc = pm.root_cause;
      lines.push(
        " ROOT CAUSE ANALYSIS (RCA):",
        `   Reason:       ${rc.trigger_reason}`,
        `   Agent Pair:   ${rc.source_agent} -> ${rc.target_agent}`,
        `   Trigger Span: ${rc.trigger_span_id}`,
        subBar
      );
    }

    lines.push(
      ` SECURITY AUDIT: Neutralized ${pm.security_summary.total_redacted} credentials`,
      ` RECOMMENDATION: ${pm.recommendations[0] || "None"}`,
      bar
    );

    return lines.join("\n");
  }
}
