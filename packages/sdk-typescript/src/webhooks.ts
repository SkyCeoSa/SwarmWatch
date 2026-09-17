/**
 * SwarmWatch Enterprise Webhook Alerting Dispatcher (TypeScript Edition).
 * Conforms to ADR 0007 & ADR 0023 / Seam 6 & 7 Extension.
 *
 * Provides non-blocking, asynchronous notifications to Slack Block Kit, Discord Embeds,
 * PagerDuty Events API v2, and custom webhooks when circuit breaker events occur.
 * Guarantees sub-microsecond non-blocking execution with in-memory Dead-Letter Queue (DLQ).
 */

import * as http from "node:http";
import * as https from "node:https";
import * as crypto from "node:crypto";
import type { CircuitEvent } from "./schema.ts";

export type WebhookFormat = "slack" | "discord" | "pagerduty" | "swarmwatch";

export interface WebhookConfig {
  url: string;
  format?: WebhookFormat;
  secret?: string | null;
  routing_key?: string | null;
  min_level?: "warning" | "trip";
  max_retries?: number;
  timeout?: number;
}

export interface DeadLetterRecord {
  id: string;
  event_id: string;
  swarm_id: string;
  destination_url: string;
  format: string;
  attempt_count: number;
  last_error: string;
  failed_at: string;
  payload: Record<string, any>;
}

export interface WebhookStats {
  delivered: number;
  failed: number;
  retried: number;
  dlq_count: number;
}

export type WebhookHttpSender = (
  url: string,
  payload: Record<string, any>,
  headers: Record<string, string>
) => Promise<boolean>;

/**
 * Formats CircuitEvent according to target webhook protocol:
 * Slack Block Kit, Discord Embeds, PagerDuty Events API v2, or native SwarmWatch JSON.
 */
export function formatWebhookPayload(event: CircuitEvent, config: WebhookConfig): Record<string, any> {
  const fmt = (config.format || "swarmwatch").toLowerCase();
  const savings = event.counterfactual_savings;
  const isTrip = (event.event_type || "").toLowerCase() === "trip";

  if (fmt === "slack") {
    const headerText = `${isTrip ? "[SWARMWATCH CIRCUIT TRIP]" : "[SWARMWATCH CIRCUIT WARNING]"} Swarm: ${event.swarm_id}`;
    const blocks: any[] = [
      {
        type: "header",
        text: { type: "plain_text", text: headerText, emoji: false },
      },
      {
        type: "section",
        fields: [
          { type: "mrkdwn", text: `*Event*: ${event.event_type.toUpperCase()}` },
          { type: "mrkdwn", text: `*Metric*: \`${Number(event.metric_val).toFixed(4)}\`` },
          { type: "mrkdwn", text: `*Reason*: ${event.reason}` },
          { type: "mrkdwn", text: `*Time*: ${new Date(event.created_at).toUTCString()}` },
        ],
      },
    ];

    if (savings) {
      const savingsText = (
        `*Counterfactual Savings (ADR 0006)*:\n` +
        `- Prevented: *${savings.tokens_saved.toLocaleString()} tokens* (~$${savings.dollars_saved.toFixed(4)})\n` +
        `- Averted Turns: *${savings.turns_averted}*\n` +
        `- Budget Consumed: $${savings.budget_consumed.toFixed(4)}`
      );
      blocks.push({
        type: "section",
        text: { type: "mrkdwn", text: savingsText },
      });
    }

    return {
      text: headerText,
      blocks,
    };
  } else if (fmt === "discord") {
    const color = isTrip ? 0xef4444 : 0xf59e0b; // Crimson for trip, Amber for warning
    const title = `${isTrip ? "[SWARMWATCH CIRCUIT TRIP]" : "[SWARMWATCH CIRCUIT WARNING]"} Swarm: ${event.swarm_id}`;

    const fields: any[] = [
      { name: "Status", value: event.event_type.toUpperCase(), inline: true },
      { name: "Metric", value: `\`${Number(event.metric_val).toFixed(4)}\``, inline: true },
      { name: "Reason", value: event.reason, inline: false },
    ];

    if (savings) {
      fields.push({
        name: "Counterfactual Savings",
        value: `${savings.tokens_saved.toLocaleString()} tokens (~$${savings.dollars_saved.toFixed(4)}) across ${savings.turns_averted} averted turns`,
        inline: false,
      });
    }

    return {
      embeds: [
        {
          title,
          color,
          fields,
          timestamp: new Date(event.created_at).toISOString(),
        },
      ],
    };
  } else if (fmt === "pagerduty") {
    const severity = isTrip ? "critical" : "warning";
    const summary = `[${isTrip ? "SWARMWATCH CIRCUIT TRIP" : "SWARMWATCH CIRCUIT WARNING"}] Swarm '${event.swarm_id}': ${event.reason}`;
    const routingKey = config.routing_key || config.secret || "";

    const customDetails: Record<string, any> = {
      swarm_id: event.swarm_id,
      span_id: event.span_id,
      event_type: event.event_type,
      metric_val: Number(event.metric_val),
      reason: event.reason,
      created_at: new Date(event.created_at).toISOString(),
    };

    if (savings) {
      Object.assign(customDetails, {
        tokens_saved: savings.tokens_saved,
        dollars_saved: savings.dollars_saved,
        turns_averted: savings.turns_averted,
        budget_consumed: savings.budget_consumed,
        budget_protected: savings.budget_protected,
      });
    }

    return {
      routing_key: routingKey,
      event_action: "trigger",
      dedup_key: `swarmwatch-${event.swarm_id}-${event.id}`,
      payload: {
        summary,
        severity,
        source: "swarmwatch-circuit-breaker",
        component: "agent_swarm",
        group: "multi_agent_system",
        class: "loop_prevention",
        timestamp: new Date(event.created_at).toISOString(),
        custom_details: customDetails,
      },
    };
  } else {
    // Default SwarmWatch normalized JSON
    const data: Record<string, any> = {
      id: event.id,
      swarm_id: event.swarm_id,
      span_id: event.span_id,
      event_type: event.event_type,
      reason: event.reason,
      metric_val: Number(event.metric_val),
      created_at: new Date(event.created_at).toISOString(),
    };
    if (savings) {
      data.counterfactual_savings = savings;
    }
    return data;
  }
}

/**
 * Standard Node.js HTTP/HTTPS sender.
 */
export async function defaultHttpSender(
  urlString: string,
  payload: Record<string, any>,
  headers: Record<string, string>
): Promise<boolean> {
  const url = new URL(urlString);
  const data = JSON.stringify(payload);
  const isHttps = url.protocol === "https:";
  const client = isHttps ? https : http;

  const reqHeaders = {
    ...headers,
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(data),
  };

  return new Promise((resolve) => {
    const req = client.request(
      url,
      {
        method: "POST",
        headers: reqHeaders,
        timeout: 5000,
      },
      (res) => {
        res.resume();
        const code = res.statusCode ?? 500;
        resolve(code >= 200 && code < 300);
      }
    );

    req.on("error", () => resolve(false));
    req.on("timeout", () => {
      req.destroy();
      resolve(false);
    });

    req.write(data);
    req.end();
  });
}

/**
 * Asynchronous, non-blocking webhook dispatcher with Dead-Letter Queue (DLQ).
 * Queues events and delivers them via background async worker to guarantee sub-microsecond agent latency.
 */
export class WebhookNotifier {
  public configs: WebhookConfig[];
  private sender: WebhookHttpSender;
  private queue: CircuitEvent[] = [];
  private deadLetterQueue: DeadLetterRecord[] = [];
  private maxDlqSize: number;
  private isProcessing = false;
  private stats: WebhookStats = {
    delivered: 0,
    failed: 0,
    retried: 0,
    dlq_count: 0,
  };

  constructor(options?: {
    configs?: WebhookConfig[];
    sender?: WebhookHttpSender;
    maxQueueSize?: number;
    maxDlqSize?: number;
  }) {
    this.configs = options?.configs || [];
    this.sender = options?.sender || defaultHttpSender;
    this.maxDlqSize = options?.maxDlqSize ?? 500;
  }

  /**
   * Submits an event for asynchronous webhook dispatch.
   * Returns immediately in sub-microsecond time on the caller thread.
   */
  public notify(event: CircuitEvent): boolean {
    if (!this.configs || this.configs.length === 0) {
      return true;
    }

    this.queue.push(event);
    this.triggerProcessing();
    return true;
  }

  private triggerProcessing(): void {
    if (this.isProcessing) {
      return;
    }
    this.isProcessing = true;
    queueMicrotask(() => {
      this.processQueue().finally(() => {
        this.isProcessing = false;
        if (this.queue.length > 0) {
          this.triggerProcessing();
        }
      });
    });
  }

  private async processQueue(): Promise<void> {
    while (this.queue.length > 0) {
      const event = this.queue.shift();
      if (!event) break;
      await this.deliverEvent(event);
    }
  }

  private async deliverEvent(event: CircuitEvent): Promise<void> {
    for (const config of this.configs) {
      if (config.min_level === "trip" && event.event_type !== "trip") {
        continue;
      }

      const payload = formatWebhookPayload(event, config);
      const headers: Record<string, string> = { "Content-Type": "application/json" };
      if (config.secret) {
        headers.Authorization = `Bearer ${config.secret}`;
      }

      let success = false;
      let lastErrMsg = "";
      const maxRetries = config.max_retries ?? 3;

      for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
          const res = await this.sender(config.url, payload, headers);
          if (res) {
            success = true;
            this.stats.delivered += 1;
            break;
          } else {
            throw new Error(`HTTP sender returned failure for ${config.url}`);
          }
        } catch (err: any) {
          lastErrMsg = err?.message || String(err);
          this.stats.retried += 1;
          if (attempt < maxRetries) {
            await new Promise((resolve) => setTimeout(resolve, 20 * attempt));
          }
        }
      }

      if (!success) {
        this.stats.failed += 1;
        const dlqRecord: DeadLetterRecord = {
          id: `dlq-${crypto.randomUUID().slice(0, 12)}`,
          event_id: event.id,
          swarm_id: event.swarm_id,
          destination_url: config.url,
          format: config.format || "swarmwatch",
          attempt_count: maxRetries,
          last_error: lastErrMsg || "Delivery attempts exhausted",
          failed_at: new Date().toISOString(),
          payload,
        };
        this.deadLetterQueue.push(dlqRecord);
        if (this.deadLetterQueue.length > this.maxDlqSize) {
          this.deadLetterQueue.shift();
        }
        this.stats.dlq_count = this.deadLetterQueue.length;
      }
    }
  }

  public getDlq(): DeadLetterRecord[] {
    return [...this.deadLetterQueue];
  }

  public clearDlq(): void {
    this.deadLetterQueue = [];
    this.stats.dlq_count = 0;
  }

  public async replayDlq(): Promise<number> {
    const records = [...this.deadLetterQueue];
    this.deadLetterQueue = [];
    this.stats.dlq_count = 0;

    let replayed = 0;
    for (const record of records) {
      const headers = { "Content-Type": "application/json" };
      try {
        const res = await this.sender(record.destination_url, record.payload, headers);
        if (res) {
          replayed += 1;
          this.stats.delivered += 1;
        } else {
          this.deadLetterQueue.push(record);
          this.stats.dlq_count = this.deadLetterQueue.length;
        }
      } catch (err: any) {
        record.last_error = `Replay failed: ${err?.message || err}`;
        this.deadLetterQueue.push(record);
        this.stats.dlq_count = this.deadLetterQueue.length;
      }
    }

    return replayed;
  }

  public getStats(): WebhookStats {
    return { ...this.stats };
  }

  public async flush(): Promise<void> {
    await this.processQueue();
  }

  public async shutdown(): Promise<void> {
    await this.flush();
  }
}
