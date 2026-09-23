/**
 * SwarmWatch Server-Sent Events (SSE) Client (Seam 28 / ADR 0039).
 *
 * Implements a lightweight W3C text/event-stream client for Node.js and browsers.
 * Handles automatic reconnects, exponential backoff, Last-Event-ID tracking,
 * and typed circuit trip / warning event dispatching.
 */

import * as http from "node:http";
import * as https from "node:https";
import { URL } from "node:url";

export interface SseEvent {
  id: string;
  event: string;
  data: Record<string, any>;
  retry?: number;
}

export interface SseClientOptions {
  url?: string;
  swarmId?: string;
  projectId?: string;
  minLevel?: "warning" | "trip";
  lastEventId?: string;
  reconnectIntervalMs?: number;
  maxReconnectAttempts?: number;
  headers?: Record<string, string>;
}

export type SseEventHandler = (event: SseEvent) => void;

/**
 * Universal SSE Event Parser that processes text/event-stream chunks
 * and emits complete SseEvents.
 */
export class SseStreamParser {
  private buffer: string;
  private currentId: string;
  private currentEvent: string;
  private currentData: string[];
  private currentRetry?: number;

  constructor() {
    this.buffer = "";
    this.currentId = "";
    this.currentEvent = "message";
    this.currentData = [];
  }

  public feed(chunk: string): SseEvent[] {
    this.buffer += chunk;
    const lines = this.buffer.split(/\r?\n/);
    // Keep incomplete trailing line in buffer
    this.buffer = lines.pop() || "";

    const events: SseEvent[] = [];

    for (const rawLine of lines) {
      const line = rawLine.trim();

      if (!line) {
        // Blank line marks event boundary
        if (this.currentData.length > 0 || this.currentEvent !== "message" || this.currentId) {
          const rawData = this.currentData.join("\n");
          let parsedData: Record<string, any> = {};
          try {
            parsedData = rawData ? JSON.parse(rawData) : {};
          } catch {
            parsedData = { raw: rawData };
          }

          events.push({
            id: this.currentId,
            event: this.currentEvent,
            data: parsedData,
            retry: this.currentRetry,
          });

          // Reset parser state
          this.currentId = "";
          this.currentEvent = "message";
          this.currentData = [];
          this.currentRetry = undefined;
        }
        continue;
      }

      if (line.startsWith(":")) {
        // Comment / ping line
        continue;
      }

      const colonIdx = line.indexOf(":");
      let field = line;
      let value = "";
      if (colonIdx !== -1) {
        field = line.slice(0, colonIdx).trim();
        value = line.slice(colonIdx + 1).trim();
      }

      if (field === "id") {
        this.currentId = value;
      } else if (field === "event") {
        this.currentEvent = value;
      } else if (field === "data") {
        this.currentData.push(value);
      } else if (field === "retry") {
        const r = parseInt(value, 10);
        if (!isNaN(r)) this.currentRetry = r;
      }
    }

    return events;
  }

  public reset(): void {
    this.buffer = "";
    this.currentId = "";
    this.currentEvent = "message";
    this.currentData = [];
    this.currentRetry = undefined;
  }
}

/**
 * Universal SwarmWatch SSE EventSource client supporting automatic reconnects
 * and Last-Event-ID state resumption.
 */
export class SwarmEventSource {
  public readonly url: string;
  public readonly swarmId?: string;
  public readonly projectId?: string;
  public readonly minLevel: "warning" | "trip";
  public lastEventId?: string;

  private reconnectIntervalMs: number;
  private maxReconnectAttempts: number;
  private reconnectAttempts: number;
  private isClosed: boolean;
  private activeRequest?: http.ClientRequest;
  private listeners: Map<string, Set<SseEventHandler>>;
  private parser: SseStreamParser;

  constructor(options: SseClientOptions = {}) {
    const baseUrl = options.url || "http://127.0.0.1:8765";
    this.swarmId = options.swarmId;
    this.projectId = options.projectId;
    this.minLevel = options.minLevel || "warning";
    this.lastEventId = options.lastEventId;
    this.reconnectIntervalMs = options.reconnectIntervalMs || 3000;
    this.maxReconnectAttempts = options.maxReconnectAttempts || 10;
    this.reconnectAttempts = 0;
    this.isClosed = false;
    this.listeners = new Map();
    this.parser = new SseStreamParser();

    // Build URL
    const targetPath = this.swarmId
      ? `/api/v1/swarms/${encodeURIComponent(this.swarmId)}/events/stream`
      : `/api/v1/events/stream`;
    const parsed = new URL(targetPath, baseUrl);
    if (this.minLevel) parsed.searchParams.set("min_level", this.minLevel);
    if (this.projectId) parsed.searchParams.set("project_id", this.projectId);
    this.url = parsed.toString();
  }

  public on(event: string, handler: SseEventHandler): this {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(handler);
    return this;
  }

  public off(event: string, handler: SseEventHandler): this {
    const handlers = this.listeners.get(event);
    if (handlers) {
      handlers.delete(handler);
      if (handlers.size === 0) this.listeners.delete(event);
    }
    return this;
  }

  public connect(): this {
    if (this.isClosed) return this;

    const parsed = new URL(this.url);
    const isHttps = parsed.protocol === "https:";
    const transport = isHttps ? https : http;

    const headers: Record<string, string> = {
      Accept: "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    };
    if (this.lastEventId) {
      headers["Last-Event-ID"] = this.lastEventId;
    }

    const req = transport.request(
      parsed,
      {
        method: "GET",
        headers,
      },
      (res) => {
        if (res.statusCode !== 200) {
          this.emit("error", {
            id: "",
            event: "error",
            data: { message: `SSE endpoint returned HTTP ${res.statusCode}` },
          });
          this.scheduleReconnect();
          return;
        }

        this.reconnectAttempts = 0;
        this.emit("open", { id: "", event: "open", data: {} });

        res.setEncoding("utf8");
        res.on("data", (chunk: string) => {
          const events = this.parser.feed(chunk);
          for (const evt of events) {
            if (evt.id) this.lastEventId = evt.id;
            if (evt.retry) this.reconnectIntervalMs = evt.retry;
            this.emit(evt.event, evt);
            this.emit("message", evt);
          }
        });

        res.on("end", () => {
          if (!this.isClosed) this.scheduleReconnect();
        });

        res.on("error", (err) => {
          this.emit("error", {
            id: "",
            event: "error",
            data: { error: err.message },
          });
          if (!this.isClosed) this.scheduleReconnect();
        });
      }
    );

    req.on("error", (err) => {
      this.emit("error", {
        id: "",
        event: "error",
        data: { error: err.message },
      });
      if (!this.isClosed) this.scheduleReconnect();
    });

    this.activeRequest = req;
    req.end();
    return this;
  }

  public close(): void {
    this.isClosed = true;
    if (this.activeRequest) {
      this.activeRequest.destroy();
      this.activeRequest = undefined;
    }
    this.parser.reset();
  }

  private scheduleReconnect(): void {
    if (this.isClosed) return;
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.emit("error", {
        id: "",
        event: "error",
        data: { message: `Max reconnection attempts reached (${this.maxReconnectAttempts})` },
      });
      return;
    }

    this.reconnectAttempts++;
    const backoff = Math.min(this.reconnectIntervalMs * Math.pow(1.5, this.reconnectAttempts - 1), 30000);
    setTimeout(() => {
      if (!this.isClosed) this.connect();
    }, backoff);
  }

  private emit(event: string, sseEvent: SseEvent): void {
    const handlers = this.listeners.get(event);
    if (handlers) {
      for (const h of handlers) {
        try {
          h(sseEvent);
        } catch {
          // Prevent user listener errors from bubbling
        }
      }
    }
  }
}
