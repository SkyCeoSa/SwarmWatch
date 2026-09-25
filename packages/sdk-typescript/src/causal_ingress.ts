/**
 * Loss-Aware Causal Ingestion & Evidentiary Completeness Rule (Seam 53 / ADR 0064).
 * Eliminates forensic blindspots and silent span dropping under queue saturation
 * by enforcing monotonic sequence allocation, explicit gap accounting, prioritized headroom,
 * and evidentiary completeness contracts.
 */

export const IngressPriority = {
  NORMAL: "NORMAL",
  HIGH: "HIGH",
  CRITICAL: "CRITICAL",
} as const;

export type IngressPriorityEnum = typeof IngressPriority[keyof typeof IngressPriority];

export const CompletenessStatus = {
  COMPLETE: "COMPLETE",
  INCOMPLETE: "INCOMPLETE",
  SATURATED: "SATURATED",
} as const;

export type CompletenessStatusEnum = typeof CompletenessStatus[keyof typeof CompletenessStatus];

export interface SequenceGapOptions {
  swarmId: string;
  streamId: string;
  expectedSeq: number;
  receivedSeq: number;
  missingCount: number;
  detectedAt?: number;
}

export class SequenceGap {
  public readonly swarmId: string;
  public readonly streamId: string;
  public readonly expectedSeq: number;
  public readonly receivedSeq: number;
  public readonly missingCount: number;
  public readonly detectedAt: number;

  constructor(opts: SequenceGapOptions) {
    this.swarmId = opts.swarmId;
    this.streamId = opts.streamId;
    this.expectedSeq = opts.expectedSeq;
    this.receivedSeq = opts.receivedSeq;
    this.missingCount = opts.missingCount;
    this.detectedAt = opts.detectedAt !== undefined ? opts.detectedAt : Date.now() / 1000.0;
  }

  public toDict(): Record<string, unknown> {
    return {
      swarm_id: this.swarmId,
      stream_id: this.streamId,
      expected_seq: this.expectedSeq,
      received_seq: this.receivedSeq,
      missing_count: this.missingCount,
      detected_at: this.detectedAt,
    };
  }

  public toJSON(): Record<string, unknown> {
    return this.toDict();
  }

  public static fromDict(data: Record<string, unknown>): SequenceGap {
    return new SequenceGap({
      swarmId: String(data.swarm_id),
      streamId: String(data.stream_id),
      expectedSeq: Number(data.expected_seq),
      receivedSeq: Number(data.received_seq),
      missingCount: Number(data.missing_count),
      detectedAt: data.detected_at !== undefined ? Number(data.detected_at) : undefined,
    });
  }
}

export interface IngressResultOptions {
  accepted: boolean;
  seqNum: number;
  swarmId: string;
  spanId: string;
  priority: string;
  status: string;
  gapDetected?: boolean;
  gap?: SequenceGap | null;
  reason?: string | null;
}

export class IngressResult {
  public readonly accepted: boolean;
  public readonly seqNum: number;
  public readonly swarmId: string;
  public readonly spanId: string;
  public readonly priority: string;
  public readonly status: string;
  public readonly gapDetected: boolean;
  public readonly gap: SequenceGap | null;
  public readonly reason: string | null;

  constructor(opts: IngressResultOptions) {
    this.accepted = opts.accepted;
    this.seqNum = opts.seqNum;
    this.swarmId = opts.swarmId;
    this.spanId = opts.spanId;
    this.priority = opts.priority;
    this.status = opts.status;
    this.gapDetected = !!opts.gapDetected;
    this.gap = opts.gap || null;
    this.reason = opts.reason || null;
  }

  public toDict(): Record<string, unknown> {
    return {
      accepted: this.accepted,
      seq_num: this.seqNum,
      swarm_id: this.swarmId,
      span_id: this.spanId,
      priority: this.priority,
      status: this.status,
      gap_detected: this.gapDetected,
      gap: this.gap ? this.gap.toDict() : null,
      reason: this.reason,
    };
  }

  public toJSON(): Record<string, unknown> {
    return this.toDict();
  }
}

export interface CompletenessReportOptions {
  swarmId: string;
  status: CompletenessStatusEnum;
  totalSubmitted: number;
  totalAccepted: number;
  totalDropped: number;
  criticalSubmitted: number;
  criticalAccepted: number;
  criticalDeliveryRatio: number;
  lossRatio: number;
  sequenceGaps: Array<Record<string, unknown>>;
  missingCausalParents: string[];
}

export class CompletenessReport {
  public readonly swarmId: string;
  public readonly status: CompletenessStatusEnum;
  public readonly totalSubmitted: number;
  public readonly totalAccepted: number;
  public readonly totalDropped: number;
  public readonly criticalSubmitted: number;
  public readonly criticalAccepted: number;
  public readonly criticalDeliveryRatio: number;
  public readonly lossRatio: number;
  public readonly sequenceGaps: Array<Record<string, unknown>>;
  public readonly missingCausalParents: string[];

  constructor(opts: CompletenessReportOptions) {
    this.swarmId = opts.swarmId;
    this.status = opts.status;
    this.totalSubmitted = opts.totalSubmitted;
    this.totalAccepted = opts.totalAccepted;
    this.totalDropped = opts.totalDropped;
    this.criticalSubmitted = opts.criticalSubmitted;
    this.criticalAccepted = opts.criticalAccepted;
    this.criticalDeliveryRatio = opts.criticalDeliveryRatio;
    this.lossRatio = opts.lossRatio;
    this.sequenceGaps = opts.sequenceGaps;
    this.missingCausalParents = opts.missingCausalParents;
  }

  public toDict(): Record<string, unknown> {
    return {
      swarm_id: this.swarmId,
      status: this.status,
      total_submitted: this.totalSubmitted,
      total_accepted: this.totalAccepted,
      total_dropped: this.totalDropped,
      critical_submitted: this.criticalSubmitted,
      critical_accepted: this.criticalAccepted,
      critical_delivery_ratio: this.criticalDeliveryRatio,
      loss_ratio: this.lossRatio,
      sequence_gaps: this.sequenceGaps,
      missing_causal_parents: this.missingCausalParents,
    };
  }

  public toJSON(): Record<string, unknown> {
    return this.toDict();
  }
}

interface SwarmStats {
  submitted: number;
  accepted: number;
  dropped: number;
  criticalSubmitted: number;
  criticalAccepted: number;
}

export class CausalIngress {
  private readonly maxCapacity: number;
  private readonly reserveCapacity: number;
  private readonly normalThreshold: number;

  private queue: Array<{ priority: string; span: Record<string, unknown> }> = [];
  private streamSeq: Map<string, number> = new Map();
  private streamReceived: Map<string, number> = new Map();
  private gaps: Map<string, SequenceGap[]> = new Map();
  private knownSpans: Map<string, Set<string>> = new Map();
  private referencedParents: Map<string, Set<string>> = new Map();
  private stats: Map<string, SwarmStats> = new Map();

  constructor(maxCapacity: number = 1000, reserveRatio: number = 0.2) {
    this.maxCapacity = maxCapacity;
    this.reserveCapacity = Math.floor(maxCapacity * reserveRatio);
    this.normalThreshold = maxCapacity - this.reserveCapacity;
  }

  private getStats(swarmId: string): SwarmStats {
    let s = this.stats.get(swarmId);
    if (!s) {
      s = {
        submitted: 0,
        accepted: 0,
        dropped: 0,
        criticalSubmitted: 0,
        criticalAccepted: 0,
      };
      this.stats.set(swarmId, s);
    }
    return s;
  }

  private makeStreamKey(swarmId: string, streamId: string): string {
    return `${swarmId}::${streamId}`;
  }

  public submit(
    span: Record<string, unknown>,
    priority: string = IngressPriority.NORMAL,
    streamId: string = "main"
  ): IngressResult {
    const spanDict: Record<string, unknown> = { ...span };
    const swarmId = String(spanDict.swarm_id || "default_swarm");
    const spanId = String(spanDict.span_id || `spn-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`);

    let parentSpanIds: string[] = [];
    if (Array.isArray(spanDict.parent_span_ids)) {
      parentSpanIds = (spanDict.parent_span_ids as unknown[]).map(String);
    } else if (typeof spanDict.parent_span_ids === "string") {
      parentSpanIds = [spanDict.parent_span_ids as string];
    }

    // 1. Monotonic sequence allocation
    const streamKey = this.makeStreamKey(swarmId, streamId);
    const currentSeq = (this.streamSeq.get(streamKey) || 0) + 1;
    this.streamSeq.set(streamKey, currentSeq);

    const stats = this.getStats(swarmId);
    stats.submitted += 1;
    if (priority === IngressPriority.CRITICAL) {
      stats.criticalSubmitted += 1;
    }

    // 2. Capacity & prioritization check
    const currentQLen = this.queue.length;
    let accepted = false;
    let dropReason: string | null = null;

    if (priority === IngressPriority.CRITICAL) {
      accepted = true;
    } else if (priority === IngressPriority.HIGH) {
      if (currentQLen < this.maxCapacity) {
        accepted = true;
      } else {
        dropReason = `Ingestion queue saturated at max capacity (${this.maxCapacity})`;
      }
    } else {
      if (currentQLen < this.normalThreshold) {
        accepted = true;
      } else {
        dropReason = `Normal priority shed: queue (${currentQLen}) >= threshold (${this.normalThreshold})`;
      }
    }

    if (!accepted) {
      stats.dropped += 1;
      return new IngressResult({
        accepted: false,
        seqNum: currentSeq,
        swarmId,
        spanId,
        priority,
        status: "DROPPED_SATURATION",
        reason: dropReason,
      });
    }

    // 3. Accepted: Check for Sequence Gap on arrival
    const lastReceived = this.streamReceived.get(streamKey);
    let gapDetected = false;
    let gapRecord: SequenceGap | null = null;

    if (lastReceived !== undefined && currentSeq > lastReceived + 1) {
      const missingCount = currentSeq - (lastReceived + 1);
      gapRecord = new SequenceGap({
        swarmId,
        streamId,
        expectedSeq: lastReceived + 1,
        receivedSeq: currentSeq,
        missingCount,
      });
      let swarmGaps = this.gaps.get(swarmId);
      if (!swarmGaps) {
        swarmGaps = [];
        this.gaps.set(swarmId, swarmGaps);
      }
      swarmGaps.push(gapRecord);
      gapDetected = true;
    }

    this.streamReceived.set(streamKey, currentSeq);
    stats.accepted += 1;
    if (priority === IngressPriority.CRITICAL) {
      stats.criticalAccepted += 1;
    }

    // 4. Record span & parent lineage
    let known = this.knownSpans.get(swarmId);
    if (!known) {
      known = new Set();
      this.knownSpans.set(swarmId, known);
    }
    known.add(spanId);

    let referenced = this.referencedParents.get(swarmId);
    if (!referenced) {
      referenced = new Set();
      this.referencedParents.set(swarmId, referenced);
    }
    for (const p of parentSpanIds) {
      if (p) {
        referenced.add(p);
      }
    }

    spanDict._seq_num = currentSeq;
    spanDict._priority = priority;
    spanDict._stream_id = streamId;

    this.queue.push({ priority, span: spanDict });

    return new IngressResult({
      accepted: true,
      seqNum: currentSeq,
      swarmId,
      spanId,
      priority,
      status: gapDetected ? "GAP_RECORDED" : "ACCEPTED",
      gapDetected,
      gap: gapRecord,
      reason: gapDetected
        ? `Gap recorded (${gapRecord!.missingCount} missing)`
        : "Span accepted for ingestion",
    });
  }

  public drain(batchSize: number = 100): Array<Record<string, unknown>> {
    const items: Array<Record<string, unknown>> = [];
    while (this.queue.length > 0 && items.length < batchSize) {
      const elem = this.queue.shift();
      if (elem) {
        items.push(elem.span);
      }
    }
    return items;
  }

  public completeness(swarmId: string): CompletenessReport {
    const stats = this.getStats(swarmId);
    const totalSubmitted = stats.submitted;
    const totalAccepted = stats.accepted;
    const totalDropped = stats.dropped;
    const critSub = stats.criticalSubmitted;
    const critAcc = stats.criticalAccepted;

    const critRatio = critSub > 0 ? critAcc / critSub : 1.0;
    const lossRatio = totalSubmitted > 0 ? totalDropped / totalSubmitted : 0.0;

    const rawGaps = this.gaps.get(swarmId) || [];
    const gapsDict = rawGaps.map((g) => g.toDict());

    const known = this.knownSpans.get(swarmId) || new Set();
    const referenced = this.referencedParents.get(swarmId) || new Set();
    const missingParents: string[] = [];
    for (const ref of referenced) {
      if (!known.has(ref)) {
        missingParents.push(ref);
      }
    }
    missingParents.sort();

    let status: CompletenessStatusEnum;
    if (totalDropped > 0) {
      status = CompletenessStatus.SATURATED;
    } else if (gapsDict.length > 0 || missingParents.length > 0) {
      status = CompletenessStatus.INCOMPLETE;
    } else {
      status = CompletenessStatus.COMPLETE;
    }

    return new CompletenessReport({
      swarmId,
      status,
      totalSubmitted,
      totalAccepted,
      totalDropped,
      criticalSubmitted: critSub,
      criticalAccepted: critAcc,
      criticalDeliveryRatio: critRatio,
      lossRatio: Number(lossRatio.toFixed(6)),
      sequenceGaps: gapsDict,
      missingCausalParents: missingParents,
    });
  }

  public status(swarmId: string = "default"): Record<string, unknown> {
    const comp = this.completeness(swarmId);
    return {
      queue_length: this.queue.length,
      max_capacity: this.maxCapacity,
      normal_threshold: this.normalThreshold,
      reserve_capacity: this.reserveCapacity,
      swarm_id: swarmId,
      completeness_status: comp.status,
      total_dropped: comp.totalDropped,
      total_gaps: comp.sequenceGaps.length,
      critical_delivery_ratio: comp.criticalDeliveryRatio,
    };
  }

  public reset(): void {
    this.queue = [];
    this.streamSeq.clear();
    this.streamReceived.clear();
    this.gaps.clear();
    this.knownSpans.clear();
    this.referencedParents.clear();
    this.stats.clear();
  }
}

export const globalCausalIngress = new CausalIngress();
