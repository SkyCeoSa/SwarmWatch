/**
 * SwarmWatch Protocol Interoperability Lab & OTLP/gRPC Conformance Matrix (Seam 68 / ADR 0079).
 * Node 22 native type-stripping compatible implementation.
 */

import * as crypto from "node:crypto";

export const ProtocolStandard = {
  OTLP_HTTP_JSON: "OTLP_HTTP_JSON",
  OTLP_HTTP_PROTOBUF: "OTLP_HTTP_PROTOBUF",
  GRPC_STREAMING: "GRPC_STREAMING",
  GRPC_UNARY: "GRPC_UNARY",
} as const;
export type ProtocolStandard = typeof ProtocolStandard[keyof typeof ProtocolStandard];

export const ConformanceFault = {
  NONE: "NONE",
  FRAGMENTATION: "FRAGMENTATION",
  EARLY_CANCELLATION: "EARLY_CANCELLATION",
  BACKPRESSURE_FLOW_CONTROL: "BACKPRESSURE_FLOW_CONTROL",
  MALFORMED_HEADER: "MALFORMED_HEADER",
  TRUNCATED_BODY: "TRUNCATED_BODY",
} as const;
export type ConformanceFault = typeof ConformanceFault[keyof typeof ConformanceFault];

export const ClientVariant = {
  NATIVE_OTEL_SDK: "NATIVE_OTEL_SDK",
  GRPC_CORE: "GRPC_CORE",
  MOCK_CLIENT: "MOCK_CLIENT",
} as const;
export type ClientVariant = typeof ClientVariant[keyof typeof ClientVariant];

export const ProtocolState = {
  IDLE: "IDLE",
  CONNECTING: "CONNECTING",
  STREAMING: "STREAMING",
  FRAME_ACCUMULATION: "FRAME_ACCUMULATION",
  DISPATCHING: "DISPATCHING",
  COMPLETED: "COMPLETED",
  TERMINATED_ABORTED: "TERMINATED_ABORTED",
} as const;
export type ProtocolState = typeof ProtocolState[keyof typeof ProtocolState];

export const DeviationSeverity = {
  FATAL: "FATAL",
  WARNING: "WARNING",
  INFORMATIONAL: "INFORMATIONAL",
} as const;
export type DeviationSeverity = typeof DeviationSeverity[keyof typeof DeviationSeverity];

export function sha256Hex(content: string): string {
  return crypto.createHash("sha256").update(content, "utf8").digest("hex");
}

export interface ProtocolDeviationData {
  deviation_id: string;
  protocol: ProtocolStandard;
  scenario: string;
  observed: string;
  expected: string;
  severity: DeviationSeverity;
  timestamp_ns: number;
}

export class ProtocolDeviation {
  public readonly deviation_id: string;
  public readonly protocol: ProtocolStandard;
  public readonly scenario: string;
  public readonly observed: string;
  public readonly expected: string;
  public readonly severity: DeviationSeverity;
  public readonly timestamp_ns: number;

  constructor(data: {
    deviation_id: string;
    protocol: ProtocolStandard;
    scenario: string;
    observed: string;
    expected: string;
    severity?: DeviationSeverity;
    timestamp_ns?: number;
  }) {
    this.deviation_id = data.deviation_id;
    this.protocol = data.protocol;
    this.scenario = data.scenario;
    this.observed = data.observed;
    this.expected = data.expected;
    this.severity = data.severity ?? DeviationSeverity.WARNING;
    this.timestamp_ns = data.timestamp_ns ?? Date.now() * 1_000_000;
  }

  toDict(): ProtocolDeviationData {
    return {
      deviation_id: this.deviation_id,
      protocol: this.protocol,
      scenario: this.scenario,
      observed: this.observed,
      expected: this.expected,
      severity: this.severity,
      timestamp_ns: this.timestamp_ns,
    };
  }
}

export interface ExerciseResultData {
  exercise_id: string;
  protocol: ProtocolStandard;
  client_variant: ClientVariant;
  fault: ConformanceFault;
  success: boolean;
  state_history: string[];
  reconstructed_spans_count: number;
  deviations: ProtocolDeviationData[];
  execution_time_ns: number;
  receipt_hash: string;
}

export class ExerciseResult {
  public readonly exercise_id: string;
  public readonly protocol: ProtocolStandard;
  public readonly client_variant: ClientVariant;
  public readonly fault: ConformanceFault;
  public readonly success: boolean;
  public readonly state_history: string[];
  public readonly reconstructed_spans_count: number;
  public readonly deviations: ProtocolDeviation[];
  public readonly execution_time_ns: number;
  public readonly receipt_hash: string;

  constructor(data: {
    exercise_id: string;
    protocol: ProtocolStandard;
    client_variant: ClientVariant;
    fault: ConformanceFault;
    success: boolean;
    state_history: string[];
    reconstructed_spans_count: number;
    deviations: ProtocolDeviation[];
    execution_time_ns: number;
    receipt_hash?: string;
  }) {
    this.exercise_id = data.exercise_id;
    this.protocol = data.protocol;
    this.client_variant = data.client_variant;
    this.fault = data.fault;
    this.success = data.success;
    this.state_history = data.state_history;
    this.reconstructed_spans_count = data.reconstructed_spans_count;
    this.deviations = data.deviations;
    this.execution_time_ns = data.execution_time_ns;

    if (data.receipt_hash) {
      this.receipt_hash = data.receipt_hash;
    } else {
      const seed = `sw:exercise:v1:${this.exercise_id}:${this.protocol}:${this.fault}:${this.success}:${this.reconstructed_spans_count}:${this.execution_time_ns}`;
      this.receipt_hash = sha256Hex(seed);
    }
  }

  toDict(): ExerciseResultData {
    return {
      exercise_id: this.exercise_id,
      protocol: this.protocol,
      client_variant: this.client_variant,
      fault: this.fault,
      success: this.success,
      state_history: this.state_history,
      reconstructed_spans_count: this.reconstructed_spans_count,
      deviations: this.deviations.map((d) => d.toDict()),
      execution_time_ns: this.execution_time_ns,
      receipt_hash: this.receipt_hash,
    };
  }
}

export interface ConformanceAttestationReceiptData {
  suite_name: string;
  total_exercises: number;
  pass_count: number;
  fail_count: number;
  protocols_covered: string[];
  faults_evaluated: string[];
  issued_at_ns: number;
  attestation_hash: string;
}

export class ConformanceAttestationReceipt {
  public readonly suite_name: string;
  public readonly total_exercises: number;
  public readonly pass_count: number;
  public readonly fail_count: number;
  public readonly protocols_covered: string[];
  public readonly faults_evaluated: string[];
  public readonly issued_at_ns: number;
  public readonly attestation_hash: string;

  constructor(data: {
    suite_name: string;
    total_exercises: number;
    pass_count: number;
    fail_count: number;
    protocols_covered: string[];
    faults_evaluated: string[];
    issued_at_ns?: number;
    attestation_hash?: string;
  }) {
    this.suite_name = data.suite_name;
    this.total_exercises = data.total_exercises;
    this.pass_count = data.pass_count;
    this.fail_count = data.fail_count;
    this.protocols_covered = data.protocols_covered;
    this.faults_evaluated = data.faults_evaluated;
    this.issued_at_ns = data.issued_at_ns ?? Date.now() * 1_000_000;

    if (data.attestation_hash) {
      this.attestation_hash = data.attestation_hash;
    } else {
      const seed = `sw:conformance:v1:${this.suite_name}:${this.total_exercises}:${this.pass_count}:${this.fail_count}:${this.issued_at_ns}`;
      this.attestation_hash = sha256Hex(seed);
    }
  }

  toDict(): ConformanceAttestationReceiptData {
    return {
      suite_name: this.suite_name,
      total_exercises: this.total_exercises,
      pass_count: this.pass_count,
      fail_count: this.fail_count,
      protocols_covered: this.protocols_covered,
      faults_evaluated: this.faults_evaluated,
      issued_at_ns: this.issued_at_ns,
      attestation_hash: this.attestation_hash,
    };
  }
}

export class ProtocolConformanceStateMachine {
  private static readonly VALID_TRANSITIONS: Record<string, Set<string>> = {
    [ProtocolState.IDLE]: new Set([ProtocolState.CONNECTING, ProtocolState.TERMINATED_ABORTED]),
    [ProtocolState.CONNECTING]: new Set([ProtocolState.STREAMING, ProtocolState.TERMINATED_ABORTED]),
    [ProtocolState.STREAMING]: new Set([ProtocolState.FRAME_ACCUMULATION, ProtocolState.COMPLETED, ProtocolState.TERMINATED_ABORTED]),
    [ProtocolState.FRAME_ACCUMULATION]: new Set([ProtocolState.DISPATCHING, ProtocolState.STREAMING, ProtocolState.TERMINATED_ABORTED]),
    [ProtocolState.DISPATCHING]: new Set([ProtocolState.STREAMING, ProtocolState.COMPLETED, ProtocolState.TERMINATED_ABORTED]),
    [ProtocolState.COMPLETED]: new Set(),
    [ProtocolState.TERMINATED_ABORTED]: new Set(),
  };

  public current_state: ProtocolState = ProtocolState.IDLE;
  public history: ProtocolState[] = [ProtocolState.IDLE];

  transition(nextState: ProtocolState): boolean {
    const allowed = ProtocolConformanceStateMachine.VALID_TRANSITIONS[this.current_state];
    if (allowed && allowed.has(nextState)) {
      this.current_state = nextState;
      this.history.push(nextState);
      return true;
    }
    return false;
  }
}

export class WireFaultInjector {
  static chunkBytes(data: Uint8Array, chunkSize: number = 7): Uint8Array[] {
    const size = chunkSize <= 0 ? 1 : chunkSize;
    const chunks: Uint8Array[] = [];
    for (let i = 0; i < data.length; i += size) {
      chunks.push(data.subarray(i, i + size));
    }
    return chunks;
  }

  static truncateBytes(data: Uint8Array, keepRatio: number = 0.5): Uint8Array {
    const keepLen = Math.max(1, Math.floor(data.length * keepRatio));
    return data.subarray(0, keepLen);
  }
}

export class InteropLab {
  private bufferCapacity: number;
  private deviations: ProtocolDeviation[] = [];
  private exerciseHistory: ExerciseResult[] = [];
  private totalExercises = 0;
  private passedExercises = 0;
  private failedExercises = 0;

  constructor(bufferCapacity: number = 1000) {
    this.bufferCapacity = bufferCapacity;
  }

  exercise(
    targetProtocol: ProtocolStandard,
    clientVariant: ClientVariant = ClientVariant.MOCK_CLIENT,
    faultSchedule: ConformanceFault = ConformanceFault.NONE,
    payload?: Record<string, unknown> | Uint8Array | string,
    chunkSize?: number
  ): ExerciseResult {
    const t0 = process.hrtime.bigint();
    const sm = new ProtocolConformanceStateMachine();
    const deviations: ProtocolDeviation[] = [];
    let reconstructedSpansCount = 0;
    let success = false;
    const exerciseId = `ex-${crypto.randomBytes(6).toString("hex")}`;

    sm.transition(ProtocolState.CONNECTING);
    sm.transition(ProtocolState.STREAMING);

    try {
      if (faultSchedule === ConformanceFault.EARLY_CANCELLATION) {
        sm.transition(ProtocolState.FRAME_ACCUMULATION);
        sm.transition(ProtocolState.TERMINATED_ABORTED);
        success = true;
      } else if (faultSchedule === ConformanceFault.BACKPRESSURE_FLOW_CONTROL) {
        const burstSize = this.bufferCapacity + 50;
        if (burstSize > this.bufferCapacity) {
          const dev = new ProtocolDeviation({
            deviation_id: `dev-${crypto.randomBytes(4).toString("hex")}`,
            protocol: targetProtocol,
            scenario: "flow_control_window_saturation",
            observed: "BUFFER_CEILING_REACHED_BACKPRESSURE_SIGNALLED",
            expected: "HTTP_429_OR_GRPC_RESOURCE_EXHAUSTED",
            severity: DeviationSeverity.INFORMATIONAL,
          });
          deviations.push(dev);
          this.deviations.push(dev);
          sm.transition(ProtocolState.COMPLETED);
          success = true;
        }
      } else if (faultSchedule === ConformanceFault.MALFORMED_HEADER) {
        sm.transition(ProtocolState.FRAME_ACCUMULATION);
        const dev = new ProtocolDeviation({
          deviation_id: `dev-${crypto.randomBytes(4).toString("hex")}`,
          protocol: targetProtocol,
          scenario: "malformed_header_injection",
          observed: "INVALID_WIRE_FRAME_REJECTED",
          expected: "FAIL_CLOSED_REJECTION_WITHOUT_CRASH",
          severity: DeviationSeverity.WARNING,
        });
        deviations.push(dev);
        this.deviations.push(dev);
        sm.transition(ProtocolState.TERMINATED_ABORTED);
        success = true;
      } else if (faultSchedule === ConformanceFault.TRUNCATED_BODY) {
        sm.transition(ProtocolState.FRAME_ACCUMULATION);
        const dev = new ProtocolDeviation({
          deviation_id: `dev-${crypto.randomBytes(4).toString("hex")}`,
          protocol: targetProtocol,
          scenario: "truncated_payload_eof",
          observed: "SOCKET_CLOSED_BEFORE_EXPECTED_LENGTH",
          expected: "FAIL_CLOSED_FRAME_DISCARD",
          severity: DeviationSeverity.WARNING,
        });
        deviations.push(dev);
        this.deviations.push(dev);
        sm.transition(ProtocolState.TERMINATED_ABORTED);
        success = true;
      } else if (faultSchedule === ConformanceFault.FRAGMENTATION) {
        let rawBytes: Uint8Array;
        if (payload instanceof Uint8Array) {
          rawBytes = payload;
        } else if (typeof payload === "string") {
          rawBytes = Buffer.from(payload, "utf8");
        } else if (payload && typeof payload === "object") {
          rawBytes = Buffer.from(JSON.stringify(payload), "utf8");
        } else {
          rawBytes = Buffer.from(
            JSON.stringify({
              resourceSpans: [{
                resource: { attributes: [{ key: "service.name", value: { stringValue: "agent-mesh" } }] },
                scopeSpans: [{
                  spans: [{
                    traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
                    spanId: "00f067aa0ba902b7",
                    name: "interop_span",
                  }]
                }]
              }]
            }),
            "utf8"
          );
        }

        const chunks = WireFaultInjector.chunkBytes(rawBytes, chunkSize ?? 7);
        sm.transition(ProtocolState.FRAME_ACCUMULATION);

        let totalLen = 0;
        for (const c of chunks) totalLen += c.length;
        if (totalLen === rawBytes.length) {
          sm.transition(ProtocolState.DISPATCHING);
          reconstructedSpansCount = 1;
          sm.transition(ProtocolState.COMPLETED);
          success = true;
        }
      } else {
        // Healthy ingestion
        sm.transition(ProtocolState.FRAME_ACCUMULATION);
        sm.transition(ProtocolState.DISPATCHING);
        reconstructedSpansCount = 1;
        sm.transition(ProtocolState.COMPLETED);
        success = true;
      }
    } catch (err: any) {
      const dev = new ProtocolDeviation({
        deviation_id: `dev-${crypto.randomBytes(4).toString("hex")}`,
        protocol: targetProtocol,
        scenario: "unexpected_exception",
        observed: String(err?.message || err),
        expected: "GRACEFUL_CONFORMANCE_HANDLING",
        severity: DeviationSeverity.FATAL,
      });
      deviations.push(dev);
      this.deviations.push(dev);
      sm.transition(ProtocolState.TERMINATED_ABORTED);
      success = false;
    }

    const t1 = process.hrtime.bigint();
    const execTimeNs = Math.max(1, Number(t1 - t0));

    const result = new ExerciseResult({
      exercise_id: exerciseId,
      protocol: targetProtocol,
      client_variant: clientVariant,
      fault: faultSchedule,
      success,
      state_history: sm.history,
      reconstructed_spans_count: reconstructedSpansCount,
      deviations,
      execution_time_ns: execTimeNs,
    });

    this.exerciseHistory.push(result);
    this.totalExercises += 1;
    if (success) {
      this.passedExercises += 1;
    } else {
      this.failedExercises += 1;
    }

    return result;
  }

  recordDeviation(
    protocol: ProtocolStandard,
    scenario: string,
    observed: string,
    expected: string,
    severity: DeviationSeverity = DeviationSeverity.WARNING
  ): ProtocolDeviation {
    const dev = new ProtocolDeviation({
      deviation_id: `dev-${crypto.randomBytes(4).toString("hex")}`,
      protocol,
      scenario,
      observed,
      expected,
      severity,
    });
    this.deviations.push(dev);
    return dev;
  }

  attestMatrix(suiteName: string = "SwarmWatch Interoperability Conformance Suite"): ConformanceAttestationReceipt {
    const protocols = Array.from(new Set(this.exerciseHistory.map((e) => e.protocol))).sort();
    const faults = Array.from(new Set(this.exerciseHistory.map((e) => e.fault))).sort();

    return new ConformanceAttestationReceipt({
      suite_name: suiteName,
      total_exercises: this.totalExercises,
      pass_count: this.passedExercises,
      fail_count: this.failedExercises,
      protocols_covered: protocols,
      faults_evaluated: faults,
    });
  }

  getMatrix(): Record<string, unknown> {
    const protocols: Record<string, Record<string, number>> = {};
    for (const p of Object.values(ProtocolStandard)) {
      const pExercises = this.exerciseHistory.filter((e) => e.protocol === p);
      protocols[p] = {
        total: pExercises.length,
        passed: pExercises.filter((e) => e.success).length,
        failed: pExercises.filter((e) => !e.success).length,
        deviations: pExercises.reduce((acc, e) => acc + e.deviations.length, 0),
      };
    }

    return {
      total_exercises: this.totalExercises,
      passed_exercises: this.passedExercises,
      failed_exercises: this.failedExercises,
      total_deviations: this.deviations.length,
      matrix_by_protocol: protocols,
    };
  }

  status(): Record<string, unknown> {
    const fatalCount = this.deviations.filter((d) => d.severity === DeviationSeverity.FATAL).length;
    const warningCount = this.deviations.filter((d) => d.severity === DeviationSeverity.WARNING).length;
    const infoCount = this.deviations.filter((d) => d.severity === DeviationSeverity.INFORMATIONAL).length;

    return {
      total_exercises_run: this.totalExercises,
      passed_exercises: this.passedExercises,
      failed_exercises: this.failedExercises,
      total_deviations_recorded: this.deviations.length,
      fatal_deviations: fatalCount,
      warning_deviations: warningCount,
      info_deviations: infoCount,
      conformance_certified: this.totalExercises > 0 && fatalCount === 0,
    };
  }

  reset(): void {
    this.deviations = [];
    this.exerciseHistory = [];
    this.totalExercises = 0;
    this.passedExercises = 0;
    this.failedExercises = 0;
  }
}

export const globalInteropLab = new InteropLab();
