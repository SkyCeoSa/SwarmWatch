/**
 * Autonomous Semantic Schema Migration & Backward-Compatible State Translation Engine (Seam 47 / ADR 0058).
 * In-process, zero-token, bidirectional schema migration and state payload translation.
 */

export const FieldTransformAction = {
  RENAME: "rename",
  CAST_TYPE: "cast_type",
  ADD_DEFAULT: "add_default",
  REMOVE_DEPRECATED: "remove_deprecated",
  NEST_FIELDS: "nest_fields",
  FLATTEN_FIELDS: "flatten_fields",
  COMPUTE_DERIVED: "compute_derived",
} as const;

export type FieldTransformActionType = typeof FieldTransformAction[keyof typeof FieldTransformAction];

export class SchemaVersion {
  public readonly major: number;
  public readonly minor: number;
  public readonly patch: number;
  public readonly prerelease: string;

  constructor(major: number, minor: number = 0, patch: number = 0, prerelease: string = "") {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.prerelease = prerelease;
  }

  public static fromString(versionStr: string): SchemaVersion {
    let clean = (versionStr || "1.0.0").trim().replace(/^v/, "");
    let prerelease = "";
    if (clean.includes("-")) {
      const parts = clean.split("-", 2);
      clean = parts[0];
      prerelease = parts[1] || "";
    }
    const nums = clean.split(".").map((n) => parseInt(n, 10) || 0);
    return new SchemaVersion(nums[0] || 0, nums[1] || 0, nums[2] || 0, prerelease);
  }

  public toString(): string {
    const base = `${this.major}.${this.minor}.${this.patch}`;
    return this.prerelease ? `${base}-${this.prerelease}` : base;
  }

  public compareTo(other: SchemaVersion): number {
    if (this.major !== other.major) return this.major - other.major;
    if (this.minor !== other.minor) return this.minor - other.minor;
    if (this.patch !== other.patch) return this.patch - other.patch;
    if (!this.prerelease && other.prerelease) return 1;
    if (this.prerelease && !other.prerelease) return -1;
    return this.prerelease.localeCompare(other.prerelease);
  }
}

/**
 * Traverses nested object hierarchy along dot-notation path.
 */
function _getPath(data: Record<string, any>, path: string): [boolean, any] {
  if (!path) return [false, undefined];
  const parts = path.split(".");
  let curr: any = data;
  for (const p of parts) {
    if (curr && typeof curr === "object" && p in curr) {
      curr = curr[p];
    } else {
      return [false, undefined];
    }
  }
  return [true, curr];
}

/**
 * Sets nested object hierarchy along dot-notation path, creating intermediate objects.
 */
function _setPath(data: Record<string, any>, path: string, val: any): void {
  if (!path) return;
  const parts = path.split(".");
  let curr: any = data;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i];
    if (!(p in curr) || typeof curr[p] !== "object" || curr[p] === null) {
      curr[p] = {};
    }
    curr = curr[p];
  }
  curr[parts[parts.length - 1]] = val;
}

/**
 * Removes key at nested object path.
 */
function _deletePath(data: Record<string, any>, path: string): void {
  if (!path) return;
  const parts = path.split(".");
  let curr: any = data;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i];
    if (curr && typeof curr === "object" && p in curr) {
      curr = curr[p];
    } else {
      return;
    }
  }
  if (curr && typeof curr === "object" && parts[parts.length - 1] in curr) {
    delete curr[parts[parts.length - 1]];
  }
}

function _deepClone<T>(obj: T): T {
  if (typeof structuredClone === "function") {
    return structuredClone(obj);
  }
  return JSON.parse(JSON.stringify(obj));
}

export class FieldTransformRule {
  public action: FieldTransformActionType;
  public sourcePath: string;
  public targetPath?: string;
  public targetType?: "str" | "int" | "float" | "bool" | "list" | "dict";
  public defaultValue?: any;
  public nestedKeys?: string[];
  public derivedFn?: (data: Record<string, any>) => any;

  constructor(options: {
    action: FieldTransformActionType;
    sourcePath: string;
    targetPath?: string;
    targetType?: "str" | "int" | "float" | "bool" | "list" | "dict";
    defaultValue?: any;
    nestedKeys?: string[];
    derivedFn?: (data: Record<string, any>) => any;
  }) {
    this.action = options.action;
    this.sourcePath = options.sourcePath;
    this.targetPath = options.targetPath;
    this.targetType = options.targetType;
    this.defaultValue = options.defaultValue;
    this.nestedKeys = options.nestedKeys;
    this.derivedFn = options.derivedFn;
  }

  public toDict(): Record<string, any> {
    return {
      action: this.action,
      source_path: this.sourcePath,
      target_path: this.targetPath,
      target_type: this.targetType,
      default_value: this.defaultValue,
      nested_keys: this.nestedKeys,
    };
  }

  public static fromDict(data: Record<string, any>): FieldTransformRule {
    return new FieldTransformRule({
      action: (data.action || "rename") as FieldTransformActionType,
      sourcePath: data.source_path || data.sourcePath || "",
      targetPath: data.target_path || data.targetPath,
      targetType: data.target_type || data.targetType,
      defaultValue: data.default_value !== undefined ? data.default_value : data.defaultValue,
      nestedKeys: data.nested_keys || data.nestedKeys,
    });
  }

  public apply(data: Record<string, any>): void {
    if (this.action === FieldTransformAction.RENAME) {
      const [found, val] = _getPath(data, this.sourcePath);
      if (found && this.targetPath) {
        _deletePath(data, this.sourcePath);
        _setPath(data, this.targetPath, val);
      }
    } else if (this.action === FieldTransformAction.CAST_TYPE) {
      const [found, val] = _getPath(data, this.sourcePath);
      if (found && this.targetType) {
        let casted: any = val;
        try {
          if (this.targetType === "int") casted = parseInt(val, 10);
          else if (this.targetType === "float") casted = parseFloat(val);
          else if (this.targetType === "str") casted = String(val);
          else if (this.targetType === "bool") casted = Boolean(val);
          else if (this.targetType === "list" && !Array.isArray(val)) casted = [val];
        } catch {
          casted = val;
        }
        _setPath(data, this.sourcePath, casted);
      }
    } else if (this.action === FieldTransformAction.ADD_DEFAULT) {
      const [found] = _getPath(data, this.sourcePath);
      if (!found) {
        _setPath(data, this.sourcePath, _deepClone(this.defaultValue));
      }
    } else if (this.action === FieldTransformAction.REMOVE_DEPRECATED) {
      _deletePath(data, this.sourcePath);
    } else if (this.action === FieldTransformAction.NEST_FIELDS) {
      const keys = this.nestedKeys || [];
      const target = this.targetPath || this.sourcePath;
      const gathered: Record<string, any> = {};
      for (const k of keys) {
        const [found, val] = _getPath(data, k);
        if (found) {
          gathered[k] = val;
          _deletePath(data, k);
        }
      }
      _setPath(data, target, gathered);
    } else if (this.action === FieldTransformAction.FLATTEN_FIELDS) {
      const [found, val] = _getPath(data, this.sourcePath);
      if (found && val && typeof val === "object" && !Array.isArray(val)) {
        _deletePath(data, this.sourcePath);
        for (const [k, v] of Object.entries(val)) {
          const targetK = this.targetPath ? `${this.targetPath}.${k}` : k;
          _setPath(data, targetK, v);
        }
      }
    } else if (this.action === FieldTransformAction.COMPUTE_DERIVED) {
      if (this.derivedFn && this.targetPath) {
        const derived = this.derivedFn(data);
        _setPath(data, this.targetPath, derived);
      }
    }
  }
}

export class MigrationStep {
  public sourceVersion: string;
  public targetVersion: string;
  public forwardRules: FieldTransformRule[];
  public backwardRules: FieldTransformRule[];
  public description: string;

  constructor(options: {
    sourceVersion: string;
    targetVersion: string;
    forwardRules?: FieldTransformRule[];
    backwardRules?: FieldTransformRule[];
    description?: string;
  }) {
    this.sourceVersion = options.sourceVersion;
    this.targetVersion = options.targetVersion;
    this.forwardRules = options.forwardRules || [];
    this.backwardRules = options.backwardRules || [];
    this.description = options.description || "";
  }

  public applyForward(payload: Record<string, any>): Record<string, any> {
    const result = _deepClone(payload);
    for (const rule of this.forwardRules) {
      rule.apply(result);
    }
    return result;
  }

  public applyBackward(payload: Record<string, any>): Record<string, any> {
    const result = _deepClone(payload);
    for (const rule of this.backwardRules) {
      rule.apply(result);
    }
    return result;
  }

  public toDict(): Record<string, any> {
    return {
      source_version: this.sourceVersion,
      target_version: this.targetVersion,
      forward_rules: this.forwardRules.map((r) => r.toDict()),
      backward_rules: this.backwardRules.map((r) => r.toDict()),
      description: this.description,
    };
  }

  public static fromDict(data: Record<string, any>): MigrationStep {
    const forward = (data.forward_rules || []).map((r: any) => FieldTransformRule.fromDict(r));
    const backward = (data.backward_rules || []).map((r: any) => FieldTransformRule.fromDict(r));
    return new MigrationStep({
      sourceVersion: data.source_version || data.sourceVersion || "1.0.0",
      targetVersion: data.target_version || data.targetVersion || "1.0.0",
      forwardRules: forward,
      backwardRules: backward,
      description: data.description || "",
    });
  }
}

export interface MigrationPlan {
  sourceVersion: string;
  targetVersion: string;
  steps: Array<{ step: MigrationStep; direction: "forward" | "backward" }>;
  totalSteps: number;
}

export interface MigrationResult {
  sourceVersion: string;
  targetVersion: string;
  transformedPayload: Record<string, any>;
  appliedStepsCount: number;
  appliedPath: string[];
  lookupLatencyNs: number;
  isValid: boolean;
  toDict(): Record<string, any>;
}

export class SchemaMigrationEngine {
  private _versions: Set<string> = new Set();
  private _schemas: Map<string, Record<string, any>> = new Map();
  private _steps: Map<string, MigrationStep> = new Map();
  private _adjacency: Map<string, Array<{ neighbor: string; step: MigrationStep; direction: "forward" | "backward" }>> = new Map();
  private _totalMigrationsExecuted: number = 0;

  public registerVersion(version: string, schemaDef?: Record<string, any>): void {
    const vNorm = SchemaVersion.fromString(version).toString();
    this._versions.add(vNorm);
    if (schemaDef) {
      this._schemas.set(vNorm, schemaDef);
    }
  }

  public registerStep(step: MigrationStep): void {
    const src = SchemaVersion.fromString(step.sourceVersion).toString();
    const tgt = SchemaVersion.fromString(step.targetVersion).toString();
    this._versions.add(src);
    this._versions.add(tgt);

    const stepKey = `${src}->${tgt}`;
    this._steps.set(stepKey, step);

    if (!this._adjacency.has(src)) this._adjacency.set(src, []);
    if (!this._adjacency.has(tgt)) this._adjacency.set(tgt, []);

    // Filter old edges
    const srcEdges = this._adjacency.get(src)!.filter((e) => e.neighbor !== tgt);
    const tgtEdges = this._adjacency.get(tgt)!.filter((e) => e.neighbor !== src);

    srcEdges.push({ neighbor: tgt, step, direction: "forward" });
    if (step.backwardRules && step.backwardRules.length > 0) {
      tgtEdges.push({ neighbor: src, step, direction: "backward" });
    }

    this._adjacency.set(src, srcEdges);
    this._adjacency.set(tgt, tgtEdges);
  }

  public findMigrationPlan(sourceVersion: string, targetVersion: string): MigrationPlan | null {
    const src = SchemaVersion.fromString(sourceVersion).toString();
    const tgt = SchemaVersion.fromString(targetVersion).toString();

    if (src === tgt) {
      return {
        sourceVersion: src,
        targetVersion: tgt,
        steps: [],
        totalSteps: 0,
      };
    }

    const queue: Array<{ current: string; path: Array<{ step: MigrationStep; direction: "forward" | "backward" }> }> = [
      { current: src, path: [] },
    ];
    const visited = new Set<string>([src]);

    while (queue.length > 0) {
      const { current, path } = queue.shift()!;
      if (current === tgt) {
        return {
          sourceVersion: src,
          targetVersion: tgt,
          steps: path,
          totalSteps: path.length,
        };
      }

      const neighbors = this._adjacency.get(current) || [];
      for (const edge of neighbors) {
        if (!visited.has(edge.neighbor)) {
          visited.add(edge.neighbor);
          queue.push({
            current: edge.neighbor,
            path: [...path, { step: edge.step, direction: edge.direction }],
          });
        }
      }
    }

    return null;
  }

  public planMigration(sourceVersion: string, targetVersion: string): MigrationPlan | null {
    return this.findMigrationPlan(sourceVersion, targetVersion);
  }

  public migrate(
    payload: Record<string, any>,
    sourceVersion: string,
    targetVersion: string
  ): MigrationResult {
    const startTime = process.hrtime.bigint ? process.hrtime.bigint() : BigInt(Date.now() * 1000000);
    const src = SchemaVersion.fromString(sourceVersion).toString();
    const tgt = SchemaVersion.fromString(targetVersion).toString();

    const plan = this.findMigrationPlan(src, tgt);
    if (!plan) {
      const endTime = process.hrtime.bigint ? process.hrtime.bigint() : BigInt(Date.now() * 1000000);
      const latencyNs = Number(endTime - startTime);
      return {
        sourceVersion: src,
        targetVersion: tgt,
        transformedPayload: _deepClone(payload),
        appliedStepsCount: 0,
        appliedPath: [src],
        lookupLatencyNs: latencyNs,
        isValid: false,
        toDict() {
          return {
            source_version: this.sourceVersion,
            target_version: this.targetVersion,
            transformed_payload: this.transformedPayload,
            applied_steps_count: this.appliedStepsCount,
            applied_path: this.appliedPath,
            lookup_latency_ns: this.lookupLatencyNs,
            is_valid: this.isValid,
          };
        },
      };
    }

    let currentData = _deepClone(payload);
    const appliedPath = [src];

    for (const stepItem of plan.steps) {
      if (stepItem.direction === "forward") {
        currentData = stepItem.step.applyForward(currentData);
        appliedPath.push(stepItem.step.targetVersion);
      } else {
        currentData = stepItem.step.applyBackward(currentData);
        appliedPath.push(stepItem.step.sourceVersion);
      }
    }

    this._totalMigrationsExecuted += 1;
    const { isValid } = this.validatePayload(currentData, tgt);
    const endTime = process.hrtime.bigint ? process.hrtime.bigint() : BigInt(Date.now() * 1000000);
    const latencyNs = Number(endTime - startTime);

    return {
      sourceVersion: src,
      targetVersion: tgt,
      transformedPayload: currentData,
      appliedStepsCount: plan.totalSteps,
      appliedPath,
      lookupLatencyNs: latencyNs,
      isValid,
      toDict() {
        return {
          source_version: this.sourceVersion,
          target_version: this.targetVersion,
          transformed_payload: this.transformedPayload,
          applied_steps_count: this.appliedStepsCount,
          applied_path: this.appliedPath,
          lookup_latency_ns: this.lookupLatencyNs,
          is_valid: this.isValid,
        };
      },
    };
  }

  public validatePayload(payload: Record<string, any>, version: string): { isValid: boolean; errors: string[] } {
    const vNorm = SchemaVersion.fromString(version).toString();
    const schema = this._schemas.get(vNorm);
    const errors: string[] = [];

    if (!schema) {
      return { isValid: true, errors: [] };
    }

    const required = (schema.required as string[]) || [];
    for (const rf of required) {
      const [found] = _getPath(payload, rf);
      if (!found) {
        errors.push(`Missing required field: '${rf}' for schema ${vNorm}`);
      }
    }

    return { isValid: errors.length === 0, errors };
  }

  public getStatus(): Record<string, any> {
    const stepsSummary: any[] = [];
    for (const [key, s] of this._steps.entries()) {
      stepsSummary.push({
        key,
        source: s.sourceVersion,
        target: s.targetVersion,
        forward_rules_count: s.forwardRules.length,
        backward_rules_count: s.backwardRules.length,
        description: s.description,
      });
    }

    return {
      registered_versions: Array.from(this._versions).sort(),
      total_versions: this._versions.size,
      total_steps: this._steps.size,
      steps: stepsSummary,
      total_migrations_executed: this._totalMigrationsExecuted,
    };
  }

  public getVersions(): string[] {
    return Array.from(this._versions).sort();
  }
}

// Global process singleton
export const globalSchemaMigrationEngine = new SchemaMigrationEngine();

// Register standard canonical versions & default links
globalSchemaMigrationEngine.registerVersion("1.0.0", { required: ["agent", "tokens"] });
globalSchemaMigrationEngine.registerVersion("1.1.0", { required: ["agent_id", "tokens", "turn_status"] });
globalSchemaMigrationEngine.registerVersion("2.0.0", { required: ["agent_id", "resource_usage", "turn_status"] });

// Step 1: 1.0.0 <-> 1.1.0
globalSchemaMigrationEngine.registerStep(
  new MigrationStep({
    sourceVersion: "1.0.0",
    targetVersion: "1.1.0",
    forwardRules: [
      new FieldTransformRule({ action: FieldTransformAction.RENAME, sourcePath: "agent", targetPath: "agent_id" }),
      new FieldTransformRule({ action: FieldTransformAction.ADD_DEFAULT, sourcePath: "turn_status", defaultValue: "completed" }),
      new FieldTransformRule({ action: FieldTransformAction.CAST_TYPE, sourcePath: "tokens", targetType: "int" }),
    ],
    backwardRules: [
      new FieldTransformRule({ action: FieldTransformAction.RENAME, sourcePath: "agent_id", targetPath: "agent" }),
      new FieldTransformRule({ action: FieldTransformAction.REMOVE_DEPRECATED, sourcePath: "turn_status" }),
    ],
    description: "Rename agent to agent_id, add turn_status default, enforce int tokens",
  })
);

// Step 2: 1.1.0 <-> 2.0.0
globalSchemaMigrationEngine.registerStep(
  new MigrationStep({
    sourceVersion: "1.1.0",
    targetVersion: "2.0.0",
    forwardRules: [
      new FieldTransformRule({
        action: FieldTransformAction.NEST_FIELDS,
        sourcePath: "",
        targetPath: "resource_usage",
        nestedKeys: ["tokens", "cost_dollars"],
      }),
    ],
    backwardRules: [
      new FieldTransformRule({
        action: FieldTransformAction.FLATTEN_FIELDS,
        sourcePath: "resource_usage",
        targetPath: "",
      }),
    ],
    description: "Nest tokens and cost_dollars into resource_usage dictionary",
  })
);
