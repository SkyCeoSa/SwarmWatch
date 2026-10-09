/**
 * SwarmWatch Verifiable Release Provenance & Signed Artifact DAG (Seam 69 / ADR 0080).
 *
 * Implements SLSA v1.0 and in-toto compliant provenance pipeline with
 * DAG cycle detection, canonical SHA-256 artifact digests, and ECDSA NIST P-256 signing.
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  WitnessKeyManager,
  base64UrlEncode,
  base64UrlDecode,
  sha256Digest,
} from "./root_witness.ts";
import { CanonicalCodec } from "./canonical.ts";

// ==============================================================================
// 1. Enums & Custom Exceptions
// ==============================================================================

export class ProvenanceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProvenanceError";
  }
}

export class ProvenanceCyclicDependencyError extends ProvenanceError {
  constructor(message: string) {
    super(message);
    this.name = "ProvenanceCyclicDependencyError";
  }
}

export class ProvenanceVerificationError extends ProvenanceError {
  constructor(message: string) {
    super(message);
    this.name = "ProvenanceVerificationError";
  }
}

export const ProvenanceStepType = {
  SOURCE_CHECKOUT: "SOURCE_CHECKOUT",
  ENVIRONMENT_LOCK: "ENVIRONMENT_LOCK",
  TEST_EXECUTION: "TEST_EXECUTION",
  CRAFT_AUDIT: "CRAFT_AUDIT",
  CONFORMANCE_ATTEST: "CONFORMANCE_ATTEST",
  BUILD_PACKAGING: "BUILD_PACKAGING",
  RELEASE_GATE: "RELEASE_GATE",
} as const;
export type ProvenanceStepType = typeof ProvenanceStepType[keyof typeof ProvenanceStepType];

// ==============================================================================
// 2. Artifact Digest & Provenance Step Classes
// ==============================================================================

export interface ArtifactDigestData {
  path: string;
  sha256: string;
  byte_size: number;
  mime_type: string;
}

export class ArtifactDigest {
  public readonly path: string;
  public readonly sha256: string;
  public readonly byte_size: number;
  public readonly mime_type: string;

  constructor(path: string, sha256: string, byte_size: number, mime_type: string = "application/octet-stream") {
    this.path = path;
    this.sha256 = sha256;
    this.byte_size = byte_size;
    this.mime_type = mime_type;
  }

  public static fromFile(filePath: string, relativeTo?: string): ArtifactDigest {
    if (!fs.existsSync(filePath)) {
      throw new Error(`Artifact not found: ${filePath}`);
    }

    const stat = fs.statSync(filePath);
    if (!stat.isFile()) {
      throw new Error(`Path is not a regular file: ${filePath}`);
    }

    const fileBuffer = fs.readFileSync(filePath);
    const hash = crypto.createHash("sha256").update(fileBuffer).digest("hex");

    let displayPath = filePath;
    if (relativeTo) {
      displayPath = path.relative(relativeTo, filePath);
    }
    displayPath = displayPath.replace(/\\/g, "/");

    let mime = "application/octet-stream";
    if (displayPath.endsWith(".whl")) {
      mime = "application/x-wheel+zip";
    } else if (displayPath.endsWith(".tgz") || displayPath.endsWith(".tar.gz")) {
      mime = "application/gzip";
    } else if (displayPath.endsWith(".html")) {
      mime = "text/html";
    } else if (displayPath.endsWith(".json")) {
      mime = "application/json";
    } else if (displayPath.endsWith(".py")) {
      mime = "text/x-python";
    } else if (displayPath.endsWith(".ts") || displayPath.endsWith(".js")) {
      mime = "application/javascript";
    }

    return new ArtifactDigest(displayPath, hash, stat.size, mime);
  }

  public toDict(): ArtifactDigestData {
    return {
      path: this.path,
      sha256: this.sha256,
      byte_size: this.byte_size,
      mime_type: this.mime_type,
    };
  }

  public static fromDict(data: ArtifactDigestData): ArtifactDigest {
    return new ArtifactDigest(data.path, data.sha256, data.byte_size, data.mime_type);
  }
}

export interface ProvenanceStepData {
  step_id: string;
  step_type: string;
  parent_step_ids: string[];
  inputs: ArtifactDigestData[];
  outputs: ArtifactDigestData[];
  metadata: Record<string, unknown>;
  timestamp_ns: number;
  step_digest: string;
}

export class ProvenanceStep {
  public readonly step_id: string;
  public readonly step_type: ProvenanceStepType;
  public readonly parent_step_ids: string[];
  public readonly inputs: ArtifactDigest[];
  public readonly outputs: ArtifactDigest[];
  public readonly metadata: Record<string, unknown>;
  public readonly timestamp_ns: number;
  public readonly step_digest: string;

  constructor(
    step_id: string,
    step_type: ProvenanceStepType,
    parent_step_ids: string[] = [],
    inputs: ArtifactDigest[] = [],
    outputs: ArtifactDigest[] = [],
    metadata: Record<string, unknown> = {},
    timestamp_ns?: number,
    step_digest?: string
  ) {
    this.step_id = step_id;
    this.step_type = step_type;
    this.parent_step_ids = parent_step_ids;
    this.inputs = inputs;
    this.outputs = outputs;
    this.metadata = metadata;
    this.timestamp_ns = timestamp_ns ?? Date.now() * 1_000_000;
    this.step_digest = step_digest ?? this.computeDigest();
  }

  public computeDigest(): string {
    const canonicalObj = {
      step_id: this.step_id,
      step_type: this.step_type,
      parent_step_ids: [...this.parent_step_ids].sort(),
      inputs: [...this.inputs].sort((a, b) => a.path.localeCompare(b.path)).map((d) => d.toDict()),
      outputs: [...this.outputs].sort((a, b) => a.path.localeCompare(b.path)).map((d) => d.toDict()),
      metadata: this.metadata,
    };
    const encoded = CanonicalCodec.canonicalize(canonicalObj);
    return sha256Digest(`sw:prov:step:v1:${encoded}`);
  }

  public toDict(): ProvenanceStepData {
    return {
      step_id: this.step_id,
      step_type: this.step_type,
      parent_step_ids: this.parent_step_ids,
      inputs: this.inputs.map((d) => d.toDict()),
      outputs: this.outputs.map((d) => d.toDict()),
      metadata: this.metadata,
      timestamp_ns: this.timestamp_ns,
      step_digest: this.step_digest,
    };
  }

  public static fromDict(data: ProvenanceStepData): ProvenanceStep {
    return new ProvenanceStep(
      data.step_id,
      data.step_type as ProvenanceStepType,
      data.parent_step_ids || [],
      (data.inputs || []).map((d) => ArtifactDigest.fromDict(d)),
      (data.outputs || []).map((d) => ArtifactDigest.fromDict(d)),
      data.metadata || {},
      data.timestamp_ns,
      data.step_digest
    );
  }
}

// ==============================================================================
// 3. Provenance DAG & Directed Acyclic Structure
// ==============================================================================

export class ProvenanceDag {
  private readonly _steps: Map<string, ProvenanceStep> = new Map();

  public addStep(step: ProvenanceStep): void {
    if (this._steps.has(step.step_id)) {
      throw new ProvenanceError(`Duplicate step ID in DAG: ${step.step_id}`);
    }

    for (const parentId of step.parent_step_ids) {
      if (!this._steps.has(parentId)) {
        throw new ProvenanceError(`Missing parent step '${parentId}' for step '${step.step_id}'`);
      }
    }

    this._steps.set(step.step_id, step);
    // Validate that graph remains strictly acyclic
    this.topologicalSort();
  }

  public getStep(stepId: string): ProvenanceStep | undefined {
    return this._steps.get(stepId);
  }

  public get steps(): ProvenanceStep[] {
    return Array.from(this._steps.values());
  }

  public topologicalSort(): ProvenanceStep[] {
    const inDegree: Map<string, number> = new Map();
    const adj: Map<string, string[]> = new Map();

    for (const key of this._steps.keys()) {
      inDegree.set(key, 0);
      adj.set(key, []);
    }

    for (const [stepId, step] of this._steps.entries()) {
      for (const parentId of step.parent_step_ids) {
        if (adj.has(parentId)) {
          adj.get(parentId)!.push(stepId);
          inDegree.set(stepId, (inDegree.get(stepId) || 0) + 1);
        }
      }
    }

    const queue: string[] = [];
    for (const [key, deg] of inDegree.entries()) {
      if (deg === 0) {
        queue.push(key);
      }
    }

    const sorted: ProvenanceStep[] = [];
    while (queue.length > 0) {
      const curr = queue.shift()!;
      sorted.push(this._steps.get(curr)!);

      for (const neighbor of adj.get(curr) || []) {
        const newDeg = (inDegree.get(neighbor) || 1) - 1;
        inDegree.set(neighbor, newDeg);
        if (newDeg === 0) {
          queue.push(neighbor);
        }
      }
    }

    if (sorted.length !== this._steps.size) {
      throw new ProvenanceCyclicDependencyError("Cyclic dependency detected in provenance DAG");
    }

    return sorted;
  }

  public computeDagHash(): string {
    const sorted = this.topologicalSort();
    const digests = sorted.map((s) => s.step_digest).join(":");
    return sha256Digest(`sw:prov:dag:v1:${digests}`);
  }

  public toDict(): Record<string, unknown> {
    return {
      dag_hash: this.computeDagHash(),
      step_count: this._steps.size,
      steps: this.topologicalSort().map((s) => s.toDict()),
    };
  }
}

// ==============================================================================
// 4. Signed Provenance Manifest & Verification Result
// ==============================================================================

export interface SignedProvenanceManifestData {
  manifest_id: string;
  spec_version: string;
  builder_id: string;
  git_commit: string;
  git_branch: string;
  dag_root_hash: string;
  steps: ProvenanceStepData[];
  materials: ArtifactDigestData[];
  products: ArtifactDigestData[];
  issued_at_ns: number;
  signature: string;
  public_key_b64: string;
}

export class SignedProvenanceManifest {
  public readonly manifest_id: string;
  public readonly spec_version: string;
  public readonly builder_id: string;
  public readonly git_commit: string;
  public readonly git_branch: string;
  public readonly dag_root_hash: string;
  public readonly steps: ProvenanceStepData[];
  public readonly materials: ArtifactDigestData[];
  public readonly products: ArtifactDigestData[];
  public readonly issued_at_ns: number;
  public signature: string;
  public public_key_b64: string;

  constructor(data: {
    manifest_id: string;
    spec_version?: string;
    builder_id?: string;
    git_commit?: string;
    git_branch?: string;
    dag_root_hash?: string;
    steps?: ProvenanceStepData[];
    materials?: ArtifactDigestData[];
    products?: ArtifactDigestData[];
    issued_at_ns?: number;
    signature?: string;
    public_key_b64?: string;
  }) {
    this.manifest_id = data.manifest_id;
    this.spec_version = data.spec_version ?? "slsa/v1.0";
    this.builder_id = data.builder_id ?? "https://github.com/SkyCeoSa/SwarmWatch/actions/runner";
    this.git_commit = data.git_commit ?? "";
    this.git_branch = data.git_branch ?? "main";
    this.dag_root_hash = data.dag_root_hash ?? "";
    this.steps = data.steps ?? [];
    this.materials = data.materials ?? [];
    this.products = data.products ?? [];
    this.issued_at_ns = data.issued_at_ns ?? Date.now() * 1_000_000;
    this.signature = data.signature ?? "";
    this.public_key_b64 = data.public_key_b64 ?? "";
  }

  public computeSigningPayload(): Buffer {
    const payloadObj = {
      manifest_id: this.manifest_id,
      spec_version: this.spec_version,
      builder_id: this.builder_id,
      git_commit: this.git_commit,
      git_branch: this.git_branch,
      dag_root_hash: this.dag_root_hash,
      steps: this.steps,
      materials: [...this.materials].sort((a, b) => (a.path || "").localeCompare(b.path || "")),
      products: [...this.products].sort((a, b) => (a.path || "").localeCompare(b.path || "")),
      issued_at_ns: this.issued_at_ns,
    };
    const jsonStr = CanonicalCodec.canonicalize(payloadObj);
    return Buffer.from(jsonStr, "utf-8");
  }

  public toDict(): SignedProvenanceManifestData {
    return {
      manifest_id: this.manifest_id,
      spec_version: this.spec_version,
      builder_id: this.builder_id,
      git_commit: this.git_commit,
      git_branch: this.git_branch,
      dag_root_hash: this.dag_root_hash,
      steps: this.steps,
      materials: this.materials,
      products: this.products,
      issued_at_ns: this.issued_at_ns,
      signature: this.signature,
      public_key_b64: this.public_key_b64,
    };
  }

  public static fromDict(data: SignedProvenanceManifestData): SignedProvenanceManifest {
    return new SignedProvenanceManifest(data);
  }
}

export interface ProvenanceVerificationResultData {
  is_valid: boolean;
  errors: string[];
  verified_steps_count: number;
  verified_products_count: number;
  verified_at_ns: number;
}

export class ProvenanceVerificationResult {
  public readonly is_valid: boolean;
  public readonly errors: string[];
  public readonly verified_steps_count: number;
  public readonly verified_products_count: number;
  public readonly verified_at_ns: number;

  constructor(
    is_valid: boolean,
    errors: string[] = [],
    verified_steps_count: number = 0,
    verified_products_count: number = 0,
    verified_at_ns?: number
  ) {
    this.is_valid = is_valid;
    this.errors = errors;
    this.verified_steps_count = verified_steps_count;
    this.verified_products_count = verified_products_count;
    this.verified_at_ns = verified_at_ns ?? Date.now() * 1_000_000;
  }

  public toDict(): ProvenanceVerificationResultData {
    return {
      is_valid: this.is_valid,
      errors: this.errors,
      verified_steps_count: this.verified_steps_count,
      verified_products_count: this.verified_products_count,
      verified_at_ns: this.verified_at_ns,
    };
  }
}

// ==============================================================================
// 5. Core Engine: ReleaseProvenance
// ==============================================================================

export class ReleaseProvenance {
  private readonly _keyManager: WitnessKeyManager;
  private _dag: ProvenanceDag = new ProvenanceDag();
  private readonly _manifestHistory: SignedProvenanceManifest[] = [];
  private readonly _signatureCache: Map<string, boolean> = new Map();
  private _verifiedCount = 0;
  private _failedVerifications = 0;

  constructor(keyManager?: WitnessKeyManager) {
    this._keyManager = keyManager ?? new WitnessKeyManager();
  }

  public get keyManager(): WitnessKeyManager {
    return this._keyManager;
  }

  public recordStep(options: {
    step_type: ProvenanceStepType;
    parent_step_ids?: string[];
    inputs?: (ArtifactDigest | ArtifactDigestData)[];
    outputs?: (ArtifactDigest | ArtifactDigestData)[];
    metadata?: Record<string, unknown>;
    step_id?: string;
  }): ProvenanceStep {
    const sId = options.step_id ?? `step-${crypto.randomBytes(4).toString("hex")}`;

    const resolvedInputs = (options.inputs || []).map((item) =>
      item instanceof ArtifactDigest ? item : ArtifactDigest.fromDict(item)
    );
    const resolvedOutputs = (options.outputs || []).map((item) =>
      item instanceof ArtifactDigest ? item : ArtifactDigest.fromDict(item)
    );

    const step = new ProvenanceStep(
      sId,
      options.step_type,
      options.parent_step_ids || [],
      resolvedInputs,
      resolvedOutputs,
      options.metadata || {}
    );

    this._dag.addStep(step);
    return step;
  }

  public getDag(): ProvenanceDag {
    return this._dag;
  }

  public generateManifest(options: {
    git_commit: string;
    git_branch?: string;
    builder_id?: string;
    manifest_id?: string;
  }): SignedProvenanceManifest {
    const mId = options.manifest_id ?? `slsa-prov-${crypto.randomBytes(6).toString("hex")}`;
    const sortedSteps = this._dag.topologicalSort();
    const dagHash = this._dag.computeDagHash();

    const allMaterials = new Map<string, ArtifactDigestData>();
    const allProducts = new Map<string, ArtifactDigestData>();

    for (const s of sortedSteps) {
      for (const inp of s.inputs) {
        allMaterials.set(inp.path, inp.toDict());
      }
      for (const out of s.outputs) {
        allProducts.set(out.path, out.toDict());
      }
    }

    const manifest = new SignedProvenanceManifest({
      manifest_id: mId,
      spec_version: "slsa/v1.0",
      builder_id: options.builder_id ?? "https://github.com/SkyCeoSa/SwarmWatch/actions/runner",
      git_commit: options.git_commit,
      git_branch: options.git_branch ?? "main",
      dag_root_hash: dagHash,
      steps: sortedSteps.map((s) => s.toDict()),
      materials: Array.from(allMaterials.values()),
      products: Array.from(allProducts.values()),
      public_key_b64: this._keyManager.publicKeyB64,
    });

    const payloadBuffer = manifest.computeSigningPayload();
    const sigRaw = this._keyManager.sign(payloadBuffer);
    manifest.signature = base64UrlEncode(sigRaw);

    const payloadHash = sha256Digest(payloadBuffer);
    const cacheKey = `${manifest.public_key_b64}:${payloadHash}:${manifest.signature}`;
    this._signatureCache.set(cacheKey, true);

    this._manifestHistory.push(manifest);
    return manifest;
  }

  public verifyManifest(options: {
    manifest: SignedProvenanceManifest | SignedProvenanceManifestData;
    publicKeyB64?: string;
    verifyDiskFiles?: boolean;
    baseDir?: string;
  }): ProvenanceVerificationResult {
    const errors: string[] = [];
    const manifest =
      options.manifest instanceof SignedProvenanceManifest
        ? options.manifest
        : SignedProvenanceManifest.fromDict(options.manifest);

    // 1. Verify ECDSA signature
    const targetPubKey = options.publicKeyB64 || manifest.public_key_b64;
    if (!targetPubKey) {
      errors.push("No public key available for signature verification");
    } else if (!manifest.signature) {
      errors.push("Manifest is unsigned (missing signature)");
    } else {
      const payloadBuffer = manifest.computeSigningPayload();
      const payloadHash = sha256Digest(payloadBuffer);
      const cacheKey = `${targetPubKey}:${payloadHash}:${manifest.signature}`;

      if (this._signatureCache.has(cacheKey)) {
        if (!this._signatureCache.get(cacheKey)) {
          errors.push("Cryptographic signature mismatch: invalid manifest signature");
        }
      } else {
        try {
          const verifier = new WitnessKeyManager({ publicKeyB64: targetPubKey });
          const sigRaw = base64UrlDecode(manifest.signature);
          const isValid = verifier.verify(payloadBuffer, sigRaw);
          this._signatureCache.set(cacheKey, isValid);
          if (!isValid) {
            errors.push("Cryptographic signature mismatch: invalid manifest signature");
          }
        } catch (e: any) {
          this._signatureCache.set(cacheKey, false);
          errors.push(`Signature verification failure: ${e.message}`);
        }
      }
    }

    // 2. Verify DAG integrity and step digests
    const dag = new ProvenanceDag();
    try {
      for (const sData of manifest.steps) {
        const step = ProvenanceStep.fromDict(sData);
        const expectedDigest = step.computeDigest();
        if (step.step_digest !== expectedDigest) {
          errors.push(
            `Step digest corruption in step '${step.step_id}': expected ${expectedDigest}, found ${step.step_digest}`
          );
        }
        dag.addStep(step);
      }

      const recalculatedDagHash = dag.computeDagHash();
      if (manifest.dag_root_hash !== recalculatedDagHash) {
        errors.push(
          `DAG root hash mismatch: expected ${manifest.dag_root_hash}, calculated ${recalculatedDagHash}`
        );
      }
    } catch (e: any) {
      errors.push(`DAG reconstruction error: ${e.message}`);
    }

    // 3. Verify disk files if requested
    let verifiedProducts = 0;
    if (options.verifyDiskFiles) {
      const bDir = options.baseDir || process.cwd();
      for (const prod of manifest.products) {
        const fullPath = path.resolve(bDir, prod.path);
        if (!fs.existsSync(fullPath)) {
          errors.push(`Missing release product file: ${prod.path}`);
          continue;
        }

        try {
          const actualDigest = ArtifactDigest.fromFile(fullPath);
          if (actualDigest.sha256 !== prod.sha256) {
            errors.push(
              `Artifact checksum tampering in '${prod.path}': expected ${prod.sha256}, found ${actualDigest.sha256}`
            );
          } else {
            verifiedProducts++;
          }
        } catch (e: any) {
          errors.push(`Failed to read artifact '${prod.path}': ${e.message}`);
        }
      }
    } else {
      verifiedProducts = manifest.products.length;
    }

    const isValid = errors.length === 0;
    if (isValid) {
      this._verifiedCount++;
    } else {
      this._failedVerifications++;
    }

    return new ProvenanceVerificationResult(
      isValid,
      errors,
      manifest.steps.length,
      verifiedProducts,
      Date.now() * 1_000_000
    );
  }

  public status(): Record<string, unknown> {
    return {
      active_dag_step_count: this._dag.steps.length,
      total_manifests_issued: this._manifestHistory.length,
      successful_verifications: this._verifiedCount,
      failed_verifications: this._failedVerifications,
      public_key: this._keyManager.publicKeyB64,
      slsa_level: "SLSA_BUILD_LEVEL_3",
    };
  }

  public reset(): void {
    this._dag = new ProvenanceDag();
    this._manifestHistory.length = 0;
    this._signatureCache.clear();
    this._verifiedCount = 0;
    this._failedVerifications = 0;
  }
}

// ==============================================================================
// 6. Global Singleton
// ==============================================================================

export const globalReleaseProvenance = new ReleaseProvenance();
