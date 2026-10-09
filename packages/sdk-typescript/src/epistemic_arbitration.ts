/**
 * SwarmWatch Multi-Cluster Epistemic Proof Verification & Automated Cross-Swarm Dispute Resolution (Seam 76 / ADR 0087).
 *
 * Implements in-process, zero-token epistemic claim registration, formal deductive proof DAG
 * verification, cycle/circularity detection, deterministic evidentiary dominance scoring,
 * cross-swarm dispute arbitration, fail-closed safety circuit coupling, and tamper-evident
 * dispute resolution receipts ('sw:dispute:v1:<sha256>').
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";

// ==============================================================================
// 0. Cryptographic & Canonical Helpers
// ==============================================================================

export function deepCanonicalJson(val: any): string {
  if (val === null || val === undefined) return "null";
  if (typeof val === "boolean" || typeof val === "number") return JSON.stringify(val);
  if (typeof val === "string") return JSON.stringify(val);
  if (Array.isArray(val)) {
    return "[" + val.map((item) => deepCanonicalJson(item)).join(",") + "]";
  }
  if (typeof val === "object") {
    const keys = Object.keys(val).sort();
    const pairs = keys.map((k) => JSON.stringify(k) + ":" + deepCanonicalJson(val[k]));
    return "{" + pairs.join(",") + "}";
  }
  return JSON.stringify(val);
}

export function sha256Hex(data: string): string {
  return crypto.createHash("sha256").update(data, "utf8").digest("hex");
}

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class EpistemicArbitrationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EpistemicArbitrationError";
  }
}

export class DisputeCircuitTrippedError extends EpistemicArbitrationError {
  constructor(message: string) {
    super(message);
    this.name = "DisputeCircuitTrippedError";
  }
}

export class InvalidEpistemicProofError extends EpistemicArbitrationError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidEpistemicProofError";
  }
}

export class EpistemicCircularityError extends InvalidEpistemicProofError {
  constructor(message: string) {
    super(message);
    this.name = "EpistemicCircularityError";
  }
}

export class EpistemicDeadlockError extends EpistemicArbitrationError {
  constructor(message: string) {
    super(message);
    this.name = "EpistemicDeadlockError";
  }
}

export class EpistemicContradictionCircuitError extends EpistemicArbitrationError {
  constructor(message: string) {
    super(message);
    this.name = "EpistemicContradictionCircuitError";
  }
}

// ==============================================================================
// 2. Domain Types & Object Enums
// ==============================================================================

export type EpistemicModality = "KNOWS" | "JUSTIFIED" | "BELIEVES";

export const EpistemicModality = {
  KNOWS: "KNOWS" as EpistemicModality,
  JUSTIFIED: "JUSTIFIED" as EpistemicModality,
  BELIEVES: "BELIEVES" as EpistemicModality,
} as const;

export type ClaimPolarity = "TRUE" | "FALSE";

export const ClaimPolarity = {
  TRUE: "TRUE" as ClaimPolarity,
  FALSE: "FALSE" as ClaimPolarity,
} as const;

export type DeductionRule =
  | "AXIOM"
  | "EVIDENTIARY_GROUNDING"
  | "MODUS_PONENS"
  | "CONJUNCTION"
  | "EPISTEMIC_NECESSITATION";

export const DeductionRule = {
  AXIOM: "AXIOM" as DeductionRule,
  EVIDENTIARY_GROUNDING: "EVIDENTIARY_GROUNDING" as DeductionRule,
  MODUS_PONENS: "MODUS_PONENS" as DeductionRule,
  CONJUNCTION: "CONJUNCTION" as DeductionRule,
  EPISTEMIC_NECESSITATION: "EPISTEMIC_NECESSITATION" as DeductionRule,
} as const;

export type DisputeStatus =
  | "OPEN"
  | "EVIDENCE_SUBMITTED"
  | "EVALUATING"
  | "RESOLVED"
  | "DEADLOCKED"
  | "ESCALATED_CIRCUIT";

export const DisputeStatus = {
  OPEN: "OPEN" as DisputeStatus,
  EVIDENCE_SUBMITTED: "EVIDENCE_SUBMITTED" as DisputeStatus,
  EVALUATING: "EVALUATING" as DisputeStatus,
  RESOLVED: "RESOLVED" as DisputeStatus,
  DEADLOCKED: "DEADLOCKED" as DisputeStatus,
  ESCALATED_CIRCUIT: "ESCALATED_CIRCUIT" as DisputeStatus,
} as const;

export type ArbitrationVerdict =
  | "CLAIMANT_PREVAILS"
  | "RESPONDENT_PREVAILS"
  | "MUTUAL_INVALIDATION"
  | "DEADLOCKED"
  | "EPISTEMIC_DIVERGENCE_CIRCUIT_TRIP";

export const ArbitrationVerdict = {
  CLAIMANT_PREVAILS: "CLAIMANT_PREVAILS" as ArbitrationVerdict,
  RESPONDENT_PREVAILS: "RESPONDENT_PREVAILS" as ArbitrationVerdict,
  MUTUAL_INVALIDATION: "MUTUAL_INVALIDATION" as ArbitrationVerdict,
  DEADLOCKED: "DEADLOCKED" as ArbitrationVerdict,
  EPISTEMIC_DIVERGENCE_CIRCUIT_TRIP: "EPISTEMIC_DIVERGENCE_CIRCUIT_TRIP" as ArbitrationVerdict,
} as const;

export interface ProofNode {
  node_id: string;
  rule: DeductionRule;
  premise_ids: string[];
  conclusion: string;
  evidence_hash?: string | null;
  evidence_weight: number;
  is_valid: boolean;
}

export class EpistemicProofDAG {
  dag_id: string;
  nodes: Map<string, ProofNode>;
  root_node_id: string;
  max_depth: number;

  constructor(options?: {
    dag_id?: string;
    root_node_id?: string;
    max_depth?: number;
    nodes?: Record<string, ProofNode> | Map<string, ProofNode>;
  }) {
    this.dag_id = options?.dag_id || `dag_${Date.now()}`;
    this.root_node_id = options?.root_node_id || "";
    this.max_depth = options?.max_depth || 16;
    this.nodes = new Map();

    if (options?.nodes) {
      if (options.nodes instanceof Map) {
        for (const [k, v] of options.nodes.entries()) {
          this.nodes.set(k, v);
        }
      } else {
        for (const k of Object.keys(options.nodes)) {
          this.nodes.set(k, options.nodes[k]);
        }
      }
    }
  }

  private _reachable: Set<string> = new Set();

  addNode(node: ProofNode): void {
    if (node.evidence_weight < 0.0 || node.evidence_weight > 1.0) {
      throw new InvalidEpistemicProofError(
        `Evidence weight ${node.evidence_weight} out of bounds [0.0, 1.0]`
      );
    }
    this.nodes.set(node.node_id, node);
  }

  validateAcyclicity(): boolean {
    if (this.nodes.size === 0) {
      this._reachable = new Set();
      return true;
    }

    const visited = new Set<string>();
    const recStack = new Set<string>();

    const dfs = (nodeId: string, depth: number): boolean => {
      if (depth > this.max_depth) {
        throw new InvalidEpistemicProofError(
          `Proof DAG exceeded maximum deduction depth (${this.max_depth})`
        );
      }
      if (recStack.has(nodeId)) {
        throw new EpistemicCircularityError(
          `Circular reasoning detected in proof DAG at node '${nodeId}'`
        );
      }
      if (visited.has(nodeId)) {
        return true;
      }

      const node = this.nodes.get(nodeId);
      if (!node) {
        throw new InvalidEpistemicProofError(`Dangling premise reference '${nodeId}'`);
      }

      visited.add(nodeId);
      recStack.add(nodeId);

      if (node.premise_ids) {
        for (const premiseId of node.premise_ids) {
          dfs(premiseId, depth + 1);
        }
      }

      recStack.delete(nodeId);
      return true;
    };

    if (this.root_node_id) {
      dfs(this.root_node_id, 1);
      if (visited.size !== this.nodes.size) {
        throw new InvalidEpistemicProofError(
          `Proof DAG contains nodes unreachable from root (reachable=${visited.size}, total=${this.nodes.size})`
        );
      }
    } else {
      for (const nId of this.nodes.keys()) {
        if (!visited.has(nId)) {
          dfs(nId, 1);
        }
      }
    }

    this._reachable = visited;
    return true;
  }

  computeDepth(): number {
    const memo = new Map<string, number>();

    const getD = (nodeId: string): number => {
      if (memo.has(nodeId)) {
        return memo.get(nodeId)!;
      }
      const node = this.nodes.get(nodeId);
      if (!node || !node.premise_ids || node.premise_ids.length === 0) {
        return 1;
      }
      let maxChild = 1;
      for (const p of node.premise_ids) {
        maxChild = Math.max(maxChild, getD(p));
      }
      const depth = 1 + maxChild;
      memo.set(nodeId, depth);
      return depth;
    };

    if (!this.root_node_id || !this.nodes.has(this.root_node_id)) {
      return 1;
    }
    return getD(this.root_node_id);
  }

  evaluateDag(): {
    isValid: boolean;
    avgLeafWeight: number;
    deductionRatio: number;
  } {
    this.validateAcyclicity();

    if (this.nodes.size === 0) {
      return { isValid: false, avgLeafWeight: 0.0, deductionRatio: 0.0 };
    }

    const evalNodes: ProofNode[] = [];
    if (this.root_node_id && this._reachable) {
      for (const nid of this._reachable) {
        const n = this.nodes.get(nid);
        if (n) evalNodes.push(n);
      }
    } else {
      for (const n of this.nodes.values()) {
        evalNodes.push(n);
      }
    }

    const leafWeights: number[] = [];
    let validRules = 0;
    let totalRules = 0;

    for (const node of evalNodes) {
      if (node.evidence_weight < 0.0 || node.evidence_weight > 1.0) {
        throw new InvalidEpistemicProofError(
          `Evidence weight ${node.evidence_weight} out of bounds [0.0, 1.0]`
        );
      }

      if (!node.is_valid) {
        return { isValid: false, avgLeafWeight: 0.0, deductionRatio: 0.0 };
      }

      if (
        !node.premise_ids ||
        node.premise_ids.length === 0 ||
        node.rule === DeductionRule.AXIOM ||
        node.rule === DeductionRule.EVIDENTIARY_GROUNDING
      ) {
        leafWeights.push(node.evidence_weight);
      } else {
        totalRules += 1;
        let ruleOk = true;
        if (node.rule === DeductionRule.MODUS_PONENS) {
          if (node.premise_ids.length !== 2) ruleOk = false;
        } else if (node.rule === DeductionRule.CONJUNCTION) {
          if (node.premise_ids.length < 2) ruleOk = false;
        } else if (node.rule === DeductionRule.EPISTEMIC_NECESSITATION) {
          if (node.premise_ids.length !== 1) ruleOk = false;
        } else if (
          node.rule === DeductionRule.AXIOM ||
          node.rule === DeductionRule.EVIDENTIARY_GROUNDING
        ) {
          if (node.premise_ids.length !== 0) ruleOk = false;
        }

        if (ruleOk && node.is_valid) {
          validRules += 1;
        }
      }
    }

    const avgLeaf =
      leafWeights.length > 0
        ? leafWeights.reduce((a, b) => a + b, 0) / leafWeights.length
        : 0.0;
    const ruleRatio = totalRules > 0 ? validRules / totalRules : 1.0;

    return { isValid: true, avgLeafWeight: avgLeaf, deductionRatio: ruleRatio };
  }

  toDict(): Record<string, any> {
    const nodesObj: Record<string, any> = {};
    for (const [k, v] of this.nodes.entries()) {
      nodesObj[k] = v;
    }
    return {
      dag_id: this.dag_id,
      root_node_id: this.root_node_id,
      nodes: nodesObj,
      max_depth: this.max_depth,
    };
  }
}

export interface EpistemicClaim {
  claim_id: string;
  swarm_id: string;
  agent_id: string;
  subject: string;
  predicate: string;
  polarity: ClaimPolarity;
  modality: EpistemicModality;
  confidence: number;
  evidence_hashes: string[];
  proof_dag?: EpistemicProofDAG | null;
  timestamp_ns: number;
  digest: string;
}

export interface DisputeCase {
  dispute_id: string;
  subject: string;
  predicate: string;
  claimant_claim: EpistemicClaim;
  respondent_claim: EpistemicClaim;
  status: DisputeStatus;
  verdict?: ArbitrationVerdict | null;
  margin: number;
  claimant_score: number;
  respondent_score: number;
  created_at_ns: number;
  resolved_at_ns?: number | null;
}

export interface DisputeResolutionReceipt {
  receipt_id: string;
  dispute_id: string;
  subject: string;
  predicate: string;
  verdict: ArbitrationVerdict;
  prevailing_swarm_id?: string | null;
  prevailing_claim_id?: string | null;
  claimant_score: number;
  respondent_score: number;
  score_delta: number;
  receipt_hash: string;
  resolved_at_ns: number;
  arbitrator_id: string;
}

// ==============================================================================
// 3. Epistemic Arbitration Engine
// ==============================================================================

export class EpistemicArbitrationEngine {
  circuitBreaker: any;
  disputeMarginThreshold: number;

  private _claims: Map<string, EpistemicClaim>;
  private _disputes: Map<string, DisputeCase>;
  private _receipts: Map<string, DisputeResolutionReceipt>;
  private _circuitTripped: boolean;
  private _tripReason: string | null;

  constructor(options?: {
    circuitBreaker?: any;
    disputeMarginThreshold?: number;
  }) {
    this.circuitBreaker = options?.circuitBreaker ?? null;
    this.disputeMarginThreshold = options?.disputeMarginThreshold ?? 0.10;

    this._claims = new Map();
    this._disputes = new Map();
    this._receipts = new Map();
    this._circuitTripped = false;
    this._tripReason = null;
  }

  // --------------------------------------------------------------------------
  // Circuit Breaker Helpers
  // --------------------------------------------------------------------------

  checkCircuit(): void {
    if (this._circuitTripped) {
      throw new DisputeCircuitTrippedError(
        `Arbitration rejected: safety circuit tripped (${this._tripReason})`
      );
    }
    if (this.circuitBreaker) {
      let isOpen = false;
      if (typeof this.circuitBreaker.isOpen === "function") {
        isOpen = this.circuitBreaker.isOpen();
      } else if (typeof this.circuitBreaker.is_open === "function") {
        isOpen = this.circuitBreaker.is_open();
      } else if (this.circuitBreaker.isOpen !== undefined) {
        isOpen = Boolean(this.circuitBreaker.isOpen);
      }
      if (isOpen) {
        this._circuitTripped = true;
        this._tripReason = "External safety circuit breaker is open";
        throw new DisputeCircuitTrippedError(
          `Arbitration rejected: ${this._tripReason}`
        );
      }
    }
  }

  tripCircuit(reason: string): void {
    this._circuitTripped = true;
    this._tripReason = reason;
    if (this.circuitBreaker && typeof this.circuitBreaker.trip === "function") {
      try {
        this.circuitBreaker.trip(reason);
      } catch {
        // ignore
      }
    }
  }

  // --------------------------------------------------------------------------
  // Claim Registration
  // --------------------------------------------------------------------------

  registerClaim(params: {
    swarmId: string;
    agentId: string;
    subject: string;
    predicate: string;
    polarity?: ClaimPolarity;
    modality?: EpistemicModality;
    confidence?: number;
    evidenceHashes?: string[];
    proofDag?: EpistemicProofDAG | null;
    claimId?: string;
  }): EpistemicClaim {
    this.checkCircuit();

    const cId =
      params.claimId ||
      `claim_${Math.floor(Date.now() / 1000)}_${crypto.randomBytes(4).toString("hex")}`;
    const polarity = params.polarity || ClaimPolarity.TRUE;
    const modality = params.modality || EpistemicModality.JUSTIFIED;
    const confidence = Math.max(0.0, Math.min(1.0, params.confidence ?? 1.0));
    const evidenceHashes = params.evidenceHashes || [];

    if (params.proofDag) {
      params.proofDag.validateAcyclicity();
    }

    const payload = {
      claim_id: cId,
      swarm_id: params.swarmId,
      agent_id: params.agentId,
      subject: params.subject,
      predicate: params.predicate,
      polarity,
      modality,
      confidence,
      evidence_hashes: evidenceHashes,
    };
    const digest = sha256Hex(deepCanonicalJson(payload));

    const claim: EpistemicClaim = {
      claim_id: cId,
      swarm_id: params.swarmId,
      agent_id: params.agentId,
      subject: params.subject,
      predicate: params.predicate,
      polarity,
      modality,
      confidence,
      evidence_hashes: evidenceHashes,
      proof_dag: params.proofDag,
      timestamp_ns: Date.now() * 1_000_000,
      digest,
    };

    this._claims.set(cId, claim);
    return claim;
  }

  // --------------------------------------------------------------------------
  // Dispute Opening
  // --------------------------------------------------------------------------

  openDispute(
    paramOrClaimant: string | {
      claimantClaimId: string;
      respondentClaimId: string;
      disputeId?: string;
    },
    optionalRespondent?: string,
    optionalDisputeId?: string
  ): DisputeCase {
    this.checkCircuit();

    const claimantClaimId =
      typeof paramOrClaimant === "string"
        ? paramOrClaimant
        : paramOrClaimant.claimantClaimId;
    const respondentClaimId =
      typeof paramOrClaimant === "string"
        ? optionalRespondent!
        : paramOrClaimant.respondentClaimId;
    const disputeId =
      typeof paramOrClaimant === "string"
        ? optionalDisputeId
        : paramOrClaimant.disputeId;

    const c1 = this._claims.get(claimantClaimId);
    const c2 = this._claims.get(respondentClaimId);

    if (!c1 || !c2) {
      throw new EpistemicArbitrationError(
        `Both claims must exist to open dispute: ${claimantClaimId}, ${respondentClaimId}`
      );
    }

    if (c1.subject !== c2.subject || c1.predicate !== c2.predicate) {
      throw new EpistemicArbitrationError(
        `Dispute claims must address identical subject and predicate. Got: (${c1.subject}, ${c1.predicate}) vs (${c2.subject}, ${c2.predicate})`
      );
    }

    if (c1.polarity === c2.polarity) {
      throw new EpistemicArbitrationError(
        `Dispute requires conflicting polarities. Both claims asserted polarity ${c1.polarity}`
      );
    }

    const dId =
      disputeId ||
      `dispute_${Math.floor(Date.now() / 1000)}_${crypto.randomBytes(4).toString("hex")}`;
    const dispute: DisputeCase = {
      dispute_id: dId,
      subject: c1.subject,
      predicate: c1.predicate,
      claimant_claim: c1,
      respondent_claim: c2,
      status: DisputeStatus.OPEN,
      margin: 0.0,
      claimant_score: 0.0,
      respondent_score: 0.0,
      created_at_ns: Date.now() * 1_000_000,
    };

    this._disputes.set(dId, dispute);
    return dispute;
  }

  // --------------------------------------------------------------------------
  // Evidentiary Dominance Scoring & Dispute Resolution
  // --------------------------------------------------------------------------

  computeClaimScore(claim: EpistemicClaim): { isValid: boolean; score: number } {
    if (!claim.proof_dag) {
      const evidenceCount = claim.evidence_hashes.length;
      if (evidenceCount === 0) {
        return { isValid: true, score: claim.confidence * 0.20 };
      }
      const leafWeight = Math.min(1.0, evidenceCount * 0.25);
      const score = 0.50 * leafWeight + 0.15 * claim.confidence;
      return { isValid: true, score };
    }

    try {
      const { isValid, avgLeafWeight, deductionRatio } = claim.proof_dag.evaluateDag();
      if (!isValid) {
        return { isValid: false, score: 0.0 };
      }

      const depth = claim.proof_dag.computeDepth();
      const depthPenalty = Math.min(0.20, (depth - 1) * 0.02);

      let modalityWeight = 1.0;
      if (claim.modality === EpistemicModality.KNOWS) {
        modalityWeight = 1.0;
      } else if (claim.modality === EpistemicModality.JUSTIFIED) {
        modalityWeight = 0.95;
      } else if (claim.modality === EpistemicModality.BELIEVES) {
        modalityWeight = 0.80;
      }

      const score =
        0.50 * avgLeafWeight +
        0.30 * deductionRatio +
        0.15 * (claim.confidence * modalityWeight) -
        depthPenalty;

      return { isValid: true, score: Math.max(0.0, Math.min(1.0, score)) };
    } catch (err) {
      if (
        err instanceof EpistemicCircularityError ||
        err instanceof InvalidEpistemicProofError
      ) {
        return { isValid: false, score: 0.0 };
      }
      throw err;
    }
  }

  resolveDispute(
    paramOrId: string | {
      disputeId: string;
      marginThreshold?: number;
    },
    optionalMarginThreshold?: number
  ): DisputeResolutionReceipt {
    this.checkCircuit();

    const disputeId =
      typeof paramOrId === "string" ? paramOrId : paramOrId.disputeId;
    const margin =
      typeof paramOrId === "string"
        ? (optionalMarginThreshold !== undefined ? optionalMarginThreshold : this.disputeMarginThreshold)
        : (paramOrId.marginThreshold !== undefined ? paramOrId.marginThreshold : this.disputeMarginThreshold);

    const dispute = this._disputes.get(disputeId);
    if (!dispute) {
      throw new EpistemicArbitrationError(`Dispute '${disputeId}' not found`);
    }

    dispute.status = DisputeStatus.EVALUATING;

    const res1 = this.computeClaimScore(dispute.claimant_claim);
    const res2 = this.computeClaimScore(dispute.respondent_claim);

    dispute.claimant_score = res1.score;
    dispute.respondent_score = res2.score;
    const scoreDelta = Math.abs(res1.score - res2.score);
    dispute.margin = scoreDelta;

    let verdict: ArbitrationVerdict;
    let prevailingSwarm: string | null = null;
    let prevailingClaim: string | null = null;

    if (!res1.isValid && !res2.isValid) {
      verdict = ArbitrationVerdict.MUTUAL_INVALIDATION;
    } else if (!res1.isValid && res2.isValid) {
      verdict = ArbitrationVerdict.RESPONDENT_PREVAILS;
      prevailingSwarm = dispute.respondent_claim.swarm_id;
      prevailingClaim = dispute.respondent_claim.claim_id;
    } else if (res1.isValid && !res2.isValid) {
      verdict = ArbitrationVerdict.CLAIMANT_PREVAILS;
      prevailingSwarm = dispute.claimant_claim.swarm_id;
      prevailingClaim = dispute.claimant_claim.claim_id;
    } else {
      if (scoreDelta >= margin) {
        if (res1.score > res2.score) {
          verdict = ArbitrationVerdict.CLAIMANT_PREVAILS;
          prevailingSwarm = dispute.claimant_claim.swarm_id;
          prevailingClaim = dispute.claimant_claim.claim_id;
        } else {
          verdict = ArbitrationVerdict.RESPONDENT_PREVAILS;
          prevailingSwarm = dispute.respondent_claim.swarm_id;
          prevailingClaim = dispute.respondent_claim.claim_id;
        }
      } else {
        verdict = ArbitrationVerdict.DEADLOCKED;
        dispute.status = DisputeStatus.DEADLOCKED;
        const reason = `Epistemic deadlock between swarm '${dispute.claimant_claim.swarm_id}' and '${dispute.respondent_claim.swarm_id}' on (${dispute.subject}, ${dispute.predicate})`;
        this.tripCircuit(reason);
      }
    }

    dispute.verdict = verdict;
    dispute.resolved_at_ns = Date.now() * 1_000_000;
    if (verdict !== ArbitrationVerdict.DEADLOCKED) {
      dispute.status = DisputeStatus.RESOLVED;
    }

    const receiptPayload = {
      dispute_id: dispute.dispute_id,
      subject: dispute.subject,
      predicate: dispute.predicate,
      verdict,
      prevailing_swarm_id: prevailingSwarm,
      prevailing_claim_id: prevailingClaim,
      claimant_score: Number(res1.score.toFixed(4)),
      respondent_score: Number(res2.score.toFixed(4)),
      score_delta: Number(scoreDelta.toFixed(4)),
    };
    const rawHash = sha256Hex(deepCanonicalJson(receiptPayload));
    const receiptHash = `sw:dispute:v1:${rawHash}`;
    const receiptId = `rcpt_${rawHash.slice(0, 16)}`;

    const receipt: DisputeResolutionReceipt = {
      receipt_id: receiptId,
      dispute_id: dispute.dispute_id,
      subject: dispute.subject,
      predicate: dispute.predicate,
      verdict,
      prevailing_swarm_id: prevailingSwarm,
      prevailing_claim_id: prevailingClaim,
      claimant_score: res1.score,
      respondent_score: res2.score,
      score_delta: scoreDelta,
      receipt_hash: receiptHash,
      resolved_at_ns: Date.now() * 1_000_000,
      arbitrator_id: "swarmwatch-epistemic-arbitrator",
    };

    this._receipts.set(receiptId, receipt);
    return receipt;
  }

  verifyResolutionReceipt(receipt: DisputeResolutionReceipt): {
    isValid: boolean;
    errorReason: string | null;
  } {
    if (!receipt.receipt_hash.startsWith("sw:dispute:v1:")) {
      return { isValid: false, errorReason: "Invalid receipt hash domain prefix" };
    }

    const expectedPayload = {
      dispute_id: receipt.dispute_id,
      subject: receipt.subject,
      predicate: receipt.predicate,
      verdict: receipt.verdict,
      prevailing_swarm_id: receipt.prevailing_swarm_id || null,
      prevailing_claim_id: receipt.prevailing_claim_id || null,
      claimant_score: Number(receipt.claimant_score.toFixed(4)),
      respondent_score: Number(receipt.respondent_score.toFixed(4)),
      score_delta: Number(receipt.score_delta.toFixed(4)),
    };
    const recomputedHash = `sw:dispute:v1:${sha256Hex(deepCanonicalJson(expectedPayload))}`;
    if (recomputedHash !== receipt.receipt_hash) {
      return {
        isValid: false,
        errorReason: "Receipt hash mismatch: payload tampering detected",
      };
    }

    return { isValid: true, errorReason: null };
  }

  verifyReceipt(receipt: DisputeResolutionReceipt): {
    isValid: boolean;
    errorReason: string | null;
  } {
    return this.verifyResolutionReceipt(receipt);
  }

  // --------------------------------------------------------------------------
  // Queries & State Inspection
  // --------------------------------------------------------------------------

  getClaim(claimId: string): EpistemicClaim | undefined {
    return this._claims.get(claimId);
  }

  getDispute(disputeId: string): DisputeCase | undefined {
    return this._disputes.get(disputeId);
  }

  getReceipt(receiptId: string): DisputeResolutionReceipt | undefined {
    return this._receipts.get(receiptId);
  }

  getClaims(): EpistemicClaim[] {
    return Array.from(this._claims.values());
  }

  getDisputes(): DisputeCase[] {
    return Array.from(this._disputes.values());
  }

  getReceipts(): DisputeResolutionReceipt[] {
    return Array.from(this._receipts.values());
  }

  getStatus(): Record<string, any> {
    return {
      claims_count: this._claims.size,
      disputes_count: this._disputes.size,
      receipts_count: this._receipts.size,
      circuit_tripped: this._circuitTripped,
      trip_reason: this._tripReason,
      margin_threshold: this.disputeMarginThreshold,
    };
  }

  reset(): void {
    this._claims.clear();
    this._disputes.clear();
    this._receipts.clear();
    this._circuitTripped = false;
    this._tripReason = null;
  }
}

// Global singleton instance
export const globalEpistemicArbitrator = new EpistemicArbitrationEngine();
