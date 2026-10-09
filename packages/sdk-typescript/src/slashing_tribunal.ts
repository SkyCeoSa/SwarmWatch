/**
 * SwarmWatch Real-Time Epistemic Dispute Resolution & Dynamic Slashing Escalation Tribunal (Seam 97 / ADR 0108)
 *
 * Provides multi-tiered dispute escalation (TRB1), severity-graded dynamic slashing (TRB2),
 * super-majority arbiter quorum finality (TRB3), and fail-closed safety circuit latching under systemic surges (TRB4).
 * Zero-token cost guarantee ($0.00 USD), sub-250 microsecond execution latency SLO.
 */

import { createHash, timingSafeEqual, createHmac } from 'node:crypto';
import { SafetyCircuitBus, globalSafetyCircuitBus } from './circuit_bus.ts';
import { HybridLogicalClock } from './hlc.ts';
import { verifyBftSignature } from './byzantine_oracle.ts';

export class TribunalError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TribunalError';
  }
}

export class DisputeNotFoundError extends TribunalError {
  constructor(message: string) {
    super(message);
    this.name = 'DisputeNotFoundError';
  }
}

export class InvalidDisputeTierError extends TribunalError {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidDisputeTierError';
  }
}

export class InsufficientArbiterQuorumError extends TribunalError {
  constructor(message: string) {
    super(message);
    this.name = 'InsufficientArbiterQuorumError';
  }
}

export class TribunalArbiterQuarantinedError extends TribunalError {
  constructor(message: string) {
    super(message);
    this.name = 'TribunalArbiterQuarantinedError';
  }
}

export class TribunalCircuitTrippedError extends TribunalError {
  constructor(message: string) {
    super(message);
    this.name = 'TribunalCircuitTrippedError';
  }
}

export interface DisputeClaim {
  disputeId: string;
  claimantId: string;
  defendantId: string;
  severity: 'LOW' | 'MEDIUM' | 'CRITICAL' | string;
  tier: number; // 1: Local, 2: Committee, 3: Tribunal Appellate
  disputeBond: number;
  status: 'OPEN' | 'ESCALATED' | 'RESOLVED_CLAIMANT' | 'RESOLVED_DEFENDANT' | 'DISMISSED' | string;
  evidenceDigest: string;
  createdAtHlc: string;
}

export interface TribunalArbiter {
  arbiterId: string;
  publicKey: string;
  bondedStake: number;
  isActive: boolean;
}

export interface TribunalVerdictReceipt {
  verdictId: string;
  disputeId: string;
  finalTier: number;
  verdict: 'RESOLVED_CLAIMANT' | 'RESOLVED_DEFENDANT' | 'DISMISSED' | string;
  slashedAmount: number;
  recipientId: string;
  arbiterSignatures: Record<string, string>;
  finalizedAtHlc: string;
}

export interface TribunalEscalationRecord {
  disputeId: string;
  fromTier: number;
  toTier: number;
  addedBond: number;
  escalatedAtHlc: string;
}

export interface TribunalStatus {
  tribunal_id: string;
  total_disputes_count: number;
  open_disputes_count: number;
  resolved_disputes_count: number;
  active_arbiters_count: number;
  total_verdicts_count: number;
  total_slashed_collateral: number;
  circuit_tripped: boolean;
}

export function computeVerdictDigest(
  disputeId: string,
  tier: number,
  verdict: string,
  slashedAmount: number,
  recipientId: string,
  hlcTs: string,
): string {
  const raw = `sw:trb:verdict:v1:${disputeId}:${tier}:${verdict}:${slashedAmount.toFixed(6)}:${recipientId}:${hlcTs}`;
  return createHash('sha256').update(raw).digest('hex');
}

export function verifyArbiterSignature(pubkey: string, digest: string, signature: string): boolean {
  if (!signature || !pubkey || !digest) return false;
  return verifyBftSignature(pubkey, digest, signature);
}

export class SlashingEscalationTribunal {
  public tribunalId: string;
  public surgeThreshold: number;
  public circuitBus: SafetyCircuitBus | null;
  private hlc: HybridLogicalClock;
  private arbiters: Map<string, TribunalArbiter> = new Map();
  private disputes: Map<string, DisputeClaim> = new Map();
  private verdicts: Map<string, TribunalVerdictReceipt> = new Map();
  private escalations: Map<string, TribunalEscalationRecord[]> = new Map();

  constructor(
    tribunalId: string = 'slashing-tribunal-primary',
    surgeThreshold: number = 5,
    circuitBus?: SafetyCircuitBus,
  ) {
    this.tribunalId = tribunalId;
    this.surgeThreshold = surgeThreshold;
    this.circuitBus = circuitBus !== undefined ? circuitBus : globalSafetyCircuitBus;
    this.hlc = new HybridLogicalClock(`trb-${tribunalId}`);
  }

  public bindCircuitBus(circuitBus: SafetyCircuitBus): void {
    this.circuitBus = circuitBus;
  }

  public registerArbiter(arbiter: TribunalArbiter): void {
    if (arbiter.publicKey.length !== 64 || !/^[0-9a-fA-F]+$/.test(arbiter.publicKey)) {
      throw new TribunalError('Arbiter publicKey must be a valid 64-character hex Ed25519 key.');
    }
    this.arbiters.set(arbiter.arbiterId, { ...arbiter });
  }

  public fileDispute(
    claimantId: string,
    defendantId: string,
    severity: string = 'MEDIUM',
    evidenceDigest: string = '',
    disputeBond: number = 10.0,
    disputeId?: string,
  ): DisputeClaim {
    if (this.circuitBus && this.circuitBus.isTripped()) {
      raiseCircuitTripped();
    }

    if (disputeBond <= 0) {
      throw new TribunalError('Dispute challenge bond must be strictly positive.');
    }

    const did =
      disputeId ||
      `dsp-${createHash('sha256')
        .update(`${claimantId}:${defendantId}:${Date.now()}:${Math.random()}`)
        .digest('hex')
        .slice(0, 16)}`;

    const claim: DisputeClaim = {
      disputeId: did,
      claimantId,
      defendantId,
      severity,
      tier: 1,
      disputeBond,
      status: 'OPEN',
      evidenceDigest: evidenceDigest || '0'.repeat(64),
      createdAtHlc: this.hlc.now().toString(),
    };

    this.disputes.set(did, claim);
    this.escalations.set(did, []);

    // Check critical dispute surge (TRB4)
    let criticalOpen = 0;
    for (const d of this.disputes.values()) {
      if ((d.status === 'OPEN' || d.status === 'ESCALATED') && d.severity === 'CRITICAL') {
        criticalOpen += 1;
      }
    }

    if (criticalOpen >= this.surgeThreshold && this.circuitBus) {
      this.circuitBus.trip(
        'TRIBUNAL_SYSTEMIC_DISPUTE_SURGE',
        `tribunal:${this.tribunalId}`,
        {
          critical_open_disputes: criticalOpen,
          surge_threshold: this.surgeThreshold,
        },
      );
    }

    return claim;
  }

  public escalateDispute(disputeId: string, additionalBond: number): DisputeClaim {
    if (this.circuitBus && this.circuitBus.isTripped()) {
      raiseCircuitTripped();
    }

    const claim = this.disputes.get(disputeId);
    if (!claim) {
      throw new DisputeNotFoundError(`Dispute '${disputeId}' not found.`);
    }

    if (claim.status !== 'OPEN' && claim.status !== 'ESCALATED') {
      throw new TribunalError(`Dispute '${disputeId}' is already finalized (status=${claim.status}).`);
    }

    if (claim.tier >= 3) {
      throw new InvalidDisputeTierError('Dispute is already at highest tier (Tier 3 Tribunal Appellate).');
    }

    const requiredBond = claim.disputeBond;
    if (additionalBond < requiredBond) {
      throw new TribunalError(
        `Additional bond ${additionalBond.toFixed(2)} is insufficient (requires at least ${requiredBond.toFixed(2)}).`,
      );
    }

    const oldTier = claim.tier;
    claim.tier += 1;
    claim.disputeBond += additionalBond;
    claim.status = 'ESCALATED';

    const escRec: TribunalEscalationRecord = {
      disputeId,
      fromTier: oldTier,
      toTier: claim.tier,
      addedBond: additionalBond,
      escalatedAtHlc: this.hlc.now().toString(),
    };

    const records = this.escalations.get(disputeId) || [];
    records.push(escRec);
    this.escalations.set(disputeId, records);

    return claim;
  }

  public adjudicateDispute(
    disputeId: string,
    ruling: string,
    defendantCollateral: number,
    arbiterVotes: Record<string, string>,
    hlcTimestamp?: string,
  ): TribunalVerdictReceipt {
    const claim = this.disputes.get(disputeId);
    if (!claim) {
      throw new DisputeNotFoundError(`Dispute '${disputeId}' not found.`);
    }

    if (claim.status !== 'OPEN' && claim.status !== 'ESCALATED') {
      throw new TribunalError(`Dispute '${disputeId}' is already finalized.`);
    }

    // 1. Super-Majority Arbiter Quorum (TRB3)
    let activeCount = 0;
    for (const arb of this.arbiters.values()) {
      if (arb.isActive) activeCount += 1;
    }
    const requiredQuorum = activeCount > 0 ? Math.floor((2 * activeCount) / 3) + 1 : 1;

    const validSignatures: Record<string, string> = {};
    const hlcTs = hlcTimestamp || this.hlc.now().toString();

    // 2. Dynamic Slashing Penalty Calculation (TRB2)
    let slashedAmount = 0.0;
    let recipientId = '';

    if (ruling === 'RESOLVED_CLAIMANT') {
      let slashRate = 0.05;
      if (claim.severity === 'MEDIUM') {
        slashRate = 0.25;
      } else if (claim.severity === 'CRITICAL') {
        slashRate = 1.0;
      }
      slashedAmount = Number((defendantCollateral * slashRate).toFixed(6));
      recipientId = claim.claimantId;
    } else {
      slashedAmount = 0.0;
      recipientId = claim.defendantId;
    }

    const digest = computeVerdictDigest(
      disputeId,
      claim.tier,
      ruling,
      slashedAmount,
      recipientId,
      hlcTs,
    );

    for (const [arbId, sig] of Object.entries(arbiterVotes)) {
      const arb = this.arbiters.get(arbId);
      if (!arb || !arb.isActive) continue;
      if (verifyArbiterSignature(arb.publicKey, digest, sig)) {
        validSignatures[arbId] = sig;
      }
    }

    if (Object.keys(validSignatures).length < requiredQuorum) {
      throw new InsufficientArbiterQuorumError(
        `Insufficient arbiter quorum: received ${Object.keys(validSignatures).length}, required ${requiredQuorum} (K=${activeCount}).`,
      );
    }

    claim.status = ruling;
    const verdictId = `vrd-${createHash('sha256')
      .update(`${disputeId}:${ruling}:${hlcTs}`)
      .digest('hex')
      .slice(0, 16)}`;

    const receipt: TribunalVerdictReceipt = {
      verdictId,
      disputeId,
      finalTier: claim.tier,
      verdict: ruling,
      slashedAmount,
      recipientId,
      arbiterSignatures: validSignatures,
      finalizedAtHlc: hlcTs,
    };

    this.verdicts.set(verdictId, receipt);
    return receipt;
  }

  public getDispute(disputeId: string): DisputeClaim | undefined {
    return this.disputes.get(disputeId);
  }

  public getStatus(): TribunalStatus {
    let openCount = 0;
    let resolvedCount = 0;
    for (const d of this.disputes.values()) {
      if (d.status === 'OPEN' || d.status === 'ESCALATED') {
        openCount += 1;
      } else {
        resolvedCount += 1;
      }
    }

    let totalSlashed = 0;
    for (const v of this.verdicts.values()) {
      totalSlashed += v.slashedAmount;
    }

    let activeArbiters = 0;
    for (const a of this.arbiters.values()) {
      if (a.isActive) activeArbiters += 1;
    }

    return {
      tribunal_id: this.tribunalId,
      total_disputes_count: this.disputes.size,
      open_disputes_count: openCount,
      resolved_disputes_count: resolvedCount,
      active_arbiters_count: activeArbiters,
      total_verdicts_count: this.verdicts.size,
      total_slashed_collateral: Number(totalSlashed.toFixed(6)),
      circuit_tripped: this.circuitBus ? this.circuitBus.isTripped() : false,
    };
  }
}

function raiseCircuitTripped(): never {
  throw new TribunalCircuitTrippedError('Cannot process tribunal operation: Safety circuit bus is TRIPPED.');
}

// Authoritative Global Singletons
export const globalSlashingTribunal = new SlashingEscalationTribunal();
export const globalTribunal = globalSlashingTribunal;
