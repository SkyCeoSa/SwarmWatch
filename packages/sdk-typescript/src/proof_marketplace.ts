/**
 * SwarmWatch Proof Marketplace & Verifier Node Staking Pool (Seam 93 / ADR 0104)
 *
 * Provides decentralized double-auction matching for cryptographic proof verification,
 * economic staking and slashing guarantees (CPM1), BFT verification quorum (CPM3),
 * and fail-closed safety circuit latching under solvency breaches (CPM4).
 * Zero-token cost guarantee ($0.00 USD), sub-250 microsecond execution latency SLO.
 */

import crypto from "node:crypto";
import { SafetyCircuitBus, globalCircuitBus } from "./circuit_bus.ts";
import { HybridLogicalClock } from "./hlc.ts";

export class ProofMarketplaceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProofMarketplaceError";
  }
}

export class InsufficientStakeError extends ProofMarketplaceError {
  constructor(message: string) {
    super(message);
    this.name = "InsufficientStakeError";
  }
}

export class VerifierSlashedError extends ProofMarketplaceError {
  constructor(message: string) {
    super(message);
    this.name = "VerifierSlashedError";
  }
}

export class BountyNotFoundError extends ProofMarketplaceError {
  constructor(message: string) {
    super(message);
    this.name = "BountyNotFoundError";
  }
}

export class BountyExpiredError extends ProofMarketplaceError {
  constructor(message: string) {
    super(message);
    this.name = "BountyExpiredError";
  }
}

export class InvalidReceiptSignatureError extends ProofMarketplaceError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidReceiptSignatureError";
  }
}

export class InsufficientVerificationQuorumError extends ProofMarketplaceError {
  constructor(message: string) {
    super(message);
    this.name = "InsufficientVerificationQuorumError";
  }
}

export class MarketplaceSolvencyBreachError extends ProofMarketplaceError {
  constructor(message: string) {
    super(message);
    this.name = "MarketplaceSolvencyBreachError";
  }
}

export interface VerifierNode {
  verifier_id: string;
  public_key: string; // Hex-encoded 32-byte Ed25519 public key
  bonded_stake: number;
  min_stake: number;
  reputation_score: number;
  is_slashed: boolean;
  is_active: boolean;
}

export interface ProofBounty {
  bounty_id: string;
  proof_type: string;
  proof_digest: string;
  max_bounty: number;
  required_quorum: number;
  deadline_epoch: number;
  status: string; // OPEN, MATCHED, FINALIZED, DISPUTED, SLASHED
  created_at_hlc?: string;
}

export interface VerifierBid {
  bid_id: string;
  bounty_id: string;
  verifier_id: string;
  fee_bid: number;
  staked_collateral: number;
  signature: string; // Hex-encoded Ed25519 signature
}

export interface ProofVerificationReceipt {
  receipt_id: string;
  bounty_id: string;
  verifier_id: string;
  verdict: boolean;
  proof_hash: string;
  signature: string;
  timestamp_ns?: number;
}

export interface SettlementCertificate {
  settlement_id: string;
  bounty_id: string;
  cleared_fee: number;
  participating_verifiers: string[];
  payout_distributions: Record<string, number>;
  slashed_verifiers: string[];
  finalized_verdict: boolean;
  created_at_hlc: string;
}

export function computeBidDigest(bountyId: string, verifierId: string, feeBid: number): string {
  const raw = `sw:bid:v1:${bountyId}:${verifierId}:${feeBid.toFixed(6)}`;
  return crypto.createHash("sha256").update(raw, "utf8").digest("hex");
}

export function computeReceiptDigest(
  bountyId: string,
  verifierId: string,
  verdict: boolean,
  proofHash: string
): string {
  const raw = `sw:receipt:v1:${bountyId}:${verifierId}:${verdict ? 1 : 0}:${proofHash}`;
  return crypto.createHash("sha256").update(raw, "utf8").digest("hex");
}

const ED25519_SPKI_PREFIX = Buffer.from("302a300506032b6570032100", "hex");

export function verifyEd25519Signature(
  publicKeyHex: string,
  signatureHex: string,
  digestHex: string
): boolean {
  try {
    const rawPub = Buffer.from(publicKeyHex, "hex");
    const rawSig = Buffer.from(signatureHex, "hex");
    const rawDigest = Buffer.from(digestHex, "hex");

    if (rawPub.length !== 32 || rawSig.length !== 64) {
      return false;
    }

    const spki = Buffer.concat([ED25519_SPKI_PREFIX, rawPub]);
    const keyObj = crypto.createPublicKey({
      key: spki,
      format: "der",
      type: "spki",
    });

    return crypto.verify(null, rawDigest, keyObj, rawSig);
  } catch {
    return false;
  }
}

export class ProofMarketplaceCoordinator {
  public marketplace_id: string;
  public min_solvency_threshold: number;
  public circuit_bus?: SafetyCircuitBus;

  private _hlc: HybridLogicalClock;
  private _verifiers: Map<string, VerifierNode> = new Map();
  private _bounties: Map<string, ProofBounty> = new Map();
  private _bids: Map<string, VerifierBid[]> = new Map();
  private _matched_verifiers: Map<string, Set<string>> = new Map();
  private _receipts: Map<string, ProofVerificationReceipt[]> = new Map();
  private _settlements: Map<string, SettlementCertificate> = new Map();

  constructor(
    marketplaceId: string = "market-primary",
    minSolvencyThreshold: number = 100.0,
    circuitBus?: SafetyCircuitBus
  ) {
    this.marketplace_id = marketplaceId;
    this.min_solvency_threshold = minSolvencyThreshold;
    this.circuit_bus = circuitBus || globalCircuitBus;
    this._hlc = new HybridLogicalClock(`mkt-${marketplaceId}`);
  }

  bindCircuitBus(circuitBus: SafetyCircuitBus): void {
    this.circuit_bus = circuitBus;
  }

  registerVerifier(node: VerifierNode): void {
    if (node.bonded_stake < node.min_stake) {
      throw new InsufficientStakeError(
        `Verifier '${node.verifier_id}' bonded stake ${node.bonded_stake} is below minimum ${node.min_stake}.`
      );
    }
    this._verifiers.set(node.verifier_id, node);
  }

  getVerifier(verifierId: string): VerifierNode | undefined {
    return this._verifiers.get(verifierId);
  }

  bondStake(verifierId: string, amount: number): number {
    const node = this._verifiers.get(verifierId);
    if (!node) {
      throw new ProofMarketplaceError(`Verifier '${verifierId}' not found.`);
    }
    if (node.is_slashed) {
      throw new VerifierSlashedError(`Cannot bond stake to slashed verifier '${verifierId}'.`);
    }
    if (amount <= 0) {
      throw new ProofMarketplaceError("Stake bonding amount must be positive.");
    }
    node.bonded_stake += amount;
    return node.bonded_stake;
  }

  unbondStake(verifierId: string, amount: number): number {
    const node = this._verifiers.get(verifierId);
    if (!node) {
      throw new ProofMarketplaceError(`Verifier '${verifierId}' not found.`);
    }
    if (node.is_slashed) {
      throw new VerifierSlashedError(`Cannot unbond stake from slashed verifier '${verifierId}'.`);
    }
    if (amount <= 0) {
      throw new ProofMarketplaceError("Stake unbonding amount must be positive.");
    }
    if (node.bonded_stake - amount < node.min_stake) {
      throw new InsufficientStakeError(
        `Unbonding ${amount} would reduce stake ${node.bonded_stake} below minimum ${node.min_stake}.`
      );
    }
    node.bonded_stake -= amount;
    return node.bonded_stake;
  }

  createBounty(bounty: ProofBounty): string {
    if (this.circuit_bus && this.circuit_bus.isTripped()) {
      throw new ProofMarketplaceError("Cannot create bounty: Safety circuit bus is currently TRIPPED.");
    }
    if (this._bounties.has(bounty.bounty_id)) {
      return bounty.bounty_id;
    }

    if (!bounty.created_at_hlc) {
      bounty.created_at_hlc = this._hlc.now().toString();
    }
    bounty.status = "OPEN";
    this._bounties.set(bounty.bounty_id, { ...bounty });
    this._bids.set(bounty.bounty_id, []);
    this._matched_verifiers.set(bounty.bounty_id, new Set());
    this._receipts.set(bounty.bounty_id, []);
    return bounty.bounty_id;
  }

  submitBid(bid: VerifierBid): void {
    const bounty = this._bounties.get(bid.bounty_id);
    if (!bounty) {
      throw new BountyNotFoundError(`Bounty '${bid.bounty_id}' not found.`);
    }
    if (bounty.status !== "OPEN") {
      throw new BountyExpiredError(`Bounty '${bid.bounty_id}' is in status '${bounty.status}', not OPEN.`);
    }

    const verifier = this._verifiers.get(bid.verifier_id);
    if (!verifier) {
      throw new ProofMarketplaceError(`Verifier '${bid.verifier_id}' not registered.`);
    }
    if (verifier.is_slashed || !verifier.is_active) {
      throw new VerifierSlashedError(`Verifier '${bid.verifier_id}' is slashed or inactive.`);
    }
    if (verifier.bonded_stake < verifier.min_stake) {
      throw new InsufficientStakeError(`Verifier '${bid.verifier_id}' bonded stake is below minimum.`);
    }

    if (bid.fee_bid > bounty.max_bounty) {
      throw new ProofMarketplaceError(
        `Bid fee ${bid.fee_bid} exceeds bounty maximum ${bounty.max_bounty}.`
      );
    }

    const digest = computeBidDigest(bid.bounty_id, bid.verifier_id, bid.fee_bid);
    if (!verifyEd25519Signature(verifier.public_key, bid.signature, digest)) {
      throw new InvalidReceiptSignatureError(
        `Bid signature from verifier '${bid.verifier_id}' is invalid.`
      );
    }

    const bids = this._bids.get(bid.bounty_id) || [];
    const idx = bids.findIndex((b) => b.verifier_id === bid.verifier_id);
    if (idx >= 0) {
      bids.splice(idx, 1);
    }
    bids.push(bid);
    this._bids.set(bid.bounty_id, bids);
  }

  matchBounty(bountyId: string): string[] {
    const bounty = this._bounties.get(bountyId);
    if (!bounty) {
      throw new BountyNotFoundError(`Bounty '${bountyId}' not found.`);
    }
    if (bounty.status !== "OPEN") {
      throw new BountyExpiredError(`Bounty '${bountyId}' cannot be matched in status '${bounty.status}'.`);
    }

    const bids = this._bids.get(bountyId) || [];
    const validBids = bids.filter((b) => {
      const v = this._verifiers.get(b.verifier_id);
      return v && !v.is_slashed && v.is_active;
    });

    if (validBids.length < bounty.required_quorum) {
      throw new InsufficientVerificationQuorumError(
        `Bounty '${bountyId}' has ${validBids.length} valid bids, requiring quorum ${bounty.required_quorum}.`
      );
    }

    validBids.sort((a, b) => {
      if (a.fee_bid !== b.fee_bid) {
        return a.fee_bid - b.fee_bid;
      }
      return b.staked_collateral - a.staked_collateral;
    });

    const selected = validBids.slice(0, bounty.required_quorum);
    const selectedIds = selected.map((b) => b.verifier_id);

    bounty.status = "MATCHED";
    this._matched_verifiers.set(bountyId, new Set(selectedIds));
    return selectedIds;
  }

  submitReceipt(receipt: ProofVerificationReceipt): void {
    const bounty = this._bounties.get(receipt.bounty_id);
    if (!bounty) {
      throw new BountyNotFoundError(`Bounty '${receipt.bounty_id}' not found.`);
    }
    if (bounty.status !== "OPEN" && bounty.status !== "MATCHED") {
      throw new BountyExpiredError(
        `Bounty '${receipt.bounty_id}' cannot accept receipts in status '${bounty.status}'.`
      );
    }

    const verifier = this._verifiers.get(receipt.verifier_id);
    if (!verifier) {
      throw new ProofMarketplaceError(`Verifier '${receipt.verifier_id}' not registered.`);
    }
    if (verifier.is_slashed || !verifier.is_active) {
      throw new VerifierSlashedError(`Verifier '${receipt.verifier_id}' is slashed or inactive.`);
    }

    const digest = computeReceiptDigest(
      receipt.bounty_id,
      receipt.verifier_id,
      receipt.verdict,
      receipt.proof_hash
    );
    if (!verifyEd25519Signature(verifier.public_key, receipt.signature, digest)) {
      throw new InvalidReceiptSignatureError(
        `Receipt signature from verifier '${receipt.verifier_id}' failed verification.`
      );
    }

    if (!receipt.timestamp_ns) {
      receipt.timestamp_ns = Date.now() * 1_000_000;
    }

    const receipts = this._receipts.get(receipt.bounty_id) || [];
    const idx = receipts.findIndex((r) => r.verifier_id === receipt.verifier_id);
    if (idx >= 0) {
      receipts.splice(idx, 1);
    }
    receipts.push(receipt);
    this._receipts.set(receipt.bounty_id, receipts);
  }

  settleBounty(bountyId: string): SettlementCertificate {
    const bounty = this._bounties.get(bountyId);
    if (!bounty) {
      throw new BountyNotFoundError(`Bounty '${bountyId}' not found.`);
    }

    const receipts = this._receipts.get(bountyId) || [];
    if (receipts.length < bounty.required_quorum) {
      throw new InsufficientVerificationQuorumError(
        `Bounty '${bountyId}' has ${receipts.length} receipts, requiring quorum ${bounty.required_quorum}.`
      );
    }

    const trueReceipts = receipts.filter((r) => r.verdict === true);
    const falseReceipts = receipts.filter((r) => r.verdict === false);

    const q = bounty.required_quorum;
    let finalizedVerdict: boolean;
    let honestReceipts: ProofVerificationReceipt[];
    let slashedReceipts: ProofVerificationReceipt[];

    if (trueReceipts.length >= q) {
      finalizedVerdict = true;
      honestReceipts = trueReceipts;
      slashedReceipts = falseReceipts;
    } else if (falseReceipts.length >= q) {
      finalizedVerdict = false;
      honestReceipts = falseReceipts;
      slashedReceipts = trueReceipts;
    } else {
      bounty.status = "DISPUTED";
      throw new InsufficientVerificationQuorumError(
        `Conflicting receipts without threshold quorum (${trueReceipts.length} vs ${falseReceipts.length}).`
      );
    }

    // Slash dissenting verifiers (CPM1)
    const slashedIds: string[] = [];
    for (const r of slashedReceipts) {
      this.slashVerifier(
        r.verifier_id,
        1.0,
        `Fraudulent attestation on bounty ${bountyId} (contradicted certified verdict ${finalizedVerdict})`
      );
      slashedIds.push(r.verifier_id);
    }

    // Payout distributions
    const bidsMap = new Map((this._bids.get(bountyId) || []).map((b) => [b.verifier_id, b]));
    const matchingBids = honestReceipts
      .map((r) => bidsMap.get(r.verifier_id)?.fee_bid)
      .filter((fee): fee is number => fee !== undefined);

    const clearingFee = matchingBids.length > 0 ? Math.max(...matchingBids) : bounty.max_bounty;
    const perVerifierPayout = clearingFee / Math.max(1, honestReceipts.length);
    const payouts: Record<string, number> = {};

    for (const r of honestReceipts) {
      payouts[r.verifier_id] = perVerifierPayout;
      const node = this._verifiers.get(r.verifier_id);
      if (node) {
        node.reputation_score = Math.min(2.0, node.reputation_score + 0.05);
      }
    }

    const settlementId = `sw:settle:v1:${crypto
      .createHash("sha256")
      .update(`${bountyId}:${finalizedVerdict}:${Date.now()}`, "utf8")
      .digest("hex")}`;

    const cert: SettlementCertificate = {
      settlement_id: settlementId,
      bounty_id: bountyId,
      cleared_fee: clearingFee,
      participating_verifiers: honestReceipts.map((r) => r.verifier_id),
      payout_distributions: payouts,
      slashed_verifiers: slashedIds,
      finalized_verdict: finalizedVerdict,
      created_at_hlc: this._hlc.now().toString(),
    };

    bounty.status = "FINALIZED";
    this._settlements.set(bountyId, cert);
    return cert;
  }

  slashVerifier(verifierId: string, penaltyFraction: number = 1.0, reason: string = ""): number {
    const node = this._verifiers.get(verifierId);
    if (!node) {
      throw new ProofMarketplaceError(`Verifier '${verifierId}' not found.`);
    }

    const frac = Math.max(0.0, Math.min(1.0, penaltyFraction));
    const slashedAmount = node.bonded_stake * frac;
    node.bonded_stake -= slashedAmount;
    node.is_slashed = true;
    node.is_active = false;
    node.reputation_score = 0.0;

    // Aggregate solvency check (CPM4)
    const activeSolvency = Array.from(this._verifiers.values())
      .filter((v) => v.is_active && !v.is_slashed)
      .reduce((sum, v) => sum + v.bonded_stake, 0);

    if (activeSolvency < this.min_solvency_threshold) {
      if (this.circuit_bus) {
        this.circuit_bus.trip(
          "PROOF_MARKETPLACE_SOLVENCY_BREACH",
          `marketplace:${this.marketplace_id}`,
          {
            active_solvency: activeSolvency,
            min_solvency_threshold: this.min_solvency_threshold,
            slashed_verifier_id: verifierId,
            reason,
          }
        );
      }
    }

    return slashedAmount;
  }

  getStatus(): Record<string, unknown> {
    const activeVerifiers = Array.from(this._verifiers.values()).filter(
      (v) => v.is_active && !v.is_slashed
    );
    const slashedCount = Array.from(this._verifiers.values()).filter((v) => v.is_slashed).length;
    const activeSolvency = activeVerifiers.reduce((sum, v) => sum + v.bonded_stake, 0);

    return {
      marketplace_id: this.marketplace_id,
      total_verifiers: this._verifiers.size,
      active_verifiers_count: activeVerifiers.length,
      slashed_verifiers_count: slashedCount,
      active_solvency: activeSolvency,
      min_solvency_threshold: this.min_solvency_threshold,
      open_bounties_count: Array.from(this._bounties.values()).filter((b) => b.status === "OPEN").length,
      finalized_bounties_count: this._settlements.size,
    };
  }
}

export const globalProofMarketplace: ProofMarketplaceCoordinator = new ProofMarketplaceCoordinator();
export const globalMarketplaceCoordinator: ProofMarketplaceCoordinator = globalProofMarketplace;
