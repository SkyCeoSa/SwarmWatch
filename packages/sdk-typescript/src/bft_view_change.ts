/**
 * SwarmWatch BFT View-Change & Accountable Primary Rotation (Seam 87 / ADR 0098).
 *
 * Implements decentralized PBFT view-change liveness (V1), third-party verifiable equivocation
 * slashing proofs (V2), and deterministic primary assignment (V3).
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";
import type { CommitteeRoster } from "./qc.ts";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class BftViewError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BftViewError";
  }
}

export class InsufficientViewVotesError extends BftViewError {
  constructor(message: string) {
    super(message);
    this.name = "InsufficientViewVotesError";
  }
}

export class InvalidVoteSignatureError extends BftViewError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidVoteSignatureError";
  }
}

export class InvalidSlashingProofError extends BftViewError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidSlashingProofError";
  }
}

// ==============================================================================
// 2. Domain Types & Interfaces
// ==============================================================================

export interface ViewChangeVote {
  readonly voter_id: string;
  readonly target_view: number;
  readonly highest_prepared_seq: number;
  readonly highest_prepared_digest: string;
  readonly signature_hex: string;
}

export interface NewViewCertificate {
  readonly view: number;
  readonly primary_id: string;
  readonly votes: readonly ViewChangeVote[];
  readonly proof_hash: string;
  readonly installed_at_ns: number;
}

export interface SlashingProof {
  readonly slashed_node_id: string;
  readonly view: number;
  readonly sequence: number;
  readonly digest_a: string;
  readonly sig_a_hex: string;
  readonly digest_b: string;
  readonly sig_b_hex: string;
  readonly proven_at_ns: number;
}

// ==============================================================================
// 3. Helpers & BFT View Coordinator
// ==============================================================================

export function getPrimary(view: number, roster: CommitteeRoster): string {
  const sortedIds = Object.keys(roster.members).sort();
  if (sortedIds.length === 0) {
    throw new Error("Cannot select primary from empty committee roster");
  }
  return sortedIds[view % sortedIds.length];
}

export function formatViewVotePayload(
  targetView: number,
  seq: number,
  digest: string
): Buffer {
  const raw = `sw:bft:view_change:v1:${targetView}:${seq}:${digest}`;
  return crypto.createHash("sha256").update(raw, "utf-8").digest();
}

export class BftViewCoordinator {
  private _currentView: number;
  private _latestCertificate: NewViewCertificate | null = null;
  private _slashedNodes: Set<string> = new Set();

  constructor(initialView: number = 0) {
    this._currentView = initialView;
  }

  get currentView(): number {
    return this._currentView;
  }

  createVote(
    targetView: number,
    highestPreparedSeq: number,
    highestPreparedDigest: string,
    voterId: string,
    voterKey: crypto.KeyObject | string
  ): ViewChangeVote {
    const payload = formatViewVotePayload(
      targetView,
      highestPreparedSeq,
      highestPreparedDigest
    );

    let privKeyObj: crypto.KeyObject;
    if (typeof voterKey === "string") {
      privKeyObj = crypto.createPrivateKey({
        key: Buffer.from(voterKey, "hex"),
        format: "der",
        type: "pkcs8",
      });
    } else {
      privKeyObj = voterKey;
    }

    const sig = crypto.sign(null, payload, privKeyObj);
    return {
      voter_id: voterId,
      target_view: targetView,
      highest_prepared_seq: highestPreparedSeq,
      highest_prepared_digest: highestPreparedDigest,
      signature_hex: sig.toString("hex"),
    };
  }

  installNewView(
    targetView: number,
    votes: ViewChangeVote[],
    roster: CommitteeRoster
  ): NewViewCertificate {
    if (targetView <= this._currentView) {
      throw new BftViewError(
        `Target view ${targetView} must be strictly greater than current view ${this._currentView}`
      );
    }

    const requiredQuorum = roster.quorum_threshold;
    const validVotes: ViewChangeVote[] = [];
    const seenVoters: Set<string> = new Set();

    for (const vote of votes) {
      if (vote.target_view !== targetView) continue;
      if (seenVoters.has(vote.voter_id)) continue;
      if (!roster.members[vote.voter_id]) continue;
      if (this._slashedNodes.has(vote.voter_id)) continue;

      const pubHex = roster.members[vote.voter_id].public_key_hex;
      const payload = formatViewVotePayload(
        vote.target_view,
        vote.highest_prepared_seq,
        vote.highest_prepared_digest
      );

      try {
        const pubKey = crypto.createPublicKey({
          key: Buffer.from(pubHex, "hex"),
          format: "der",
          type: "spki",
        });
        const isValid = crypto.verify(
          null,
          payload,
          pubKey,
          Buffer.from(vote.signature_hex, "hex")
        );
        if (isValid) {
          validVotes.push(vote);
          seenVoters.add(vote.voter_id);
        }
      } catch {
        continue;
      }
    }

    if (validVotes.length < requiredQuorum) {
      throw new InsufficientViewVotesError(
        `Collected ${validVotes.length} valid votes, required ${requiredQuorum} (2f+1)`
      );
    }

    const primaryId = getPrimary(targetView, roster);
    const proofMaterial = `${targetView}:${primaryId}:${Array.from(seenVoters).sort().join(":")}`;
    const proofHash = crypto
      .createHash("sha256")
      .update(proofMaterial, "utf-8")
      .digest("hex");

    const cert: NewViewCertificate = {
      view: targetView,
      primary_id: primaryId,
      votes: validVotes,
      proof_hash: proofHash,
      installed_at_ns: Date.now() * 1_000_000,
    };

    this._currentView = targetView;
    this._latestCertificate = cert;
    return cert;
  }

  verifyAndApplySlashing(
    proof: SlashingProof,
    roster: CommitteeRoster
  ): boolean {
    if (!roster.members[proof.slashed_node_id]) {
      throw new InvalidSlashingProofError(
        `Node '${proof.slashed_node_id}' is not in the committee roster`
      );
    }

    if (proof.digest_a === proof.digest_b) {
      throw new InvalidSlashingProofError(
        "Equivocation requires two different digests for the same view/seq"
      );
    }

    const pubHex = roster.members[proof.slashed_node_id].public_key_hex;
    const pubKey = crypto.createPublicKey({
      key: Buffer.from(pubHex, "hex"),
      format: "der",
      type: "spki",
    });

    const payloadA = Buffer.from(
      `sw:bft:msg:${proof.view}:${proof.sequence}:${proof.digest_a}`,
      "utf-8"
    );
    const payloadB = Buffer.from(
      `sw:bft:msg:${proof.view}:${proof.sequence}:${proof.digest_b}`,
      "utf-8"
    );

    try {
      const okA = crypto.verify(
        null,
        payloadA,
        pubKey,
        Buffer.from(proof.sig_a_hex, "hex")
      );
      const okB = crypto.verify(
        null,
        payloadB,
        pubKey,
        Buffer.from(proof.sig_b_hex, "hex")
      );

      if (!okA || !okB) {
        throw new InvalidSlashingProofError(
          "Cryptographic verification of equivocation signatures failed"
        );
      }
    } catch (err) {
      throw new InvalidSlashingProofError(
        `Verification failed: ${err instanceof Error ? err.message : String(err)}`
      );
    }

    this._slashedNodes.add(proof.slashed_node_id);
    return true;
  }

  isSlashed(nodeId: string): boolean {
    return this._slashedNodes.has(nodeId);
  }
}
