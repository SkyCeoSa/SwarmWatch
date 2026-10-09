/**
 * SwarmWatch Autonomous Cloud SaaS Billing, Usage Metering & Micro-Payment Channel Ledger (Seam 95 / ADR 0106)
 *
 * Implements off-chain micro-payment channels with deposit balance conservation (MPC1),
 * monotonic nonces and dual cryptographic signatures (MPC2), dispute windows and fraud challenge slashing (MPC3),
 * and real-time usage metering with fail-closed safety circuit latching (MPC4).
 * Zero-token cost guarantee ($0.00 USD), sub-250 microsecond execution latency SLO.
 */

import { createHash, timingSafeEqual, createHmac } from 'node:crypto';
import { SafetyCircuitBus, globalSafetyCircuitBus } from './circuit_bus.ts';
import { HybridLogicalClock } from './hlc.ts';
import { verifyBftSignature } from './byzantine_oracle.ts';

export class BillingChannelError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BillingChannelError';
  }
}

export class ChannelNotFoundError extends BillingChannelError {
  constructor(message: string) {
    super(message);
    this.name = 'ChannelNotFoundError';
  }
}

export class ChannelBalanceConservationError extends BillingChannelError {
  constructor(message: string) {
    super(message);
    this.name = 'ChannelBalanceConservationError';
  }
}

export class StaleChannelNonceError extends BillingChannelError {
  constructor(message: string) {
    super(message);
    this.name = 'StaleChannelNonceError';
  }
}

export class InvalidChannelSignatureError extends BillingChannelError {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidChannelSignatureError';
  }
}

export class ChannelDisputeActiveError extends BillingChannelError {
  constructor(message: string) {
    super(message);
    this.name = 'ChannelDisputeActiveError';
  }
}

export class BillingChannelExhaustedError extends BillingChannelError {
  constructor(message: string) {
    super(message);
    this.name = 'BillingChannelExhaustedError';
  }
}

export class BillingCircuitTrippedError extends BillingChannelError {
  constructor(message: string) {
    super(message);
    this.name = 'BillingCircuitTrippedError';
  }
}

export interface ChannelStateCommitment {
  channelId: string;
  nonce: number;
  agentBalance: number;
  providerBalance: number;
  commitmentDigest: string;
  agentSignature: string;
  providerSignature: string;
  hlcTimestamp: string;
}

export interface MicroPaymentChannel {
  channelId: string;
  tenantId: string;
  agentPubkey: string;
  providerPubkey: string;
  initialDeposit: number;
  currentNonce: number;
  agentBalance: number;
  providerBalance: number;
  reserveThreshold: number;
  status: 'OPEN' | 'CLOSING' | 'SETTLED' | 'SLASHED';
  closureInitiatedAtHlc?: string;
  latestCommitment?: ChannelStateCommitment;
}

export interface UsageMeterEvent {
  meterId: string;
  channelId: string;
  unitType: 'TELEMETRY_SPAN' | 'SNARK_PROOF' | 'BARRIER_CUT' | 'METER_UNIT';
  quantity: number;
  unitPrice: number;
  totalCharge: number;
  hlcTimestamp: string;
}

export interface ChannelSettlementCertificate {
  settlementId: string;
  channelId: string;
  finalNonce: number;
  finalAgentBalance: number;
  finalProviderBalance: number;
  settlementType: 'COOPERATIVE' | 'DISPUTE_FINALIZED' | 'CHEATER_SLASHED';
  createdAtHlc: string;
}

export function computeChannelCommitmentDigest(
  channelId: string,
  nonce: number,
  agentBalance: number,
  providerBalance: number,
  hlcTimestamp: string,
): string {
  const raw = `sw:mpc:v1:${channelId}:${nonce}:${agentBalance.toFixed(6)}:${providerBalance.toFixed(6)}:${hlcTimestamp}`;
  return createHash('sha256').update(raw, 'utf8').digest('hex');
}

export function verifyChannelSignature(pubkey: string, digest: string, signature: string): boolean {
  if (!signature || !pubkey || !digest) return false;
  return verifyBftSignature(pubkey, digest, signature);
}

export class MicroPaymentChannelLedger {
  public readonly ledgerId: string;
  public circuitBus: SafetyCircuitBus;
  private readonly hlc: HybridLogicalClock;
  private readonly channels: Map<string, MicroPaymentChannel> = new Map();
  private readonly meterEvents: Map<string, UsageMeterEvent> = new Map();
  private readonly settlementCerts: Map<string, ChannelSettlementCertificate> = new Map();

  constructor(ledgerId: string = 'billing-ledger-primary', circuitBus?: SafetyCircuitBus) {
    this.ledgerId = ledgerId;
    this.circuitBus = circuitBus || globalSafetyCircuitBus;
    this.hlc = new HybridLogicalClock(`bil-${ledgerId}`);
  }

  public bindCircuitBus(circuitBus: SafetyCircuitBus): void {
    this.circuitBus = circuitBus;
  }

  public openChannel(
    tenantId: string,
    agentPubkey: string,
    providerPubkey: string,
    initialDeposit: number,
    reserveThreshold: number = 5.0,
    channelId?: string,
  ): MicroPaymentChannel {
    if (this.circuitBus && this.circuitBus.isTripped()) {
      throw new BillingCircuitTrippedError('Cannot open channel: Safety circuit bus is TRIPPED.');
    }

    if (initialDeposit <= 0) {
      throw new ChannelBalanceConservationError('Initial deposit must be strictly positive.');
    }

    const cid =
      channelId ||
      `mpc-${createHash('sha256').update(`${tenantId}:${Date.now()}:${Math.random()}`).digest('hex').slice(0, 16)}`;

    const channel: MicroPaymentChannel = {
      channelId: cid,
      tenantId,
      agentPubkey,
      providerPubkey,
      initialDeposit,
      currentNonce: 0,
      agentBalance: initialDeposit,
      providerBalance: 0.0,
      reserveThreshold,
      status: 'OPEN',
    };

    this.channels.set(cid, channel);
    return channel;
  }

  public submitStateUpdate(commitment: ChannelStateCommitment): ChannelStateCommitment {
    if (this.circuitBus && this.circuitBus.isTripped()) {
      throw new BillingCircuitTrippedError('Cannot update channel: Safety circuit bus is TRIPPED.');
    }

    const channel = this.channels.get(commitment.channelId);
    if (!channel) {
      throw new ChannelNotFoundError(`Payment channel '${commitment.channelId}' not found.`);
    }

    if (channel.status !== 'OPEN') {
      throw new ChannelDisputeActiveError(`Cannot update state: Channel is in status ${channel.status}.`);
    }

    // 1. Monotonic Nonce (MPC2)
    if (commitment.nonce <= channel.currentNonce) {
      throw new StaleChannelNonceError(
        `Nonce ${commitment.nonce} is stale; channel is already at nonce ${channel.currentNonce}.`,
      );
    }

    // 2. Channel Balance Conservation (MPC1)
    const total = Math.round((commitment.agentBalance + commitment.providerBalance) * 1e6) / 1e6;
    const expected = Math.round(channel.initialDeposit * 1e6) / 1e6;
    if (total !== expected || commitment.agentBalance < 0 || commitment.providerBalance < 0) {
      throw new ChannelBalanceConservationError(
        `Balance conservation breach: agent (${commitment.agentBalance}) + provider (${commitment.providerBalance}) != deposit (${expected}).`,
      );
    }

    // 3. Dual Cryptographic Signatures (MPC2)
    const computedDigest = computeChannelCommitmentDigest(
      commitment.channelId,
      commitment.nonce,
      commitment.agentBalance,
      commitment.providerBalance,
      commitment.hlcTimestamp,
    );
    if (commitment.commitmentDigest !== computedDigest) {
      throw new InvalidChannelSignatureError('State commitment digest mismatch.');
    }

    if (!verifyChannelSignature(channel.agentPubkey, computedDigest, commitment.agentSignature)) {
      throw new InvalidChannelSignatureError('Agent signature verification failed.');
    }

    if (!verifyChannelSignature(channel.providerPubkey, computedDigest, commitment.providerSignature)) {
      throw new InvalidChannelSignatureError('Provider signature verification failed.');
    }

    channel.currentNonce = commitment.nonce;
    channel.agentBalance = commitment.agentBalance;
    channel.providerBalance = commitment.providerBalance;
    channel.latestCommitment = commitment;

    // 4. Safety Circuit Latching under low reserve threshold (MPC4)
    if (channel.agentBalance < channel.reserveThreshold) {
      if (this.circuitBus) {
        this.circuitBus.trip(
          'BILLING_CHANNEL_DEPLETED',
          `channel:${channel.channelId}`,
          {
            channelId: channel.channelId,
            agentBalance: channel.agentBalance,
            reserveThreshold: channel.reserveThreshold,
          },
        );
      }
    }

    return commitment;
  }

  public recordMeterEvent(
    channelId: string,
    unitType: 'TELEMETRY_SPAN' | 'SNARK_PROOF' | 'BARRIER_CUT' | 'METER_UNIT',
    quantity: number,
    unitPrice: number = 0.001,
  ): UsageMeterEvent {
    if (this.circuitBus && this.circuitBus.isTripped()) {
      throw new BillingCircuitTrippedError('Cannot meter usage: Safety circuit bus is TRIPPED.');
    }

    const channel = this.channels.get(channelId);
    if (!channel) {
      throw new ChannelNotFoundError(`Payment channel '${channelId}' not found.`);
    }

    if (channel.status !== 'OPEN') {
      throw new BillingChannelExhaustedError(`Channel '${channelId}' is not open (status=${channel.status}).`);
    }

    const totalCharge = Math.round(quantity * unitPrice * 1e6) / 1e6;
    if (channel.agentBalance < totalCharge) {
      throw new BillingChannelExhaustedError(
        `Insufficient channel balance (${channel.agentBalance.toFixed(6)}) for charge ${totalCharge.toFixed(6)}.`,
      );
    }

    channel.agentBalance = Math.round((channel.agentBalance - totalCharge) * 1e6) / 1e6;
    channel.providerBalance = Math.round((channel.providerBalance + totalCharge) * 1e6) / 1e6;
    channel.currentNonce += 1;

    const hlcTs = this.hlc.now().toString();
    const meterId = `mtr-${createHash('sha256').update(`${channelId}:${channel.currentNonce}:${hlcTs}`).digest('hex').slice(0, 16)}`;
    const evt: UsageMeterEvent = {
      meterId,
      channelId,
      unitType,
      quantity,
      unitPrice,
      totalCharge,
      hlcTimestamp: hlcTs,
    };
    this.meterEvents.set(meterId, evt);

    if (channel.agentBalance < channel.reserveThreshold) {
      if (this.circuitBus) {
        this.circuitBus.trip(
          'BILLING_CHANNEL_DEPLETED',
          `channel:${channelId}`,
          {
            channelId,
            agentBalance: channel.agentBalance,
            reserveThreshold: channel.reserveThreshold,
          },
        );
      }
    }

    return evt;
  }

  public initiateClosure(
    channelId: string,
    proposedCommitment?: ChannelStateCommitment,
  ): MicroPaymentChannel {
    const channel = this.channels.get(channelId);
    if (!channel) {
      throw new ChannelNotFoundError(`Payment channel '${channelId}' not found.`);
    }

    if (channel.status !== 'OPEN') {
      throw new ChannelDisputeActiveError(`Channel '${channelId}' is already in status ${channel.status}.`);
    }

    if (proposedCommitment) {
      if (proposedCommitment.nonce < channel.currentNonce) {
        throw new StaleChannelNonceError(
          `Cannot initiate closure with obsolete nonce ${proposedCommitment.nonce}.`,
        );
      }
      this.submitStateUpdate(proposedCommitment);
    }

    channel.status = 'CLOSING';
    channel.closureInitiatedAtHlc = this.hlc.now().toString();
    return channel;
  }

  public challengeClosure(
    channelId: string,
    higherCommitment: ChannelStateCommitment,
  ): ChannelSettlementCertificate {
    const channel = this.channels.get(channelId);
    if (!channel) {
      throw new ChannelNotFoundError(`Payment channel '${channelId}' not found.`);
    }

    if (channel.status !== 'CLOSING') {
      throw new ChannelDisputeActiveError(`Channel '${channelId}' is not in CLOSING dispute window.`);
    }

    if (higherCommitment.nonce <= channel.currentNonce) {
      throw new StaleChannelNonceError(
        `Challenge rejected: nonce ${higherCommitment.nonce} is not strictly higher than channel nonce ${channel.currentNonce}.`,
      );
    }

    const computedDigest = computeChannelCommitmentDigest(
      higherCommitment.channelId,
      higherCommitment.nonce,
      higherCommitment.agentBalance,
      higherCommitment.providerBalance,
      higherCommitment.hlcTimestamp,
    );
    if (higherCommitment.commitmentDigest !== computedDigest) {
      throw new InvalidChannelSignatureError('Challenge commitment digest mismatch.');
    }

    if (!verifyChannelSignature(channel.agentPubkey, computedDigest, higherCommitment.agentSignature)) {
      throw new InvalidChannelSignatureError('Challenge agent signature invalid.');
    }
    if (!verifyChannelSignature(channel.providerPubkey, computedDigest, higherCommitment.providerSignature)) {
      throw new InvalidChannelSignatureError('Challenge provider signature invalid.');
    }

    const finalProviderBal = channel.initialDeposit;
    const finalAgentBal = 0.0;
    channel.status = 'SLASHED';
    channel.agentBalance = finalAgentBal;
    channel.providerBalance = finalProviderBal;
    channel.currentNonce = higherCommitment.nonce;

    const certId = `set-${createHash('sha256').update(`slash:${channelId}:${Date.now()}`).digest('hex').slice(0, 16)}`;
    const cert: ChannelSettlementCertificate = {
      settlementId: certId,
      channelId,
      finalNonce: higherCommitment.nonce,
      finalAgentBalance: finalAgentBal,
      finalProviderBalance: finalProviderBal,
      settlementType: 'CHEATER_SLASHED',
      createdAtHlc: this.hlc.now().toString(),
    };
    this.settlementCerts.set(certId, cert);
    return cert;
  }

  public finalizeClosure(channelId: string): ChannelSettlementCertificate {
    const channel = this.channels.get(channelId);
    if (!channel) {
      throw new ChannelNotFoundError(`Payment channel '${channelId}' not found.`);
    }

    if (channel.status !== 'CLOSING' && channel.status !== 'OPEN') {
      throw new ChannelDisputeActiveError(`Channel '${channelId}' is already ${channel.status}.`);
    }

    channel.status = 'SETTLED';
    const certId = `set-${createHash('sha256').update(`final:${channelId}:${Date.now()}`).digest('hex').slice(0, 16)}`;
    const cert: ChannelSettlementCertificate = {
      settlementId: certId,
      channelId,
      finalNonce: channel.currentNonce,
      finalAgentBalance: channel.agentBalance,
      finalProviderBalance: channel.providerBalance,
      settlementType: channel.closureInitiatedAtHlc ? 'DISPUTE_FINALIZED' : 'COOPERATIVE',
      createdAtHlc: this.hlc.now().toString(),
    };
    this.settlementCerts.set(certId, cert);
    return cert;
  }

  public getChannel(channelId: string): MicroPaymentChannel | undefined {
    return this.channels.get(channelId);
  }

  public getStatus(): Record<string, any> {
    let activeCount = 0;
    let totalDeposit = 0;
    let totalProviderEarned = 0;

    for (const c of this.channels.values()) {
      if (c.status === 'OPEN') activeCount++;
      totalDeposit += c.initialDeposit;
      totalProviderEarned += c.providerBalance;
    }

    return {
      ledgerId: this.ledgerId,
      totalChannelsCount: this.channels.size,
      activeChannelsCount: activeCount,
      totalMeterEventsCount: this.meterEvents.size,
      settlementCertificatesCount: this.settlementCerts.size,
      totalLockedDeposit: Math.round(totalDeposit * 1e6) / 1e6,
      totalProviderEarned: Math.round(totalProviderEarned * 1e6) / 1e6,
      circuitTripped: this.circuitBus ? this.circuitBus.isTripped() : false,
    };
  }
}

// Authoritative Global Singleton
export const globalBillingLedger = new MicroPaymentChannelLedger();
export const globalPaymentChannelLedger = globalBillingLedger;
