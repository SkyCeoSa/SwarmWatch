// @ts-check
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';

import {
  DownstreamEffectBoundary,
  DownstreamIdempotencyContract,
  EffectPhase,
  DownstreamReceipt,
  UncoordinatedRetryBlockedError,
  StaleEpochError,
  globalDownstreamEffectBoundary,
} from '../src/downstream_effect.ts';

describe('Seam 62: Downstream Effect Protocol & 2PC Outcome-Unknown (TypeScript Edition)', () => {
  test('Happy Path: prepare, dispatch, acknowledge transitions to COMMITTED', async () => {
    const boundary = new DownstreamEffectBoundary();
    const prep = boundary.prepare({
      tenantId: 'tenant-ts-alpha',
      effectKey: 'stripe:charge:cust_101',
      targetUri: 'https://stripe.mock/v1/charges',
      payload: { amount: 3500 },
      epoch: 5,
      contract: DownstreamIdempotencyContract.DOWNSTREAM_FENCED,
    });

    assert.equal(prep.status, EffectPhase.PREPARED);
    assert.ok(prep.effectId.startsWith('eff_'));
    assert.ok(prep.idempotencyKey.length > 10);
    assert.equal(prep.epoch, 5);

    // Dispatch
    const dispatchRes = boundary.dispatch(prep.effectId, (eff) => {
      assert.equal(eff.status, EffectPhase.PREPARED);
      return { ok: true, providerId: 'ch_stripe_999' };
    });
    assert.equal(dispatchRes.status, EffectPhase.DISPATCHED);

    // Acknowledge
    const ackRes = boundary.acknowledge(
      prep.effectId,
      new DownstreamReceipt('ch_stripe_999', 'sha256:stripe_ack', 12.5),
      5
    );
    assert.equal(ackRes.status, EffectPhase.COMMITTED);
    assert.equal(ackRes.isOutcomeUnknown, false);
    assert.equal(ackRes.receipt.downstreamConfirmationId, 'ch_stripe_999');
  });

  test('Network timeout transitions to OUTCOME_UNKNOWN and blocks uncoordinated retry', () => {
    const boundary = new DownstreamEffectBoundary();
    const prep = boundary.prepare({
      tenantId: 'tenant-ts-beta',
      effectKey: 'uncoordinated:send_sms',
      targetUri: 'https://sms.mock/v1/send',
      payload: { to: '+1234567890' },
      epoch: 8,
      contract: DownstreamIdempotencyContract.UNCOORDINATED,
    });

    const timeoutFn = () => {
      throw new Error('ETIMEDOUT: Connection dropped mid-request');
    };

    const res = boundary.dispatch(prep.effectId, timeoutFn);
    assert.equal(res.status, EffectPhase.OUTCOME_UNKNOWN);
    assert.equal(res.isOutcomeUnknown, true);
    assert.ok(res.errorMessage.includes('ETIMEDOUT'));

    // Attempting blind retry must throw UncoordinatedRetryBlockedError
    assert.throws(
      () => boundary.dispatch(prep.effectId, timeoutFn),
      UncoordinatedRetryBlockedError
    );
  });

  test('Reconciliation probe detects downstream commit and reconciles to COMMITTED', () => {
    const boundary = new DownstreamEffectBoundary();
    const prep = boundary.prepare({
      tenantId: 'tenant-ts-gamma',
      effectKey: 'db:write:user_status',
      targetUri: 'https://db.mock/commit',
      payload: { user: 'u_42', active: true },
      epoch: 14,
      contract: DownstreamIdempotencyContract.DOWNSTREAM_FENCED,
    });

    // Dispatch drops connection
    boundary.dispatch(prep.effectId, () => {
      throw new Error('ECONNRESET');
    });
    assert.equal(boundary.getEffect(prep.effectId).status, EffectPhase.OUTCOME_UNKNOWN);

    // Probe confirms commit
    const probeFn = (effectId, idemKey) => {
      return {
        found: true,
        status: 'COMMITTED',
        downstreamId: 'tx_db_007',
        digest: 'sha256:verified_db_digest',
      };
    };

    const reconRes = boundary.reconcile(prep.effectId, probeFn);
    assert.equal(reconRes.status, EffectPhase.COMMITTED);
    assert.equal(reconRes.reconciled, true);
    assert.equal(reconRes.downstreamConfirmationId, 'tx_db_007');

    const updated = boundary.getEffect(prep.effectId);
    assert.equal(updated.status, EffectPhase.COMMITTED);
    assert.equal(updated.reconciled, true);
  });

  test('Reconciliation probe detects downstream not found and aborts', () => {
    const boundary = new DownstreamEffectBoundary();
    const prep = boundary.prepare({
      tenantId: 'tenant-ts-delta',
      effectKey: 'payment:refund:ref_1',
      targetUri: 'https://payment.mock/refund',
      payload: { id: 'ref_1' },
      epoch: 19,
    });

    boundary.dispatch(prep.effectId, () => {
      throw new Error('504 Gateway Timeout');
    });
    assert.equal(boundary.getEffect(prep.effectId).status, EffectPhase.OUTCOME_UNKNOWN);

    const probeNotFound = () => ({ found: false });
    const reconRes = boundary.reconcile(prep.effectId, probeNotFound);
    assert.equal(reconRes.status, EffectPhase.ABORTED);
    assert.equal(reconRes.reconciled, true);

    const updated = boundary.getEffect(prep.effectId);
    assert.equal(updated.status, EffectPhase.ABORTED);
  });

  test('Stale epoch rejection on acknowledge', () => {
    const boundary = new DownstreamEffectBoundary();
    const prep = boundary.prepare({
      tenantId: 'tenant-stale',
      effectKey: 'test:stale',
      targetUri: 'https://api.mock/action',
      payload: {},
      epoch: 30,
    });

    boundary.setTenantEpochFence('tenant-stale', 35);

    assert.throws(
      () =>
        boundary.acknowledge(
          prep.effectId,
          new DownstreamReceipt('tx_stale', 'sha256:d'),
          30
        ),
      StaleEpochError
    );
  });

  test('In-memory latency & micro-benchmark: sub-250 microsecond execution (audit SLO)', () => {
    const boundary = new DownstreamEffectBoundary();
    // Warmup JIT
    for (let w = 0; w < 20; w++) {
      const wp = boundary.prepare({
        tenantId: 'warmup',
        effectKey: `warm:${w}`,
        targetUri: 'https://warmup.mock',
        payload: { w },
        epoch: 1,
      });
      boundary.dispatch(wp.effectId, () => ({ ok: true }));
      boundary.acknowledge(wp.effectId, new DownstreamReceipt(`tx_${w}`, 'hash'), 1);
    }

    const t0 = performance.now();
    const N = 100;
    for (let i = 0; i < N; i++) {
      const p = boundary.prepare({
        tenantId: 'bench-tenant',
        effectKey: `bench:${i}`,
        targetUri: 'https://bench.mock',
        payload: { i },
        epoch: 1,
      });
      boundary.dispatch(p.effectId, () => ({ ok: true }));
      boundary.acknowledge(p.effectId, new DownstreamReceipt(`tx_${i}`, 'hash'), 1);
    }
    const elapsedUs = ((performance.now() - t0) * 1000) / N;
    assert.ok(elapsedUs < 250.0, `Average latency too high: ${elapsedUs.toFixed(2)}us`);
  });

  test('Global singleton & status reporting', () => {
    const status = globalDownstreamEffectBoundary.getStatus();
    assert.ok(typeof status.total_effects === 'number');
    assert.ok(typeof status.outcome_unknown_count === 'number');
    assert.ok(typeof status.committed_count === 'number');
  });
});
