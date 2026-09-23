import { test, describe } from 'node:test';
import assert from 'node:assert';

import {
  AnomalyType,
  SelfHealingActionType,
  SwarmSelfHealingController,
} from '../src/selfhealing.ts';

describe('Autonomous Counterfactual Swarm Optimization & Self-Healing Controller (TypeScript / ADR 0042)', () => {
  test('detects repeated tool failures and synthesizes dynamic blacklist action', () => {
    const controller = new SwarmSelfHealingController({ maxConsecutiveToolFailures: 3 });

    const spanError1 = {
      spanId: 's-1',
      swarmId: 'swarm-omega',
      sourceAgent: 'coder',
      targetAgent: 'broken_db_query',
      spanType: 'tool',
      status: 'error',
      outputPayload: { error: 'Connection refused: DB offline' },
    };
    const spanError2 = {
      spanId: 's-2',
      swarmId: 'swarm-omega',
      sourceAgent: 'coder',
      targetAgent: 'broken_db_query',
      spanType: 'tool',
      status: 'error',
      outputPayload: { error: 'Connection refused: DB offline' },
    };
    const spanError3 = {
      spanId: 's-3',
      swarmId: 'swarm-omega',
      sourceAgent: 'coder',
      targetAgent: 'broken_db_query',
      spanType: 'tool',
      status: 'error',
      outputPayload: { error: 'Connection refused: DB offline' },
    };

    assert.strictEqual(controller.observeSpan(spanError1), null);
    assert.strictEqual(controller.observeSpan(spanError2), null);

    const action = controller.observeSpan(spanError3);
    assert.ok(action, 'Should trigger self-healing action on 3rd failure');
    assert.strictEqual(action.actionType, SelfHealingActionType.BLACKLIST_TOOL);
    assert.strictEqual(action.anomalyDetected, AnomalyType.REPEATED_TOOL_FAILURES);
    assert.strictEqual(action.target, 'broken_db_query');
    assert.ok(action.counterfactual.tokensSaved > 0);
    assert.ok(action.counterfactual.costSaved > 0);

    const status = controller.status();
    assert.strictEqual(status.active_actions_count, 1);
    assert.ok(status.cumulative_tokens_saved >= 2500);
  });

  test('detects Shannon entropy collapse and routes agent turns to fallback model', () => {
    const controller = new SwarmSelfHealingController({ entropyFloorTrigger: 0.35 });

    const spanLoop = {
      spanId: 's-loop-1',
      swarmId: 'swarm-loop',
      sourceAgent: 'writer',
      spanType: 'turn',
      entropyScore: 0.22,
      inputPayload: { model: 'gpt-4o' },
    };

    const action = controller.observeSpan(spanLoop);
    assert.ok(action, 'Should trigger model fallback action on entropy collapse');
    assert.strictEqual(action.actionType, SelfHealingActionType.ROUTE_MODEL_FALLBACK);
    assert.strictEqual(action.target, 'gpt-4o-mini');

    const routedModel = controller.getModelRoute('writer', 'gpt-4o');
    assert.strictEqual(routedModel, 'gpt-4o-mini');
  });

  test('successfully rolls back actions and restores original state', () => {
    const controller = new SwarmSelfHealingController();

    const action = controller.remediate({
      swarmId: 'swarm-test',
      agentName: 'researcher',
      actionType: SelfHealingActionType.ROUTE_MODEL_FALLBACK,
      target: 'claude-3-5-haiku',
    });

    assert.strictEqual(action.status, 'active');
    assert.strictEqual(controller.getModelRoute('researcher'), 'claude-3-5-haiku');

    // Rollback action
    const rolled = controller.rollbackAction(action.id);
    assert.strictEqual(rolled, true);

    const status = controller.status();
    assert.strictEqual(status.active_actions_count, 0);

    // Rollback non-existent action
    assert.strictEqual(controller.rollbackAction('fake-id'), false);
  });

  test('rollbackAll reverts all active mitigations for a swarm session', () => {
    const controller = new SwarmSelfHealingController();

    controller.remediate({
      swarmId: 'swarm-batch',
      agentName: 'agent-1',
      actionType: SelfHealingActionType.BLACKLIST_TOOL,
      target: 'bad_api',
    });
    controller.remediate({
      swarmId: 'swarm-batch',
      agentName: 'agent-2',
      actionType: SelfHealingActionType.BLACKLIST_TOOL,
      target: 'bad_sql',
    });
    controller.remediate({
      swarmId: 'swarm-other',
      agentName: 'agent-3',
      actionType: SelfHealingActionType.BLACKLIST_TOOL,
      target: 'bad_search',
    });

    assert.strictEqual(controller.status().active_actions_count, 3);

    const count = controller.rollbackAll('swarm-batch');
    assert.strictEqual(count, 2);
    assert.strictEqual(controller.status().active_actions_count, 1);

    const countRemaining = controller.rollbackAll();
    assert.strictEqual(countRemaining, 1);
    assert.strictEqual(controller.status().active_actions_count, 0);
  });

  test('controller honors disabled state and skips analysis', () => {
    const controller = new SwarmSelfHealingController({ enabled: false });

    const spanError = {
      spanId: 's-dis',
      swarmId: 'swarm-dis',
      sourceAgent: 'coder',
      targetAgent: 'broken_db',
      spanType: 'tool',
      status: 'error',
    };

    for (let i = 0; i < 5; i++) {
      assert.strictEqual(controller.observeSpan(spanError), null);
    }
    assert.strictEqual(controller.status().total_actions_recorded, 0);
  });
});
