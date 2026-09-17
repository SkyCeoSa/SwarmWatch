/**
 * Unit and Integration Tests for SwarmWatch Declarative Guardrail Policy Engine & Policy DSL
 * (TypeScript Edition - Seam 12 Parity / ADR 0021 & ADR 0022).
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import {
  SafePolicyParser,
  PolicyEngine,
  globMatch,
  mergeRuleConfigs,
  EntropyCircuitBreaker,
  SwarmCircuitBreakException,
} from "../src/index.ts";

test("Declarative Guardrail Policy Engine (TypeScript Parity - Seam 12 / ADR 0022)", async (t) => {
  await t.test("SafePolicyParser parses standard JSON guardrail documents", () => {
    const rawJson = JSON.stringify({
      version: "1.0",
      name: "enterprise_guardrail",
      description: "Production trading and data guardrails",
      defaults: {
        min_entropy: 0.40,
        max_similarity: 0.85,
        max_turns: 50,
        token_budget: 100000,
      },
      agents: {
        sql_agent: {
          min_entropy: 0.15,
          prohibited_tools: ["drop_table", "truncate_table"],
        },
      },
      rules: [
        {
          id: "prevent-data-destruction",
          agent_pattern: "sql_*",
          condition: {
            payload_contains_any: ["DROP TABLE", "TRUNCATE"],
          },
          action: "trip",
          message: "Destructive SQL operation intercepted",
        },
      ],
    });

    const policy = SafePolicyParser.fromJson(rawJson);
    assert.strictEqual(policy.version, "1.0");
    assert.strictEqual(policy.name, "enterprise_guardrail");
    assert.strictEqual(policy.defaults?.min_entropy, 0.40);
    assert.strictEqual(policy.defaults?.max_turns, 50);
    assert.ok(policy.agents?.sql_agent);
    assert.strictEqual(policy.agents.sql_agent.min_entropy, 0.15);
    assert.deepStrictEqual(policy.agents.sql_agent.prohibited_tools, ["drop_table", "truncate_table"]);
    assert.strictEqual(policy.rules?.length, 1);
    assert.strictEqual(policy.rules[0].id, "prevent-data-destruction");
  });

  await t.test("SafePolicyParser parses structured YAML with indentation, lists, and comments", () => {
    const yamlText = `
    # SwarmWatch Enterprise Guardrail Policy
    version: "1.0"
    name: "trading_desk_policy"
    description: "Strict risk constraints for algorithmic trading bots"
    defaults:
      min_entropy: 0.35
      max_similarity: 0.80
      max_turns: 40
      token_budget: 80000
    agents:
      execution_bot:
        min_entropy: 0.20
        token_budget: 150000
        prohibited_tools: [liquidate_all, override_stoploss]
      researcher_*:
        token_budget: 300000
        min_entropy: 0.50
    rules:
      - id: "block-emergency-override"
        agent_pattern: "execution_*"
        condition:
          tool_name_in: [override_stoploss, disable_circuit_breaker]
        action: "trip"
        message: "Emergency stoploss override attempted by trading agent"
    `;

    const policy = SafePolicyParser.fromYaml(yamlText);
    assert.strictEqual(policy.version, "1.0");
    assert.strictEqual(policy.name, "trading_desk_policy");
    assert.strictEqual(policy.defaults?.min_entropy, 0.35);
    assert.strictEqual(policy.defaults?.max_turns, 40);

    assert.ok(policy.agents?.execution_bot);
    assert.strictEqual(policy.agents.execution_bot.min_entropy, 0.20);
    assert.strictEqual(policy.agents.execution_bot.token_budget, 150000);
    assert.ok(policy.agents.execution_bot.prohibited_tools?.includes("liquidate_all"));
    assert.ok(policy.agents.execution_bot.prohibited_tools?.includes("override_stoploss"));

    assert.ok(policy.agents["researcher_*"]);
    assert.strictEqual(policy.agents["researcher_*"].token_budget, 300000);
    assert.strictEqual(policy.agents["researcher_*"].min_entropy, 0.50);

    assert.strictEqual(policy.rules?.length, 1);
    assert.strictEqual(policy.rules[0].id, "block-emergency-override");
  });

  await t.test("SafePolicyParser round-trips files from filesystem", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "sw-ts-policy-"));
    const yamlPath = path.join(tmpDir, "policy.yaml");
    const jsonPath = path.join(tmpDir, "policy.json");

    try {
      const yamlContent = `
      version: "1.0"
      name: "fs_policy"
      defaults:
        min_entropy: 0.30
        max_turns: 25
      `;
      fs.writeFileSync(yamlPath, yamlContent, "utf-8");

      const jsonContent = JSON.stringify({
        version: "1.0",
        name: "json_fs_policy",
        defaults: { min_entropy: 0.45 },
      });
      fs.writeFileSync(jsonPath, jsonContent, "utf-8");

      const pYaml = SafePolicyParser.fromFile(yamlPath);
      assert.strictEqual(pYaml.name, "fs_policy");
      assert.strictEqual(pYaml.defaults?.min_entropy, 0.30);
      assert.strictEqual(pYaml.defaults?.max_turns, 25);

      const pJson = SafePolicyParser.fromFile(jsonPath);
      assert.strictEqual(pJson.name, "json_fs_policy");
      assert.strictEqual(pJson.defaults?.min_entropy, 0.45);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  await t.test("PolicyEngine resolves hierarchical role-based inheritance & glob matching", () => {
    const policy = SafePolicyParser.fromObject({
      version: "1.0",
      name: "role_inheritance_test",
      defaults: {
        min_entropy: 0.40,
        max_similarity: 0.85,
        max_turns: 50,
        token_budget: 100000,
        prohibited_tools: ["system_reboot"],
      },
      agents: {
        "coder-*": {
          min_entropy: 0.25,
          token_budget: 200000,
          prohibited_tools: ["git_push_force"],
        },
        "coder-senior": {
          token_budget: 500000,
        },
        "analyst": {
          min_entropy: 0.55,
        },
      },
    });

    const engine = new PolicyEngine(policy);

    // 1. Unknown agent falls back to global defaults
    const generalConfig = engine.getEffectiveConfig("general_agent");
    assert.strictEqual(generalConfig.min_entropy, 0.40);
    assert.strictEqual(generalConfig.token_budget, 100000);
    assert.deepStrictEqual(generalConfig.prohibited_tools, ["system_reboot"]);

    // 2. coder-junior matches pattern 'coder-*'
    const juniorConfig = engine.getEffectiveConfig("coder-junior");
    assert.strictEqual(juniorConfig.min_entropy, 0.25); // Overridden
    assert.strictEqual(juniorConfig.token_budget, 200000); // Overridden
    assert.strictEqual(juniorConfig.max_turns, 50); // Inherited from defaults
    assert.ok(juniorConfig.prohibited_tools?.includes("system_reboot")); // Inherited
    assert.ok(juniorConfig.prohibited_tools?.includes("git_push_force")); // Appended

    // 3. coder-senior matches both 'coder-*' and 'coder-senior'
    const seniorConfig = engine.getEffectiveConfig("coder-senior");
    assert.strictEqual(seniorConfig.min_entropy, 0.25); // From coder-*
    assert.strictEqual(seniorConfig.token_budget, 500000); // Specific coder-senior override

    // 4. analyst matches exact role
    const analystConfig = engine.getEffectiveConfig("analyst");
    assert.strictEqual(analystConfig.min_entropy, 0.55);
    assert.strictEqual(analystConfig.token_budget, 100000);
  });

  await t.test("PolicyEngine enforces prohibited tools", () => {
    const policy = SafePolicyParser.fromObject({
      name: "tool_security_policy",
      defaults: {
        prohibited_tools: ["drop_*", "rm_rf"],
        action: "trip",
      },
    });

    const engine = new PolicyEngine(policy);

    // Allowed tool passes
    const resAllowed = engine.evaluateTurn({
      agent: "db_worker",
      tool_name: "select_query",
    });
    assert.strictEqual(resAllowed.decision, "pass");

    // Prohibited exact tool trips
    const resRm = engine.evaluateTurn({
      agent: "db_worker",
      tool_name: "rm_rf",
    });
    assert.strictEqual(resRm.decision, "trip");
    assert.ok(resRm.reason?.includes("rm_rf"));

    // Prohibited wildcard tool trips
    const resDrop = engine.evaluateTurn({
      agent: "db_worker",
      tool_name: "drop_database",
    });
    assert.strictEqual(resDrop.decision, "trip");
    assert.ok(resDrop.reason?.includes("drop_database"));
  });

  await t.test("PolicyEngine evaluates declarative event-condition-action rules", () => {
    const policy = SafePolicyParser.fromObject({
      name: "custom_rules_policy",
      rules: [
        {
          id: "prevent-sql-injection-simulation",
          agent_pattern: "db_*",
          condition: {
            payload_contains_any: ["drop table", "truncate table", "-- shutdown"],
          },
          action: "trip",
          message: "Potential malicious SQL injection pattern intercepted",
        },
        {
          id: "warn-on-credit-card-number",
          agent_pattern: "*",
          condition: {
            payload_contains_all: ["card_number", "cvv"],
          },
          action: "warn",
          message: "Payment card payload flagged for auditing",
        },
      ],
    });

    const engine = new PolicyEngine(policy);

    // Normal query passes
    const resNormal = engine.evaluateTurn({
      agent: "db_analyst",
      text: "SELECT count(*) FROM users WHERE active = 1;",
    });
    assert.strictEqual(resNormal.decision, "pass");

    // Malicious query trips
    const resMalicious = engine.evaluateTurn({
      agent: "db_analyst",
      text: "DROP TABLE users;",
    });
    assert.strictEqual(resMalicious.decision, "trip");
    assert.strictEqual(resMalicious.matched_rule_id, "prevent-sql-injection-simulation");
    assert.ok(resMalicious.reason?.includes("malicious SQL injection"));

    // Card payload triggers warning
    const resCard = engine.evaluateTurn({
      agent: "checkout_agent",
      payload: { card_number: "4111222233334444", cvv: "123" },
    });
    assert.strictEqual(resCard.decision, "warn");
    assert.strictEqual(resCard.matched_rule_id, "warn-on-credit-card-number");
  });

  await t.test("EntropyCircuitBreaker integrates seamlessly with PolicyEngine", () => {
    const policy = SafePolicyParser.fromObject({
      name: "circuit_integration_policy",
      defaults: {
        min_entropy: 0.40,
        max_similarity: 0.80,
        token_budget: 50000,
        prohibited_tools: ["emergency_halt_cluster"],
      },
      agents: {
        "creative_writer": {
          min_entropy: 0.70, // Strict entropy required for creative agents
        },
        "repetitive_worker": {
          min_entropy: 0.10, // Lenient entropy
        },
      },
    });

    const engine = new PolicyEngine(policy);
    const breaker = new EntropyCircuitBreaker({ policyEngine: engine });

    // 1. Prohibited tool trips circuit breaker immediately
    assert.throws(
      () => {
        breaker.evaluate(
          "Running cluster maintenance command",
          "admin_agent",
          "swarm-1",
          10,
          "emergency_halt_cluster"
        );
      },
      (err) => {
        return (
          err instanceof SwarmCircuitBreakException &&
          err.message.includes("Prohibited tool 'emergency_halt_cluster'")
        );
      }
    );

    // 2. Strict creative agent trips warning/halt on low entropy
    breaker.reset();
    const repetitiveText = "word ".repeat(15);
    // Evaluating for creative_writer (min_entropy=0.70) trips warning because entropy < 0.70
    const decision = breaker.evaluate(
      repetitiveText,
      "creative_writer",
      "swarm-1",
      15
    );
    assert.strictEqual(decision.decision, "warning");
    assert.ok(decision.reason?.includes("entropy") || decision.reason?.includes("repetition"));
  });

  await t.test("PolicyEngine hot-reloads policy dynamically without process restart", () => {
    const initialPolicy = SafePolicyParser.fromObject({
      name: "v1_policy",
      defaults: { min_entropy: 0.30 },
      rules: [
        {
          id: "rule-v1",
          agent_pattern: "*",
          condition: { payload_contains_any: ["forbidden_v1"] },
          action: "trip",
        },
      ],
    });

    const engine = new PolicyEngine(initialPolicy);

    assert.strictEqual(
      engine.evaluateTurn({ agent: "bot", text: "forbidden_v1 query" }).decision,
      "trip"
    );
    assert.strictEqual(
      engine.evaluateTurn({ agent: "bot", text: "forbidden_v2 query" }).decision,
      "pass"
    );

    // Hot-reload with v2 policy
    const v2Policy = SafePolicyParser.fromObject({
      name: "v2_policy",
      defaults: { min_entropy: 0.50 },
      rules: [
        {
          id: "rule-v2",
          agent_pattern: "*",
          condition: { payload_contains_any: ["forbidden_v2"] },
          action: "trip",
        },
      ],
    });

    engine.hotReload(v2Policy);

    // Now v1 passes and v2 trips immediately!
    assert.strictEqual(
      engine.evaluateTurn({ agent: "bot", text: "forbidden_v1 query" }).decision,
      "pass"
    );
    assert.strictEqual(
      engine.evaluateTurn({ agent: "bot", text: "forbidden_v2 query" }).decision,
      "trip"
    );
    assert.strictEqual(engine.getEffectiveConfig("bot").min_entropy, 0.50);
  });

  await t.test("high-throughput benchmark: evaluates turn in < 25 microseconds", () => {
    const policy = SafePolicyParser.fromObject({
      name: "perf_policy",
      defaults: {
        min_entropy: 0.35,
        max_similarity: 0.85,
        token_budget: 100000,
        prohibited_tools: ["bad_tool_1", "bad_tool_2"],
      },
      agents: {
        "worker-*": {
          token_budget: 250000,
        },
      },
      rules: [
        {
          id: "rule-keyword",
          agent_pattern: "worker-*",
          condition: {
            payload_contains_any: ["dangerous_token"],
          },
          action: "trip",
        },
      ],
    });

    const engine = new PolicyEngine(policy);

    // Warm-up
    for (let i = 0; i < 500; i++) {
      engine.evaluateTurn({
        agent: "worker-compute-1",
        text: "Analyzing transaction data for portfolio optimization",
        token_count: 24,
        tool_name: "calculate_risk",
        entropy_score: 0.82,
        similarity_score: 0.12,
      });
    }

    const iterations = 5000;
    const start = performance.now();
    for (let i = 0; i < iterations; i++) {
      engine.evaluateTurn({
        agent: "worker-compute-1",
        text: "Analyzing transaction data for portfolio optimization",
        token_count: 24,
        tool_name: "calculate_risk",
        entropy_score: 0.82,
        similarity_score: 0.12,
      });
    }
    const elapsedMs = performance.now() - start;
    const avgUs = (elapsedMs / iterations) * 1000;

    assert.ok(
      avgUs < 25.0,
      `PolicyEngine evaluation took ${avgUs.toFixed(3)}µs (must be < 25µs)`
    );
  });
});
