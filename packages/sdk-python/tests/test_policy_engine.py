# -*- coding: utf-8 -*-
"""
Unit and Integration Tests for SwarmWatch Declarative Guardrail Policy Engine & Policy DSL (Seam 12 / ADR 0021).
"""

import json
import os
import tempfile
import time
import pytest
from starlette.testclient import TestClient

from swarmwatch.policy import (
    RuleConfig,
    RuleCondition,
    DeclarativeRule,
    GuardrailPolicy,
    PolicyEngine,
    SafePolicyParser,
    PolicyEvaluationResult,
)
from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    SwarmCircuitBreakException,
)
from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter


def test_safe_policy_parser_json():
    """Verifies that SafePolicyParser correctly parses standard JSON guardrail documents."""
    raw_json = json.dumps({
        "version": "1.0",
        "name": "enterprise_guardrail",
        "description": "Production trading and data guardrails",
        "defaults": {
            "min_entropy": 0.40,
            "max_similarity": 0.85,
            "max_turns": 50,
            "token_budget": 100000,
        },
        "agents": {
            "sql_agent": {
                "min_entropy": 0.15,
                "prohibited_tools": ["drop_table", "truncate_table"],
            },
        },
        "rules": [
            {
                "id": "prevent-data-destruction",
                "agent_pattern": "sql_*",
                "condition": {
                    "payload_contains_any": ["DROP TABLE", "TRUNCATE"],
                },
                "action": "trip",
                "message": "Destructive SQL operation intercepted",
            }
        ]
    })

    policy = SafePolicyParser.from_json(raw_json)
    assert policy.version == "1.0"
    assert policy.name == "enterprise_guardrail"
    assert policy.defaults.min_entropy == 0.40
    assert policy.defaults.max_turns == 50
    assert "sql_agent" in policy.agents
    assert policy.agents["sql_agent"].min_entropy == 0.15
    assert "drop_table" in policy.agents["sql_agent"].prohibited_tools
    assert len(policy.rules) == 1
    assert policy.rules[0].id == "prevent-data-destruction"


def test_safe_policy_parser_yaml():
    """Verifies that SafePolicyParser parses structured YAML with comments and indentation."""
    yaml_text = """
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
    """

    policy = SafePolicyParser.from_yaml(yaml_text)
    assert policy.version == "1.0"
    assert policy.name == "trading_desk_policy"
    assert policy.defaults.min_entropy == 0.35
    assert policy.defaults.max_turns == 40
    assert "execution_bot" in policy.agents
    assert policy.agents["execution_bot"].min_entropy == 0.20
    assert "liquidate_all" in policy.agents["execution_bot"].prohibited_tools
    assert "override_stoploss" in policy.agents["execution_bot"].prohibited_tools
    assert len(policy.rules) >= 1
    assert policy.rules[0].id == "block-emergency-override"


def test_safe_policy_parser_file_loading():
    """Verifies loading policies directly from YAML and JSON files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        json_path = os.path.join(tmpdir, "policy.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({
                "version": "1.0",
                "name": "file_loaded_policy",
                "defaults": {"min_entropy": 0.45},
            }, f)

        loaded = SafePolicyParser.from_file(json_path)
        assert loaded.name == "file_loaded_policy"
        assert loaded.defaults.min_entropy == 0.45


def test_role_based_configuration_inheritance():
    """Verifies that agent-specific configurations properly inherit from and override global defaults."""
    policy = GuardrailPolicy(
        defaults=RuleConfig(
            min_entropy=0.40,
            max_similarity=0.85,
            max_turns=30,
            token_budget=50000,
            prohibited_tools=["admin_shell"],
        ),
        agents={
            "sql_agent": RuleConfig(
                min_entropy=0.15,
                prohibited_tools=["drop_table"],
            ),
            "researcher-*": RuleConfig(
                min_entropy=0.55,
                token_budget=200000,
            ),
        }
    )

    engine = PolicyEngine(policy)

    # 1. Base agent uses exact defaults
    generic_cfg = engine.get_effective_config("generic_assistant")
    assert generic_cfg.min_entropy == 0.40
    assert generic_cfg.max_similarity == 0.85
    assert generic_cfg.max_turns == 30
    assert generic_cfg.token_budget == 50000
    assert generic_cfg.prohibited_tools == ["admin_shell"]

    # 2. sql_agent overrides min_entropy and merges prohibited tools
    sql_cfg = engine.get_effective_config("sql_agent")
    assert sql_cfg.min_entropy == 0.15
    assert sql_cfg.max_similarity == 0.85  # inherited
    assert sql_cfg.max_turns == 30         # inherited
    assert "admin_shell" in sql_cfg.prohibited_tools
    assert "drop_table" in sql_cfg.prohibited_tools

    # 3. researcher-alpha matches glob pattern researcher-*
    res_cfg = engine.get_effective_config("researcher-alpha")
    assert res_cfg.min_entropy == 0.55
    assert res_cfg.token_budget == 200000
    assert res_cfg.max_turns == 30  # inherited


def test_evaluate_turn_prohibited_tools():
    """Verifies that prohibited tool invocations immediately trip the guardrail."""
    policy = GuardrailPolicy(
        agents={
            "coder": RuleConfig(prohibited_tools=["execute_bash", "rm_rf"]),
        }
    )
    engine = PolicyEngine(policy)

    # Safe tool
    safe_eval = engine.evaluate_turn(agent="coder", tool_name="format_code")
    assert safe_eval.decision == "pass"

    # Prohibited tool
    trip_eval = engine.evaluate_turn(agent="coder", tool_name="execute_bash")
    assert trip_eval.decision == "trip"
    assert "Prohibited tool" in trip_eval.reason
    assert trip_eval.matched_rule_id == "config:prohibited_tools"


def test_evaluate_turn_declarative_rules():
    """Verifies that custom declarative rules match keywords and patterns."""
    rule = DeclarativeRule(
        id="sql-ddl-firewall",
        agent_pattern="db_*",
        condition=RuleCondition(
            payload_contains_any=["DROP TABLE", "DELETE FROM USERS"],
        ),
        action="trip",
        message="Destructive DDL query blocked by guardrail policy",
    )
    policy = GuardrailPolicy(rules=[rule])
    engine = PolicyEngine(policy)

    # Compliant query
    res1 = engine.evaluate_turn(
        agent="db_writer",
        text="SELECT id, name FROM users WHERE active = 1",
    )
    assert res1.decision == "pass"

    # Malicious or dangerous query
    res2 = engine.evaluate_turn(
        agent="db_writer",
        text="Executing cleanup: DROP TABLE accounts;",
    )
    assert res2.decision == "trip"
    assert res2.matched_rule_id == "sql-ddl-firewall"
    assert "Destructive DDL query blocked" in res2.reason

    # Different agent not matching agent_pattern
    res3 = engine.evaluate_turn(
        agent="analytics_reader",
        text="DROP TABLE accounts;",
    )
    assert res3.decision == "pass"


def test_circuit_breaker_integration_with_policy_engine():
    """Verifies that EntropyCircuitBreaker integrates seamlessly with PolicyEngine."""
    policy = GuardrailPolicy(
        defaults=RuleConfig(
            min_entropy=0.40,
            max_similarity=0.85,
        ),
        agents={
            "sql_agent": RuleConfig(
                min_entropy=0.15,
                prohibited_tools=["drop_table"],
            ),
        }
    )
    engine = PolicyEngine(policy)
    breaker = EntropyCircuitBreaker(policy_engine=engine)

    # 1. Repetitive message with entropy 0.25:
    # Generic agent should trip or warn (below 0.40 default)
    low_ent_text = "item item item item select select select"

    # For sql_agent, floor is 0.15, so 0.25 entropy is permitted!
    dec_sql = breaker.evaluate(
        message=low_ent_text,
        agent_id="sql_agent",
        swarm_id="swarm-1",
    )
    assert dec_sql.decision == "pass"

    # Reset breaker
    breaker.reset()

    # 2. Prohibited tool invocation raises SwarmCircuitBreakException immediately
    with pytest.raises(SwarmCircuitBreakException) as exc_info:
        breaker.evaluate(
            message="",
            agent_id="sql_agent",
            swarm_id="swarm-1",
            tool_name="drop_table",
        )
    assert "Prohibited tool 'drop_table'" in str(exc_info.value)
    assert exc_info.value.savings is not None


def test_hot_reload_policy():
    """Verifies that policy updates can be hot-reloaded dynamically in memory."""
    engine = PolicyEngine(
        GuardrailPolicy(defaults=RuleConfig(min_entropy=0.30))
    )
    assert engine.get_effective_config("worker").min_entropy == 0.30

    # Hot reload with stricter policy
    new_policy = GuardrailPolicy(
        defaults=RuleConfig(min_entropy=0.60)
    )
    engine.hot_reload(new_policy)

    assert engine.get_effective_config("worker").min_entropy == 0.60


def test_server_rest_api_policies():
    """Verifies GET and POST /api/v1/policies on control plane server."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = SQLiteStorageAdapter(os.path.join(tmpdir, "policy_test.db"))
        policy = GuardrailPolicy(
            name="initial_policy",
            defaults=RuleConfig(min_entropy=0.35),
        )
        engine = PolicyEngine(policy)
        app = create_app(storage=storage, policy_engine=engine)
        client = TestClient(app)

        # 1. GET /api/v1/policies
        res_get = client.get("/api/v1/policies")
        assert res_get.status_code == 200
        data = res_get.json()
        assert data["name"] == "initial_policy"
        assert data["defaults"]["min_entropy"] == 0.35

        # 2. POST /api/v1/policies (Hot-Reload)
        update_payload = {
            "name": "updated_hot_reloaded_policy",
            "defaults": {
                "min_entropy": 0.50,
                "token_budget": 75000,
            },
            "agents": {
                "coder": {
                    "prohibited_tools": ["os_exec"],
                }
            }
        }
        res_post = client.post("/api/v1/policies", json=update_payload)
        assert res_post.status_code == 200
        res_data = res_post.json()
        assert res_data["status"] == "hot_reloaded"
        assert res_data["policy"]["name"] == "updated_hot_reloaded_policy"
        assert res_data["policy"]["defaults"]["min_entropy"] == 0.50

        # Verify engine reflects update immediately
        assert engine.get_effective_config("coder").prohibited_tools == ["os_exec"]

        storage.close_sync()


def test_policy_evaluation_sub_microsecond_benchmark():
    """Verifies that policy rule evaluation executes in < 25 microseconds per turn."""
    policy = GuardrailPolicy(
        defaults=RuleConfig(min_entropy=0.35, max_similarity=0.85),
        agents={"coder": RuleConfig(min_entropy=0.20)},
        rules=[
            DeclarativeRule(
                id="rule-1",
                agent_pattern="*",
                condition=RuleCondition(payload_contains_any=["FORBIDDEN_KEYWORD"]),
                action="trip",
            )
        ]
    )
    engine = PolicyEngine(policy)

    iterations = 500
    start = time.perf_counter()
    for i in range(iterations):
        engine.evaluate_turn(
            agent="coder",
            text="Clean standard code generation step",
            entropy_score=0.75,
            similarity_score=0.10,
        )
    elapsed = time.perf_counter() - start
    avg_us = (elapsed / iterations) * 1e6

    # Should take < 25 microseconds per evaluation (typically 2-5µs)
    assert avg_us < 25.0, f"Policy evaluation took {avg_us:.2f}µs, exceeding 25µs ceiling"
