# -*- coding: utf-8 -*-
"""
Tests for Differential Privacy & Cryptographic State Anonymization (Seam 38 / ADR 0049).
Verifies calibrated Laplace mechanism, deterministic HMAC pseudonymization,
privacy budget accounting, REST endpoints, and CLI interface.
"""

import json
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient

from swarmwatch.diff_privacy import (
    DifferentialPrivacyPolicy,
    LaplaceMechanism,
    DifferentialPrivacyEngine,
    global_privacy_engine,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main


class TestDifferentialPrivacy:

    def test_laplace_mechanism_mathematical_properties(self):
        """Validates that Laplace noise generates expected bounds and distribution characteristics."""
        mech = LaplaceMechanism(seed=42)

        # Scale <= 0 must return exact 0.0
        assert mech.sample_noise(0.0) == 0.0
        assert mech.sample_noise(-1.0) == 0.0

        # Sample 1000 draws at scale=2.0
        samples = [mech.sample_noise(2.0) for _ in range(1000)]
        mean = sum(samples) / len(samples)
        # Sample mean of zero-mean Laplace distribution should be close to 0
        assert abs(mean) < 0.5

        # Perturbation clamping
        val = 150000.0  # Above upper bound of 50000
        perturbed = mech.perturb(val, lower_bound=0.0, upper_bound=50000.0, epsilon=1.0)
        # Should be perturbed around 50000, not around 150000
        assert perturbed < 120000.0

    def test_hmac_pseudonymization_deterministic_and_topology_preserving(self):
        """Verifies deterministic pseudonymization preserving causal link consistency."""
        policy1 = DifferentialPrivacyPolicy(tenant_salt="secret_corp_salt_a")
        engine1 = DifferentialPrivacyEngine(policy=policy1, seed=123)

        # Same input yields identical pseudonym
        id1 = engine1.pseudonymize("SecurityAuditor", prefix="agent_")
        id2 = engine1.pseudonymize("SecurityAuditor", prefix="agent_")
        assert id1 == id2
        assert id1.startswith("agent_")

        # Different input yields different pseudonym
        id3 = engine1.pseudonymize("BackendEngineer", prefix="agent_")
        assert id1 != id3

        # Different salt yields different pseudonym for same agent
        policy2 = DifferentialPrivacyPolicy(tenant_salt="secret_corp_salt_b")
        engine2 = DifferentialPrivacyEngine(policy=policy2, seed=123)
        id_diff_salt = engine2.pseudonymize("SecurityAuditor", prefix="agent_")
        assert id1 != id_diff_salt

    def test_differential_privacy_engine_anonymize_span(self):
        """Validates end-to-end anonymization of causal telemetry span fields."""
        policy = DifferentialPrivacyPolicy(
            epsilon=1.0,
            tenant_salt="test_salt_alpha",
            anonymize_identities=True,
            anonymize_metrics=True,
            anonymize_payloads=True,
        )
        engine = DifferentialPrivacyEngine(policy=policy, seed=99)

        original_span = {
            "id": "span_test_01",
            "swarm_id": "swarm-live-01",
            "agent": "ArchitectAgent",
            "source": "PlannerAgent",
            "target": "ExecutorAgent",
            "role": "Master Architect",
            "tokens": 2500,
            "duration": 450.0,
            "cost": 0.05,
            "entropy": 2.15,
            "payload": "Super secret database password postgres://admin:hunter2@db.internal",
        }

        anon = engine.anonymize_span(original_span)

        # 1. Identities and roles pseudonymized
        assert anon["agent"].startswith("agent_")
        assert anon["agent"] != "ArchitectAgent"
        assert anon["source"].startswith("agent_")
        assert anon["target"].startswith("agent_")
        assert anon["role"].startswith("role_")
        assert anon["role"] != "Master Architect"

        # 2. Numeric metrics perturbed
        assert isinstance(anon["tokens"], int)
        assert isinstance(anon["duration"], float)
        assert isinstance(anon["cost"], float)
        assert isinstance(anon["entropy"], float)
        assert 0.0 <= anon["entropy"] <= 8.0

        # 3. Payload masked with SHA256 digest
        assert "hunter2" not in anon["payload"]
        assert anon["payload"].startswith("[ANONYMIZED_PAYLOAD:")
        assert "SHA256:" in anon["payload"]

        # 4. Metadata attached
        assert anon["_privacy"]["anonymized"] is True
        assert anon["_privacy"]["epsilon"] == 1.0
        assert "Laplace" in anon["_privacy"]["mechanism"]

    def test_privacy_budget_depletion_and_exhaustion(self):
        """Validates strict epsilon-budget depletion and exhaustion tracking."""
        policy = DifferentialPrivacyPolicy(epsilon=1.0, total_epsilon_budget=0.03)
        engine = DifferentialPrivacyEngine(policy=policy, seed=77)

        status_init = engine.get_status()
        assert status_init["is_exhausted"] is False
        assert status_init["spent_budget"] == 0.0

        sample_span = {"agent": "Worker", "tokens": 100}

        # Anonymize 3 times (each charges eff_eps * 0.01 = 0.01)
        engine.anonymize_span(sample_span)
        engine.anonymize_span(sample_span)
        engine.anonymize_span(sample_span)

        status_depleted = engine.get_status()
        assert status_depleted["spent_budget"] >= 0.03
        assert status_depleted["is_exhausted"] is True
        assert status_depleted["remaining_budget"] == 0.0

        # Further calls should indicate budget_exhausted in metadata
        anon_after = engine.anonymize_span(sample_span)
        assert anon_after["_privacy"]["budget_exhausted"] is True

        # Reset budget restores full capacity
        engine.reset_budget()
        status_reset = engine.get_status()
        assert status_reset["spent_budget"] == 0.0
        assert status_reset["is_exhausted"] is False

    def test_server_privacy_endpoints(self):
        """Tests FastAPI REST control plane endpoints for differential privacy."""
        app = create_app()
        client = TestClient(app)

        # GET /api/v1/privacy/status
        resp_status = client.get("/api/v1/privacy/status")
        assert resp_status.status_code == 200
        data = resp_status.json()
        assert "epsilon" in data
        assert "total_budget" in data
        assert "spent_budget" in data

        # POST /api/v1/privacy/anonymize
        spans_payload = [
            {"id": "s1", "agent": "Alice", "tokens": 500, "payload": "Confidential data"},
            {"id": "s2", "agent": "Bob", "tokens": 1200, "payload": "Internal API key"},
        ]
        resp_anon = client.post("/api/v1/privacy/anonymize", json={"spans": spans_payload, "epsilon": 0.8})
        assert resp_anon.status_code == 200
        anon_data = resp_anon.json()
        assert anon_data["status"] == "anonymized"
        assert anon_data["count"] == 2
        assert len(anon_data["spans"]) == 2
        assert anon_data["spans"][0]["agent"].startswith("agent_")
        assert "Confidential" not in anon_data["spans"][0]["payload"]

        # POST /api/v1/privacy/policy
        resp_policy = client.post("/api/v1/privacy/policy", json={"epsilon": 2.5, "tenant_salt": "new_salt_777"})
        assert resp_policy.status_code == 200
        pol_data = resp_policy.json()
        assert pol_data["status"] == "policy_updated"
        assert pol_data["policy"]["epsilon"] == 2.5
        assert pol_data["policy"]["tenant_salt"] == "new_salt_777"

        # POST /api/v1/privacy/reset
        resp_reset = client.post("/api/v1/privacy/reset")
        assert resp_reset.status_code == 200
        assert resp_reset.json()["status"] == "budget_reset"

    def test_cli_privacy_subcommands(self, capsys):
        """Tests CLI command execution for privacy status, test, and reset."""
        # 1. `privacy status`
        with pytest.raises(SystemExit) as exc:
            main(["privacy", "status", "--format", "json"])
        assert exc.value.code == 0
        captured = capsys.readouterr()
        status_json = json.loads(captured.out)
        assert "epsilon" in status_json
        assert "total_budget" in status_json

        # 2. `privacy test`
        with pytest.raises(SystemExit) as exc:
            main(["privacy", "test", "--format", "json"])
        assert exc.value.code == 0
        captured = capsys.readouterr()
        test_json = json.loads(captured.out)
        assert test_json["status"] == "anonymized"
        assert len(test_json["spans"]) == 1

        # 3. `privacy reset`
        with pytest.raises(SystemExit) as exc:
            main(["privacy", "reset", "--format", "json"])
        assert exc.value.code == 0
        captured = capsys.readouterr()
        reset_json = json.loads(captured.out)
        assert reset_json["status"] == "budget_reset"
