# -*- coding: utf-8 -*-
"""
Unit and integration tests for Enterprise Acceptance Harness (Seam 60 / ADR 0071).
Verifies:
1. Multi-Dimensional Gate Execution (Security, Interoperability, Loss, Latency, Parity, UI Craft).
2. Acceptance Invariant: Release candidate is CERTIFIED iff all gates pass with zero regressions.
3. Dashboard Artifact Craft Audit (Palette, Tabular Numbers, Zero Emojis, No Neon, Badge Length <= 30).
4. Attestation generation, canonical hashing, and cryptographic signature verification.
5. Control plane REST API endpoints and CLI commands.
"""

import json
import os
import tempfile
import pytest
from starlette.testclient import TestClient

from swarmwatch.acceptance import (
    AcceptanceGateName,
    AcceptanceStatus,
    AcceptanceProfile,
    AcceptanceError,
    AttestationVerificationError,
    AcceptanceHarness,
    global_acceptance_harness,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main


@pytest.fixture(autouse=True)
def reset_acceptance():
    global_acceptance_harness.reset()
    yield
    global_acceptance_harness.reset()


def test_acceptance_harness_run_all_gates():
    harness = AcceptanceHarness()
    report = harness.run(profile=AcceptanceProfile.PRODUCTION)

    assert report.report_id.startswith("acc-rep-")
    assert report.status == AcceptanceStatus.CERTIFIED
    assert report.overall_score == 1.0
    assert report.duration_ms > 0
    assert len(report.report_hash) == 64

    # All 6 gate dimensions must be present
    for gate_name in AcceptanceGateName.ALL_GATES:
        assert gate_name in report.gates
        g_sum = report.gates[gate_name]
        assert g_sum.passed is True
        assert g_sum.score == 1.0
        assert len(g_sum.violations) == 0


def test_acceptance_attest_and_cryptographic_verification():
    harness = AcceptanceHarness()
    report = harness.run()

    # Generate attestation receipt
    secret_key = "test-enterprise-audit-secret-key"
    attestation = harness.attest(report, signer_identity="Security Officer Alpha", secret_key=secret_key)

    assert attestation.attestation_id.startswith("acc-attest-")
    assert attestation.report_id == report.report_id
    assert attestation.status == AcceptanceStatus.CERTIFIED
    assert len(attestation.attestation_hash) == 64
    assert len(attestation.signature) == 64

    # Verify authentic attestation
    is_valid = harness.verify_attestation(attestation, secret_key=secret_key)
    assert is_valid is True

    # Mutated attestation signature must fail closed
    bad_sig_attest = harness.attest(report, secret_key=secret_key)
    bad_sig_attest.signature = "0" * 64
    with pytest.raises(AttestationVerificationError):
        harness.verify_attestation(bad_sig_attest, secret_key=secret_key)

    # Wrong secret key must fail closed
    with pytest.raises(AttestationVerificationError):
        harness.verify_attestation(attestation, secret_key="wrong-secret-key")


def test_acceptance_dashboard_artifact_audit_physical_file():
    harness = AcceptanceHarness()
    audit_res = harness.audit_dashboard_artifact()

    assert audit_res["compliant"] is True
    assert len(audit_res["violations"]) == 0
    assert audit_res["total_badges"] > 0

    # Ensure every single fleet badge meets the craft floor
    for b in audit_res["badges_checked"]:
        assert b["length"] <= 30, f"Badge exceeded 30 chars: {b['text']}"
        assert b["is_upper"] is True, f"Badge is not uppercase: {b['text']}"
        assert b["valid"] is True


def test_acceptance_dashboard_audit_detects_violations():
    harness = AcceptanceHarness()

    bad_html = """
    <html>
      <head>
        <style>
          body { background: #FFFFFF; } /* missing canvas background */
          .glow { box-shadow: 0 0 25px rgba(255, 0, 0, 1); } /* neon glow */
        </style>
      </head>
      <body>
        <div class="fleet-badge">
          <span>🚨 ALERT BADGE HAS LOWERCASE AND IS WAY TOO LONG BEYOND THIRTY CHARACTERS</span>
        </div>
      </body>
    </html>
    """

    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as f:
        f.write(bad_html)
        tmp_name = f.name

    try:
        audit_res = harness.audit_dashboard_artifact(html_path=tmp_name)
        assert audit_res["compliant"] is False
        violations_str = " ".join(audit_res["violations"])
        assert "Palette invariant" in violations_str
        assert "Emoji invariant" in violations_str or "neon glow" in violations_str
        assert "exceeded 30 chars" in violations_str or "uppercase" in violations_str
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)


def test_acceptance_attest_rejected_fails_closed():
    harness = AcceptanceHarness()
    # Run with a non-existent gate to create empty / failed report
    report = harness.run()
    report.status = AcceptanceStatus.REJECTED

    with pytest.raises(AcceptanceError) as exc_info:
        harness.attest(report)
    assert "Cannot attest rejected" in str(exc_info.value)


def test_server_acceptance_endpoints():
    app = create_app()
    client = TestClient(app)

    # 1. Run endpoint
    run_resp = client.post("/api/v1/acceptance/run", json={"profile": "production"})
    assert run_resp.status_code == 200
    report_data = run_resp.json()
    assert report_data["status"] == "CERTIFIED"
    assert report_data["overall_score"] == 1.0
    assert "security" in report_data["gates"]
    assert "ui_craft" in report_data["gates"]

    # 2. Attest endpoint
    attest_resp = client.post("/api/v1/acceptance/attest", json={
        "report": report_data,
        "signer_identity": "Lead Release Officer",
        "secret_key": "audit-secret",
    })
    assert attest_resp.status_code == 200
    attest_data = attest_resp.json()
    assert attest_data["attestation_id"].startswith("acc-attest-")
    assert attest_data["status"] == "CERTIFIED"

    # 3. Verify attestation endpoint
    verify_resp = client.post("/api/v1/acceptance/verify-attestation", json={
        "attestation": attest_data,
        "secret_key": "audit-secret",
    })
    assert verify_resp.status_code == 200
    assert verify_resp.json()["verified"] is True

    # 4. Audit UI endpoint
    audit_resp = client.get("/api/v1/acceptance/audit-ui")
    assert audit_resp.status_code == 200
    assert audit_resp.json()["compliant"] is True

    # 5. Status endpoint
    status_resp = client.get("/api/v1/acceptance/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["status"] == "CERTIFIED"
    assert status_resp.json()["registered_gates_count"] == 6


def test_cli_acceptance_commands(capsys):
    # CLI acceptance run
    with pytest.raises(SystemExit) as exc:
        cli_main(["acceptance", "run", "--profile", "production", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    report_out = json.loads(captured.out)
    assert report_out["status"] == "CERTIFIED"
    assert report_out["overall_score"] == 1.0

    # CLI acceptance audit-ui
    with pytest.raises(SystemExit) as exc:
        cli_main(["acceptance", "audit-ui"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "COMPLIANT" in captured.out

    # CLI acceptance status
    with pytest.raises(SystemExit) as exc:
        cli_main(["acceptance", "status"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "SWARMWATCH ACCEPTANCE HARNESS STATUS" in captured.out
