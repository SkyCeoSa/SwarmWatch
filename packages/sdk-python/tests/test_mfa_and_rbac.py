# -*- coding: utf-8 -*-
"""
TDD Test Suite for Enterprise MFA / TOTP and Role-Based Access Control (Seam 26 / ADR 0037).
"""
import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.rbac import (
    Role,
    Permission,
    ROLE_PERMISSIONS,
    has_permission,
    UserAccount,
    TotpEngine,
)
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.server import create_app
from swarmwatch.cli import main


# =========================================================================
# 1. TOTP Engine & Backup Codes
# =========================================================================

def test_totp_engine_generation_and_drift_verification():
    secret = TotpEngine.generate_secret(length=32)
    assert len(secret) == 32
    assert all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567" for c in secret)

    # Test code generation at fixed timestamp
    ts = 1700000000.0
    code = TotpEngine.generate_totp(secret, timestamp=ts)
    assert len(code) == 6
    assert code.isdigit()

    # Exact step verification
    assert TotpEngine.verify_totp(code, secret, timestamp=ts, drift_steps=1) is True

    # Drift backward by 1 step (30 seconds)
    assert TotpEngine.verify_totp(code, secret, timestamp=ts + 30.0, drift_steps=1) is True

    # Drift forward by 1 step (-30 seconds)
    assert TotpEngine.verify_totp(code, secret, timestamp=ts - 30.0, drift_steps=1) is True

    # Outside drift window (65 seconds away)
    assert TotpEngine.verify_totp(code, secret, timestamp=ts + 65.0, drift_steps=1) is False
    assert TotpEngine.verify_totp(code, secret, timestamp=ts - 65.0, drift_steps=1) is False

    # Invalid code
    assert TotpEngine.verify_totp("badcode", secret, timestamp=ts, drift_steps=1) is False
    assert TotpEngine.verify_totp("", secret, timestamp=ts, drift_steps=1) is False


def test_totp_backup_codes_lifecycle():
    raw_codes, hashed_codes = TotpEngine.generate_backup_codes(count=8)
    assert len(raw_codes) == 8
    assert len(hashed_codes) == 8
    for c in raw_codes:
        assert len(c) == 9
        assert c[4] == "-"

    first_code = raw_codes[0]

    # Normalize test: lowercase and without hyphen
    normalized_input = first_code.replace("-", "").lower()
    success, updated = TotpEngine.verify_and_consume_backup_code(normalized_input, hashed_codes)
    assert success is True
    assert len(updated) == 7

    # Single-use consumption test: second attempt must fail
    success2, updated2 = TotpEngine.verify_and_consume_backup_code(first_code, updated)
    assert success2 is False
    assert len(updated2) == 7

    # Invalid code test
    success3, updated3 = TotpEngine.verify_and_consume_backup_code("ZZZZ-9999", updated)
    assert success3 is False
    assert len(updated3) == 7


def test_totp_provisioning_uri():
    secret = "JBSWY3DPEHPK3PXP"
    uri = TotpEngine.build_otpauth_uri("operator-1@enterprise.local", secret, issuer="SwarmWatch")
    assert uri.startswith("otpauth://totp/SwarmWatch:operator-1%40enterprise.local?")
    assert "secret=JBSWY3DPEHPK3PXP" in uri
    assert "issuer=SwarmWatch" in uri


# =========================================================================
# 2. RBAC Permissions Matrix
# =========================================================================

def test_rbac_permissions_matrix():
    # Admin has all permissions
    assert has_permission(Role.ADMIN, Permission.FLEET_MANAGE) is True
    assert has_permission(Role.ADMIN, Permission.POLICY_WRITE) is True
    assert has_permission(Role.ADMIN, Permission.MFA_MANAGE) is True
    assert has_permission(Role.ADMIN, Permission.KEYS_REVOKE) is True

    # Operator has fleet:manage and swarms:trip, but not policy:write or keys:revoke
    assert has_permission(Role.OPERATOR, Permission.FLEET_MANAGE) is True
    assert has_permission(Role.OPERATOR, Permission.SWARMS_TRIP) is True
    assert has_permission(Role.OPERATOR, Permission.SWARMS_READ) is True
    assert has_permission(Role.OPERATOR, Permission.POLICY_WRITE) is False
    assert has_permission(Role.OPERATOR, Permission.KEYS_REVOKE) is False

    # Auditor has audit:read and swarms:read, but no trip or mutations
    assert has_permission(Role.AUDITOR, Permission.AUDIT_READ) is True
    assert has_permission(Role.AUDITOR, Permission.SWARMS_READ) is True
    assert has_permission(Role.AUDITOR, Permission.SWARMS_TRIP) is False
    assert has_permission(Role.AUDITOR, Permission.POLICY_WRITE) is False

    # Viewer has read-only access
    assert has_permission(Role.VIEWER, Permission.SWARMS_READ) is True
    assert has_permission(Role.VIEWER, Permission.SWARMS_TRIP) is False
    assert has_permission(Role.VIEWER, Permission.AUDIT_READ) is False
    assert has_permission(Role.VIEWER, Permission.KEYS_REVOKE) is False

    # String role lookups
    assert has_permission("admin", "swarms:trip") is True
    assert has_permission("operator", "policy:write") is False
    assert has_permission("invalid_role", "swarms:read") is False
    assert has_permission(Role.VIEWER, "nonexistent:perm") is False


# =========================================================================
# 3. Storage Layer User & Audit Persistence
# =========================================================================

def test_storage_user_and_audit_persistence(tmp_path):
    db_file = str(tmp_path / "auth_storage.db")
    storage = SQLiteStorageAdapter(db_path=db_file)

    user = UserAccount(
        id="usr-test-1",
        username="lead_operator",
        role=Role.OPERATOR,
        mfa_enabled=False,
    )
    storage.save_user_sync(user)

    fetched = storage.get_user_sync("usr-test-1")
    assert fetched is not None
    assert fetched.username == "lead_operator"
    assert fetched.role == Role.OPERATOR

    fetched_by_name = storage.get_user_by_username_sync("lead_operator")
    assert fetched_by_name is not None
    assert fetched_by_name.id == "usr-test-1"

    users = storage.list_users_sync()
    assert len(users) >= 1
    assert any(u.username == "lead_operator" for u in users)

    # Audit event logging
    storage.record_audit_event_sync(
        action="mfa:enrolled",
        user_id="usr-test-1",
        resource="usr-test-1",
        status="success",
        details={"ip": "127.0.0.1"}
    )
    storage.record_audit_event_sync(
        action="fleet:tripped",
        user_id="usr-test-1",
        resource="swarm-01",
        status="success",
        details={"reason": "Manual test trip"}
    )

    events = storage.list_audit_events_sync()
    assert len(events) >= 2
    assert any(e["action"] == "fleet:tripped" for e in events)


# =========================================================================
# 4. Server REST Control Plane & Step-Up Security Flow
# =========================================================================

def test_server_mfa_enrollment_and_verification_flow(tmp_path):
    db_file = str(tmp_path / "server_auth.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. Create a user
    resp = client.post(
        "/api/v1/auth/users",
        json={"username": "ops_jane", "email": "ops_jane@swarmwatch.local", "role": "operator"}
    )
    assert resp.status_code == 200
    user_data = resp.json()
    assert user_data["username"] == "ops_jane"
    assert user_data["role"] == "operator"
    assert user_data["mfa_enabled"] is False

    # 2. List users
    resp_list = client.get("/api/v1/auth/users")
    assert resp_list.status_code == 200
    assert any(u["username"] == "ops_jane" for u in resp_list.json())

    # 3. Enroll in MFA
    resp_enroll = client.post(
        "/api/v1/auth/mfa/enroll",
        json={"username": "ops_jane"}
    )
    assert resp_enroll.status_code == 200
    enroll_data = resp_enroll.json()
    secret = enroll_data["secret"]
    backup_codes = enroll_data["backup_codes"]
    assert len(backup_codes) == 8

    # 4. Verify enrollment with TOTP code
    valid_code = TotpEngine.generate_totp(secret)
    resp_verify = client.post(
        "/api/v1/auth/mfa/verify",
        json={"username": "ops_jane", "code": valid_code}
    )
    assert resp_verify.status_code == 200
    assert resp_verify.json()["status"] == "mfa_enabled"

    # User is now mfa_enabled = True
    user_after = storage.get_user_by_username_sync("ops_jane")
    assert user_after.mfa_enabled is True

    # 5. Challenge step-up with TOTP code
    resp_challenge = client.post(
        "/api/v1/auth/mfa/challenge",
        json={"username": "ops_jane", "code": TotpEngine.generate_totp(secret)}
    )
    assert resp_challenge.status_code == 200
    assert resp_challenge.json()["status"] == "verified"
    assert resp_challenge.json()["used_backup_code"] is False

    # 6. Challenge step-up with backup code
    first_backup = backup_codes[0]
    resp_challenge_backup = client.post(
        "/api/v1/auth/mfa/challenge",
        json={"username": "ops_jane", "code": first_backup}
    )
    assert resp_challenge_backup.status_code == 200
    assert resp_challenge_backup.json()["status"] == "verified"
    assert resp_challenge_backup.json()["used_backup_code"] is True

    # Single-use verified: second use of same backup code fails
    resp_challenge_backup2 = client.post(
        "/api/v1/auth/mfa/challenge",
        json={"username": "ops_jane", "code": first_backup}
    )
    assert resp_challenge_backup2.status_code == 401


def test_server_rbac_and_mfa_step_up_enforcement(tmp_path):
    db_file = str(tmp_path / "server_rbac.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # Setup roles: Viewer, Operator with MFA
    storage.save_user_sync(UserAccount(id="u-view", username="viewer_bob", email="bob@test.local", role=Role.VIEWER, mfa_enabled=False))

    op_secret = TotpEngine.generate_secret()
    _, hashed_backups = TotpEngine.generate_backup_codes(count=4)
    storage.save_user_sync(UserAccount(
        id="u-op",
        username="op_dan",
        email="dan@test.local",
        role=Role.OPERATOR,
        mfa_enabled=True,
        totp_secret=op_secret,
        backup_codes=hashed_backups
    ))

    # 1. Viewer attempts destructive fleet trip -> 403 (Permission denied)
    resp_viewer = client.post(
        "/api/v1/fleet/redis/trip",
        headers={"X-User": "viewer_bob"},
        json={"swarm_id": "sw-test", "reason": "Viewer unauthorized attempt"}
    )
    assert resp_viewer.status_code == 403
    assert "Permission denied" in resp_viewer.json()["detail"]

    # 2. Operator has fleet:manage, but destructive operation requires MFA -> 403 if missing header
    resp_op_no_mfa = client.post(
        "/api/v1/fleet/redis/trip",
        headers={"X-User": "op_dan"},
        json={"swarm_id": "sw-test", "reason": "Trip without step-up code"}
    )
    assert resp_op_no_mfa.status_code == 403
    assert "MFA verification required" in resp_op_no_mfa.json()["detail"]

    # 3. Operator with invalid MFA code -> 403
    resp_op_bad_mfa = client.post(
        "/api/v1/fleet/redis/trip",
        headers={"X-User": "op_dan", "X-MFA-Code": "999999"},
        json={"swarm_id": "sw-test", "reason": "Trip with bad code"}
    )
    assert resp_op_bad_mfa.status_code == 403
    assert "MFA verification required" in resp_op_bad_mfa.json()["detail"]

    # 4. Operator with valid TOTP code in X-MFA-Code -> 200 OK
    valid_op_code = TotpEngine.generate_totp(op_secret)
    resp_op_valid = client.post(
        "/api/v1/fleet/redis/trip",
        headers={"X-User": "op_dan", "X-MFA-Code": valid_op_code},
        json={"swarm_id": "sw-test", "reason": "Trip with valid step-up code"}
    )
    assert resp_op_valid.status_code == 200
    assert resp_op_valid.json()["status"] == "tripped"

    # 5. Check audit events endpoint
    resp_audit = client.get("/api/v1/auth/audit")
    assert resp_audit.status_code == 200
    audit_events = resp_audit.json()
    assert len(audit_events) >= 1


# =========================================================================
# 5. CLI Subcommands
# =========================================================================

def test_cli_mfa_and_rbac(capsys):
    # 1. MFA Generate
    with pytest.raises(SystemExit) as exc:
        main(["mfa", "generate", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    gen_data = json.loads(captured.out)
    assert "secret" in gen_data
    assert len(gen_data["backup_codes"]) == 8
    secret = gen_data["secret"]

    # 2. MFA Verify
    code = TotpEngine.generate_totp(secret)
    with pytest.raises(SystemExit) as exc_v:
        main(["mfa", "verify", "--secret", secret, "--code", code, "--format", "json"])
    assert exc_v.value.code == 0
    cap_v = capsys.readouterr()
    v_data = json.loads(cap_v.out)
    assert v_data["valid"] is True

    # 3. RBAC Check - Granted
    with pytest.raises(SystemExit) as exc_chk1:
        main(["rbac", "check", "--role", "operator", "--permission", "fleet:manage"])
    assert exc_chk1.value.code == 0
    cap_c1 = capsys.readouterr()
    assert "GRANTED" in cap_c1.out

    # 4. RBAC Check - Denied
    with pytest.raises(SystemExit) as exc_chk2:
        main(["rbac", "check", "--role", "operator", "--permission", "policy:write"])
    assert exc_chk2.value.code == 1
    cap_c2 = capsys.readouterr()
    assert "DENIED" in cap_c2.out

    # 5. RBAC Matrix
    with pytest.raises(SystemExit) as exc_mat:
        main(["rbac", "matrix", "--format", "json"])
    assert exc_mat.value.code == 0
    cap_m = capsys.readouterr()
    mat_data = json.loads(cap_m.out)
    assert "admin" in mat_data
    assert "fleet:manage" in mat_data["admin"]
