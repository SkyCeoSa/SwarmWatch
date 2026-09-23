# -*- coding: utf-8 -*-
"""
SwarmWatch Enterprise Multi-Factor Authentication (MFA / TOTP) & Strict RBAC Engine.
Provides RFC 6238 Time-Based One-Time Passwords, single-use hashed recovery backup codes,
and a 4-tier Role-Based Access Control matrix (Admin, Operator, Auditor, Viewer).
Zero third-party pip dependencies (pure standard library: hashlib, hmac, struct, base64).
"""

from datetime import datetime, timezone
from enum import Enum
import base64
import hashlib
import hmac
import secrets
import struct
import time
from typing import Dict, List, Optional, Set, Tuple
import uuid
from pydantic import BaseModel, Field


class Role(str, Enum):
    """Canonical SwarmWatch authorization roles."""
    ADMIN = "admin"
    OPERATOR = "operator"
    AUDITOR = "auditor"
    VIEWER = "viewer"


class Permission(str, Enum):
    """Granular action permissions within the SwarmWatch control plane."""
    SWARMS_READ = "swarms:read"
    SWARMS_RUN = "swarms:run"
    SWARMS_TRIP = "swarms:trip"
    SWARMS_RESUME = "swarms:resume"
    SWARMS_FORK = "swarms:fork"
    POLICY_READ = "policy:read"
    POLICY_WRITE = "policy:write"
    STORAGE_ARCHIVE = "storage:archive"
    STORAGE_RESTORE = "storage:restore"
    KEYS_READ = "keys:read"
    KEYS_WRITE = "keys:write"
    KEYS_REVOKE = "keys:revoke"
    MFA_MANAGE = "mfa:manage"
    AUDIT_READ = "audit:read"
    FLEET_MANAGE = "fleet:manage"


# Formal Role-to-Permission Mapping Matrix
ROLE_PERMISSIONS: Dict[Role, Set[Permission]] = {
    Role.ADMIN: {p for p in Permission},  # Complete dominion
    Role.OPERATOR: {
        Permission.SWARMS_READ,
        Permission.SWARMS_RUN,
        Permission.SWARMS_TRIP,
        Permission.SWARMS_RESUME,
        Permission.SWARMS_FORK,
        Permission.POLICY_READ,
        Permission.STORAGE_ARCHIVE,
        Permission.STORAGE_RESTORE,
        Permission.KEYS_READ,
        Permission.FLEET_MANAGE,
    },
    Role.AUDITOR: {
        Permission.SWARMS_READ,
        Permission.POLICY_READ,
        Permission.KEYS_READ,
        Permission.AUDIT_READ,
    },
    Role.VIEWER: {
        Permission.SWARMS_READ,
        Permission.POLICY_READ,
    },
}


def has_permission(role: Role, permission: Permission) -> bool:
    """Verifies whether a given role holds the specified action permission."""
    if isinstance(role, str):
        try:
            role = Role(role.lower())
        except ValueError:
            return False
    if isinstance(permission, str):
        try:
            permission = Permission(permission)
        except ValueError:
            return False
    perms = ROLE_PERMISSIONS.get(role, set())
    return permission in perms


class UserAccount(BaseModel):
    """Enterprise user identity record."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    username: str
    email: str = ""
    role: Role = Role.VIEWER
    mfa_enabled: bool = False
    totp_secret: Optional[str] = None
    backup_codes: List[str] = Field(default_factory=list)  # SHA-256 hashes of unconsumed recovery codes
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TotpEngine:
    """
    Zero-dependency RFC 6238 TOTP (Time-Based One-Time Password) Engine.
    Implements standard HMAC-SHA1 dynamic truncation with 30-second time steps,
    drift window tolerance (+/- 1 step), and SHA-256 single-use recovery backup codes.
    """

    TIME_STEP_SECONDS = 30
    CODE_DIGITS = 6

    @classmethod
    def generate_secret(cls, byte_length: int = 20, length: Optional[int] = None) -> str:
        """Generates a cryptographically random Base32 encoded secret."""
        nb = (length * 5 // 8) if length else byte_length
        raw_bytes = secrets.token_bytes(nb)
        # RFC 3548 Base32 without padding characters
        return base64.b32encode(raw_bytes).decode("ascii").rstrip("=")

    @classmethod
    def _clean_secret(cls, secret: str) -> bytes:
        """Normalizes and decodes a Base32 secret string into raw bytes."""
        normalized = secret.strip().replace(" ", "").upper()
        # Add back padding if necessary
        missing_padding = len(normalized) % 8
        if missing_padding:
            normalized += "=" * (8 - missing_padding)
        return base64.b32decode(normalized, casefold=True)

    @classmethod
    def generate_totp(
        cls,
        secret: str,
        time_step: Optional[int] = None,
        timestamp: Optional[float] = None
    ) -> str:
        """
        Calculates a 6-digit TOTP code for a secret at the given time step
        (defaults to current Unix epoch // 30).
        """
        if time_step is None:
            if timestamp is not None:
                time_step = int(timestamp) // cls.TIME_STEP_SECONDS
            else:
                time_step = int(time.time()) // cls.TIME_STEP_SECONDS

        key_bytes = cls._clean_secret(secret)
        # Pack counter C as an 8-byte big-endian unsigned integer
        counter_bytes = struct.pack(">Q", time_step)

        # HMAC-SHA1
        hmac_digest = hmac.new(key_bytes, counter_bytes, hashlib.sha1).digest()

        # Dynamic truncation (RFC 4226 Section 5.4)
        offset = hmac_digest[-1] & 0x0F
        code_int = (
            ((hmac_digest[offset] & 0x7F) << 24)
            | ((hmac_digest[offset + 1] & 0xFF) << 16)
            | ((hmac_digest[offset + 2] & 0xFF) << 8)
            | (hmac_digest[offset + 3] & 0xFF)
        ) % (10 ** cls.CODE_DIGITS)

        return f"{code_int:0{cls.CODE_DIGITS}d}"

    @classmethod
    def verify_totp(
        cls,
        code: str,
        secret: str,
        window: int = 1,
        timestamp: Optional[float] = None,
        drift_steps: Optional[int] = None
    ) -> bool:
        """
        Validates a provided 6-digit TOTP code against a secret across
        a configurable clock drift window (+/- window steps, default +/- 1 = 90s).
        Uses constant-time comparison to prevent side-channel timing attacks.
        """
        if not code or len(code.strip()) != cls.CODE_DIGITS:
            return False

        effective_window = drift_steps if drift_steps is not None else window
        clean_code = code.strip()
        current_step = (int(timestamp) if timestamp is not None else int(time.time())) // cls.TIME_STEP_SECONDS

        for step_offset in range(-effective_window, effective_window + 1):
            expected = cls.generate_totp(secret, time_step=current_step + step_offset)
            if hmac.compare_digest(clean_code, expected):
                return True

        return False

    @classmethod
    def hash_backup_code(cls, raw_code: str) -> str:
        """Calculates canonical SHA-256 hash of a backup code (uppercase, no hyphens)."""
        canonical = raw_code.replace("-", "").strip().upper()
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def generate_backup_codes(cls, count: int = 8) -> Tuple[List[str], List[str]]:
        """
        Generates human-readable recovery codes (e.g. 'A1B2-C3D4') and their SHA-256 hashes.
        Returns a tuple of: (raw_codes_for_user, sha256_hashed_codes_for_storage).
        """
        raw_codes = []
        hashed_codes = []
        for _ in range(count):
            part1 = secrets.token_hex(2).upper()
            part2 = secrets.token_hex(2).upper()
            raw_code = f"{part1}-{part2}"
            raw_codes.append(raw_code)

            # Store canonical hash (without hyphen, uppercase)
            canonical = raw_code.replace("-", "").strip().upper()
            h = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            hashed_codes.append(h)

        return raw_codes, hashed_codes

    @classmethod
    def verify_and_consume_backup_code(
        cls,
        raw_code: str,
        hashed_codes: List[str]
    ) -> Tuple[bool, List[str]]:
        """
        Verifies a raw recovery code against a list of SHA-256 hashes.
        If valid, the code is burned (consumed) and the remaining hashes are returned.
        Returns: (is_valid, remaining_hashed_codes).
        """
        if not raw_code:
            return False, list(hashed_codes)

        canonical = raw_code.replace("-", "").strip().upper()
        test_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

        remaining: List[str] = []
        matched = False

        for stored_hash in hashed_codes:
            if not matched and hmac.compare_digest(test_hash, stored_hash):
                matched = True
                # Do not re-append; consumed/burned
                continue
            remaining.append(stored_hash)

        return matched, remaining

    @classmethod
    def build_otpauth_uri(
        cls,
        username: str,
        secret: str,
        issuer: str = "SwarmWatch"
    ) -> str:
        """
        Constructs a standard otpauth://totp URI for Google Authenticator,
        Authy, 1Password, Bitwarden, etc.
        """
        from urllib.parse import quote
        safe_issuer = quote(issuer)
        safe_user = quote(username)
        return f"otpauth://totp/{safe_issuer}:{safe_user}?secret={secret}&issuer={safe_issuer}&algorithm=SHA1&digits=6&period=30"

    generate_code = generate_totp
    verify_code = verify_totp
    consume_backup_code = verify_and_consume_backup_code
    get_provisioning_uri = build_otpauth_uri
