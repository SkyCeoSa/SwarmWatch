# -*- coding: utf-8 -*-
"""
SwarmWatch Multi-Tenant Core & Cryptographic API Key Engine.
Provides organization and project hierarchy, tenant context propagation,
and secure SHA-256 API key generation with constant-time verification.
"""

from datetime import datetime, timezone
import hashlib
import hmac
import secrets
from typing import Dict, List, Optional
import uuid
from pydantic import BaseModel, Field

from swarmwatch.rbac import Role


class Organization(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    slug: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Project(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    org_id: str
    name: str
    slug: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ApiKey(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    project_id: str
    name: str
    key_prefix: str
    key_hash: str
    scopes: List[str] = Field(default_factory=lambda: ["telemetry:write", "telemetry:read"])
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    revoked_at: Optional[datetime] = None

    @property
    def is_active(self) -> bool:
        return self.revoked_at is None


class TenantContext(BaseModel):
    org_id: str = "default-org"
    project_id: str = "default-project"
    user_id: Optional[str] = None
    role: Role = Role.ADMIN
    mfa_verified: bool = False
    key_id: Optional[str] = None
    scopes: List[str] = Field(default_factory=lambda: ["*"])


class ApiKeyManager:
    """
    Cryptographic API Key Generator & Fast In-Memory Verification Engine.
    Keys are prefixed with 'sw_live_' or 'sw_test_' and use Base64/Base58 random bytes.
    Only the SHA-256 hash is persisted in the database; raw keys cannot be recovered.
    """

    DEFAULT_PREFIX_LIVE = "sw_live_"
    DEFAULT_PREFIX_TEST = "sw_test_"

    def __init__(self):
        # In-memory validation cache: key_hash -> ApiKey
        self._cache: Dict[str, ApiKey] = {}

    @classmethod
    def generate_raw_key(cls, is_test: bool = False) -> str:
        prefix = cls.DEFAULT_PREFIX_TEST if is_test else cls.DEFAULT_PREFIX_LIVE
        random_bytes = secrets.token_urlsafe(24)
        return f"{prefix}{random_bytes}"

    @classmethod
    def hash_key(cls, raw_key: str) -> str:
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    @classmethod
    def extract_prefix(cls, raw_key: str) -> str:
        if len(raw_key) >= 12:
            return raw_key[:12] + "..."
        return raw_key[:6] + "..."

    def create_api_key_record(
        self,
        project_id: str,
        name: str = "Default Key",
        is_test: bool = False,
        scopes: Optional[List[str]] = None,
    ) -> tuple[str, ApiKey]:
        """
        Generates a new API key.
        Returns a tuple of (raw_secret_key, ApiKey_model).
        The raw secret key is presented ONLY ONCE to the user.
        """
        raw_key = self.generate_raw_key(is_test=is_test)
        key_hash = self.hash_key(raw_key)
        prefix = self.extract_prefix(raw_key)

        api_key = ApiKey(
            id=str(uuid.uuid4()),
            project_id=project_id,
            name=name,
            key_prefix=prefix,
            key_hash=key_hash,
            scopes=scopes or ["telemetry:write", "telemetry:read"],
            created_at=datetime.now(timezone.utc),
        )

        self._cache[key_hash] = api_key
        return raw_key, api_key

    def verify_raw_key(self, raw_key: str) -> Optional[ApiKey]:
        """
        Verifies a raw API key using constant-time digest comparison.
        Returns the matching ApiKey if valid and active, otherwise None.
        """
        if not raw_key or not (raw_key.startswith(self.DEFAULT_PREFIX_LIVE) or raw_key.startswith(self.DEFAULT_PREFIX_TEST)):
            return None

        target_hash = self.hash_key(raw_key)
        cached = self._cache.get(target_hash)
        if cached:
            if cached.is_active and hmac.compare_digest(cached.key_hash, target_hash):
                return cached
            return None

        return None

    def register_in_cache(self, api_key: ApiKey) -> None:
        self._cache[api_key.key_hash] = api_key

    def invalidate_cache(self, key_hash: str) -> None:
        self._cache.pop(key_hash, None)

    def clear_cache(self) -> None:
        self._cache.clear()
