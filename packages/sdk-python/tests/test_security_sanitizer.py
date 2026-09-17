# -*- coding: utf-8 -*-
"""
TDD Tests for SwarmWatch Enterprise Security & Secret Redaction Engine (Seam 10 & ADR 0019).
Verifies high-throughput regex redaction for cloud keys, database DSNs, JWTs,
PEM private keys, credit cards, PII, and automatic transport-layer sanitization.
"""

from datetime import datetime, timezone
import pytest

import swarmwatch
from swarmwatch.security import SensitiveDataMasker, SecuritySanitizationPolicy
from swarmwatch.schema import SwarmSpan


def test_redact_llm_and_cloud_api_keys():
    masker = SensitiveDataMasker()

    sample_openai = "Header authorization: Bearer sk-proj-1234567890abcdef1234567890 ready."
    sample_anthropic = "Connecting via key: anthropic-sk-ant-api03-abcdef1234567890."
    sample_hf = "Downloading model with token hf_abcdefghijklmnopqrstuvwxyz12345678 now."
    sample_aws = "Deploying to AWS using access key AKIAIOSFODNN7EXAMPLE in region us-east-1."

    out_openai = masker.mask_text(sample_openai)
    out_anthropic = masker.mask_text(sample_anthropic)
    out_hf = masker.mask_text(sample_hf)
    out_aws = masker.mask_text(sample_aws)

    assert "sk-proj-1234567890abcdef1234567890" not in out_openai
    assert "[REDACTED_API_KEY]" in out_openai

    assert "anthropic-sk-ant-api03-abcdef1234567890" not in out_anthropic
    assert "[REDACTED_API_KEY]" in out_anthropic

    assert "hf_abcdefghijklmnopqrstuvwxyz12345678" not in out_hf
    assert "[REDACTED_API_KEY]" in out_hf

    assert "AKIAIOSFODNN7EXAMPLE" not in out_aws
    assert "[REDACTED_AWS_KEY]" in out_aws


def test_redact_database_dsn_credentials():
    masker = SensitiveDataMasker()

    postgres_dsn = "Connecting to postgresql://postgres_admin:SuperSecretPass123!@db.internal.corp:5432/finance_db"
    mysql_dsn = "Executing on mysql://app_user:MySqlSecret999@10.0.0.45:3306/production"
    redis_dsn = "Connecting to redis://:P@ssw0rdRedis@redis-master.internal:6379/0"

    out_pg = masker.mask_text(postgres_dsn)
    out_mysql = masker.mask_text(mysql_dsn)
    out_redis = masker.mask_text(redis_dsn)

    assert "SuperSecretPass123!" not in out_pg
    assert "[REDACTED_DB_DSN]" in out_pg or "[REDACTED]" in out_pg

    assert "MySqlSecret999" not in out_mysql
    assert "[REDACTED_DB_DSN]" in out_mysql or "[REDACTED]" in out_mysql

    assert "P@ssw0rdRedis" not in out_redis
    assert "[REDACTED_DB_DSN]" in out_redis or "[REDACTED]" in out_redis


def test_redact_jwt_and_bearer_tokens():
    masker = SensitiveDataMasker()

    jwt_token = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ."
        "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    )
    sample_text = f"User session active with token: Bearer {jwt_token} until midnight."

    out = masker.mask_text(sample_text)
    assert jwt_token not in out
    assert "[REDACTED_JWT]" in out or "[REDACTED_TOKEN]" in out


def test_redact_pem_private_keys():
    masker = SensitiveDataMasker()

    pem_key = (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "MIIEowIBAAKCAQEA0YQY75...base64payload...\n"
        "-----END RSA PRIVATE KEY-----"
    )
    sample_text = f"Loaded server SSL configuration:\n{pem_key}\nStarting listener."

    out = masker.mask_text(sample_text)
    assert "MIIEowIBAAKCAQEA0YQY75" not in out
    assert "[REDACTED_PRIVATE_KEY]" in out


def test_redact_financial_cards_and_pii():
    masker = SensitiveDataMasker()

    sample_card = "Customer charged with Visa card 4532-1234-5678-9012 for subscription."
    sample_email = "Send audit notifications to security-officer@enterprise-corp.io immediately."

    out_card = masker.mask_text(sample_card)
    out_email = masker.mask_text(sample_email)

    assert "4532-1234-5678-9012" not in out_card
    assert "[REDACTED_CARD]" in out_card

    assert "security-officer@enterprise-corp.io" not in out_email
    assert "[REDACTED_EMAIL]" in out_email


def test_recursive_nested_payload_sanitization():
    masker = SensitiveDataMasker()

    payload = {
        "user_id": 1042,
        "email": "customer@bank.com",
        "api_credentials": {
            "token": "sk-proj-99887766554433221100",
            "db_url": "postgresql://root:secretpass@cluster.internal/core",
        },
        "tags": ["prod", "finance", "card: 4111-2222-3333-4444"],
        "metadata": {
            "status": "active",
            "password": "ClearTextPasswordShouldBeRedacted",
        },
    }

    sanitized = masker.mask_payload(payload)

    assert sanitized["user_id"] == 1042
    assert sanitized["email"] == "[REDACTED_EMAIL]"
    assert sanitized["api_credentials"]["token"] == "[REDACTED_API_KEY]"
    assert "secretpass" not in sanitized["api_credentials"]["db_url"]

    assert "[REDACTED_CARD]" in sanitized["tags"][2]
    assert sanitized["metadata"]["password"] == "[REDACTED]"
    assert sanitized["metadata"]["status"] == "active"


def test_transport_integration_auto_redaction(tmp_path):
    db_path = str(tmp_path / "security_transport.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-security-test")

    raw_input = {
        "action": "execute_query",
        "dsn": "postgresql://dbuser:super_pass_123@sql.prod:5432/orders",
        "aws_key": "AKIAIOSFODNN7EXAMPLE",
    }
    raw_output = {
        "status": "success",
        "bearer": "Bearer sk-proj-1234567890abcdef1234567890",
    }

    with client.span("tool_exec", source="DatabaseWorker", target="PostgresTool", input_payload=raw_input) as ctx:
        ctx.output_payload.update(raw_output)

    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-security-test")
    assert len(spans) == 1
    span = spans[0]

    # Stored payload on disk MUST be redacted
    assert "super_pass_123" not in str(span.input_payload)
    assert "AKIAIOSFODNN7EXAMPLE" not in str(span.input_payload)
    assert "sk-proj-1234567890abcdef1234567890" not in str(span.output_payload)
    assert "[REDACTED" in str(span.input_payload)
    assert "[REDACTED" in str(span.output_payload)
