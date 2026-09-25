# -*- coding: utf-8 -*-
"""
Tests for Canonical Causal Encoding & Byte-Deterministic Contract Engine (Seam 50 / ADR 0061).
Verifies RFC 8785 compliance, domain separation hashing, float normalization,
circular reference guards, REST endpoints, and CLI interface.
"""

import json
import math
import pytest
from fastapi.testclient import TestClient

from swarmwatch.canonical import (
    CanonicalCodec,
    CanonicalDomainTag,
    CanonicalEncodingError,
    global_canonical_codec,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main


def test_key_sorting_and_whitespace():
    payload = {
        "zeta": 1,
        "alpha": 2,
        "beta": {"d": 4, "c": 3},
    }
    encoded = CanonicalCodec.canonicalize(payload)
    assert encoded == '{"alpha":2,"beta":{"c":3,"d":4},"zeta":1}'


def test_utf16_code_unit_sorting():
    # RFC 8785 section 3.2.3 requires sorting by UTF-16 code units
    payload = {
        "\u007e": "tilde",
        "\u00e9": "e-acute",
        "\u0100": "A-macron",
        "a": "latin-a",
    }
    encoded = CanonicalCodec.canonicalize(payload)
    # 'a' (0x61) < '~' (0x7e) < '\u00e9' (0xe9) < '\u0100' (0x100)
    assert encoded == '{"a":"latin-a","~":"tilde","\u00e9":"e-acute","\u0100":"A-macron"}'


def test_float_formatting_and_normalization():
    # Negative zero must normalize to 0
    assert CanonicalCodec.canonicalize(-0.0) == "0"
    assert CanonicalCodec.canonicalize(0.0) == "0"

    # Floats with integer value serialize without decimal
    assert CanonicalCodec.canonicalize(1.0) == "1"
    assert CanonicalCodec.canonicalize(100.0) == "100"
    assert CanonicalCodec.canonicalize(-42.0) == "-42"

    # Standard floats
    assert CanonicalCodec.canonicalize(1.5) == "1.5"
    assert CanonicalCodec.canonicalize(-1.5) == "-1.5"
    assert CanonicalCodec.canonicalize(123.456) == "123.456"

    # Small numbers matching ECMAScript rules
    assert CanonicalCodec.canonicalize(1e-5) == "0.00001"
    assert CanonicalCodec.canonicalize(1e-6) == "0.000001"
    assert CanonicalCodec.canonicalize(1e-7) == "1e-7"


def test_non_finite_rejection():
    with pytest.raises(CanonicalEncodingError, match="Non-finite"):
        CanonicalCodec.canonicalize(float("nan"))

    with pytest.raises(CanonicalEncodingError, match="Non-finite"):
        CanonicalCodec.canonicalize(float("inf"))

    with pytest.raises(CanonicalEncodingError, match="Non-finite"):
        CanonicalCodec.canonicalize(float("-inf"))


def test_unicode_preservation_unescaped():
    payload = {"persian": "سلام دنیا", "emoji": "⚡", "german": "münchen"}
    canonical = CanonicalCodec.canonicalize(payload)
    assert "سلام دنیا" in canonical
    assert "⚡" in canonical
    assert "münchen" in canonical
    assert "\\u" not in canonical


def test_circular_reference_guard():
    circular_list = [1, 2]
    circular_list.append(circular_list)
    with pytest.raises(CanonicalEncodingError, match="Circular reference"):
        CanonicalCodec.canonicalize(circular_list)

    circular_dict = {"a": 1}
    circular_dict["self"] = circular_dict
    with pytest.raises(CanonicalEncodingError, match="Circular reference"):
        CanonicalCodec.canonicalize(circular_dict)


def test_object_with_to_dict():
    class CustomState:
        def __init__(self, agent: str, score: float):
            self.agent = agent
            self.score = score

        def to_dict(self):
            return {"agent": self.agent, "score": self.score}

    obj = CustomState("agent-01", 99.5)
    canonical = CanonicalCodec.canonicalize(obj)
    assert canonical == '{"agent":"agent-01","score":99.5}'


def test_domain_separation_hash():
    payload = {"turn": 1, "agent": "worker-1"}
    canonical_bytes = CanonicalCodec.encode(payload)
    assert canonical_bytes == b'{"agent":"worker-1","turn":1}'

    # Hash without domain
    digest_raw = CanonicalCodec.hash(payload)
    import hashlib
    assert digest_raw == hashlib.sha256(canonical_bytes).hexdigest()

    # Hash with domain tag
    digest_tagged = CanonicalCodec.hash(payload, domain_tag=CanonicalDomainTag.LEAF)
    expected_prefix = b"sw:leaf:v1\x00"
    assert digest_tagged == hashlib.sha256(expected_prefix + canonical_bytes).hexdigest()
    assert digest_raw != digest_tagged


def test_validate_helper():
    valid_obj = {"x": [1, 2, 3], "y": None, "z": True}
    is_valid, err = CanonicalCodec.validate(valid_obj)
    assert is_valid is True
    assert err is None

    invalid_obj = {"bad": float("nan")}
    is_valid, err = CanonicalCodec.validate(invalid_obj)
    assert is_valid is False
    assert "Non-finite" in err


def test_rest_api_canonicalize_and_hash():
    app = create_app()
    client = TestClient(app)

    # 1. Test canonicalize endpoint
    res = client.post("/api/v1/codec/canonicalize", json={"payload": {"y": 2, "x": 1}})
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "success"
    assert data["canonical_str"] == '{"x":1,"y":2}'
    assert data["length_bytes"] == 13

    # 2. Test hash endpoint
    res2 = client.post(
        "/api/v1/codec/hash",
        json={"payload": {"y": 2, "x": 1}, "domain_tag": "sw:snap:v1"},
    )
    assert res2.status_code == 200
    data2 = res2.json()
    assert data2["status"] == "success"
    assert data2["domain_tag"] == "sw:snap:v1"
    assert len(data2["digest"]) == 64


def test_cli_codec_canonicalize_and_hash(capsys):
    # Test canonicalize
    with pytest.raises(SystemExit) as exc:
        main(["codec", "canonicalize", "--payload", '{"z": 10, "a": 20}', "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    res = json.loads(captured.out)
    assert res["status"] == "success"
    assert res["canonical_str"] == '{"a":20,"z":10}'

    # Test hash
    with pytest.raises(SystemExit) as exc2:
        main(["codec", "hash", "--payload", '{"z": 10, "a": 20}', "--domain", "sw:leaf:v1", "--format", "json"])
    assert exc2.value.code == 0
    captured2 = capsys.readouterr()
    res2 = json.loads(captured2.out)
    assert res2["status"] == "success"
    assert res2["domain_tag"] == "sw:leaf:v1"
    assert len(res2["digest"]) == 64
