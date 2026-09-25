# -*- coding: utf-8 -*-
"""
Canonical Causal Encoding & Byte-Deterministic Contract Engine (Seam 50 / ADR 0061).
Conforms to RFC 8785 (JSON Canonicalization Scheme - JCS) principles to guarantee
100% byte-for-byte identical state hashes and Merkle leaves across Python and TypeScript.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Dict, List, Optional, Set, Tuple, Union


class CanonicalEncodingError(ValueError):
    """Raised when an object violates canonical serialization invariants."""
    pass


class CanonicalDomainTag:
    """Standardized cryptographic domain separation tags preventing cross-context substitution."""
    LEAF = "sw:leaf:v1"
    SNAPSHOT = "sw:snap:v1"
    RECEIPT = "sw:rcpt:v1"
    PERMIT = "sw:pmt:v1"
    TOPOLOGY = "sw:topo:v1"
    STATE = "sw:state:v1"
    METRIC = "sw:metric:v1"


def _utf16_sort_key(s: str) -> bytes:
    """Sorts strings by UTF-16 code units as required by RFC 8785 section 3.2.3."""
    return s.encode("utf-16-be")


class CanonicalCodec:
    """
    Deterministic, zero-token canonical causal codec implementing RFC 8785 (JCS).
    Ensures identical byte sequences across diverse execution runtimes (Python, Node.js, Go).
    """

    SUPPORTED_VERSIONS = {1}

    @classmethod
    def canonicalize(cls, obj: Any, version: int = 1) -> str:
        """
        Serializes an arbitrary Python object into a canonical JSON string.
        Enforces lexicographical key sorting, whitespace elimination, negative zero normalization,
        and unescaped UTF-8 string representation.
        """
        if version not in cls.SUPPORTED_VERSIONS:
            raise CanonicalEncodingError(f"Unsupported canonical encoding version: {version}")

        seen_ids: Set[int] = set()

        def _encode_val(val: Any) -> str:
            if val is None:
                return "null"
            elif isinstance(val, bool):
                return "true" if val else "false"
            elif isinstance(val, int):
                return str(val)
            elif isinstance(val, float):
                if math.isnan(val) or math.isinf(val):
                    raise CanonicalEncodingError("Non-finite numbers (NaN, Infinity) are prohibited in canonical encoding")
                if val == 0.0:
                    # Normalize -0.0 to 0 as required by RFC 8785 Section 3.2.2.3
                    return "0"
                if val.is_integer() and -9007199254740991 <= int(val) <= 9007199254740991:
                    return str(int(val))
                s = str(val)
                if "e" in s or "E" in s:
                    parts = s.lower().split("e")
                    mantissa = parts[0]
                    exp = int(parts[1])
                    sign = "-" if mantissa.startswith("-") else ""
                    if sign:
                        mantissa = mantissa[1:]
                    zeros = "0"
                    if -6 <= exp < 0:
                        digits = mantissa.replace(".", "")
                        pad = zeros * (abs(exp) - 1)
                        return sign + "0." + pad + digits
                    elif 0 <= exp < 21:
                        digits = mantissa.replace(".", "")
                        dec_pos = mantissa.find(".")
                        if dec_pos == -1:
                            dec_pos = len(mantissa)
                        shift = dec_pos + exp
                        if shift >= len(digits):
                            pad = zeros * (shift - len(digits))
                            return sign + digits + pad
                        else:
                            return sign + digits[:shift] + "." + digits[shift:]
                    else:
                        exp_sign = "+" if exp > 0 else "-"
                        return sign + mantissa + "e" + exp_sign + str(abs(exp))
                return s
            elif isinstance(val, str):
                return json.dumps(val, ensure_ascii=False)
            elif isinstance(val, (list, tuple)):
                obj_id = id(val)
                if obj_id in seen_ids:
                    raise CanonicalEncodingError("Circular reference detected in array structure")
                seen_ids.add(obj_id)
                try:
                    items = [_encode_val(item) for item in val]
                    return "[" + ",".join(items) + "]"
                finally:
                    seen_ids.remove(obj_id)
            elif isinstance(val, dict):
                obj_id = id(val)
                if obj_id in seen_ids:
                    raise CanonicalEncodingError("Circular reference detected in object structure")
                seen_ids.add(obj_id)
                try:
                    # Convert all keys to strings and sort by UTF-16 code unit order
                    sorted_keys = sorted(val.keys(), key=lambda k: _utf16_sort_key(str(k)))
                    pairs = []
                    for k in sorted_keys:
                        k_str = str(k)
                        encoded_key = json.dumps(k_str, ensure_ascii=False)
                        encoded_val = _encode_val(val[k])
                        pairs.append(f"{encoded_key}:{encoded_val}")
                    return "{" + ",".join(pairs) + "}"
                finally:
                    seen_ids.remove(obj_id)
            elif hasattr(val, "to_dict") and callable(getattr(val, "to_dict")):
                return _encode_val(val.to_dict())
            else:
                raise CanonicalEncodingError(f"Unsupported type for canonical encoding: {type(val).__name__}")

        return _encode_val(obj)

    @classmethod
    def encode(cls, obj: Any, version: int = 1) -> bytes:
        """Serializes object to canonical UTF-8 bytes."""
        canonical_str = cls.canonicalize(obj, version=version)
        return canonical_str.encode("utf-8")

    @classmethod
    def decode(cls, payload: bytes | str) -> Any:
        """Parses canonical JSON bytes or string back into Python objects."""
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return json.loads(payload)

    @classmethod
    def hash(cls, obj: Any, domain_tag: str = "", version: int = 1) -> str:
        """
        Computes domain-separated SHA-256 digest of canonical byte representation.
        H_v(domain_tag, x) = SHA256(domain_tag || 0x00 || C_v(x))
        """
        canonical_bytes = cls.encode(obj, version=version)
        if domain_tag:
            prefix = f"{domain_tag}\x00".encode("utf-8")
        else:
            prefix = b""
        return hashlib.sha256(prefix + canonical_bytes).hexdigest()

    @classmethod
    def validate(cls, obj: Any, version: int = 1) -> Tuple[bool, Optional[str]]:
        """
        Validates whether an object is canonically encodable without throwing exceptions.
        Returns (is_valid, error_message).
        """
        try:
            cls.canonicalize(obj, version=version)
            return True, None
        except Exception as e:
            return False, str(e)


# Global singleton helper
global_canonical_codec = CanonicalCodec()
