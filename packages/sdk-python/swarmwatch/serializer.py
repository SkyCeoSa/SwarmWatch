# -*- coding: utf-8 -*-
"""
SwarmWatch Safe State Serializer & Time-Machine Branching.
Handles non-serializable objects, circular references, regex-based secret redaction,
and Git-like DAG execution branching.
"""

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Dict, Optional, Set
import uuid

from swarmwatch.schema import StateSnapshot
from swarmwatch.storage import StorageAdapter


class SafeStateSerializer:
    """
    Safely serializes complex agent execution state.
    Converts unknown objects via __repr__, breaks circular loops,
    and redacts secrets/API keys.
    """

    SECRET_PATTERNS = [
        (re.compile(r"sk-[a-zA-Z0-9_\-]{15,}", re.IGNORECASE), "sk-[REDACTED]"),
        (re.compile(r"bearer\s+[a-zA-Z0-9_\-\.]+", re.IGNORECASE), "bearer [REDACTED]"),
    ]

    SENSITIVE_KEY_NAMES = {"password", "secret", "token", "api_key", "auth", "private_key"}

    def __init__(self):
        pass

    def redact_text(self, text: str) -> str:
        for pattern, replacement in self.SECRET_PATTERNS:
            text = pattern.sub(replacement, text)
        return text

    def sanitize(self, obj: Any, seen: Optional[Set[int]] = None) -> Any:
        if seen is None:
            seen = set()

        if obj is None or isinstance(obj, (int, float, bool)):
            return obj

        if isinstance(obj, str):
            return self.redact_text(obj)

        if isinstance(obj, datetime):
            return obj.isoformat()

        obj_id = id(obj)
        if obj_id in seen:
            return "[Circular Reference]"

        # Handle Dict
        if isinstance(obj, dict):
            seen.add(obj_id)
            result = {}
            for k, v in obj.items():
                str_k = str(k)
                # Check if key implies sensitive data
                if any(sens in str_k.lower() for sens in self.SENSITIVE_KEY_NAMES):
                    if isinstance(v, str):
                        result[str_k] = self.redact_text(v)
                        # If no regex matched but key is sensitive, mask value
                        if result[str_k] == v and len(v) > 0:
                            result[str_k] = "[REDACTED]"
                    else:
                        result[str_k] = self.sanitize(v, seen)
                else:
                    result[str_k] = self.sanitize(v, seen)
            seen.remove(obj_id)
            return result

        # Handle List / Tuple / Set
        if isinstance(obj, (list, tuple, set)):
            seen.add(obj_id)
            sanitized_list = [self.sanitize(item, seen) for item in obj]
            seen.remove(obj_id)
            return sanitized_list

        # Fallback for custom objects / connections / closures / threads
        return repr(obj)

    def serialize(self, obj: Any) -> str:
        sanitized = self.sanitize(obj)
        return json.dumps(sanitized, sort_keys=True)

    def compute_hash(self, obj: Any) -> str:
        serialized = self.serialize(obj)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class TimeMachine:
    """
    State snapshotting and DAG branch forking coordinator.
    Enables non-destructive rewind ('step_back') and parallel branch exploration.
    """

    def __init__(self, storage: StorageAdapter):
        self.storage = storage
        self.serializer = SafeStateSerializer()

    async def snapshot(
        self,
        span_id: str,
        state_data: Dict[str, Any],
        branch_id: str = "main",
    ) -> StateSnapshot:
        sanitized_state = self.serializer.sanitize(state_data)
        state_hash = self.serializer.compute_hash(sanitized_state)

        snapshot = StateSnapshot(
            id=str(uuid.uuid4()),
            span_id=span_id,
            branch_id=branch_id,
            state_hash=state_hash,
            state_data=sanitized_state,
            created_at=datetime.now(timezone.utc),
        )
        await self.storage.save_snapshot(snapshot)
        return snapshot

    async def step_back(
        self,
        target_span_id: str,
        new_branch_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Retrieves snapshot associated with target_span_id,
        and returns state ready to resume on new_branch_id.
        """
        snapshot = await self.storage.get_snapshot(target_span_id)
        if not snapshot:
            raise ValueError(f"No state snapshot found for span '{target_span_id}'")

        return snapshot.state_data
