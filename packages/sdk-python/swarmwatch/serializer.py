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
from typing import Any, Dict, List, Optional, Set
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

    def __init__(self, masker: Optional[Any] = None):
        from swarmwatch.security import get_default_masker
        self.masker = masker or get_default_masker()

    def redact_text(self, text: str) -> str:
        return self.masker.mask_text(text)


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
                    result[str_k] = "[REDACTED]"
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

    def create_snapshot(
        self,
        span_id: str,
        state_data: Dict[str, Any],
        branch_id: str = "main",
    ) -> StateSnapshot:
        """Creates a sanitized, hashed StateSnapshot instance."""
        sanitized = self.sanitize(state_data)
        state_hash = self.compute_hash(sanitized)
        return StateSnapshot(
            id=str(uuid.uuid4()),
            span_id=span_id,
            branch_id=branch_id,
            state_hash=state_hash,
            state_data=sanitized,
            created_at=datetime.now(timezone.utc),
        )


class TimeMachine:
    """
    State snapshotting and DAG branch forking coordinator.
    Enables non-destructive Time-Machine Fork ('step_back') and parallel branch exploration.
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

    async def fork_branch(
        self,
        swarm_id: str,
        target_span_id: str,
        new_branch_id: str,
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Forks state from target_span_id into new_branch_id, replicating antecedent
        spans and saving the root snapshot of the new branch.
        """
        state = await self.step_back(target_span_id=target_span_id, new_branch_id=new_branch_id)

        raw_spans = await self.storage.get_swarm_spans(swarm_id, project_id=project_id)
        target_idx = -1
        for idx, s in enumerate(raw_spans):
            sid = s.id if hasattr(s, "id") else s.get("id")
            if sid == target_span_id:
                target_idx = idx
                break

        forked_spans = []
        if target_idx >= 0:
            for s in raw_spans[:target_idx + 1]:
                span_dict = s.model_dump() if hasattr(s, "model_dump") else dict(s)
                orig_id = span_dict["id"]
                span_dict["id"] = f"{orig_id}_fork_{new_branch_id}"
                span_dict["branch_id"] = new_branch_id
                from swarmwatch.schema import SwarmSpan
                forked_spans.append(SwarmSpan.model_validate(span_dict))
            if forked_spans:
                await self.storage.save_spans(forked_spans)

        fork_root_span_id = f"{target_span_id}_fork_{new_branch_id}"
        await self.snapshot(
            span_id=fork_root_span_id,
            state_data=state,
            branch_id=new_branch_id,
        )

        return {
            "status": "forked",
            "swarm_id": swarm_id,
            "new_branch_id": new_branch_id,
            "target_span_id": target_span_id,
            "root_span_id": fork_root_span_id,
            "replicated_spans_count": len(forked_spans),
            "state": state,
        }

    async def get_snapshots(
        self,
        swarm_id: str,
        branch_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> List[StateSnapshot]:
        """Returns state snapshots recorded for the given swarm and optional branch."""
        if hasattr(self.storage, "get_snapshots_for_swarm"):
            return await self.storage.get_snapshots_for_swarm(
                swarm_id=swarm_id,
                branch_id=branch_id,
                project_id=project_id,
            )
        return []

    async def list_branches(
        self,
        swarm_id: str,
        project_id: Optional[str] = None,
    ) -> List[str]:
        """Lists distinct branches existing within the given swarm."""
        if hasattr(self.storage, "list_branches"):
            return await self.storage.list_branches(swarm_id=swarm_id, project_id=project_id)
        return ["main"]
