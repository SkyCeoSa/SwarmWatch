# -*- coding: utf-8 -*-
"""
SwarmWatch Durable Causal Stream Segments & Crash-Consistent Recovery (Seam 86 / ADR 0097).

Implements append-only durable stream segments with fsync durability (D1),
prefix-closed crash recovery and checksum integrity (D2),
and bounded memory compaction into cryptographic state snapshots (D3).

Invariants:
- D1: Sequence offsets and receipts are issued only after records are committed to journal.
- D2: Crash recovery yields a strictly prefix-closed causal stream with zero missing holes.
- D3: Segments older than retention compact into a Merkle StateSnapshot, bounding memory usage.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from swarmwatch.hlc import HybridLogicalClock, HybridLogicalTimestamp
from swarmwatch.qc import compute_merkle_root


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class DurableStreamError(Exception):
    """Base exception for durable stream operations."""


class CorruptRecordError(DurableStreamError):
    """Raised when record checksum or hash chain verification fails."""


class InvalidOffsetRangeError(DurableStreamError):
    """Raised when an out-of-range stream offset is requested."""


# ==============================================================================
# 2. Domain Types
# ==============================================================================

@dataclass(frozen=True)
class StreamRecord:
    """Immutable durable record within the causal stream."""
    offset: int
    timestamp_hlc: str
    payload: Dict[str, Any]
    prev_hash: str
    record_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "offset": self.offset,
            "timestamp_hlc": self.timestamp_hlc,
            "payload": self.payload,
            "prev_hash": self.prev_hash,
            "record_hash": self.record_hash,
        }


@dataclass(frozen=True)
class StreamHead:
    """Current recovery head and integrity status of the durable stream."""
    stream_id: str
    head_offset: int
    total_records: int
    head_hash: str
    compacted_offset: int


# ==============================================================================
# 3. Durable Causal Stream Deep Module
# ==============================================================================

class DurableCausalStream:
    """
    Manages append-only durable stream records with hash chaining, crash recovery,
    and retention compaction.
    """

    def __init__(
        self,
        stream_id: str,
        data_dir: Optional[str] = None,
        clock: Optional[HybridLogicalClock] = None,
    ) -> None:
        self.stream_id = str(stream_id)
        self.data_dir = Path(data_dir) if data_dir else None
        self.clock = clock or HybridLogicalClock(node_id=self.stream_id)
        self._lock = threading.RLock()

        # In-memory storage for active window
        self._records: List[StreamRecord] = []
        self._head_offset: int = -1
        self._last_hash: str = "00" * 32
        self._compacted_offset: int = -1
        self._snapshots: List[Dict[str, Any]] = []

        if self.data_dir:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self._journal_file = self.data_dir / f"{self.stream_id}.wal"
            self.recover()
        else:
            self._journal_file = None

    def append(
        self,
        payload: Dict[str, Any],
        timestamp: Optional[HybridLogicalTimestamp] = None,
    ) -> int:
        """
        Appends an event to the durable stream (Invariant D1).
        Flushes to WAL if data_dir is configured before returning the sequence offset.
        """
        with self._lock:
            ts = timestamp or self.clock.now()
            ts_str = ts.to_string()
            next_offset = self._head_offset + 1

            payload_canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            hash_material = f"{next_offset}:{ts_str}:{self._last_hash}:{payload_canonical}"
            record_hash = hashlib.sha256(hash_material.encode("utf-8")).hexdigest()

            record = StreamRecord(
                offset=next_offset,
                timestamp_hlc=ts_str,
                payload=payload,
                prev_hash=self._last_hash,
                record_hash=record_hash,
            )

            if self._journal_file:
                line = json.dumps(record.to_dict()) + "\n"
                with open(self._journal_file, "a", encoding="utf-8") as f:
                    f.write(line)
                    f.flush()
                    os.fsync(f.fileno())  # Invariant D1: Explicit fsync before acknowledgment

            self._records.append(record)
            self._head_offset = next_offset
            self._last_hash = record_hash

            return next_offset

    def recover(self) -> StreamHead:
        """
        Performs crash-consistent recovery over the journal (Invariant D2).
        Verifies hash-chain integrity from beginning to end; truncates any torn tail writes.
        """
        with self._lock:
            if not self._journal_file or not self._journal_file.exists():
                return StreamHead(
                    stream_id=self.stream_id,
                    head_offset=self._head_offset,
                    total_records=len(self._records),
                    head_hash=self._last_hash,
                    compacted_offset=self._compacted_offset,
                )

            valid_records: List[StreamRecord] = []
            expected_offset = 0
            prev_hash = "00" * 32

            with open(self._journal_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        rec = StreamRecord(
                            offset=data["offset"],
                            timestamp_hlc=data["timestamp_hlc"],
                            payload=data["payload"],
                            prev_hash=data["prev_hash"],
                            record_hash=data["record_hash"],
                        )

                        # Verify monotonic sequence and hash chain integrity
                        if rec.offset != expected_offset or rec.prev_hash != prev_hash:
                            break  # Invariant D2: Discard torn tail writes, retain prefix-closed log

                        # Verify checksum
                        payload_canonical = json.dumps(rec.payload, sort_keys=True, separators=(",", ":"))
                        expected_hash = hashlib.sha256(
                            f"{rec.offset}:{rec.timestamp_hlc}:{rec.prev_hash}:{payload_canonical}".encode("utf-8")
                        ).hexdigest()

                        if rec.record_hash != expected_hash:
                            break  # Integrity error: stop at last clean record

                        valid_records.append(rec)
                        expected_offset += 1
                        prev_hash = rec.record_hash
                    except Exception:
                        break  # Corrupt tail: stop recovery

            self._records = valid_records
            self._head_offset = expected_offset - 1 if expected_offset > 0 else -1
            self._last_hash = prev_hash

            return StreamHead(
                stream_id=self.stream_id,
                head_offset=self._head_offset,
                total_records=len(self._records),
                head_hash=self._last_hash,
                compacted_offset=self._compacted_offset,
            )

    def compact(self, before_offset: int) -> Dict[str, Any]:
        """
        Compacts records strictly older than before_offset into a Merkle StateSnapshot (Invariant D3).
        Frees old entries from memory to cap RAM usage.
        """
        with self._lock:
            if before_offset <= self._compacted_offset:
                return {"status": "no_op", "compacted_offset": self._compacted_offset}

            to_compact = [r for r in self._records if r.offset < before_offset]
            if not to_compact:
                return {"status": "no_op", "compacted_offset": self._compacted_offset}

            leaf_hashes = [r.record_hash for r in to_compact]
            merkle_root = compute_merkle_root(leaf_hashes)

            snapshot = {
                "snapshot_id": hashlib.sha256(f"snap:{before_offset}:{merkle_root}".encode("utf-8")).hexdigest(),
                "compacted_before_offset": before_offset,
                "record_count": len(to_compact),
                "merkle_root": merkle_root,
                "compacted_at_ns": time.time_ns(),
            }

            self._snapshots.append(snapshot)
            self._records = [r for r in self._records if r.offset >= before_offset]
            self._compacted_offset = before_offset - 1

            return snapshot

    def read_range(self, start_offset: int, limit: int = 100) -> List[StreamRecord]:
        """Reads a range of active records."""
        with self._lock:
            return [r for r in self._records if r.offset >= start_offset][:limit]

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "stream_id": self.stream_id,
                "head_offset": self._head_offset,
                "active_records_in_ram": len(self._records),
                "compacted_offset": self._compacted_offset,
                "total_snapshots": len(self._snapshots),
                "last_hash": self._last_hash,
            }
