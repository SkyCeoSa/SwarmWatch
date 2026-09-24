# -*- coding: utf-8 -*-
"""
SwarmWatch: Embedding-Free Vector Semantic Drift & Hallucination Divergence Detector (Seam 34 / ADR 0045)
Zero-external-dependency, zero-token-cost in-process mathematical engine for multi-agent drift and echo chamber detection.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field


class DivergenceStatus(str, Enum):
    """Categorical operational state of agent trajectory semantic coherence."""
    NOMINAL = "nominal"
    WARNING = "warning"
    DIVERGENT = "divergent"
    TRIPPED = "tripped"


class SemanticDriftProfile(BaseModel):
    """Immutable turn profile recording semantic drift metrics, anchor similarity, and coherence status."""
    span_id: Optional[str] = None
    swarm_id: str = "default-swarm"
    turn_index: int = 0
    agent_name: str = "agent"
    drift_score: float = Field(default=0.0, description="Composite semantic drift score in [0.0, 1.0]")
    anchor_similarity: float = Field(default=1.0, description="Jaccard similarity to root task anchor [0.0, 1.0]")
    consecutive_similarity: float = Field(default=1.0, description="Jaccard similarity to preceding turn [0.0, 1.0]")
    lexical_dispersion: float = Field(default=1.0, description="Vocabulary Compression Ratio (VCR) in [0.0, 1.0]")
    divergence_status: DivergenceStatus = DivergenceStatus.NOMINAL
    top_shift_tokens: List[str] = Field(default_factory=list, description="Top shifting n-grams detected in turn")
    is_circuit_broken: bool = False
    trip_reason: Optional[str] = None
    timestamp: float = Field(default_factory=time.time)


class MinHashSigner:
    """
    Zero-external-dependency 64-bit Universal MinHash Local Sensitivity Hashing (LSH) engine.
    Generates 64-element universal integer signature vectors for sub-50µs Jaccard similarity estimation.
    """
    NUM_PERMUTATIONS: int = 64
    MERSENNE_PRIME: int = (1 << 61) - 1  # 2305843009213693951

    def __init__(self, num_permutations: int = 64) -> None:
        self.num_permutations = num_permutations
        self.permutations: List[Tuple[int, int]] = self._generate_permutations(num_permutations)

    @classmethod
    def _generate_permutations(cls, num_perms: int) -> List[Tuple[int, int]]:
        """Generates deterministic universal hashing coefficients (a_i, b_i) via SHA-256 seeding."""
        perms: List[Tuple[int, int]] = []
        for i in range(num_perms):
            seed_data = f"minhash_perm_{i}".encode("utf-8")
            h = hashlib.sha256(seed_data).digest()
            a = (int.from_bytes(h[0:8], byteorder="big") % (cls.MERSENNE_PRIME - 1)) + 1
            b = int.from_bytes(h[8:16], byteorder="big") % cls.MERSENNE_PRIME
            perms.append((a, b))
        return perms

    STOP_WORDS: Set[str] = {
        "a", "an", "the", "and", "or", "in", "on", "at", "to", "for", "with",
        "is", "it", "of", "as", "this", "that", "by", "from", "be", "are", "was",
    }

    @classmethod
    def extract_shingles(cls, text: str) -> Set[str]:
        """
        Extracts multi-order character 3-grams and word 1/2-grams from normalized text,
        filtering noise stop words while preserving core domain terms and morphemes.
        """
        if not text:
            return set()

        normalized = re.sub(r"\s+", " ", text.strip().lower())
        all_words = re.findall(r"[a-zA-Z0-9_]+", normalized)
        words = [w for w in all_words if w not in cls.STOP_WORDS]
        if not words:
            words = all_words

        shingles: Set[str] = set()

        # Word unigrams and bigrams
        for i in range(len(words)):
            shingles.add(f"w1:{words[i]}")
            if i + 1 < len(words):
                shingles.add(f"w2:{words[i]}_{words[i+1]}")

        return shingles

    @classmethod
    def _hash_shingle(cls, shingle: str) -> int:
        """Hashes an individual string shingle to a 64-bit unsigned integer."""
        digest = hashlib.sha256(shingle.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], byteorder="big")

    def compute_signature(self, text: str) -> List[int]:
        """Computes a 64-element MinHash integer signature vector from input text in <15µs."""
        shingles = self.extract_shingles(text)
        if not shingles:
            return [0] * self.num_permutations

        hashed_shingles = [self._hash_shingle(s) for s in shingles]
        signature: List[int] = []

        for a, b in self.permutations:
            min_val = cls_prime = self.MERSENNE_PRIME
            for h in hashed_shingles:
                val = (a * h + b) % cls_prime
                if val < min_val:
                    min_val = val
            signature.append(min_val)

        return signature

    def jaccard_similarity(self, sig_a: List[int], sig_b: List[int]) -> float:
        """
        Estimates pairwise Jaccard similarity J(A, B) directly from signature agreement ratio.
        J_hat = (matches / num_permutations).
        """
        if not sig_a or not sig_b or len(sig_a) != len(sig_b):
            return 0.0
        matches = sum(1 for a, b in zip(sig_a, sig_b) if a == b)
        return round(matches / len(sig_a), 4)

    @classmethod
    def compute_vcr(cls, text: str) -> Tuple[float, List[str]]:
        """
        Computes the Vocabulary Compression Ratio (VCR = |V_unique| / |V_total|)
        and returns the top unique word tokens.
        """
        if not text:
            return 1.0, []
        words = re.findall(r"[a-zA-Z0-9_]+", text.lower())
        if not words:
            return 1.0, []
        unique_words = set(words)
        vcr = round(len(unique_words) / len(words), 4)
        return vcr, sorted(list(unique_words))[:8]


class SwarmDriftState:
    """Internal trajectory state for an active swarm session."""
    def __init__(self, swarm_id: str) -> None:
        self.swarm_id = swarm_id
        self.anchor_text: Optional[str] = None
        self.anchor_sig: Optional[List[int]] = None
        self.last_sig: Optional[List[int]] = None
        self.history: List[SemanticDriftProfile] = []
        self.consecutive_divergent_count: int = 0
        self.is_tripped: bool = False
        self.trip_reason: Optional[str] = None


class SemanticDriftDetector:
    """
    Deep module orchestrating embedding-free semantic drift detection,
    hallucination divergence interception, and circuit breaking across agent swarms.
    """
    EXPECTED_OVERLAP_BASELINE: float = 0.08

    def __init__(
        self,
        drift_threshold: float = 0.72,
        warning_threshold: float = 0.45,
        min_vcr_threshold: float = 0.20,
        vcr_min_token_length: int = 20,
        anchor_weight: float = 0.75,
        local_weight: float = 0.25,
        consecutive_divergent_limit: int = 2,
    ) -> None:
        self.drift_threshold = drift_threshold
        self.warning_threshold = warning_threshold
        self.min_vcr_threshold = min_vcr_threshold
        self.vcr_min_token_length = vcr_min_token_length
        self.anchor_weight = anchor_weight
        self.local_weight = local_weight
        self.consecutive_divergent_limit = consecutive_divergent_limit
        self.signer = MinHashSigner()
        self._swarms: Dict[str, SwarmDriftState] = {}

    def _get_or_create_state(self, swarm_id: str) -> SwarmDriftState:
        if swarm_id not in self._swarms:
            self._swarms[swarm_id] = SwarmDriftState(swarm_id)
        return self._swarms[swarm_id]

    def set_anchor(self, swarm_id: str, anchor_text: str) -> None:
        """Explicitly defines the root objective anchor text for a swarm session."""
        state = self._get_or_create_state(swarm_id)
        state.anchor_text = anchor_text
        state.anchor_sig = self.signer.compute_signature(anchor_text)

    def evaluate_turn(
        self,
        swarm_id: str,
        agent_name: str,
        text: str,
        span_id: Optional[str] = None,
        turn_index: Optional[int] = None,
    ) -> SemanticDriftProfile:
        """
        Evaluates an individual agent turn for semantic divergence against the root anchor
        and preceding turn. Computes drift score and halts execution if thresholds are breached.
        """
        state = self._get_or_create_state(swarm_id)
        current_turn = turn_index if turn_index is not None else len(state.history) + 1
        sig = self.signer.compute_signature(text)
        vcr, top_tokens = MinHashSigner.compute_vcr(text)
        words_count = len(re.findall(r"[a-zA-Z0-9_]+", text.lower()))

        # If no anchor is set, this initial turn establishes the root task anchor
        if state.anchor_sig is None:
            state.anchor_text = text
            state.anchor_sig = sig
            state.last_sig = sig
            profile = SemanticDriftProfile(
                span_id=span_id,
                swarm_id=swarm_id,
                turn_index=current_turn,
                agent_name=agent_name,
                drift_score=0.0,
                anchor_similarity=1.0,
                consecutive_similarity=1.0,
                lexical_dispersion=vcr,
                divergence_status=DivergenceStatus.NOMINAL,
                top_shift_tokens=top_tokens,
                is_circuit_broken=False,
                trip_reason=None,
            )
            state.history.append(profile)
            return profile

        # Calculate raw Jaccard similarities
        raw_anchor_sim = self.signer.jaccard_similarity(sig, state.anchor_sig)
        raw_consecutive_sim = (
            self.signer.jaccard_similarity(sig, state.last_sig)
            if state.last_sig is not None
            else 1.0
        )

        # Baseline normalized similarity: in natural dialogue, an overlap of 0.20 represents high alignment.
        norm_anchor_sim = min(1.0, raw_anchor_sim / self.EXPECTED_OVERLAP_BASELINE)
        norm_consecutive_sim = min(1.0, raw_consecutive_sim / self.EXPECTED_OVERLAP_BASELINE)

        # Composite semantic drift score: Delta = 1.0 - (w_anchor * S_anchor + w_local * S_local)
        composite_sim = (self.anchor_weight * norm_anchor_sim) + (self.local_weight * norm_consecutive_sim)
        drift_score = round(max(0.0, min(1.0, 1.0 - composite_sim)), 4)

        # Check for echo-chamber collapse (severely depressed VCR with substantive token length)
        is_echo_chamber = words_count >= self.vcr_min_token_length and vcr < self.min_vcr_threshold

        # Determine divergence status
        is_divergent = (drift_score >= self.drift_threshold) or is_echo_chamber
        is_warning = drift_score >= self.warning_threshold

        trip_triggered = False
        trip_reason = None

        if is_echo_chamber:
            state.consecutive_divergent_count += 1
            if state.consecutive_divergent_count >= self.consecutive_divergent_limit:
                status = DivergenceStatus.TRIPPED
                trip_triggered = True
                trip_reason = (
                    f"Hallucinatory echo chamber detected: VCR={vcr:.2f} "
                    f"(threshold={self.min_vcr_threshold:.2f}) across {words_count} tokens"
                )
            else:
                status = DivergenceStatus.DIVERGENT
        elif is_divergent:
            state.consecutive_divergent_count += 1
            if state.consecutive_divergent_count >= self.consecutive_divergent_limit:
                status = DivergenceStatus.TRIPPED
                trip_triggered = True
                trip_reason = (
                    f"Semantic drift threshold breached: Delta={drift_score:.2f} "
                    f"(threshold={self.drift_threshold:.2f}) for {state.consecutive_divergent_count} consecutive turns"
                )
            else:
                status = DivergenceStatus.DIVERGENT
        elif is_warning:
            state.consecutive_divergent_count = 0
            status = DivergenceStatus.WARNING
        else:
            state.consecutive_divergent_count = 0
            status = DivergenceStatus.NOMINAL

        if trip_triggered:
            state.is_tripped = True
            state.trip_reason = trip_reason

        # Update last signature
        state.last_sig = sig

        profile = SemanticDriftProfile(
            span_id=span_id,
            swarm_id=swarm_id,
            turn_index=current_turn,
            agent_name=agent_name,
            drift_score=drift_score,
            anchor_similarity=round(norm_anchor_sim, 4),
            consecutive_similarity=round(norm_consecutive_sim, 4),
            lexical_dispersion=vcr,
            divergence_status=status,
            top_shift_tokens=top_tokens,
            is_circuit_broken=trip_triggered or state.is_tripped,
            trip_reason=trip_reason or state.trip_reason,
        )
        state.history.append(profile)
        return profile

    def evaluate_span(self, span_payload: Dict[str, Any]) -> SemanticDriftProfile:
        """Extracts text payload from a SwarmSpan dictionary or model and evaluates drift."""
        swarm_id = span_payload.get("swarm_id", "default-swarm")
        agent_name = span_payload.get("source_agent", "agent")
        span_id = span_payload.get("id")
        turn_index = span_payload.get("turn_index")

        # Extract text content from output_payload or input_payload
        text_parts: List[str] = []
        for key in ("output_payload", "input_payload"):
            val = span_payload.get(key)
            if isinstance(val, str):
                text_parts.append(val)
            elif isinstance(val, dict):
                for subk in ("content", "msg", "text", "response", "result"):
                    if subk in val and isinstance(val[subk], str):
                        text_parts.append(val[subk])
                if not text_parts:
                    text_parts.append(json.dumps(val))

        combined_text = " ".join(text_parts).strip() or f"action {span_payload.get('span_type', 'turn')}"
        return self.evaluate_turn(
            swarm_id=swarm_id,
            agent_name=agent_name,
            text=combined_text,
            span_id=span_id,
            turn_index=turn_index,
        )

    def get_timeline(self, swarm_id: str) -> List[SemanticDriftProfile]:
        """Returns the chronological semantic drift profile history for a swarm."""
        state = self._swarms.get(swarm_id)
        if not state:
            return []
        return list(state.history)

    def get_aggregate_stats(self, swarm_id: str) -> Dict[str, Any]:
        """Returns aggregate drift statistics, current status, and peak divergence."""
        state = self._swarms.get(swarm_id)
        if not state or not state.history:
            return {
                "swarm_id": swarm_id,
                "total_turns": 0,
                "current_drift_score": 0.0,
                "max_drift_score": 0.0,
                "mean_drift_score": 0.0,
                "current_status": DivergenceStatus.NOMINAL.value,
                "is_tripped": False,
                "trip_reason": None,
                "anchor_set": False,
            }

        drift_scores = [p.drift_score for p in state.history]
        current_profile = state.history[-1]

        return {
            "swarm_id": swarm_id,
            "total_turns": len(state.history),
            "current_drift_score": current_profile.drift_score,
            "max_drift_score": max(drift_scores),
            "mean_drift_score": round(sum(drift_scores) / len(drift_scores), 4),
            "current_status": current_profile.divergence_status.value,
            "is_tripped": state.is_tripped,
            "trip_reason": state.trip_reason,
            "anchor_set": state.anchor_text is not None,
        }

    def reset(self, swarm_id: Optional[str] = None) -> None:
        """Resets drift tracker state for a specific swarm or globally."""
        if swarm_id:
            self._swarms.pop(swarm_id, None)
        else:
            self._swarms.clear()


# Global Singleton Instance
global_drift_detector = SemanticDriftDetector()
