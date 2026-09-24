# -*- coding: utf-8 -*-
"""
Tests for Seam 34 / ADR 0045: Embedding-Free Vector Semantic Drift & Hallucination Divergence Detector.
Verifies MinHashSigner, SemanticDriftDetector, REST endpoints, and CLI commands.
"""

import json
import pytest
from fastapi.testclient import TestClient

from swarmwatch.drift import (
    DivergenceStatus,
    MinHashSigner,
    SemanticDriftDetector,
    SemanticDriftProfile,
    global_drift_detector,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main


def test_minhash_signer_shingles_and_determinism():
    signer = MinHashSigner()
    text = "The quick brown fox jumps over the lazy dog"
    shingles = signer.extract_shingles(text)
    assert len(shingles) > 0
    assert any(s.startswith("w1:") for s in shingles)
    assert any(s.startswith("w2:") for s in shingles)

    sig1 = signer.compute_signature(text)
    sig2 = signer.compute_signature(text)
    assert len(sig1) == 64
    assert sig1 == sig2
    assert signer.jaccard_similarity(sig1, sig2) == 1.0


def test_minhash_jaccard_similarity_semantics():
    signer = MinHashSigner()
    anchor = "Implement a secure PostgreSQL database schema with migration scripts"
    similar = "Design an encrypted PostgreSQL relational schema and write SQL migration files"
    completely_different = "Astronauts discovered ancient lunar rocks containing rare extraterrestrial crystals"

    sig_anchor = signer.compute_signature(anchor)
    sig_similar = signer.compute_signature(similar)
    sig_different = signer.compute_signature(completely_different)

    sim_high = signer.jaccard_similarity(sig_anchor, sig_similar)
    sim_low = signer.jaccard_similarity(sig_anchor, sig_different)

    assert sim_high > sim_low
    assert sim_low < 0.20


def test_vocabulary_compression_ratio():
    varied_text = "The architect designed scalable microservices for distributed cloud deployment across multiple regions"
    vcr_high, tokens_high = MinHashSigner.compute_vcr(varied_text)
    assert vcr_high > 0.80

    echo_chamber = "loop error failed retry loop error failed retry loop error failed retry loop error failed retry"
    vcr_low, tokens_low = MinHashSigner.compute_vcr(echo_chamber)
    assert vcr_low < 0.35


def test_semantic_drift_detector_lifecycle():
    detector = SemanticDriftDetector(consecutive_divergent_limit=2)
    swarm_id = "test-drift-swarm-01"

    # Turn 1: Sets anchor
    t1 = detector.evaluate_turn(
        swarm_id=swarm_id,
        agent_name="Architect",
        text="Build a high-performance Python FastAPI service with Redis caching and JWT authentication",
    )
    assert t1.turn_index == 1
    assert t1.drift_score == 0.0
    assert t1.divergence_status == DivergenceStatus.NOMINAL
    assert not t1.is_circuit_broken

    # Turn 2: Coherent follow-up
    t2 = detector.evaluate_turn(
        swarm_id=swarm_id,
        agent_name="BackendEngineer",
        text="Configured FastAPI application with Redis connection pool and JWT bearer token middleware",
    )
    assert t2.turn_index == 2
    assert t2.drift_score < 0.50
    assert t2.divergence_status in (DivergenceStatus.NOMINAL, DivergenceStatus.WARNING)
    assert not t2.is_circuit_broken

    # Turn 3: Severe topic derailment (first divergent turn)
    t3 = detector.evaluate_turn(
        swarm_id=swarm_id,
        agent_name="RogueAgent",
        text="Let's write poetry about medieval castles and knights defending the ancient fortress against dragons",
    )
    assert t3.turn_index == 3
    assert t3.drift_score >= 0.70
    assert t3.divergence_status == DivergenceStatus.DIVERGENT
    assert not t3.is_circuit_broken

    # Turn 4: Second consecutive divergent turn -> Trips the circuit!
    t4 = detector.evaluate_turn(
        swarm_id=swarm_id,
        agent_name="RogueAgent",
        text="The wizard cast a powerful lightning spell illuminating the enchanted forest in mystical colors",
    )
    assert t4.turn_index == 4
    assert t4.divergence_status == DivergenceStatus.TRIPPED
    assert t4.is_circuit_broken
    assert "Semantic drift threshold breached" in (t4.trip_reason or "")


def test_echo_chamber_hallucination_trip():
    detector = SemanticDriftDetector(
        min_vcr_threshold=0.25,
        vcr_min_token_length=20,
        consecutive_divergent_limit=1,
    )
    swarm_id = "test-echo-swarm-01"

    # Set initial anchor
    detector.set_anchor(swarm_id, "Analyze financial trading trends and arbitrage opportunities")

    # Echo chamber message with repetitive words (>= 20 tokens)
    echo_payload = (
        "arbitrage trade error trade error trade error trade error "
        "trade error trade error trade error trade error trade error trade error"
    )
    turn = detector.evaluate_turn(
        swarm_id=swarm_id,
        agent_name="TraderBot",
        text=echo_payload,
    )
    assert turn.divergence_status == DivergenceStatus.TRIPPED
    assert turn.is_circuit_broken
    assert "echo chamber" in (turn.trip_reason or "").lower()


def test_server_drift_endpoints_and_span_pipeline():
    app = create_app()
    client = TestClient(app)
    swarm_id = "server-drift-swarm-01"

    # 1. Set explicit anchor
    resp_anchor = client.post(
        f"/api/v1/swarms/{swarm_id}/drift/anchor",
        json={"anchor_text": "System optimization for low-latency streaming pipeline"},
    )
    assert resp_anchor.status_code == 200
    assert resp_anchor.json()["status"] == "anchor_set"

    # 2. Ingest spans through standard /api/v1/spans endpoint
    spans = [
        {
            "id": "span-drift-01",
            "trace_id": "trace-drift-01",
            "swarm_id": swarm_id,
            "source_agent": "pipeline_agent",
            "target_agent": "environment",
            "span_type": "turn",
            "input_payload": {"msg": "Optimized memory buffers for streaming ingestion"},
            "output_payload": {"content": "Buffers allocated with zero-copy ring buffers"},
            "token_count": 80,
            "entropy_score": 0.85,
        }
    ]
    resp_spans = client.post("/api/v1/spans", json=spans)
    assert resp_spans.status_code == 200

    # 3. Check /api/v1/swarms/{id}/drift endpoint
    resp_drift = client.get(f"/api/v1/swarms/{swarm_id}/drift")
    assert resp_drift.status_code == 200
    data = resp_drift.json()
    assert data["swarm_id"] == swarm_id
    assert data["total_turns"] >= 1
    assert "timeline" in data
    assert len(data["timeline"]) >= 1

    # 4. Ad-hoc evaluation endpoint
    resp_eval = client.post(
        f"/api/v1/swarms/{swarm_id}/drift/evaluate",
        json={"text": "Streaming throughput reached 65000 frames per second", "agent_name": "worker_01"},
    )
    assert resp_eval.status_code == 200
    eval_data = resp_eval.json()
    assert "drift_score" in eval_data
    assert "divergence_status" in eval_data

    # 5. Reset endpoint
    resp_reset = client.post(f"/api/v1/swarms/{swarm_id}/drift/reset")
    assert resp_reset.status_code == 200
    assert resp_reset.json()["status"] == "reset"


def test_cli_drift_commands(capsys):
    # Test `swarmwatch drift anchor`
    with pytest.raises(SystemExit) as exc:
        cli_main(["drift", "anchor", "--swarm-id", "cli-drift-01", "--anchor", "Kubernetes cluster orchestrator", "--format", "json"])
    assert exc.value.code == 0
    out, _ = capsys.readouterr()
    assert "anchor_set" in out

    # Test `swarmwatch drift analyze`
    with pytest.raises(SystemExit) as exc:
        cli_main(["drift", "analyze", "--swarm-id", "cli-drift-01", "--text", "Deployed pod replica set successfully", "--format", "json"])
    assert exc.value.code == 0
    out, _ = capsys.readouterr()
    data = json.loads(out)
    assert "drift_score" in data
    assert "divergence_status" in data

    # Test `swarmwatch drift status`
    with pytest.raises(SystemExit) as exc:
        cli_main(["drift", "status", "--swarm-id", "cli-drift-01", "--format", "json"])
    assert exc.value.code == 0
    out, _ = capsys.readouterr()
    data = json.loads(out)
    assert "total_turns" in data
