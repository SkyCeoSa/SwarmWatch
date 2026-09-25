# -*- coding: utf-8 -*-
"""
Tests for Multi-Agent Epistemic Logic & Knowledge Base State Synchronization Engine.
Seam 48 / ADR 0059.
"""

import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.epistemic import (
    EpistemicTruthValue,
    EpistemicStatus,
    PropositionalClaim,
    EpistemicConflict,
    EpistemicKnowledgeBase,
    EpistemicEngine,
    global_epistemic_engine,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main


def test_propositional_claim_creation_and_serialization():
    claim = PropositionalClaim(
        claim_id="prop-test-01",
        agent_id="agent-architect",
        topic="database.schema",
        predicate="is_migrated",
        truth_value=EpistemicTruthValue.TRUE,
        confidence=0.95,
        evidence_span_id="span-12345",
        metadata={"source": "unit_test"},
    )
    d = claim.to_dict()
    assert d["claim_id"] == "prop-test-01"
    assert d["agent_id"] == "agent-architect"
    assert d["truth_value"] == "TRUE"
    assert d["confidence"] == 0.95
    assert d["evidence_span_id"] == "span-12345"

    claim2 = PropositionalClaim.from_dict(d)
    assert claim2.claim_id == claim.claim_id
    assert claim2.truth_value == EpistemicTruthValue.TRUE
    assert claim2.confidence == claim.confidence


def test_epistemic_contradiction_detection():
    kb = EpistemicKnowledgeBase(swarm_id="swarm-epistemic-01")

    # Agent A asserts TRUE
    claim_a, is_cons_a, conf_a = kb.assert_claim(
        agent_id="agent-alpha",
        topic="cache.redis",
        predicate="is_clustered",
        truth_value=EpistemicTruthValue.TRUE,
        confidence=0.9,
    )
    assert is_cons_a is True
    assert conf_a is None
    assert kb.compute_dissonance_score() == 0.0
    assert kb.get_epistemic_status() == EpistemicStatus.CONSISTENT

    # Agent B asserts FALSE (contradiction!)
    claim_b, is_cons_b, conf_b = kb.assert_claim(
        agent_id="agent-beta",
        topic="cache.redis",
        predicate="is_clustered",
        truth_value=EpistemicTruthValue.FALSE,
        confidence=0.8,
    )
    assert is_cons_b is False
    assert conf_b is not None
    assert conf_b.prior_agent_id == "agent-alpha"
    assert conf_b.conflicting_agent_id == "agent-beta"
    assert conf_b.severity == pytest.approx(0.8)
    assert kb.compute_dissonance_score() > 0.0
    assert kb.get_epistemic_status() == EpistemicStatus.DISSONANT


def test_epistemic_agent_self_update():
    kb = EpistemicKnowledgeBase(swarm_id="swarm-epistemic-self")

    # Agent asserts TRUE
    kb.assert_claim(
        agent_id="agent-worker",
        topic="task.worker",
        predicate="is_running",
        truth_value=EpistemicTruthValue.TRUE,
        confidence=0.8,
    )
    # Agent updates its own assertion to FALSE
    claim, is_cons, conf = kb.assert_claim(
        agent_id="agent-worker",
        topic="task.worker",
        predicate="is_running",
        truth_value=EpistemicTruthValue.FALSE,
        confidence=0.85,
    )
    # Self-update must NOT produce conflict
    assert is_cons is True
    assert conf is None
    assert len(kb.claims) == 1
    assert kb.claims[0].truth_value == EpistemicTruthValue.FALSE
    assert kb.compute_dissonance_score() == 0.0


def test_epistemic_mutual_knowledge():
    kb = EpistemicKnowledgeBase(swarm_id="swarm-epistemic-mutual")

    kb.assert_claim(
        agent_id="agent-1",
        topic="network.vpc",
        predicate="is_isolated",
        truth_value=EpistemicTruthValue.TRUE,
        confidence=0.9,
    )
    kb.assert_claim(
        agent_id="agent-2",
        topic="network.vpc",
        predicate="is_isolated",
        truth_value=EpistemicTruthValue.TRUE,
        confidence=0.85,
    )

    mutual = kb.get_mutual_knowledge()
    assert len(mutual) == 1
    assert mutual[0]["topic"] == "network.vpc"
    assert mutual[0]["predicate"] == "is_isolated"
    assert mutual[0]["truth_value"] == "TRUE"
    assert "agent-1" in mutual[0]["affirming_agents"]
    assert "agent-2" in mutual[0]["affirming_agents"]


def test_epistemic_agm_belief_revision_confidence():
    kb = EpistemicKnowledgeBase(swarm_id="swarm-epistemic-agm-conf")

    # Strong assertion from Senior Architect
    kb.assert_claim(
        agent_id="senior-architect",
        topic="auth.method",
        predicate="is_jwt",
        truth_value=EpistemicTruthValue.TRUE,
        confidence=0.95,
    )
    # Weak assertion from Junior Agent
    kb.assert_claim(
        agent_id="junior-agent",
        topic="auth.method",
        predicate="is_jwt",
        truth_value=EpistemicTruthValue.FALSE,
        confidence=0.35,
    )

    assert kb.get_epistemic_status() == EpistemicStatus.DISSONANT

    result = kb.reconcile_beliefs(strategy="CONFIDENCE_ENTRENCHMENT")
    assert result["reconciled_conflicts_count"] == 1
    assert result["retracted_claims_count"] == 1
    assert result["post_reconciliation_dissonance"] == 0.0
    assert result["epistemic_status"] == "CONSISTENT"

    # Verify that only the Senior Architect's claim remains active
    active = [c for c in kb.claims if c.is_active]
    assert len(active) == 1
    assert active[0].agent_id == "senior-architect"
    assert active[0].truth_value == EpistemicTruthValue.TRUE


def test_epistemic_agm_belief_revision_recency():
    kb = EpistemicKnowledgeBase(swarm_id="swarm-epistemic-agm-rec")

    # Old claim
    claim_old, _, _ = kb.assert_claim(
        agent_id="agent-legacy",
        topic="cluster.mode",
        predicate="is_standalone",
        truth_value=EpistemicTruthValue.TRUE,
        confidence=0.9,
    )
    claim_old.asserted_at = 1000.0

    # Newer claim
    claim_new, _, _ = kb.assert_claim(
        agent_id="agent-modern",
        topic="cluster.mode",
        predicate="is_standalone",
        truth_value=EpistemicTruthValue.FALSE,
        confidence=0.8,
    )
    claim_new.asserted_at = 2000.0

    assert kb.get_epistemic_status() == EpistemicStatus.DISSONANT

    result = kb.reconcile_beliefs(strategy="TEMPORAL_RECENCY")
    assert result["reconciled_conflicts_count"] == 1
    assert result["retracted_claims_count"] == 1
    assert result["epistemic_status"] == "CONSISTENT"

    active = [c for c in kb.claims if c.is_active]
    assert len(active) == 1
    assert active[0].agent_id == "agent-modern"
    assert active[0].truth_value == EpistemicTruthValue.FALSE


def test_epistemic_rest_endpoints():
    app = create_app()
    client = TestClient(app)
    swarm_id = "swarm-rest-epistemic-test"

    # Reset
    resp = client.post(f"/api/v1/swarms/{swarm_id}/epistemic/reset")
    assert resp.status_code == 200

    # Assert 1
    resp = client.post(
        f"/api/v1/swarms/{swarm_id}/epistemic/assert",
        json={
            "agent_id": "agent-lead",
            "topic": "db.driver",
            "predicate": "supports_async",
            "truth_value": "TRUE",
            "confidence": 0.95,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "asserted"
    assert data["is_consistent"] is True

    # Assert 2 (Conflicting)
    resp = client.post(
        f"/api/v1/swarms/{swarm_id}/epistemic/assert",
        json={
            "agent_id": "agent-sub",
            "topic": "db.driver",
            "predicate": "supports_async",
            "truth_value": "FALSE",
            "confidence": 0.70,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["is_consistent"] is False
    assert data["conflict"] is not None

    # Status
    resp = client.get(f"/api/v1/swarms/{swarm_id}/epistemic/status")
    assert resp.status_code == 200
    st = resp.json()
    assert st["status"] == "DISSONANT"
    assert st["unresolved_conflicts_count"] == 1
    assert st["total_propositions"] == 2

    # Propositions
    resp = client.get(f"/api/v1/swarms/{swarm_id}/epistemic/propositions")
    assert resp.status_code == 200
    assert resp.json()["propositions_count"] == 2

    # Conflicts
    resp = client.get(f"/api/v1/swarms/{swarm_id}/epistemic/conflicts")
    assert resp.status_code == 200
    assert resp.json()["conflicts_count"] == 1

    # Reconcile
    resp = client.post(
        f"/api/v1/swarms/{swarm_id}/epistemic/reconcile",
        json={"strategy": "CONFIDENCE_ENTRENCHMENT"},
    )
    assert resp.status_code == 200
    rec = resp.json()
    assert rec["reconciled_conflicts_count"] == 1
    assert rec["epistemic_status"] == "CONSISTENT"

    # Status after reconcile
    resp = client.get(f"/api/v1/swarms/{swarm_id}/epistemic/status")
    assert resp.status_code == 200
    st2 = resp.json()
    assert st2["status"] == "CONSISTENT"
    assert st2["dissonance_score"] == 0.0


def test_epistemic_cli_in_process(capsys):
    test_swarm = "swarm-cli-test-01"

    # Reset
    with pytest.raises(SystemExit) as exc:
        main(["epistemic", "reset", "--swarm-id", test_swarm])
    assert exc.value.code == 0

    # Assert claim
    with pytest.raises(SystemExit) as exc:
        main([
            "epistemic",
            "assert",
            "--swarm-id", test_swarm,
            "--agent", "agent-x",
            "--topic", "service.health",
            "--predicate", "is_healthy",
            "--truth-value", "TRUE",
            "--confidence", "0.9",
        ])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "SWARMWATCH EPISTEMIC ASSERTION" in captured.out
    assert "CONSISTENT" in captured.out

    # Status
    with pytest.raises(SystemExit) as exc:
        main(["epistemic", "status", "--swarm-id", test_swarm, "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["status"] == "CONSISTENT"
    assert data["total_propositions"] == 1
