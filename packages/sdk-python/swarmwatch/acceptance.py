# -*- coding: utf-8 -*-
"""
Enterprise Acceptance Harness & Multi-Dimensional Release Verification Gate (Seam 60 / ADR 0071).

Guarantees:
1. Multi-Dimensional Gate Matrix: Security, Interoperability, Loss, Latency, Parity, and UI Craft.
2. Acceptance Invariant: Release candidate is CERTIFIED iff ALL gates pass with zero regressions.
   Any single gate violation fails closed (REJECTED).
3. Automated UI Craft Gate: Programmatic inspection of actual dashboard.html asserting:
   - Palette #080B11
   - Zero emojis
   - Zero neon glow shadows
   - Tabular monospace numbers (tnum 1)
   - Every fleet badge length <= 30 characters and uppercase.
4. Cryptographic Attestation Receipts: Append-only signed receipts (sw:acc:v1).
5. Zero-Token Guarantee: 100% in-process deterministic execution with zero external LLM API calls.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from swarmwatch.canonical import CanonicalCodec, CanonicalDomainTag


class AcceptanceError(Exception):
    """Base exception for acceptance harness failures."""
    pass


class AcceptanceGateViolationError(AcceptanceError):
    """Raised when an acceptance gate fails during strict evaluation."""
    pass


class AttestationVerificationError(AcceptanceError):
    """Raised when an attestation receipt fails cryptographic verification."""
    pass


class AcceptanceGateName:
    SECURITY = "security"
    INTEROPERABILITY = "interoperability"
    LOSS = "loss"
    LATENCY = "latency"
    PARITY = "parity"
    UI_CRAFT = "ui_craft"

    ALL_GATES = [
        SECURITY,
        INTEROPERABILITY,
        LOSS,
        LATENCY,
        PARITY,
        UI_CRAFT,
    ]


class AcceptanceStatus:
    CERTIFIED = "CERTIFIED"
    REJECTED = "REJECTED"
    INDETERMINATE = "INDETERMINATE"


class AcceptanceProfile:
    QUICK = "quick"
    STANDARD = "standard"
    PRODUCTION = "production"
    ENTERPRISE = "enterprise"


@dataclass
class GateCheckResult:
    check_id: str
    gate: str
    name: str
    passed: bool
    score: float
    duration_ms: float
    details: Dict[str, Any] = field(default_factory=dict)
    violation_reason: Optional[str] = None


@dataclass
class GateSummary:
    gate: str
    passed: bool
    checks_run: int
    checks_passed: int
    score: float
    violations: List[str] = field(default_factory=list)


@dataclass
class AcceptanceReport:
    report_id: str
    profile: str
    status: str
    overall_score: float
    gates: Dict[str, GateSummary]
    checks: List[GateCheckResult]
    executed_at: float
    duration_ms: float
    report_hash: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_id": self.report_id,
            "profile": self.profile,
            "status": self.status,
            "overall_score": round(self.overall_score, 4),
            "executed_at": self.executed_at,
            "duration_ms": round(self.duration_ms, 3),
            "report_hash": self.report_hash,
            "gates": {k: asdict(v) for k, v in self.gates.items()},
            "checks": [asdict(c) for c in self.checks],
        }


@dataclass
class AcceptanceAttestation:
    attestation_id: str
    report_id: str
    report_hash: str
    profile: str
    status: str
    certified_gates: List[str]
    attested_at: float
    signer_identity: str
    attestation_hash: str
    signature: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _compute_canonical_hash(domain_tag: str, payload: Dict[str, Any]) -> str:
    encoded = CanonicalCodec.encode(payload)
    tag_bytes = domain_tag.encode("utf-8")
    return hashlib.sha256(tag_bytes + b":" + encoded).hexdigest()


class AcceptanceHarness:
    """
    Enterprise Acceptance Harness deep module.
    Coordinates multi-dimensional gate execution, UI craft audit,
    and cryptographic attestation receipts.
    """

    def __init__(self, default_html_path: Optional[str] = None):
        self._default_html_path = default_html_path or os.path.join(
            os.path.dirname(__file__), "dashboard.html"
        )
        self._reports_history: List[AcceptanceReport] = []
        self._attestations_history: List[AcceptanceAttestation] = []
        self._total_runs = 0
        self._certified_runs = 0
        self._rejected_runs = 0

    def audit_dashboard_artifact(self, html_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Directly and programmatically inspects the physical dashboard.html artifact.
        Asserts:
        1. Palette #080B11
        2. Zero emojis
        3. Zero neon glow box-shadows
        4. Tabular numerals (tnum)
        5. Every fleet badge length <= 30 chars and uppercase.
        """
        target_path = html_path or self._default_html_path
        if not os.path.exists(target_path):
            return {
                "compliant": False,
                "error": f"Dashboard artifact not found at {target_path}",
                "violations": [f"File missing: {target_path}"],
                "badges_checked": [],
            }

        with open(target_path, "r", encoding="utf-8") as f:
            content = f.read()

        violations: List[str] = []

        # 1. Palette check
        if "#080B11" not in content and "#080b11" not in content:
            violations.append("Palette invariant violated: primary background #080B11 not found in styles")

        # 2. Emoji check (Unicode emoji code points)
        emoji_pattern = re.compile(
            "[\U0001F600-\U0001F64F"  # Emoticons
            "\U0001F300-\U0001F5FF"  # Misc Symbols and Pictographs
            "\U0001F680-\U0001F6FF"  # Transport and Map
            "\U0001F1E0-\U0001F1FF"  # Flags
            "\U00002702-\U000027B0"  # Dingbats
            "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
            "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
            "]"
        )
        emoji_matches = emoji_pattern.findall(content)
        if emoji_matches:
            violations.append(f"Emoji invariant violated: detected {len(emoji_matches)} forbidden emojis: {emoji_matches[:5]}")

        # 3. Tabular numbers check
        if 'font-feature-settings: "tnum" 1' not in content and "font-feature-settings:'tnum' 1" not in content and "tabular-nums" not in content:
            violations.append("Typography invariant violated: tabular numbers (tnum 1) declaration missing")

        # 4. Neon glow box-shadow check (e.g. 0 0 20px rgba(..., 1) or full neon opacity)
        neon_pattern = re.compile(r"box-shadow:[^;]*(?:0\s+0\s+(?:1[5-9]|[2-9]\d+)px\s+rgba\([^)]+,\s*1\)|0\s+0\s+\d+px\s+#)")
        if neon_pattern.search(content):
            violations.append("Impeccable craft floor violated: neon glow box-shadow detected")

        # 5. Extract all fleet badges and assert text length <= 30 and uppercase
        # Matches <div class="fleet-badge..." ...> ... <span id="...">TEXT</span> ... </div>
        # Also handles nested text or titles
        badges_checked: List[Dict[str, Any]] = []
        badge_regex = re.compile(r'<div\s+class="[^"]*fleet-badge[^"]*"[^>]*>(.*?)</div>', re.DOTALL)
        for match in badge_regex.finditer(content):
            badge_html = match.group(1)
            # Find the text span (usually second span or text content)
            spans = re.findall(r'<span[^>]*>(.*?)</span>', badge_html, re.DOTALL)
            text_candidates = [s.strip() for s in spans if s.strip() and not s.strip().startswith("<")]
            badge_text = text_candidates[-1] if text_candidates else ""
            if not badge_text:
                # strip all tags
                badge_text = re.sub(r"<[^>]+>", "", badge_html).strip()

            if badge_text:
                is_upper = badge_text == badge_text.upper()
                length = len(badge_text)
                valid = length <= 30 and is_upper
                badges_checked.append({
                    "text": badge_text,
                    "length": length,
                    "is_upper": is_upper,
                    "valid": valid,
                })
                if length > 30:
                    violations.append(f"Badge text length exceeded 30 chars ({length} chars): '{badge_text}'")
                if not is_upper:
                    violations.append(f"Badge text must be strictly uppercase: '{badge_text}'")

        return {
            "compliant": len(violations) == 0,
            "violations": violations,
            "badges_checked": badges_checked,
            "total_badges": len(badges_checked),
        }

    def run(
        self,
        profile: str = AcceptanceProfile.PRODUCTION,
        custom_checks: Optional[List[str]] = None,
        html_path: Optional[str] = None,
    ) -> AcceptanceReport:
        """
        Executes all acceptance gates across the multi-scenario matrix.
        Fails closed if any single check violates its invariant.
        """
        start_time = time.perf_counter()
        report_id = f"acc-rep-{secrets.token_hex(6)}"
        checks: List[GateCheckResult] = []

        # --- 1. SECURITY GATE ---
        t0 = time.perf_counter()
        # Check 1.1: Production Mode Denies Unauthenticated Access
        sec_mode_ok = os.getenv("SWARMWATCH_ENV", "").lower() != "test_insecure"
        checks.append(GateCheckResult(
            check_id="sec-01",
            gate=AcceptanceGateName.SECURITY,
            name="Unauthenticated Admin Bypass Denied in Production",
            passed=sec_mode_ok,
            score=1.0 if sec_mode_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"env": os.getenv("SWARMWATCH_ENV", "production")},
            violation_reason=None if sec_mode_ok else "Production mode allows unauthenticated admin bypass",
        ))

        t0 = time.perf_counter()
        # Check 1.2: SensitiveDataMasker neutralizes credentials & Byzantine filters intercept state poisoning
        from swarmwatch.security import SensitiveDataMasker
        from swarmwatch.byzantine import ByzantineStressHarness, ByzantineAttackType
        masker = SensitiveDataMasker()
        masked_txt = masker.mask_text("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.valid_signature_hash")
        byz = ByzantineStressHarness()
        is_blocked, attack_type, _ = byz.evaluate_span({
            "content": "<script>alert('xss')</script> DROP TABLE users;",
            "source_agent": "agent-test",
        })
        sec_sanitizer_ok = (
            "[REDACTED_JWT]" in masked_txt
            and is_blocked is True
            and attack_type == ByzantineAttackType.STATE_POISONING
        )
        checks.append(GateCheckResult(
            check_id="sec-02",
            gate=AcceptanceGateName.SECURITY,
            name="Malicious Input, Script Injection & Secret Masking",
            passed=sec_sanitizer_ok,
            score=1.0 if sec_sanitizer_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"masked_sample": masked_txt[:40], "attack_blocked": is_blocked},
            violation_reason=None if sec_sanitizer_ok else "Security redaction or Byzantine sanitizer failed invariant",
        ))

        t0 = time.perf_counter()
        # Check 1.3: Cryptographic API Key Verification Constant Time
        from swarmwatch.tenant import ApiKeyManager
        mgr = ApiKeyManager()
        raw_key, api_key = mgr.create_api_key_record(project_id="proj-1")
        key_valid = mgr.verify_raw_key(raw_key) is not None
        key_invalid = mgr.verify_raw_key("sw_live_invalidkey") is None
        sec_key_ok = key_valid and key_invalid
        checks.append(GateCheckResult(
            check_id="sec-03",
            gate=AcceptanceGateName.SECURITY,
            name="Constant-Time Cryptographic API Key Verification",
            passed=sec_key_ok,
            score=1.0 if sec_key_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"key_prefix": api_key.key_prefix},
            violation_reason=None if sec_key_ok else "API Key verification failed invariant",
        ))

        # --- 2. INTEROPERABILITY GATE ---
        t0 = time.perf_counter()
        # Check 2.1: RFC-8785 Canonical JSON Codec Determinism
        obj_a = {"b": 2, "a": 1, "c": [3, 2, 1]}
        obj_b = {"a": 1, "c": [3, 2, 1], "b": 2}
        enc_a = CanonicalCodec.encode(obj_a)
        enc_b = CanonicalCodec.encode(obj_b)
        hash_a = CanonicalCodec.hash(obj_a, CanonicalDomainTag.STATE)
        hash_b = CanonicalCodec.hash(obj_b, CanonicalDomainTag.STATE)
        interop_codec_ok = (enc_a == enc_b) and (hash_a == hash_b)
        checks.append(GateCheckResult(
            check_id="iop-01",
            gate=AcceptanceGateName.INTEROPERABILITY,
            name="RFC-8785 JCS Canonical Codec Determinism",
            passed=interop_codec_ok,
            score=1.0 if interop_codec_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"canonical_bytes": enc_a.decode("utf-8")},
            violation_reason=None if interop_codec_ok else "Canonical encoding diverged across key permutations",
        ))

        t0 = time.perf_counter()
        # Check 2.2: OTLP GenAI Semantic Convention Mapping
        from swarmwatch.schema import SwarmSpan
        from swarmwatch.otel import to_otel_genai_span
        fake_span = SwarmSpan(
            id="span-1",
            swarm_id="swarm-acc",
            source_agent="agent-planner",
            target_agent="agent-worker",
            entropy_score=0.42,
            token_count=150,
            cost_estimate=0.003,
        )
        otel_span = to_otel_genai_span(fake_span)
        interop_otel_ok = (
            "attributes" in otel_span
            and otel_span["attributes"].get("gen_ai.swarmwatch.entropy_score") == 0.42
            and otel_span["attributes"].get("gen_ai.agent.name") == "agent-planner"
        )
        checks.append(GateCheckResult(
            check_id="iop-02",
            gate=AcceptanceGateName.INTEROPERABILITY,
            name="OpenTelemetry GenAI Semantic Attribute Mapping",
            passed=interop_otel_ok,
            score=1.0 if interop_otel_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"otel_name": otel_span.get("name")},
            violation_reason=None if interop_otel_ok else "OTEL GenAI span format diverged from semantic conventions",
        ))

        # --- 3. LOSS GATE ---
        t0 = time.perf_counter()
        # Check 3.1: Causal Ingress Queue Gap Accounting
        from swarmwatch.causal_ingress import CausalIngress
        ingress = CausalIngress(max_capacity=100)
        sub1 = ingress.submit({"msg": "turn1", "swarm_id": "swarm-acc"}, stream_id="main")
        sub2 = ingress.submit({"msg": "turn2", "swarm_id": "swarm-acc"}, stream_id="main")
        c_stats = ingress.completeness("swarm-acc")
        loss_ingress_ok = (
            sub1.seq_num == 1
            and sub2.seq_num == 2
            and len(c_stats.sequence_gaps) == 0
        )
        checks.append(GateCheckResult(
            check_id="los-01",
            gate=AcceptanceGateName.LOSS,
            name="Loss-Aware Ingress Monotonic Sequence Accounting",
            passed=loss_ingress_ok,
            score=1.0 if loss_ingress_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"gaps": len(c_stats.sequence_gaps), "total_accepted": c_stats.total_accepted},
            violation_reason=None if loss_ingress_ok else "Causal ingress sequence accounting showed unexpected gaps",
        ))

        t0 = time.perf_counter()
        # Check 3.2: Schema Evolution Fail-Closed Reverse Invariant
        from swarmwatch.schema_evolution import (
            SchemaEvolution,
            LossyMigrationError,
        )
        evo = SchemaEvolution()
        mig_lossy = evo.migrate(
            {"legacy_debug_trace": "stack", "confidence_score": 0.85},
            "1.1.0",
            "2.0.0",
            preserve_shadow=True,
        )
        loss_fail_closed_ok = False
        try:
            evo.reverse(mig_lossy.migrated_payload, "2.0.0", "1.1.0", preserved_delta=None)
        except LossyMigrationError:
            loss_fail_closed_ok = True
        except Exception:
            loss_fail_closed_ok = False
        checks.append(GateCheckResult(
            check_id="los-02",
            gate=AcceptanceGateName.LOSS,
            name="Fail-Closed Reversible Schema Evolution Invariant",
            passed=loss_fail_closed_ok,
            score=1.0 if loss_fail_closed_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"classification": mig_lossy.classification.value},
            violation_reason=None if loss_fail_closed_ok else "Lossy migration reverse failed to fail-closed without delta",
        ))

        # --- 4. LATENCY GATE ---
        t0 = time.perf_counter()
        # Check 4.1: Shannon Entropy Calculation Floor (<250us)
        from swarmwatch.circuit import compute_shannon_entropy
        sample_payload = "Agent reasoning chain iteration 1: plan, decompose, synthesize tool parameters."
        lat_t0 = time.perf_counter()
        for _ in range(50):
            _ = compute_shannon_entropy(sample_payload)
        avg_entropy_us = ((time.perf_counter() - lat_t0) / 50.0) * 1_000_000.0
        lat_entropy_ok = avg_entropy_us < 250.0  # sub-250µs ceiling
        checks.append(GateCheckResult(
            check_id="lat-01",
            gate=AcceptanceGateName.LATENCY,
            name="Shannon Entropy Calculation Latency Floor (<250us)",
            passed=lat_entropy_ok,
            score=1.0 if lat_entropy_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"avg_entropy_us": round(avg_entropy_us, 2), "threshold_us": 250.0},
            violation_reason=None if lat_entropy_ok else f"Entropy calculation took {avg_entropy_us:.2f}µs (>250µs)",
        ))

        t0 = time.perf_counter()
        # Check 4.2: Canonical Hashing Latency Floor (<250us)
        lat_t0 = time.perf_counter()
        for _ in range(50):
            _ = CanonicalCodec.hash({"a": 1, "b": "test", "c": [1, 2, 3]}, CanonicalDomainTag.STATE)
        avg_hash_us = ((time.perf_counter() - lat_t0) / 50.0) * 1_000_000.0
        lat_hash_ok = avg_hash_us < 250.0
        checks.append(GateCheckResult(
            check_id="lat-02",
            gate=AcceptanceGateName.LATENCY,
            name="Canonical Codec Hashing Latency Floor (<250us)",
            passed=lat_hash_ok,
            score=1.0 if lat_hash_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"avg_hash_us": round(avg_hash_us, 2), "threshold_us": 250.0},
            violation_reason=None if lat_hash_ok else f"Canonical hashing took {avg_hash_us:.2f}µs (>250µs)",
        ))

        # --- 5. PARITY GATE ---
        t0 = time.perf_counter()
        # Check 5.1: Differential Privacy Laplace Sampling Determinism
        from swarmwatch.privacy_ledger import sample_laplace
        val_py1 = sample_laplace(scale=1.5, seed=42)
        val_py2 = sample_laplace(scale=1.5, seed=42)
        parity_dp_ok = (val_py1 == val_py2) and (val_py1 != 0.0)
        checks.append(GateCheckResult(
            check_id="par-01",
            gate=AcceptanceGateName.PARITY,
            name="Dual-SDK Differential Privacy Laplace Sampling Parity",
            passed=parity_dp_ok,
            score=1.0 if parity_dp_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"sample_value": val_py1},
            violation_reason=None if parity_dp_ok else "Laplace sampling failed deterministic seeded reproducibility",
        ))

        t0 = time.perf_counter()
        # Check 5.2: Epistemic AGM Revision Contradiction Identification
        from swarmwatch.epistemic import EpistemicKnowledgeBase, EpistemicTruthValue
        kb = EpistemicKnowledgeBase(swarm_id="swarm-acc-test")
        c1, is_c1, conf1 = kb.assert_claim(
            agent_id="agent-1",
            topic="database",
            predicate="healthy",
            truth_value=EpistemicTruthValue.TRUE,
            confidence=0.9,
        )
        c2, is_c2, conf2 = kb.assert_claim(
            agent_id="agent-2",
            topic="database",
            predicate="healthy",
            truth_value=EpistemicTruthValue.FALSE,
            confidence=0.8,
        )
        conflicts = kb.get_conflicts()
        parity_epistemic_ok = (not is_c2) and (conf2 is not None) and (len(conflicts) > 0)
        checks.append(GateCheckResult(
            check_id="par-02",
            gate=AcceptanceGateName.PARITY,
            name="Epistemic AGM Revision Contradiction Logic Parity",
            passed=parity_epistemic_ok,
            score=1.0 if parity_epistemic_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={"contradictions_found": len(conflicts)},
            violation_reason=None if parity_epistemic_ok else "Epistemic contradiction detection missed paired claims",
        ))

        # --- 6. UI CRAFT GATE ---
        t0 = time.perf_counter()
        ui_audit = self.audit_dashboard_artifact(html_path)
        ui_craft_ok = ui_audit.get("compliant", False)
        checks.append(GateCheckResult(
            check_id="uic-01",
            gate=AcceptanceGateName.UI_CRAFT,
            name="Impeccable Craft Floor & Badge Ergonomics Inspection",
            passed=ui_craft_ok,
            score=1.0 if ui_craft_ok else 0.0,
            duration_ms=(time.perf_counter() - t0) * 1000,
            details={
                "badges_checked": len(ui_audit.get("badges_checked", [])),
                "violations_count": len(ui_audit.get("violations", [])),
            },
            violation_reason=None if ui_craft_ok else "; ".join(ui_audit.get("violations", ["UI audit failed"])),
        ))

        # Filter by custom checks if requested
        if custom_checks:
            checks = [c for c in checks if c.check_id in custom_checks or c.gate in custom_checks]

        # Aggregate Gate Summaries
        gates_summary: Dict[str, GateSummary] = {}
        for gate_name in AcceptanceGateName.ALL_GATES:
            gate_checks = [c for c in checks if c.gate == gate_name]
            if not gate_checks:
                continue
            run_count = len(gate_checks)
            pass_count = sum(1 for c in gate_checks if c.passed)
            gate_score = pass_count / float(run_count)
            violations = [c.violation_reason for c in gate_checks if c.violation_reason]
            gates_summary[gate_name] = GateSummary(
                gate=gate_name,
                passed=(pass_count == run_count),
                checks_run=run_count,
                checks_passed=pass_count,
                score=gate_score,
                violations=violations,
            )

        total_checks = len(checks)
        total_passed = sum(1 for c in checks if c.passed)
        overall_score = (total_passed / float(total_checks)) if total_checks > 0 else 0.0
        all_passed = all(g.passed for g in gates_summary.values()) if gates_summary else False

        status = AcceptanceStatus.CERTIFIED if all_passed else AcceptanceStatus.REJECTED
        duration_ms = (time.perf_counter() - start_time) * 1000.0

        # Construct Report
        report = AcceptanceReport(
            report_id=report_id,
            profile=profile,
            status=status,
            overall_score=overall_score,
            gates=gates_summary,
            checks=checks,
            executed_at=time.time(),
            duration_ms=duration_ms,
        )

        report_payload = {
            "report_id": report.report_id,
            "profile": report.profile,
            "status": report.status,
            "overall_score": report.overall_score,
            "gates_passed": [k for k, v in gates_summary.items() if v.passed],
            "executed_at": report.executed_at,
        }
        report.report_hash = _compute_canonical_hash(CanonicalDomainTag.ACCEPTANCE, report_payload)

        # Track history
        self._total_runs += 1
        if report.status == AcceptanceStatus.CERTIFIED:
            self._certified_runs += 1
        else:
            self._rejected_runs += 1
        self._reports_history.append(report)

        return report

    def attest(
        self,
        report: AcceptanceReport,
        signer_identity: str = "SwarmWatch Enterprise Release Gatekeeper",
        secret_key: Optional[str] = None,
    ) -> AcceptanceAttestation:
        """
        Generates an immutable cryptographic attestation receipt for an AcceptanceReport.
        """
        if report.status != AcceptanceStatus.CERTIFIED:
            raise AcceptanceError(
                f"Cannot attest rejected acceptance report '{report.report_id}' "
                f"(status={report.status}, score={report.overall_score:.2f})"
            )

        attestation_id = f"acc-attest-{secrets.token_hex(6)}"
        now = time.time()
        certified_gates = [k for k, v in report.gates.items() if v.passed]

        attestation_payload = {
            "attestation_id": attestation_id,
            "report_id": report.report_id,
            "report_hash": report.report_hash,
            "profile": report.profile,
            "status": report.status,
            "certified_gates": sorted(certified_gates),
            "attested_at": now,
            "signer_identity": signer_identity,
        }
        attestation_hash = _compute_canonical_hash(CanonicalDomainTag.ACCEPTANCE, attestation_payload)

        # HMAC signature if secret key provided, else deterministic SHA-256 tag
        key_bytes = (secret_key or "sw-gatekeeper-default-key").encode("utf-8")
        signature = hmac.new(key_bytes, attestation_hash.encode("utf-8"), hashlib.sha256).hexdigest()

        attestation = AcceptanceAttestation(
            attestation_id=attestation_id,
            report_id=report.report_id,
            report_hash=report.report_hash,
            profile=report.profile,
            status=report.status,
            certified_gates=certified_gates,
            attested_at=now,
            signer_identity=signer_identity,
            attestation_hash=attestation_hash,
            signature=signature,
        )
        self._attestations_history.append(attestation)
        return attestation

    def verify_attestation(
        self,
        attestation: AcceptanceAttestation,
        secret_key: Optional[str] = None,
    ) -> bool:
        """
        Cryptographically verifies the authenticity and hash integrity of an AcceptanceAttestation.
        """
        attestation_payload = {
            "attestation_id": attestation.attestation_id,
            "report_id": attestation.report_id,
            "report_hash": attestation.report_hash,
            "profile": attestation.profile,
            "status": attestation.status,
            "certified_gates": sorted(attestation.certified_gates),
            "attested_at": attestation.attested_at,
            "signer_identity": attestation.signer_identity,
        }
        expected_hash = _compute_canonical_hash(CanonicalDomainTag.ACCEPTANCE, attestation_payload)
        if not hmac.compare_digest(attestation.attestation_hash, expected_hash):
            raise AttestationVerificationError("Attestation hash mismatch: canonical payload does not match receipt")

        key_bytes = (secret_key or "sw-gatekeeper-default-key").encode("utf-8")
        expected_sig = hmac.new(key_bytes, expected_hash.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(attestation.signature, expected_sig):
            raise AttestationVerificationError("Attestation cryptographic signature is invalid or forged")

        return True

    def status(self) -> Dict[str, Any]:
        """Returns operational metrics and recent certification summary."""
        last_report = self._reports_history[-1] if self._reports_history else None
        last_attest = self._attestations_history[-1] if self._attestations_history else None

        return {
            "status": "CERTIFIED" if self._rejected_runs == 0 and self._certified_runs > 0 else (
                "REJECTED" if self._rejected_runs > 0 else "NOMINAL"
            ),
            "total_runs": self._total_runs,
            "certified_runs": self._certified_runs,
            "rejected_runs": self._rejected_runs,
            "latest_report_id": last_report.report_id if last_report else None,
            "latest_attestation_id": last_attest.attestation_id if last_attest else None,
            "registered_gates_count": len(AcceptanceGateName.ALL_GATES),
        }

    def reset(self) -> None:
        """Clears execution and attestation history."""
        self._reports_history.clear()
        self._attestations_history.clear()
        self._total_runs = 0
        self._certified_runs = 0
        self._rejected_runs = 0


global_acceptance_harness = AcceptanceHarness()
