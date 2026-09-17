# -*- coding: utf-8 -*-
"""
SwarmWatch Incident Post-Mortem & Financial ROI Report Generator (Seam 11 / ADR 0020).

Provides deterministic, zero-token forensic analysis over swarm executions and circuit breaker halts,
computing counterfactual dollar/token savings, root-cause attribution, entropy trajectories,
and credential neutralization audits.
"""

from datetime import datetime, timezone
import json
import re
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from pydantic import BaseModel, Field

from swarmwatch.schema import (
    CircuitEvent,
    CounterfactualSavings,
    SwarmSpan,
)
from swarmwatch.storage import StorageAdapter


class FinancialRoiMetrics(BaseModel):
    """Quantitative calculation of compute dollars and token expenditure prevented by circuit breaking."""
    dollars_saved: float = Field(default=0.0, description="Counterfactual dollar spend avoided")
    tokens_saved: int = Field(default=0, description="Counterfactual tokens prevented from runaway execution")
    turns_prevented: int = Field(default=0, description="Number of runaway turns halted")
    tokens_used: int = Field(default=0, description="Actual tokens consumed prior to intervention")


class RootCauseDiagnosis(BaseModel):
    """Attribution of failure mode, causal agent pair, and trigger turn responsible for circuit intervention."""
    trigger_reason: str = Field(..., description="Explanation of why circuit broke")
    source_agent: str = Field(..., description="Agent sending triggering turn")
    target_agent: str = Field(..., description="Agent receiving triggering turn")
    trigger_span_id: str = Field(..., description="Span ID where breaker intervened")
    trigger_entropy: Optional[float] = Field(default=None, description="Normalized Shannon entropy at trip")
    trigger_similarity: Optional[float] = Field(default=None, description="Jaccard similarity at trip")


class EntropyTrajectoryPoint(BaseModel):
    """Time-series sample of lexical entropy and similarity for a single conversational turn."""
    step_index: int
    source_agent: str
    target_agent: str
    entropy_score: Optional[float] = None
    similarity_score: Optional[float] = None
    tokens: int = 0


class SecurityRedactionSummary(BaseModel):
    """Categorized tally of sensitive authentication tokens and PII neutralized during swarm execution."""
    total_redacted: int = 0
    redacted_categories: Dict[str, int] = Field(default_factory=dict)


class IncidentPostMortem(BaseModel):
    """Structured forensic audit document generated upon swarm completion or circuit trip."""
    swarm_id: str
    status: str = Field(default="COMPLETED_SUCCESSFULLY", description="Execution status")
    circuit_tripped: bool = False
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: float = 0.0
    total_spans: int = 0
    total_tokens: int = 0
    roi: FinancialRoiMetrics = Field(default_factory=FinancialRoiMetrics)
    root_cause: Optional[RootCauseDiagnosis] = None
    entropy_trajectory: List[EntropyTrajectoryPoint] = Field(default_factory=list)
    security_summary: SecurityRedactionSummary = Field(default_factory=SecurityRedactionSummary)
    recommendations: List[str] = Field(default_factory=list)


class IncidentReportGenerator:
    """
    Deep module synthesizing forensic incident reports and financial ROI audits
    for multi-agent swarms without remote LLM or cloud dependencies.
    """

    REDACTION_PATTERN = re.compile(r"\[REDACTED(?:_([A-Z_]+))?\]")

    def __init__(self, storage: StorageAdapter):
        self.storage = storage

    def analyze(self, swarm_id: str) -> Optional[IncidentPostMortem]:
        """Performs deterministic forensic analysis for a target swarm session."""
        if hasattr(self.storage, "get_spans"):
            spans = self.storage.get_spans(swarm_id=swarm_id)
        elif hasattr(self.storage, "query_spans"):
            spans = self.storage.query_spans(swarm_id=swarm_id)
        elif hasattr(self.storage, "get_swarm_spans_sync"):
            spans = self.storage.get_swarm_spans_sync(swarm_id=swarm_id)
        else:
            spans = []

        if not spans:
            return None

        def get_span_time(s: Any) -> datetime:
            t = getattr(s, "start_time", None)
            if isinstance(t, datetime):
                return t
            if isinstance(t, str):
                try:
                    return datetime.fromisoformat(t.replace("Z", "+00:00"))
                except Exception:
                    pass
            return datetime.now(timezone.utc)

        def get_span_tokens(s: Any) -> int:
            return (
                getattr(s, "total_tokens", 0)
                or getattr(s, "token_count", 0)
                or ((getattr(s, "tokens_in", 0) or 0) + (getattr(s, "tokens_out", 0) or 0))
            )

        # Sort spans chronologically
        spans.sort(key=lambda s: (getattr(s, "step_index", 0), get_span_time(s)))

        if hasattr(self.storage, "get_circuit_events_sync"):
            circuit_events = self.storage.get_circuit_events_sync(swarm_id=swarm_id)
        elif hasattr(self.storage, "get_circuit_events"):
            events_res = self.storage.get_circuit_events(swarm_id=swarm_id)
            circuit_events = events_res if isinstance(events_res, list) else []
        else:
            circuit_events = []

        tripped_event: Optional[CircuitEvent] = None
        for evt in circuit_events:
            if getattr(evt, "event_type", "").upper() in ("TRIP", "HALT", "BREACH"):
                tripped_event = evt
                break

        total_tokens = sum(get_span_tokens(s) for s in spans)

        t0 = get_span_time(spans[0])
        t1 = get_span_time(spans[-1])
        duration_sec = max(0.0, (t1 - t0).total_seconds())
        start_time_iso = t0.isoformat()
        end_time_iso = t1.isoformat()

        # Build trajectory
        trajectory: List[EntropyTrajectoryPoint] = []
        for idx, s in enumerate(spans):
            step = getattr(s, "step_index", idx + 1)
            trajectory.append(
                EntropyTrajectoryPoint(
                    step_index=step,
                    source_agent=s.source_agent,
                    target_agent=s.target_agent,
                    entropy_score=getattr(s, "entropy_score", None),
                    similarity_score=getattr(s, "similarity_score", None),
                    tokens=get_span_tokens(s),
                )
            )

        # Tally security redactions
        total_redacted = 0
        cat_counts: Dict[str, int] = {}
        for s in spans:
            payload_str = json.dumps(getattr(s, "input_payload", {}) or {}) + " " + json.dumps(getattr(s, "output_payload", {}) or {})
            for match in self.REDACTION_PATTERN.finditer(payload_str):
                total_redacted += 1
                cat = match.group(1) or "CREDENTIAL"
                cat_counts[cat] = cat_counts.get(cat, 0) + 1

        security_summary = SecurityRedactionSummary(
            total_redacted=total_redacted,
            redacted_categories=cat_counts,
        )

        # Evaluate circuit status & ROI
        circuit_tripped = tripped_event is not None
        status = "HALTED_BY_CIRCUIT_BREAKER" if circuit_tripped else "COMPLETED_SUCCESSFULLY"

        roi = FinancialRoiMetrics(tokens_used=total_tokens)
        root_cause: Optional[RootCauseDiagnosis] = None
        recommendations: List[str] = []

        if circuit_tripped and tripped_event:
            savings = getattr(tripped_event, "counterfactual_savings", None)
            if savings:
                roi.dollars_saved = getattr(savings, "dollars_saved", 0.0)
                roi.tokens_saved = getattr(savings, "tokens_saved", 0)
                roi.turns_prevented = getattr(savings, "turns_prevented", 0) or getattr(savings, "turns_averted", 0)

            # Find triggering span
            trigger_span = next(
                (s for s in spans if getattr(s, "span_id", s.id) == tripped_event.span_id or s.id == tripped_event.span_id),
                spans[-1]
            )
            trigger_span_id = getattr(trigger_span, "span_id", trigger_span.id)
            root_cause = RootCauseDiagnosis(
                trigger_reason=tripped_event.reason,
                source_agent=trigger_span.source_agent,
                target_agent=trigger_span.target_agent,
                trigger_span_id=trigger_span_id,
                trigger_entropy=getattr(trigger_span, "entropy_score", None),
                trigger_similarity=getattr(trigger_span, "similarity_score", None),
            )

            # Recommendations based on failure mode
            reason_lower = tripped_event.reason.lower()
            if "entropy" in reason_lower:
                recommendations.append(
                    f"Agent dialogue between '{root_cause.source_agent}' and '{root_cause.target_agent}' "
                    f"collapsed in vocabulary diversity (H_norm={root_cause.trigger_entropy or 0.0:.2f}). "
                    f"Consider increasing temperature or applying system prompt diversity guardrails."
                )
            elif "recursion" in reason_lower or "tool" in reason_lower:
                recommendations.append(
                    f"Tool invocation loop detected for '{root_cause.target_agent}'. "
                    f"Add exponential backoff or validate tool error handling before re-trying."
                )
            elif "budget" in reason_lower or "quota" in reason_lower:
                recommendations.append(
                    f"Swarm reached session budget limits. "
                    f"Inspect sub-task decomposition to reduce turn count per swarm run."
                )
            else:
                recommendations.append(
                    f"Review conversational boundaries between '{root_cause.source_agent}' and '{root_cause.target_agent}'."
                )
        else:
            recommendations.append("Swarm executed within healthy entropy and quota boundaries.")

        return IncidentPostMortem(
            swarm_id=swarm_id,
            status=status,
            circuit_tripped=circuit_tripped,
            start_time=start_time_iso,
            end_time=end_time_iso,
            duration_seconds=duration_sec,
            total_spans=len(spans),
            total_tokens=total_tokens,
            roi=roi,
            root_cause=root_cause,
            entropy_trajectory=trajectory,
            security_summary=security_summary,
            recommendations=recommendations,
        )

    def generate_report(
        self,
        swarm_id: str,
        format: Literal["markdown", "json", "terminal"] = "markdown",
    ) -> Union[str, Dict[str, Any]]:
        """Generates a formatted post-mortem report in the requested representation."""
        post_mortem = self.analyze(swarm_id)
        if not post_mortem:
            if format == "json":
                return {"error": f"Swarm '{swarm_id}' not found"}
            return f"Swarm '{swarm_id}' not found in storage."

        if format == "json":
            return post_mortem.model_dump()

        if format == "terminal":
            return self._format_terminal(post_mortem)

        return self._format_markdown(post_mortem)

    def _format_markdown(self, pm: IncidentPostMortem) -> str:
        status_badge = "🔴 HALTED_BY_CIRCUIT_BREAKER" if pm.circuit_tripped else "🟢 COMPLETED_SUCCESSFULLY"

        lines = [
            f"# SwarmWatch Incident Post-Mortem: {pm.swarm_id}",
            "",
            f"**Execution Status**: `{pm.status}` ({status_badge})  ",
            f"**Total Spans**: `{pm.total_spans}` | **Duration**: `{pm.duration_seconds:.2f}s` | **Tokens Used**: `{pm.total_tokens:,}`  ",
            "",
            "---",
            "",
            "## 1. Financial ROI & Prevented Waste",
            "",
            "| Metric | Value |",
            "| :--- | :--- |",
            f"| **Counterfactual Dollars Saved** | **\\${pm.roi.dollars_saved:,.2f}** |",
            f"| **Runaway Tokens Prevented** | **{pm.roi.tokens_saved:,} tokens** |",
            f"| **Runaway Turns Prevented** | **{pm.roi.turns_prevented} turns** |",
            f"| **Actual Tokens Consumed** | {pm.roi.tokens_used:,} tokens |",
            "",
        ]

        if pm.circuit_tripped and pm.root_cause:
            rc = pm.root_cause
            lines.extend([
                "---",
                "",
                "## 2. Root Cause Analysis (RCA)",
                "",
                f"- **Failure Mode**: `{rc.trigger_reason}`",
                f"- **Primary Agent Vector**: `{rc.source_agent}` -> `{rc.target_agent}`",
                f"- **Trigger Span ID**: `{rc.trigger_span_id}`",
            ])
            if rc.trigger_entropy is not None:
                lines.append(f"- **Final Shannon Entropy ($H_{{norm}}$)**: `{rc.trigger_entropy:.3f}`")
            if rc.trigger_similarity is not None:
                lines.append(f"- **Final Lexical Similarity**: `{rc.trigger_similarity:.3f}`")
            lines.append("")

        lines.extend([
            "---",
            "",
            "## 3. Entropy Trajectory & Diversity Timeline",
            "",
            "| Turn | Causal Vector | Shannon Entropy ($H_{norm}$) | Lexical Similarity | Tokens |",
            "| :--- | :--- | :--- | :--- | :--- |",
        ])

        for pt in pm.entropy_trajectory:
            ent_str = f"{pt.entropy_score:.3f}" if pt.entropy_score is not None else "N/A"
            sim_str = f"{pt.similarity_score:.3f}" if pt.similarity_score is not None else "N/A"
            lines.append(
                f"| {pt.step_index} | `{pt.source_agent}` -> `{pt.target_agent}` | `{ent_str}` | `{sim_str}` | {pt.tokens:,} |"
            )
        lines.append("")

        lines.extend([
            "---",
            "",
            "## 4. Security & Credential Neutralization Audit",
            "",
            f"- **Total Secrets Neutralized**: `{pm.security_summary.total_redacted}`",
        ])
        if pm.security_summary.redacted_categories:
            lines.append("| Category | Count |")
            lines.append("| :--- | :--- |")
            for cat, count in sorted(pm.security_summary.redacted_categories.items()):
                lines.append(f"| `{cat}` | {count} |")
        else:
            lines.append("_No sensitive credentials or PII patterns detected in payloads._")
        lines.append("")

        lines.extend([
            "---",
            "",
            "## 5. Actionable Recommendations",
            "",
        ])
        for rec in pm.recommendations:
            lines.append(f"- {rec}")
        lines.append("")

        return "\n".join(lines)

    def _format_terminal(self, pm: IncidentPostMortem) -> str:
        status_line = "🔴 HALTED BY CIRCUIT BREAKER" if pm.circuit_tripped else "🟢 COMPLETED SUCCESSFULLY"

        bar = "=" * 76
        sub_bar = "-" * 76

        lines = [
            bar,
            f" SWARMWATCH INCIDENT POST-MORTEM: {pm.swarm_id}",
            bar,
            f" Status:        {status_line}",
            f" Duration:      {pm.duration_seconds:.2f}s | Spans: {pm.total_spans} | Tokens Consumed: {pm.total_tokens:,}",
            sub_bar,
            " FINANCIAL ROI & PREVENTED WASTE:",
            f"   Dollars Saved:       ${pm.roi.dollars_saved:,.2f}",
            f"   Tokens Prevented:    {pm.roi.tokens_saved:,} tokens",
            f"   Turns Prevented:     {pm.roi.turns_prevented} turns",
            sub_bar,
        ]

        if pm.circuit_tripped and pm.root_cause:
            rc = pm.root_cause
            lines.extend([
                " ROOT CAUSE ANALYSIS (RCA):",
                f"   Reason:       {rc.trigger_reason}",
                f"   Agent Pair:   {rc.source_agent} -> {rc.target_agent}",
                f"   Trigger Span: {rc.trigger_span_id}",
                sub_bar,
            ])

        lines.extend([
            f" SECURITY AUDIT: Neutralized {pm.security_summary.total_redacted} credentials",
            f" RECOMMENDATION: {pm.recommendations[0] if pm.recommendations else 'None'}",
            bar,
        ])
        return "\n".join(lines)
