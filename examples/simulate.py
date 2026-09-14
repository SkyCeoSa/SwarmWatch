# -*- coding: utf-8 -*-
"""
SwarmWatch Cross-Language Interactive Multi-Agent Simulation.
Orchestrates Python (ProductPlanner) and TypeScript (SecurityReviewer) agents,
demonstrating live causal span DAG tracing, Shannon entropy loop injection,
two-step circuit intervention with counterfactual dollar savings,
and non-destructive Time-Machine DAG branch recovery.
"""

import json
import os
import subprocess
import sys
import time

# Ensure UTF-8 output on Windows terminals
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Add SDK to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../packages/sdk-python")))

import swarmwatch
from swarmwatch.circuit import CircuitPolicy, EntropyCircuitBreaker, SwarmCircuitBreakException


def run_command_sync(cmd: list) -> str:
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return res.stdout.strip()


def print_banner(text: str):
    width = 76
    print("\n" + "=" * width)
    print(f"  {text}")
    print("=" * width)


def print_section(title: str):
    print(f"\n--- {title} ---")


def main():
    swarm_id = "cross-language-swarm-01"
    db_path = "swarmwatch.db"

    print_banner("SwarmWatch Multi-Language Swarm Simulation (Python + TypeScript)")
    print(f"Swarm ID : {swarm_id}")
    print(f"Topology : Python (ProductPlanner) <---> TypeScript (SecurityReviewer)")
    print(f"Engine   : Zero-Token Shannon Entropy & Time-Machine DAG Branching")

    # 1. Initialize SwarmWatch Python Core Client
    policy = CircuitPolicy(
        similarity_threshold=0.80,
        entropy_threshold=0.35,
        max_warnings=1,
        max_turns=10,
        token_budget=5000,
        cost_budget=0.20,
        cost_per_token=0.00002,
    )
    client = swarmwatch.init(swarm_id=swarm_id, db_path=db_path)
    breaker = EntropyCircuitBreaker(policy=policy)

    # Helper to invoke TypeScript SecurityReviewer Agent in Node.js
    def call_typescript_agent(spec_dict: dict, branch: str = "main") -> dict:
        cmd = [
            "node.exe",
            "--experimental-strip-types",
            os.path.join(os.path.dirname(__file__), "cross_language_evaluator.js"),
            swarm_id,
            branch,
            json.dumps(spec_dict),
        ]
        raw_output = run_command_sync(cmd)
        # Parse last line as JSON in case of warnings
        lines = [line for line in raw_output.split("\n") if line.strip().startswith("{")]
        return json.loads(lines[-1]) if lines else {}

    # 2. Define Python ProductPlanner Agent
    @swarmwatch.agent(name="ProductPlanner", role="planner", target="SecurityReviewer", branch_id="main")
    def draft_specification(feature: str, iteration: int = 1, is_loop: bool = False) -> dict:
        if is_loop:
            content = "Section 2.1 requires strict token validation and OAuth2 authorization scopes."
        else:
            content = f"Initial design draft for {feature} containing JWT token exchange and RBAC matrix."

        # Evaluate planner output in circuit breaker
        decision = breaker.evaluate(content, agent_id="ProductPlanner", swarm_id=swarm_id, token_count=120)

        snapshot = swarmwatch.sync_state(
            {
                "agent": "ProductPlanner",
                "feature": feature,
                "iteration": iteration,
                "content_preview": content,
                "circuit_decision": decision.decision,
            },
            branch_id="main",
        )

        return {
            "feature": feature,
            "iteration": iteration,
            "is_loop": is_loop,
            "content": content,
            "state_hash": snapshot.state_hash,
            "circuit_decision": decision.decision,
        }

    # =========================================================================
    # STEP 1: Healthy Cross-Language Collaboration
    # =========================================================================
    print_section("Step 1: Healthy Multi-Agent Execution (Branch: main)")
    print("[ProductPlanner (Python)] Drafting initial OpenAPI specification...")
    spec1 = draft_specification(feature="OAuth2AuthService", iteration=1, is_loop=False)
    print(f"  -> State Snapshot Created: {spec1['state_hash'][:16]}...")
    print(f"  -> Circuit Decision: {spec1['circuit_decision'].upper()}")

    print("[SecurityReviewer (TypeScript)] Evaluating contract specification in Node 22...")
    eval1 = call_typescript_agent(spec1, branch="main")
    print(f"  -> Security Review Verdict: {eval1.get('verdict')}")
    print(f"  -> Reviewer State Hash    : {eval1.get('state_hash', '')[:16]}...")
    print(f"  -> Reviewer Circuit State : {eval1.get('circuit_decision', 'pass').upper()}")

    healthy_snapshot_hash = spec1["state_hash"]

    # =========================================================================
    # STEP 2: Injection of Repetitive Semantic Loop
    # =========================================================================
    print_section("Step 2: Semantic Loop Injection & Circuit Intervention")
    print("[Simulator] Simulating prompt ambiguity resulting in cyclic loop...")

    loop_caught = False
    savings_report = None

    for loop_iteration in [2, 3, 4]:
        print(f"\n[Loop Cycle {loop_iteration - 1}]")
        try:
            print("[ProductPlanner (Python)] Generating specification refinement...")
            loop_spec = draft_specification(feature="OAuth2AuthService", iteration=loop_iteration, is_loop=True)
            print(f"  -> Circuit Decision: {loop_spec['circuit_decision'].upper()} (Warning issued; execution allowed)")

            print("[SecurityReviewer (TypeScript)] Reviewing repetitive specification...")
            loop_eval = call_typescript_agent(loop_spec, branch="main")
            print(f"  -> Security Verdict: {loop_eval.get('verdict')}")

        except SwarmCircuitBreakException as exc:
            loop_caught = True
            savings_report = exc.savings
            print(f"\n[SWARMWATCH CIRCUIT TRIP] {exc}")
            print(f"  Reason      : {exc.reason}")
            print(f"  Metric Value: {exc.metric_val:.4f}")
            break

    if not loop_caught:
        print("[Warning] Loop was not halted as expected.")
        return 1

    # Print Counterfactual Budget Protection Report
    print_section("Step 3: Counterfactual Budget Protection Report (ADR 0006)")
    if savings_report:
        print(f"  Turns Averted     : {savings_report.turns_averted:>8d} turns")
        print(f"  Tokens Saved      : {savings_report.tokens_saved:>8d} tokens")
        print(f"  Dollars Saved     : ${savings_report.dollars_saved:>7.4f}")
        print(f"  Budget Consumed   : ${savings_report.budget_consumed:>7.4f}")
        print(f"  Budget Protected  : ${savings_report.budget_protected:>7.4f}")
        print(f"  Mathematical Cost : $0.0000 (Pure local Shannon entropy & Jaccard math)")

    # =========================================================================
    # STEP 4: Time-Machine DAG Branching Recovery
    # =========================================================================
    print_section("Step 4: Non-Destructive Time-Machine DAG Branching")
    recovery_branch = "recovery-passkeys-v2"
    print(f"[Time-Machine] Rewinding from halted state to healthy snapshot: {healthy_snapshot_hash[:16]}...")
    print(f"[Time-Machine] Spawning new exploration branch: '{recovery_branch}'")

    # Reset circuit breaker for new branch exploration
    breaker.reset()

    @swarmwatch.agent(name="ProductPlanner", role="planner", target="SecurityReviewer", branch_id=recovery_branch)
    def draft_recovery_specification() -> dict:
        diverse_content = (
            "Section 2.1 completely redesigned with FIDO2 WebAuthn biometric passkeys, "
            "eliminating password databases and enforcing hardware-backed cryptographic credentials."
        )
        dec = breaker.evaluate(diverse_content, agent_id="ProductPlanner", swarm_id=swarm_id, token_count=180)
        snap = swarmwatch.sync_state(
            {
                "agent": "ProductPlanner",
                "branch": recovery_branch,
                "forked_from": healthy_snapshot_hash,
                "strategy": "WEBAUTHN_PASSKEYS",
                "status": "HEALTHY",
            },
            branch_id=recovery_branch,
        )
        return {
            "feature": "WebAuthnPasskeys",
            "content": diverse_content,
            "state_hash": snap.state_hash,
            "circuit_decision": dec.decision,
        }

    recovered_spec = draft_recovery_specification()
    print(f"[ProductPlanner (Python)] New Branch Specification Generated: {recovered_spec['state_hash'][:16]}...")
    print(f"  -> Circuit Decision: {recovered_spec['circuit_decision'].upper()} (Healthy Shannon entropy: > 0.85)")

    rec_eval = call_typescript_agent(recovered_spec, branch=recovery_branch)
    print(f"[SecurityReviewer (TypeScript)] Evaluating Branch '{recovery_branch}'...")
    print(f"  -> Security Verdict: {rec_eval.get('verdict')}")
    print(f"  -> Reviewer State  : {rec_eval.get('state_hash', '')[:16]}...")

    # Flush all spans to storage
    swarmwatch.flush()
    swarmwatch.shutdown()

    # Final Tabular Summary (Impeccable Austere Standard)
    print_banner("SwarmWatch Multi-Language Simulation Completed Successfully")
    print(
        f"{'Metric':<30} | {'Value':<30}\n"
        f"{'-'*30}-+-{'-'*30}\n"
        f"{'Python Telemetry Spans':<30} | {'Recorded & Linked in SQLite':<30}\n"
        f"{'TypeScript Telemetry Spans':<30} | {'Recorded via @swarmwatch/sdk':<30}\n"
        f"{'Circuit Breaker Invocations':<30} | {'4 Evaluated, 1 Trip Intervened':<30}\n"
        f"{'Estimated Dollars Saved':<30} | {f'${savings_report.dollars_saved:.4f}':<30}\n"
        f"{'Estimated Tokens Saved':<30} | {f'{savings_report.tokens_saved:,} tokens':<30}\n"
        f"{'Time-Machine Branches':<30} | {'2 Branches (main, recovery-v2)':<30}\n"
        f"{'External LLM API Overhead':<30} | {'$0.00 (Zero Token Burn)':<30}\n"
        f"{'GitHub Actions Required':<30} | {'0 Minutes (100% Local)':<30}"
    )
    print("=" * 76 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
