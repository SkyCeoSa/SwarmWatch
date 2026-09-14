/**
 * SwarmWatch Cross-Language Simulation: TypeScript Security Evaluator Agent.
 * Runs inside Node.js 22 LTS, utilizing native @swarmwatch/sdk.
 * Evaluates specifications, records state snapshots, and applies circuit breaker checks.
 */

import {
  init,
  agent,
  syncState,
  evaluateOutput,
  flush,
  shutdown,
} from "../packages/sdk-typescript/src/index.ts";

async function run() {
  const args = process.argv.slice(2);
  const swarmId = args[0] || "cross-language-swarm-01";
  const branchId = args[1] || "main";
  const rawPayload = args[2] || "{}";

  let spec;
  try {
    spec = JSON.parse(rawPayload);
  } catch {
    spec = { feature: "Unknown", schema_contract: "RawText", iteration: 1 };
  }

  // Initialize SwarmWatch TypeScript client
  init({ swarm_id: swarmId });

  // Define Evaluator Agent function
  const evaluateSpec = agent(
    "SecurityReviewer",
    async (inputSpec) => {
      // 1. Synchronize state snapshot in Time Machine
      const snapshot = syncState(
        {
          evaluator_state: "ANALYZING_CONTRACT",
          target_feature: inputSpec.feature || "auth_tokens",
          strict_auth_required: true,
          api_key_redaction_check: "api_key_********",
          iteration: inputSpec.iteration || 1,
        },
        branchId
      );

      // 2. Perform specification audit logic
      const isRepetitive = (inputSpec.iteration || 1) >= 2 && inputSpec.is_loop;
      let reviewVerdict;
      let reviewComment;

      if (isRepetitive) {
        // Repeated identical feedback triggering semantic stagnation
        reviewVerdict = "RETRY_REQUIRED";
        reviewComment = "Please review Section 2.1 authentication requirements again carefully.";
      } else {
        reviewVerdict = "APPROVED";
        reviewComment = "Contract schema verified. Authentication scheme OAuth2_JWT and rate limits acceptable.";
      }

      // 3. Evaluate output through Shannon entropy circuit breaker
      const decision = evaluateOutput(reviewComment, "SecurityReviewer");

      return {
        verdict: reviewVerdict,
        comment: reviewComment,
        circuit_decision: decision.decision,
        state_hash: snapshot.state_hash,
      };
    },
    { target: "ProductPlanner", branch_id: branchId }
  );

  const result = await evaluateSpec(spec);

  // Flush pending spans before process termination
  await flush();
  await shutdown();

  // Print JSON result to stdout for parent orchestrator
  process.stdout.write(JSON.stringify(result) + "\n");
}

run().catch((err) => {
  process.stderr.write(`[SecurityReviewer Error] ${err?.message || err}\n`);
  process.exit(1);
});
