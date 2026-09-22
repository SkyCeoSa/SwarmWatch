# ADR 0028: Industrial Production Monitoring Stack & Grafana Dashboard Topology

## Status
Accepted (Seam 17)

## Context
In production deployments, multi-agent AI systems generate high-frequency telemetry encompassing token consumption, circuit breaker state transitions, causal DAG executions, Shannon loop entropy, and time-machine forks. While SwarmWatch provides an austere, low-latency in-browser developer console (`dashboard.html`) and an OpenMetrics 1.0.0 / Prometheus 0.0.4 exporter (`GET /metrics`, Seam 8 / ADR 0014), enterprise operations and SRE teams require centralized operational dashboards within standard monitoring infrastructure (Grafana and Prometheus).

Manually configuring Prometheus scrape targets, writing PromQL queries, designing Grafana panels, and managing alert thresholds across teams causes operational friction, configuration drift, and error-prone setup. Furthermore, existing Grafana community templates often leak external cloud dependencies or assume Kubernetes operator complexities.

SwarmWatch requires an out-of-the-box, self-provisioning industrial monitoring topology runnable via a single command (`docker compose -f docker-compose.monitoring.yml up -d`) with zero external cloud dependencies and strict compliance with the SwarmWatch design tokens.

## Decision
We implement the **Industrial Production Monitoring Stack & Grafana Dashboard Topology (Seam 17)**:

1. **Prometheus Scrape Configuration (`deploy/prometheus/prometheus.yml`)**:
   - Configures Prometheus (v2.54+) to scrape the SwarmWatch control plane (`swarmwatch:8765/metrics`) every 5 seconds.
   - Tags samples with static cluster labels (`job: swarmwatch`, `env: production-local`).
   - Retains Prometheus standard TSDB storage with zero external dependencies.

2. **Automated Grafana Provisioning (`deploy/grafana/provisioning/`)**:
   - **Datasources (`datasources/datasources.yml`)**: Automatically registers the local Prometheus instance (`http://prometheus:9090`) as the default datasource (`isDefault: true`), eliminating manual UI setup.
   - **Dashboard Providers (`dashboards/dashboards.yml`)**: Configures an automatic filesystem provider scanning `/var/lib/grafana/dashboards` with folder organizational grouping (`SwarmWatch`).

3. **Production Grafana Dashboard Preset (`deploy/grafana/dashboards/swarmwatch-overview.json`)**:
   - Pre-built, high-density dashboard structured into 5 cohesive operational tiers:
     - **Tier 1: Executive ROI & Safety Metrics**: Stat panels for Counterfactual Dollars Saved (`sum(swarmwatch_dollars_saved_total)`), Tokens Saved (`sum(swarmwatch_tokens_saved_total)`), Tripped Circuit Breakers (`sum(swarmwatch_circuit_trips_total)`), and Active Swarms / Connected WebSockets.
     - **Tier 2: Ingestion & Throughput**: Time series graphs for span ingestion rate (`rate(swarmwatch_spans_ingested_total[1m])`) partitioned by agent type and project ID, alongside non-blocking telemetry queue depth (`swarmwatch_telemetry_queue_depth_gauge`).
     - **Tier 3: Anomaly & Circuit Trip Breakdown**: Categorized distribution of safety halts by triggering reason (`entropy_collapse`, `infinite_tool_recursion`, `budget_quota_exhaustion`, `conversational_deadlock`).
     - **Tier 4: Latency & Shannon Entropy Histograms**: P50, P90, and P99 latency percentiles (`histogram_quantile`) and entropy computation evaluation latencies.
     - **Tier 5: Time-Machine DAG & State Branching**: Historical cumulative state snapshots and DAG branch forks created.
   - **Aesthetic Harmony**: Dark canvas palette strictly aligned with SwarmWatch design tokens (`#080B11` / `#0E1422` theme, tabular numerals, instrument cyan `#38BDF8`, emerald green `#10B981`, amber `#F59E0B`, crimson `#EF4444`, branch purple `#A78BFA`).

4. **Multi-Service Docker Compose Topology (`docker-compose.monitoring.yml`)**:
   - Orchestrates `swarmwatch`, `prometheus` (`prom/prometheus:v2.54.1`), and `grafana` (`grafana/grafana:11.2.0`).
   - Configures anonymous viewer access (`GF_AUTH_ANONYMOUS_ENABLED=true`, `GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer`) for instant dashboard availability on `http://localhost:3000`.
   - Utilizes persistent named volumes (`swarmwatch_data`, `prometheus_data`, `grafana_data`) and an isolated bridge network (`swarmwatch_net`).

5. **Automated TDD Verification Suite (`test_grafana_and_docker_topology.py`)**:
   - Validates JSON schema integrity, panel coordinate layouts, and unique panel IDs.
   - Verifies that every PromQL query referenced in the Grafana preset corresponds to an active metric registered in `swarmwatch.metrics.MetricsRegistry`.
   - Validates YAML configuration syntax for both Prometheus scraping and Docker Compose orchestration.

## Consequences
- **Positive**: Single-command startup (`docker compose -f docker-compose.monitoring.yml up -d`) delivers a full industrial observability stack with zero manual dashboard configuration.
- **Positive**: Zero external cloud costs, data egress concerns, or remote LLM tokens consumed.
- **Positive**: Strict parity between developer console metrics and enterprise SRE Prometheus/Grafana infrastructure.
