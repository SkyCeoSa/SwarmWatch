# ADR 0031: Cloud-Native Kubernetes Helm Chart Topology & Production Manifests

## Status
Accepted

## Context
As multi-agent swarms scale from local developer environments into enterprise production workflows (comprising tens or hundreds of autonomous agents executing in microservice fleets), operators require cloud-native deployment automation.

While SwarmWatch previously provided `docker-compose.yml` (ADR 0012) and `docker-compose.monitoring.yml` (ADR 0028) for local and single-node staging environments, enterprise deployment across Amazon EKS, Google GKE, Azure AKS, and on-premises Kubernetes requires:
1. **Reproducible Package Management**: Standard Helm 3 chart versioning, schema verification, and parameterization.
2. **Dual-Storage Topologies**:
   - *Standalone Mode*: Single replica with persistent volume claims (`PVC`) and SQLite WAL journal mode.
   - *Distributed Cloud Mode*: Multiple horizontally scalable stateless replicas backed by a managed or self-hosted ClickHouse cluster and S3/GCS state storage.
3. **High-Availability Ingress & WebSockets**: Ingress reverse-proxy definitions with long-lived WebSocket streaming timeouts (`proxy-read-timeout: 3600`) and TLS termination.
4. **Autonomous Horizontal Scaling**: `HorizontalPodAutoscaler` (HPA v2) scaling pod replicas based on CPU and memory thresholds.
5. **Observability Integration**: First-class `ServiceMonitor` custom resource for the Prometheus Operator scraping SwarmWatch's `/metrics` endpoint at 5s intervals.
6. **Hardened Security Profiles**: Non-root container execution (`runAsNonRoot: true`, `runAsUser: 10001`, `runAsGroup: 10001`), dropping all default Linux capabilities, and preventing privilege escalation.

## Decision
We implement a first-class, zero-dependency Helm 3 production chart under `deploy/helm/swarmwatch/` (Seam 20).

### 1. Chart Layout
```
deploy/helm/swarmwatch/
├── Chart.yaml              # Helm 3 chart metadata (apiVersion: v2)
├── values.yaml             # Production defaults and documentation
└── templates/
    ├── _helpers.tpl        # Standard template helper macros
    ├── deployment.yaml     # SwarmWatch workload deployment with probes & security context
    ├── service.yaml        # ClusterIP service on port 8765
    ├── ingress.yaml        # Ingress with WebSocket annotations and TLS support
    ├── pvc.yaml            # PersistentVolumeClaim for SQLite storage
    ├── hpa.yaml            # HorizontalPodAutoscaler manifest
    ├── configmap.yaml      # Environment variable configuration
    ├── secret.yaml         # Secret template for API keys & ClickHouse credentials
    ├── servicemonitor.yaml # Prometheus Operator ServiceMonitor
    └── NOTES.txt           # Post-install terminal instructions (Impeccable compliant)
```

### 2. Dual Deployment Topologies
- **Standalone Mode (`storage.backend: sqlite`)**:
  - Automatically provisions a `PersistentVolumeClaim` (default `10Gi`) mounted at `/data`.
  - Configures `SWARMWATCH_DB=/data/swarmwatch.db`.
  - Enforces `replicaCount: 1` to prevent concurrent write contention on SQLite file locks.
- **Distributed Mode (`storage.backend: clickhouse`)**:
  - Bypasses PVC creation.
  - Automatically enables `replicaCount: 2+` and `autoscaling.enabled: true`.
  - Injects ClickHouse connection parameters into environment variables.

### 3. Hardened Security Standard
Pods execute under unprivileged user `10001` with `allowPrivilegeEscalation: false` and capability drops (`ALL`), adhering to Kubernetes Restricted Pod Security Standards.

### 4. Zero-Cloud Deterministic Test Suite
We implement `packages/sdk-python/tests/test_helm_kubernetes_topology.py` to validate Helm chart YAML schemas, default values, template rendering across both standalone and distributed modes, security contexts, probes, and Ingress configurations locally without requiring an active Kubernetes cluster or cloud egress.

## Consequences
### Positive
- One-command enterprise installation: `helm install swarmwatch ./deploy/helm/swarmwatch`.
- Native compatibility with cloud Kubernetes platforms (EKS, GKE, AKS, OpenShift) and GitOps tooling (ArgoCD, Flux).
- Strict adherence to high-density standards (zero emojis, austere ASCII NOTES.txt, clean tabular configuration).

### Negative
- Helm template files must be maintained in sync with backend configuration environment variables and port mappings.
