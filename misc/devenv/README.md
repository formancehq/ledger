# Pulumi Development Environment

This directory contains three independent Pulumi projects for deploying the Ledger v3 POC infrastructure. Each can be installed and managed separately.

## Projects

| Project | Directory | Pulumi Name | Description |
|---------|-----------|-------------|-------------|
| **Monitoring** | `monitoring/` | `ledger-monitoring` | VictoriaMetrics, Grafana, Loki, Tempo, Pyroscope, OTLP |
| **Operator** | `operator/` | `ledger-operator` | Docker images, CRDs, ledger-operator, operator-ui, LedgerDefaults, cold storage |
| **Testing** | `testing/` | `ledger-testing` | k6-operator, benchmark-operator |

A `shared/` Go module contains common helpers used by all projects.

## Available Environments

| Environment | Stack Name | Kubernetes Context | Namespace |
|-------------|------------|-------------------|-----------|
| **devenv-ledger-exp** | `devenv-ledger-exp` | `ledger-exp-devenv-operator.tailedcc0.ts.net` | `ledger-exp` |
| **staging** | `staging` | `staging-eu-west-1-hosting` | `ledger-exp` |

## Prerequisites

- [Pulumi CLI](https://www.pulumi.com/docs/get-started/install/)
- [Go](https://golang.org/) (v1.21 or higher)
- Access to a Kubernetes cluster with `kubectl` configured

## Deployment

Each project is deployed independently. Deploy **monitoring first** (it creates the namespace), then operator and testing.

```bash
# 1. Deploy monitoring stack
cd monitoring
pulumi stack select formance/ledger-monitoring/devenv-ledger-exp
pulumi up

# 2. Deploy operator (builds images, installs CRDs + operator)
cd ../operator
pulumi stack select formance/ledger-operator/devenv-ledger-exp
pulumi up

# 3. Deploy testing tools (optional)
cd ../testing
pulumi stack select formance/ledger-testing/devenv-ledger-exp
pulumi up
```

## Creating a New Environment

### 1. Create stack configs for each project

```bash
# In each project directory:
cp Pulumi.staging.yaml Pulumi.my-new-env.yaml
```

### 2. Create values directories

```bash
for project in monitoring operator testing; do
  cp -r $project/values/staging $project/values/my-new-env
done
```

### 3. Update config

Edit each `Pulumi.my-new-env.yaml` and update:
- `k8s-context` - your kubectl context
- `namespace` - target namespace
- `registry` / `pull-registry` - Docker registries (operator and testing only)
- Values file paths to point to `values/my-new-env/`

### 4. Set secrets (operator and testing projects)

```bash
cd operator
pulumi stack select my-new-env
pulumi config set --secret formance-dev-registry-username <username>
pulumi config set --secret formance-dev-registry-password <password>
```

### 5. Deploy

```bash
cd monitoring && pulumi stack init formance/ledger-monitoring/my-new-env && pulumi up
cd ../operator && pulumi stack init formance/ledger-operator/my-new-env && pulumi up
cd ../testing && pulumi stack init formance/ledger-testing/my-new-env && pulumi up
```

## Configuration

### Monitoring (`ledger-monitoring`)

| Key | Required | Description |
|-----|----------|-------------|
| `k8s-context` | Yes | Kubernetes context |
| `namespace` | No | Namespace (defaults to stack name) |
| `pyroscope-enabled` | No | Enable Pyroscope (default: `true`) |
| `victoriametrics` | Yes | VictoriaMetrics Helm values |
| `tempo` | Yes | Tempo Helm values |
| `loki` | Yes | Loki Helm values |
| `otlp` | Yes | OTLP Collector Helm values |
| `grafana` | Yes | Grafana Helm values |
| `pyroscope` | No | Pyroscope Helm values |

### Operator (`ledger-operator`)

| Key | Required | Description |
|-----|----------|-------------|
| `k8s-context` | Yes | Kubernetes context |
| `namespace` | No | Namespace (defaults to stack name) |
| `registry` | No | Docker push registry (default: `ghcr.io`) |
| `pull-registry` | No | Docker pull registry (default: same as `registry`) |
| `formance-dev-registry-username` | No | Registry username (secret) |
| `formance-dev-registry-password` | No | Registry password (secret) |
| `operatorUI-enabled` | No | Enable operator UI (default: `true`) |
| `operatorUI-auth-enabled` | No | Enable OIDC auth for UI (default: `false`) |
| `coldStorage-enabled` | No | Enable S3 cold storage (default: `false`) |
| `ledger` | Yes | Ledger/LedgerDefaults Helm values |

### Testing (`ledger-testing`)

| Key | Required | Description |
|-----|----------|-------------|
| `k8s-context` | Yes | Kubernetes context |
| `namespace` | No | Namespace (defaults to stack name) |
| `registry` | No | Docker push registry (default: `ghcr.io`) |
| `pull-registry` | No | Docker pull registry |
| `k6operator-enabled` | No | Enable k6-operator (default: `true`) |
| `benchmarkOperator-enabled` | No | Enable benchmark-operator (default: `false`) |
| `k6operator` | No | k6-operator Helm values |
| `benchmarkOperator` | No | Benchmark operator Helm values |

## Accessing Services

- **Grafana**: `kubectl get svc -n <namespace> grafana`
- **VictoriaMetrics**: `http://vm-victoria-metrics-single-server:8428`
- **Loki**: `http://loki:3100`
- **Tempo**: `http://tempo:3200`
- **OTLP Collector**: gRPC on port 4317

## Destroying

```bash
# Reverse order
cd testing && pulumi destroy
cd ../operator && pulumi destroy
cd ../monitoring && pulumi destroy
```
