# Pulumi Development Environment

This Pulumi application deploys the observability stack and the Ledger v3 POC application, including:

- **VictoriaMetrics**: Metrics storage and querying
- **Grafana**: Metrics visualization and dashboards
- **Loki**: Log aggregation
- **OpenTelemetry Collector**: Metrics, traces, and logs collection
- **Tempo**: Distributed tracing backend
- **Pyroscope**: Continuous profiling for performance analysis
- **Ledger v3 POC**: The main ledger application with Raft consensus cluster
- **k6-operator**: Kubernetes operator for running k6 performance tests

All components are deployed in a single namespace for easier management and service discovery.

## Available Environments

| Environment | Stack Name | Kubernetes Context | Namespace | Description |
|-------------|------------|-------------------|-----------|-------------|
| **devenv-ledger-exp** | `devenv-ledger-exp` | `ledger-exp-devenv-operator.tailedcc0.ts.net` | `ledger-exp` | Development environment on Waays (Tailscale) |
| **staging** | `staging` | `staging-eu-west-1-hosting` | `ledger-exp` | Staging environment on AWS (formance.cloud) |

### Staging Environment

The staging environment is deployed on the Formance AWS staging cluster:

- **Kubernetes context**: `staging-eu-west-1-hosting`
- **Namespace**: `ledger-exp`
- **Ledger URL**: `https://ledger-exp.staging.formance.cloud`
- **Grafana URL**: `https://grafana-ledger-exp.staging.formance.cloud`
- **Pull registry**: ECR mirror (`760319359092.dkr.ecr.eu-west-1.amazonaws.com/ghcr`)

## Prerequisites

- [Pulumi CLI](https://www.pulumi.com/docs/get-started/install/) installed
- [Go](https://golang.org/) (v1.21 or higher)
- Access to a Kubernetes cluster with `kubectl` configured
- Helm repositories added (the code will add them automatically)

## Installation

All stacks are managed under the **`formance`** organization on [Pulumi Cloud](https://app.pulumi.com/formance).

1. Install dependencies:

```bash
go mod download
```

2. Configure Pulumi (if not already done):

```bash
pulumi login
```

3. Select an existing stack (from the `formance` organization):

```bash
pulumi stack select formance/ledger-exp-devenv/staging
# or
pulumi stack select formance/ledger-exp-devenv/devenv-ledger-exp
```

You can also use short names if you're already in the correct project context:

```bash
pulumi stack select staging
```

## Creating a New Environment

To create a new environment, follow these steps:

### 1. Create the Pulumi stack configuration

Copy an existing configuration file and adapt it:

```bash
cp Pulumi.staging.yaml Pulumi.my-new-env.yaml
```

### 2. Create the values directory

```bash
cp -r values/staging values/my-new-env
```

### 3. Update the Pulumi configuration

Edit `Pulumi.my-new-env.yaml` and update the **important variables**:

```yaml
config:
  # === REQUIRED: Kubernetes configuration ===
  ledger-exp-devenv:k8s-context: my-kubernetes-context    # kubectl context name
  ledger-exp-devenv:namespace: my-namespace               # Kubernetes namespace

  # === REQUIRED: Docker registry configuration ===
  ledger-exp-devenv:registry: ghcr.io                     # Registry to push images to
  ledger-exp-devenv:pull-registry: ghcr.io                # Registry to pull images from (can be different, e.g., ECR mirror)
  
  # === OPTIONAL: Registry credentials (use `pulumi config set --secret`) ===
  # Only needed if pushing to a private registry
  ledger-exp-devenv:formance-dev-registry-username:
    secure: <encrypted-value>
  ledger-exp-devenv:formance-dev-registry-password:
    secure: <encrypted-value>

  # === OPTIONAL: Enable/disable components ===
  ledger-exp-devenv:k6operator-enabled: true              # k6 load testing operator
  ledger-exp-devenv:benchmarkOperator-enabled: false      # Benchmark reporting operator
  ledger-exp-devenv:pyroscope-enabled: true               # Continuous profiling with Pyroscope

  # === VALUES FILES: Point to your new values directory ===
  ledger-exp-devenv:grafana:
    file: values/my-new-env/grafana.yaml
  ledger-exp-devenv:ledger:
    file: values/my-new-env/ledger.yaml
  # ... (update all file references)
```

### 4. Update environment-specific values

Edit the values files in `values/my-new-env/` to update:

**`grafana.yaml`** - Update URLs:
```yaml
grafana.ini:
  server:
    root_url: https://grafana.my-app.example.com
ingress:
  hosts:
    - grafana.my-app.example.com
  tls:
    - hosts:
        - grafana.my-app.example.com
```

**`ledger.yaml`** - Update ingress:
```yaml
ingress:
  hosts:
    - host: ledger.my-app.example.com
      paths:
        - path: /
          pathType: Prefix
```

**`benchmark-operator.yaml`** - Update Grafana URL:
```yaml
env:
  GRAFANA_URL: "https://grafana.my-app.example.com"
```

### 5. Set registry credentials (optional)

Only needed if pushing to a private registry:

```bash
pulumi stack select my-new-env
pulumi config set --secret formance-dev-registry-username <username>
pulumi config set --secret formance-dev-registry-password <password>
```

### 6. Initialize and deploy

```bash
# Create the stack in the formance organization
pulumi stack init formance/ledger-exp-devenv/my-new-env
pulumi preview
pulumi up
```

## Deployment

Preview the changes:

```bash
pulumi preview
```

Deploy the stack:

```bash
pulumi up
```

## Accessing Services

After deployment, you can access the services (replace `<namespace>` with your configured namespace):

- **Grafana**: Check the service URL with `kubectl get svc -n <namespace> grafana`
- **VictoriaMetrics**: `http://vm-victoria-metrics-single-server:8428`
- **Loki**: `http://loki:3100`
- **Tempo**: `http://tempo:3200`
- **OpenTelemetry Collector**: OTLP gRPC endpoint on port 4317
- **Ledger v3 POC**: HTTP API on port 9000, gRPC/Raft on port 8888
- **k6-operator**: Operator for running k6 performance tests (see `k6/README.md` for usage)

Since all services are in the same namespace, they can communicate using short service names (e.g., `tempo:4317` instead of `tempo.<namespace>.svc.cluster.local:4317`).

## Configuration

### Pulumi Configuration Keys

The application reads configuration from `Pulumi.<stack>.yaml`:

| Key | Required | Description |
|-----|----------|-------------|
| `k8s-context` | Yes | Kubernetes context name |
| `namespace` | No | Kubernetes namespace (defaults to stack name) |
| `registry` | No | Docker registry to push images (defaults to `ghcr.io`) |
| `pull-registry` | No | Docker registry to pull images (defaults to `registry` value) |
| `formance-dev-registry-username` | No | Registry username (secret, for private registries) |
| `formance-dev-registry-password` | No | Registry password (secret, for private registries) |
| `k6operator-enabled` | No | Enable k6-operator (defaults to `true`) |
| `benchmarkOperator-enabled` | No | Enable benchmark-operator (defaults to `false`) |
| `pyroscope-enabled` | No | Enable Pyroscope continuous profiling (defaults to `true`) |

### Helm Values

Helm values are read from YAML files referenced in the Pulumi configuration:

- `victoriametrics` - VictoriaMetrics Helm values
- `grafana` - Grafana Helm values
- `loki` - Loki Helm values
- `otlp` - OpenTelemetry Collector Helm values
- `tempo` - Tempo Helm values
- `ledger` - Ledger v3 POC Helm values
- `k6operator` - k6-operator Helm values
- `benchmarkOperator` - Benchmark operator values

### Grafana Provisioning

Grafana provisioning files (dashboards and datasources) are read from the `config/grafana/provisioning/` directory:

- `config/grafana/provisioning/dashboards/` - Grafana dashboard definitions
- `config/grafana/provisioning/datasources/` - Grafana datasource provisioning
- The VictoriaMetrics datasource uses a fixed `uid` (`VictoriaMetrics`) so dashboards can reference it reliably.
- The Ledger Metrics dashboard node selector derives values from `raft.node.lead` using `query_result` + regex to keep it reliably populated.

## Updating Configuration

To update the Helm values:

1. Modify the values files in `values/<stack>/`
2. Run `pulumi preview` to see changes
3. Run `pulumi up` to apply changes

To update Grafana provisioning files:

1. Modify the files in `config/grafana/provisioning/`
2. Run `pulumi preview` to see changes
3. Run `pulumi up` to apply changes

## Optional Components

Optional components can be enabled/disabled directly in the Pulumi stack configuration file (`Pulumi.<stack>.yaml`).

### k6-operator

The k6-operator is **enabled by default**. To disable it:

```yaml
# Pulumi.<stack>.yaml
config:
  ledger-exp-devenv:k6operator-enabled: false
```

Or via CLI:

```bash
pulumi config set k6operator-enabled false
```

### Benchmark Operator

The benchmark operator is **disabled by default**. To enable it:

```yaml
# Pulumi.<stack>.yaml
config:
  ledger-exp-devenv:benchmarkOperator-enabled: true
```

Or via CLI:

```bash
pulumi config set benchmarkOperator-enabled true
```

Configure Grafana credentials in the values file (`values/<stack>/benchmark-operator.yaml`):

```yaml
env:
  GRAFANA_URL: "https://grafana.example.com"
  GRAFANA_USER: "admin"
  GRAFANA_PASSWORD: "admin"
```

Then run:

1. `pulumi preview` to see changes
2. `pulumi up` to apply changes

The operator is also available as a Helm chart at `misc/benchmark-operator/chart`.

### Pyroscope

Pyroscope continuous profiling is **enabled by default**. To disable it:

```yaml
# Pulumi.<stack>.yaml
config:
  ledger-exp-devenv:pyroscope-enabled: false
```

Or via CLI:

```bash
pulumi config set pyroscope-enabled false
```

Configure Pyroscope values in the values file (`values/<stack>/pyroscope.yaml`):

```yaml
pyroscope:
  enabled: true
  persistence:
    enabled: true
    size: 10Gi
```

The ledger application will automatically send profiles to Pyroscope when `config.monitoring.pyroscope.enabled` is set to `true` in the ledger values file (`values/<stack>/ledger.yaml`):

```yaml
config:
  monitoring:
    pyroscope:
      enabled: true
      serverAddress: "http://pyroscope:4040"
      profileTypes: "cpu,alloc_objects,alloc_space,inuse_objects,inuse_space"
```

Pyroscope is available as a data source in Grafana under the name "Pyroscope".

## Destroying the Stack

To remove all resources:

```bash
pulumi destroy
```

## Troubleshooting

Replace `<namespace>` with your configured namespace (defaults to the pulumi stack name).

### Check Helm releases

```bash
helm list -n <namespace>
```

### Check pod status

```bash
kubectl get pods -n <namespace>
```

### View logs

```bash
kubectl logs -n <namespace> -l app.kubernetes.io/name=grafana
```

### Check ConfigMaps

```bash
kubectl get configmaps -n <namespace>
```
