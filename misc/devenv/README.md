# Pulumi Development Environment

This Pulumi application deploys the observability stack and the Ledger v3 POC application, including:

- **VictoriaMetrics**: Metrics storage and querying
- **Grafana**: Metrics visualization and dashboards
- **Loki**: Log aggregation
- **OpenTelemetry Collector**: Metrics, traces, and logs collection
- **Tempo**: Distributed tracing backend
- **Ledger v3 POC**: The main ledger application with Raft consensus cluster
- **k6-operator**: Kubernetes operator for running k6 performance tests

## Prerequisites

- [Pulumi CLI](https://www.pulumi.com/docs/get-started/install/) installed
- [Go](https://golang.org/) (v1.21 or higher)
- Access to a Kubernetes cluster with `kubectl` configured
- Helm repositories added (the code will add them automatically)

## Installation

1. Install dependencies:

```bash
go mod download
```

2. Configure Pulumi (if not already done):

```bash
pulumi login
```

3. Create or select a Pulumi stack:

```bash
pulumi stack init dev
# or
pulumi stack select dev
```

4. Configure the namespace (optional, defaults to `monitoring`):

```bash
pulumi config set kubernetes:namespace monitoring
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

After deployment, you can access the services:

- **Grafana**: Check the service URL with `kubectl get svc -n monitoring grafana`
- **VictoriaMetrics**: `http://vm-victoria-metrics-single-server.monitoring.svc.cluster.local:8428`
- **Loki**: `http://loki.monitoring.svc.cluster.local:3100`
- **Tempo**: `http://tempo.monitoring.svc.cluster.local:3200`
- **OpenTelemetry Collector**: OTLP gRPC endpoint on port 4317
- **Ledger v3 POC**: HTTP API on port 9000, gRPC/Raft on port 8888
- **k6-operator**: Operator for running k6 performance tests (see `k6/README.md` for usage)

## Configuration

The application reads Helm values from the Pulumi configuration file `Pulumi.devenv.yaml`:

- `victoriametrics` - VictoriaMetrics Helm values
- `grafana` - Grafana Helm values
- `loki` - Loki Helm values
- `otlp` - OpenTelemetry Collector Helm values
- `tempo` - Tempo Helm values
- `ledger` - Ledger v3 POC Helm values
- `k6operator` - k6-operator Helm values (optional, defaults to empty)
- `benchmarkOperator` - Benchmark operator values (optional, defaults to disabled)

Grafana provisioning files (dashboards and datasources) are still read from the `config/grafana/provisioning/` directory:

- `config/grafana/provisioning/dashboards/` - Grafana dashboard definitions
- `config/grafana/provisioning/datasources/` - Grafana datasource provisioning
- The VictoriaMetrics datasource uses a fixed `uid` (`VictoriaMetrics`) so dashboards can reference it reliably.
- The Ledger Metrics dashboard node selector derives values from `raft.node.lead` using `query_result` + regex to keep it reliably populated.

## Updating Configuration

To update the Helm values:

1. Modify the configuration in `Pulumi.dev.yaml` under the `devenv:` keys
2. Run `pulumi preview` to see changes
3. Run `pulumi up` to apply changes

To update Grafana provisioning files:

1. Modify the files in `config/grafana/provisioning/`
2. Run `pulumi preview` to see changes
3. Run `pulumi up` to apply changes

## Benchmark Operator

The benchmark operator can be deployed via Pulumi to watch `TestRun` objects and generate Grafana snapshots plus a Markdown report.

1. Update `values/benchmark-operator.yaml` to set `enabled: true` and configure Grafana credentials.
2. Run `pulumi preview` to see changes
3. Run `pulumi up` to apply changes

The operator is also available as a Helm chart at `misc/benchmark-operator/chart`.

## Destroying the Stack

To remove all resources:

```bash
pulumi destroy
```

## Troubleshooting

### Check Helm releases

```bash
helm list -n monitoring
```

### Check pod status

```bash
kubectl get pods -n monitoring
```

### View logs

```bash
kubectl logs -n monitoring -l app.kubernetes.io/name=grafana
```

### Check ConfigMaps

```bash
kubectl get configmaps -n monitoring
```
