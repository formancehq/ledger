# Benchmark Operator

This operator watches k6 `TestRun` objects and generates Grafana snapshots plus a Markdown report when a run completes.

## Features

- Watches `k6.io/v1alpha1` `TestRun` resources.
- Creates Grafana snapshots after completion.
- Optionally creates per-node snapshots (static filtering).
- Outputs a JSON report to logs and writes a ConfigMap named `k6-report-<testrun-name>`.
- Marks processed TestRuns with `benchmark.formance.com/processed`.
- Deletes the report ConfigMap and Grafana snapshots when a TestRun is deleted.

## Configuration

Set these environment variables on the operator container:

- `GRAFANA_URL` (required)
- `GRAFANA_USER` / `GRAFANA_PASSWORD` (optional)
- `WATCH_NAMESPACE` (optional, empty = all namespaces)
- `SNAPSHOT_PER_NODE` (default `true`)
- `NODE_METRIC` (default `raft.node.lead`)
- `NODE_LABEL` (default `service.node_id`)
- `DATASOURCE_NAME` (default `VictoriaMetrics`)
- `SNAPSHOT_NAME_PREFIX` (default `k6-benchmark`)

## Helm chart

The Helm chart lives in `misc/benchmark-operator/chart` and supports the same environment variables as the operator.

```bash
helm upgrade --install benchmark-operator misc/benchmark-operator/chart \
  --namespace bench --create-namespace \
  --set image.repository=ghcr.io/formancehq/benchmark-operator \
  --set image.tag=latest \
  --set image.pullSecrets[0]=registry-secret \
  --set env.GRAFANA_URL=https://grafana.example.com \
  --set env.GRAFANA_USER=admin \
  --set env.GRAFANA_PASSWORD=admin
```

If you run the operator outside the `default` namespace, update the ClusterRoleBinding subject namespace accordingly.

## Report retrieval

Retrieve the report ConfigMap named `k6-report-<testrun-name>`:

```bash
kubectl get configmap k6-report-<testrun-name> -n <namespace> -o jsonpath='{.data.report\.json}'
```

The JSON report includes both snapshot URLs and live Grafana URLs with the TestRun time range.

## Notes

- Grafana snapshots are static. For per-node views, the operator generates one snapshot per node.
- The operator does not delete snapshots; use the Grafana API if you need cleanup.
