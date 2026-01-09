# Grafana Configuration for VictoriaMetrics

This directory contains the Grafana configuration for visualizing metrics from VictoriaMetrics.

## Structure

- `provisioning/datasources/` - Data source configuration (VictoriaMetrics)
- `provisioning/dashboards/` - Dashboard definitions

## Data Source

The VictoriaMetrics data source is automatically configured and set as the default. It connects to VictoriaMetrics at `http://victoria-metrics:8428`.

## Dashboards

### Ledger Metrics Dashboard

A pre-configured dashboard with:
- **Channel Full Errors**: Shows `propose_full` metric over time
- **Raft Apply Entries Duration**: Shows P95 and P99 percentiles of `raft_apply_entries_duration`
- **Available Metrics**: Table showing available metrics

## Creating Custom Dashboards

You can create custom dashboards in Grafana UI:
1. Click on "+" → "Create" → "Dashboard"
2. Add panels with PromQL queries
3. Save the dashboard

Common metrics to query:
- `propose_full` - Channel full errors
- `raft_apply_entries_duration` - Raft apply entries duration histogram
- `propose_inflight` - In-flight propose messages
- Any other metrics exported by your application

## PromQL Examples

```promql
# Channel full errors rate
rate(propose_full[5m])

# Raft apply entries duration P95
histogram_quantile(0.95, sum(rate(raft_apply_entries_duration_bucket[5m])) by (le))

# All metrics with "propose" in the name
{__name__=~".*propose.*"}
```
