# Metrics

## Overview

Ledger v3 POC exposes OpenTelemetry-compatible metrics that can be collected via OTLP and stored in any compatible backend (VictoriaMetrics, Prometheus, etc.).

Metrics are organized into several categories:
- **Raft Consensus Metrics**: Performance of the consensus layer
- **Transport Metrics**: Inter-node communication
- **Storage Metrics**: Pebble storage engine performance
- **Queue Metrics**: Internal queue monitoring

## Raft Consensus Metrics

### Node Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.node.lead` | Gauge | - | Current leader node ID (0 if no leader) |
| `raft.apply_entries.duration` | Histogram | µs | Time spent applying entries to FSM |
| `raft.apply_entries.batch_size` | Counter | 1 | Total count of entries in batches passed to ApplyEntries |
| `raft.apply_entries.batch_size_distribution` | Histogram | 1 | Distribution of batch sizes passed to ApplyEntries |
| `raft.append_entries` | Histogram | µs | Time spent appending entries to WAL |
| `raft.process_entry` | Histogram | µs | Time spent processing ready from Raft |

### Syncer Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.syncer.create_snapshot.duration` | Histogram | ms | Time spent creating snapshot in syncer |

### Queue Metrics

The propose queue monitors the internal proposal queue:

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.node.propose.load` | Histogram | 1 | Current load of the propose queue |
| `raft.node.propose.full` | Counter | 1 | Number of times the propose queue was full (dropped proposals) |

## Transport Metrics

### Global Transport Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.ping.latency` | Histogram | µs | Latency of ping requests to peers |
| `raft.transport.sending.pending_response` | UpDownCounter | 1 | Number of pending responses from peers |

### Per-Peer Queue Metrics

Each peer connection has its own queue metrics with `peer` and `priority` attributes:

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.peer.sending.load` | Histogram | 1 | Current load of the peer sending queue |
| `raft.transport.peer.sending.full` | Counter | 1 | Number of times the peer sending queue was full |

**Attributes**:
- `peer`: Peer node ID
- `priority`: Queue priority level (lower = higher priority)
- `type`: Raft message type (e.g., `MsgApp`, `MsgHeartbeat`, `MsgVote`)

## Pebble Storage Metrics

The Pebble storage driver exposes metrics via an event listener:

### Flush Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.flush.total` | Counter | 1 | Number of Pebble flush operations |
| `pebble.flush.duration.milliseconds` | Histogram | ms | Duration of Pebble flush operations |
| `pebble.flush.input.bytes` | Histogram | By | Input bytes flushed from memtables |

**Attributes**:
- `reason`: Flush reason (e.g., `capacity`, `delete_only_compaction`)
- `status`: `ok` or `error`

### Compaction Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.compaction.total` | Counter | 1 | Number of Pebble compaction operations |
| `pebble.compaction.duration.milliseconds` | Histogram | ms | Duration of Pebble compactions |

**Attributes**:
- `reason`: Compaction reason
- `status`: `ok` or `error`

### Write Stall Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.write_stall.total` | Counter | 1 | Number of Pebble write stalls |
| `pebble.write_stall.duration.milliseconds` | Histogram | ms | Duration of Pebble write stalls |
| `pebble.write_stall.active` | Gauge | 1 | Whether Pebble is currently stalling writes (1/0) |

**Attributes**:
- `reason`: Stall reason (e.g., `memtable`, `l0`, `flush_slowdown`)

> **⚠️ Alert**: A high `pebble.write_stall.total` or `pebble.write_stall.active = 1` indicates that Pebble is experiencing backpressure. This typically means the disk cannot keep up with the write rate. Consider:
> - Using faster storage (NVMe)
> - Increasing Pebble cache size
> - Reducing write rate

## Configuration

### Enabling Metrics Export

Configure metrics export via environment variables or command-line flags:

```bash
# Environment variables
export OTEL_METRICS_ENABLED=true
export OTEL_METRICS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=otel-collector:4317
export OTEL_METRICS_EXPORTER_INTERVAL=15s

# Or via Helm values
config:
  monitoring:
    metrics:
      enabled: true
      exporter: "otlp"
      endpoint: "otel-collector"
      port: "4317"
      exporterPushInterval: "15s"
```

### Runtime Metrics

Go runtime metrics can be enabled:

```yaml
config:
  monitoring:
    metrics:
      runtime: true
      runtimeMinimumReadMemStatsInterval: "15s"
```

This exposes standard Go runtime metrics including:
- Memory allocation statistics
- Garbage collection metrics
- Goroutine counts

## Histogram Bucket Boundaries

### Apply Entries Duration

Fine-grained buckets for latency analysis (in microseconds):
```
0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
12000, 15000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000,
60000, 70000, 80000, 90000, 100000,
125000, 150000, 175000, 200000, 250000, 300000, 350000, 400000, 450000, 500000
```

### Snapshot Creation Duration

Buckets for snapshot timing (in milliseconds):
```
0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
12, 15, 18, 20, 25, 30, 35, 40, 45, 50,
60, 70, 80, 90, 100,
125, 150, 175, 200, 250, 300, 350, 400, 450, 500,
600, 700, 800, 900, 1000, 1500, 2000, 2500, 3000, 4000, 5000
```

### Queue Load

Queue load uses logarithmic bucket boundaries calculated based on queue capacity for better distribution.

## Grafana Dashboards

The development environment includes pre-configured Grafana dashboards:

### Ledger Metrics Dashboard

Located at `misc/devenv/config/grafana/provisioning/dashboards/ledger-metrics.json`

Key panels:
- **Leader Status**: Shows which node is the current leader
- **Apply Entries Latency**: P50, P95, P99 latency histograms
- **Batch Size Distribution**: Shows how entries are batched
- **Pebble Write Stalls**: Alerts on storage backpressure

### k6 Dashboard

Located at `misc/devenv/config/grafana/provisioning/dashboards/k6.json`

Displays k6 load test metrics in real-time.

## Alerting Recommendations

### Critical Alerts

1. **No Leader**
   ```promql
   max(raft_node_lead) == 0
   ```
   Duration: 30s
   
2. **Pebble Write Stall Active**
   ```promql
   pebble_write_stall_active == 1
   ```
   Duration: 10s

3. **High Apply Entries Latency**
   ```promql
   histogram_quantile(0.99, rate(raft_apply_entries_duration_bucket[5m])) > 100000
   ```
   Duration: 5m

### Warning Alerts

1. **Queue Near Capacity**
   ```promql
   histogram_quantile(0.95, rate(raft_node_propose_load_bucket[5m])) > 0.8 * <queue_capacity>
   ```
   Duration: 1m

2. **High Snapshot Duration**
   ```promql
   histogram_quantile(0.99, rate(raft_syncer_create_snapshot_duration_bucket[5m])) > 1000
   ```
   Duration: 5m

## Next Steps

- [Deployment](./deployment.md) - Configure observability stack
- [Architecture](./architecture.md) - Understand system components
- [Storage](./storage.md) - Storage configuration and tuning
