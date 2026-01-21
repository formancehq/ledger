# Metrics

## Overview

Ledger v3 POC exposes OpenTelemetry-compatible metrics that can be collected via OTLP and stored in any compatible backend (VictoriaMetrics, Prometheus, etc.).

Metrics are organized into several categories:
- **System Metrics**: CPU, memory, network, and Go runtime
- **HTTP Server Metrics**: Request latency and throughput
- **Raft Consensus Metrics**: Performance of the consensus layer
- **Transport Metrics**: Inter-node communication (reception, sending, unreachable channels)
- **Queue Metrics**: Internal queue monitoring (propose, reception, sending)
- **Storage Metrics**: Pebble storage engine performance

For a complete reference, see the [Grafana Dashboard](#grafana-dashboards) section.

## System Metrics

System and Go runtime metrics are provided by the OpenTelemetry SDK and `go-libs` modules.

### Process Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `process.cpu.time` | Counter | s | CPU time spent by the process (user/system) |
| `system.memory.usage` | Gauge | By | System memory usage by state (used, free, cached, etc.) |
| `system.memory.utilization` | Gauge | 1 | System memory utilization ratio (0-1) |
| `system.network.io` | Counter | By | Network I/O bytes (receive/transmit) |

### Go Runtime Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `go.goroutine.count` | Gauge | 1 | Current number of goroutines |
| `go.memory.allocated` | Counter | By | Total bytes allocated (cumulative) |
| `go.memory.allocations` | Counter | 1 | Total number of allocations |
| `go.memory.used` | Gauge | By | Memory currently in use by type (stack, heap) |
| `go.memory.gc.goal` | Gauge | By | Target heap size for next GC cycle |
| `go.processor.limit` | Gauge | 1 | Number of OS threads that can execute user-level Go code |

## HTTP Server Metrics

HTTP server metrics are provided by `go-libs/httpserver` instrumentation.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `http.server.request.duration` | Histogram | s | Time to process HTTP requests |

**Attributes**:
- `http.request.method`: HTTP method (GET, POST, PUT, DELETE, etc.)
- `http.response.status_code`: HTTP response status code
- `http.route`: Request route pattern
- `url.scheme`: URL scheme (http, https)

## Raft Consensus Metrics

### Node Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.node.lead` | Gauge | - | Current leader node ID as seen by this node (0 if no leader known) |
| `raft.apply_entries.duration` | Histogram | µs | Time spent applying committed log entries to the FSM. This is the critical path for transaction processing. |
| `raft.apply_entries.batch_size` | Counter | 1 | Total count of entries applied (cumulative). Use `rate()` to get entries/second. |
| `raft.apply_entries.batch_size_distribution` | Histogram | 1 | Distribution of batch sizes when applying entries. Higher batches indicate better throughput efficiency. |
| `raft.append_entries` | Histogram | µs | Time spent appending entries to the Write-Ahead Log (WAL) before replication. |
| `raft.process_entry` | Histogram | µs | Time spent processing a ready state from the Raft library. Includes sending messages, applying entries, and advancing state. |

### Snapshot Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.syncer.create_snapshot.duration` | Histogram | ms | Time spent creating a Raft snapshot. Snapshots are taken periodically to compact the log. |

### Propose Queue Metrics

The propose queue buffers proposals (transactions) before they are submitted to Raft consensus.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.node.propose.load` | Histogram | 1 | Current number of items in the propose queue. High values indicate backpressure. |
| `raft.node.propose.full` | Counter | 1 | Number of times the propose queue was full and proposals were dropped. **Alert if non-zero**. |

## Transport Metrics

Transport metrics track inter-node gRPC communication for Raft consensus.

### Global Transport Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.ping.latency` | Histogram | µs | Round-trip latency of ping requests to peer nodes. Useful for detecting network issues. |
| `raft.transport.sending.pending_response` | UpDownCounter | 1 | Number of pending responses awaited from peer nodes. High values may indicate slow peers. |

**Attributes**:
- `peer`: Peer node ID

### Reception Channel Metrics

Messages received from other nodes are queued in reception channels by priority.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.recv.load` | Histogram | 1 | Current load of the reception queue. Measures queue depth over time. |
| `raft.transport.recv.full` | Counter | 1 | Number of times the reception queue was full. **Alert if non-zero**. |

**Attributes**:
- `priority`: Queue priority level (1 = high priority: AppResp, Vote, VoteResp, PreVote; 2 = lower priority: App)
- `type`: Raft message type

### Unreachable Channel Metrics

Tracks notifications when a peer becomes unreachable.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.unreachable.load` | Histogram | 1 | Current load of the unreachable notification queue. |
| `raft.transport.unreachable.full` | Counter | 1 | Number of times the unreachable queue was full. |

### Per-Peer Sending Metrics

Each peer connection has its own sending queue metrics.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.peer.sending.load` | Histogram | 1 | Current load of the per-peer sending queue. |
| `raft.transport.peer.sending.full` | Counter | 1 | Number of times the per-peer sending queue was full. **Alert if consistently non-zero**. |

**Attributes**:
- `peer`: Peer node ID
- `priority`: Queue priority level (1 = high priority, 2 = lower priority)
- `type`: Raft message type (e.g., `MsgApp`, `MsgHeartbeat`, `MsgVote`)

## Pebble Storage Metrics

The Pebble storage driver exposes metrics via an event listener. Pebble is used for the runtime store (balances, metadata).

### Flush Metrics

Flushes write data from memory (memtable) to disk (SSTable).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.flush.total` | Counter | 1 | Number of Pebble flush operations |
| `pebble.flush.duration.milliseconds` | Histogram | ms | Duration of Pebble flush operations (CPU + I/O time) |
| `pebble.flush.input.bytes` | Histogram | By | Input bytes flushed from memtables to SSTables |

**Attributes**:
- `reason`: Flush reason (e.g., `capacity`, `delete_only_compaction`)
- `status`: `ok` or `error`

### Compaction Metrics

Compactions merge and reorganize SSTables to optimize read performance and reclaim space.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.compaction.total` | Counter | 1 | Number of Pebble compaction operations |
| `pebble.compaction.duration.milliseconds` | Histogram | ms | Duration of Pebble compactions |

**Attributes**:
- `reason`: Compaction reason (e.g., `elision`, `default`, `move`)
- `status`: `ok` or `error`

### Write Stall Metrics

Write stalls occur when Pebble cannot keep up with write rate due to compaction backlog.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.write_stall.total` | Counter | 1 | Number of Pebble write stalls |
| `pebble.write_stall.duration.milliseconds` | Histogram | ms | Duration of Pebble write stalls |
| `pebble.write_stall.active` | Gauge | 1 | Whether Pebble is currently stalling writes (1/0) |

**Attributes**:
- `reason`: Stall reason (e.g., `memtable`, `l0`, `flush_slowdown`)

> **Warning**: A high `pebble.write_stall.total` or `pebble.write_stall.active = 1` indicates that Pebble is experiencing backpressure. This typically means the disk cannot keep up with the write rate. Consider:
> - Using faster storage (NVMe SSD)
> - Increasing Pebble cache size
> - Reducing write rate
> - Scaling horizontally

## SQLite Metrics

SQLite is used for the log store (WAL). These metrics are provided by `go-libs` SQL instrumentation.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `db.sql.latency` | Histogram | ms | Time spent executing SQL operations |

**Attributes**:
- `method`: SQL operation type (`sql.conn.begin_tx`, `sql.conn.exec`, `sql.conn.prepare`, `sql.conn.query`, `sql.rows`, `sql.stmt.exec`, `sql.tx.commit`)
- `store.type`: Store identifier (`log-store`)

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

Queue load uses logarithmic bucket boundaries calculated based on queue capacity for better distribution across the full range.

## Grafana Dashboards

The development environment includes pre-configured Grafana dashboards:

### Ledger Metrics Dashboard

Located at `misc/devenv/config/grafana/provisioning/dashboards/ledger-metrics.json`

The dashboard is organized into the following sections:

**System Section**:
- Transactions per Second
- Ping Latency
- HTTP Requests Count
- Memory Utilization
- Process CPU Time
- System Network Traffic
- System Memory Usage
- Go Memory Allocated
- Leadership Status
- Goroutine Count
- Go Memory Used
- Go Memory Allocations
- Go GC Goal

**Transport & Queues Section**:
- Reception Channel Incoming Messages
- Reception Channel Load (by priority)
- Reception Channel Full Count
- Transport Unreachable Channel
- Unreachable Channel Full Count
- Pending Responses
- Snapshot Creation
- Propose Queue Incoming Messages
- Propose Channel Load
- Propose Queue Full Count
- Send Channel Incoming Messages
- Send Channel Full Count
- Send Channel Load (by priority)

**Ready Loop Section**:
- Process Ready Entry Time Passed
- Applying Entries Time Passed
- Append Entries Time Passed
- Batch Size Distribution
- Applying Entries Percentiles
- Applying Entries Rate

**SQLite Section** (collapsed by default):
- Log Store SQL Time Passed

**Pebble Section**:
- Flush / Second
- Flush Duration (ms)
- Flush Input Bytes / Second
- Compactions / Second
- Compaction Duration (ms)
- Compaction Errors / Second
- Write Stall Active (max)
- Write Stalls / Second
- Write Stall Duration

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

4. **Queue Full Events**
   ```promql
   increase(raft_node_propose_full[5m]) > 0
   ```
   Duration: 1m

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

3. **High Ping Latency**
   ```promql
   histogram_quantile(0.99, rate(raft_transport_ping_latency_bucket[5m])) > 10000
   ```
   Duration: 5m

## Next Steps

- [Deployment](./deployment.md) - Configure observability stack
- [Architecture](./architecture.md) - Understand system components
- [Storage](./storage.md) - Storage configuration and tuning
