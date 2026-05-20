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
- **Storage Disk Usage**: Disk space consumption per component and volume

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

### FSM Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.fsm.logs_appended` | Counter | 1 | Total number of logs appended to the store. Use `rate()` to get logs per second. This is the primary throughput metric. |

### Node Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.node.lead` | Gauge | - | Current leader node ID as seen by this node (0 if no leader known) |
| `raft.apply_entries.duration` | Histogram | µs | Time spent applying committed log entries to the FSM. This is the critical path for transaction processing. |
| `raft.apply_entries.batch_size` | Counter | 1 | Total count of entries applied (cumulative). Use `rate()` to get entries/second. |
| `raft.apply_entries.batch_size_distribution` | Histogram | 1 | Distribution of batch sizes when applying entries. Higher batches indicate better throughput efficiency. |
| `raft.append_entries` | Histogram | µs | Time spent appending entries to the Write-Ahead Log (WAL) before replication. |
| `raft.process_entry` | Histogram | µs | Time spent processing a ready state from the Raft library. Includes sending messages, applying entries, and advancing state. |

### Gating Metrics

Gating occurs when the node performs a maintenance task (snapshot install, checkpoint restore). During gating, Raft Readies are spooled instead of applied directly to the FSM.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.node.gating.wait_duration` | Histogram | µs | Time spent waiting for gatingTerminated (maintenance task completion) in the processReadies goroutine. High values indicate long snapshot/restore operations stalling the ready pipeline. |
| `raft.node.gating.readies_processed` | Histogram | 1 | Number of Raft Readies processed during each gating period. Higher values indicate more Readies were spooled while the maintenance task was running. |

### WAL Metrics

The Write-Ahead Log (WAL) metrics track the performance of the WAL append operations.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `wal.append.save.duration` | Histogram | µs | Time spent saving entries to the WAL on disk. This is the actual disk I/O time. |
| `wal.append.batch_size` | Histogram | 1 | Number of entries appended at once. Higher values indicate efficient batching under load. |

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

### Pending Send Queue Metrics

Outgoing messages are first queued in a global pending send queue before being distributed to per-peer queues.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.send.pending_messages.load` | Histogram | 1 | Current load of the pending send queue. High values indicate messages are being queued faster than they can be dispatched to peers. |
| `raft.send.pending_messages.full` | Counter | 1 | Number of times the pending send queue was full. **Alert if non-zero**. |

### Reception Channel Metrics

Messages received from other nodes are queued in 3 priority reception channels. The Raft node consumes directly from these channels with priority ordering (high > medium > low).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.recv.load` | Histogram | 1 | Current load of the reception queue per priority. Measures queue depth over time. |
| `raft.transport.recv.full` | Counter | 1 | Number of times the reception queue was full. **Alert if non-zero**. |

**Attributes**:
- `priority`: Queue priority level (0 = high, 1 = medium, 2 = low)
- `priority_name`: Human-readable priority name (`high`, `medium`, `low`)

**Priority Classification**:
| Priority | Name | Message Types |
|----------|------|---------------|
| 0 | high | `MsgHeartbeat`, `MsgHeartbeatResp` |
| 1 | medium | `MsgVote`, `MsgVoteResp`, `MsgPreVote`, `MsgPreVoteResp`, `MsgAppResp` |
| 2 | low | All others (`MsgApp`, `MsgSnap`, etc.) |

### Unreachable Channel Metrics

Tracks notifications when a peer becomes unreachable.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.unreachable.load` | Histogram | 1 | Current load of the unreachable notification queue. |
| `raft.transport.unreachable.full` | Counter | 1 | Number of times the unreachable queue was full. |

### Per-Peer Sending Metrics

Each peer connection has 3 priority queues for sending messages (one per priority level).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.peer.sending.load` | Histogram | 1 | Current load of the per-peer sending queue. |
| `raft.transport.peer.sending.full` | Counter | 1 | Number of times the per-peer sending queue was full. **Alert if consistently non-zero**. |

**Attributes**:
- `peer`: Peer node ID
- `priority`: Queue priority level (0 = high, 1 = medium, 2 = low)
- `priority_name`: Human-readable priority name (`high`, `medium`, `low`)

## Admission Metrics

The admission service handles order processing before Raft consensus. It preloads attribute values from the store when they are not available in the cache.

### Preload Metrics

When a value is not guaranteed to be in cache (based on the cache generation), the admission service loads it from the persistent store. These metrics track the performance and volume of these preload operations.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `admission.preload.duration` | Histogram | µs | Time spent loading a preload value from the store. Includes the actual disk read and computation time. High values indicate slow storage or expensive computations. |
| `admission.preload.total` | Counter | 1 | Total number of preload operations from store (cache misses). High rates may indicate cache miss issues or cold startup. |
| `admission.preload.keys_needed` | Counter | 1 | Total number of keys that needed resolving during preload. This is the total demand before cache filtering. |
| `admission.preload.cache_hits` | Counter | 1 | Total number of keys found guaranteed in cache (no store read needed). Use with `keys_needed` to compute cache hit ratio. |

**Attributes**:
- `type`: Attribute type being preloaded (`input`, `output`, `ledgers`, `reversions`, `idempotency_keys`, `references`, `boundaries`)

**Attribute Types**:
| Type | Description |
|------|-------------|
| `input` | Account input volumes (credits received) |
| `output` | Account output volumes (debits sent) |
| `ledgers` | Ledger info and metadata |
| `reversions` | Transaction reversion status |
| `idempotency_keys` | Idempotency key mappings |
| `references` | Transaction reference mappings |
| `boundaries` | Ledger boundaries (next IDs) |

**Derived Metrics**:
- **Cache hit ratio**: `cache_hits / keys_needed * 100` — percentage of keys served from cache
- **Store read ratio**: `total / keys_needed * 100` — percentage requiring store reads

**Preload Flow**: When processing a transaction, the admission service checks if required values (volumes, reversion status, idempotency keys, references, boundaries) are in cache. If not guaranteed in cache due to generation rotation, it loads them from the persistent store. These metrics help identify:
- Storage performance issues (high preload duration)
- Cache efficiency problems (low cache hit ratio after warmup)
- Cold start behavior (expected low cache hit ratio initially)
- Read volume per transaction type (keys_needed by type)

### Command Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `admission.command.duration` | Histogram | µs | Total time from Apply call to future resolution. Includes preload, proposal, and FSM application. |
| `admission.propose.duration` | Histogram | µs | Time waiting for Raft to accept and replicate a proposal (Propose + Wait). |
| `admission.command.size` | Histogram | By | Size of marshalled Raft commands in bytes. Large commands may indicate many postings or metadata. |

### Propose Queue Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `admission.propose_queue.load` | Histogram | 1 | Current number of in-flight proposals. High values indicate backpressure from Raft consensus. |
| `admission.propose_queue.full` | Counter | 1 | Number of times the propose queue was full and proposals were rejected. **Alert if non-zero**. |

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

## Storage Disk Usage Metrics

Filesystem-level disk usage is tracked per volume via `syscall.Statfs`. A background collector samples usage at a regular interval (default 5s).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `storage.disk.volume.bytes` | Gauge | By | Disk space used on a storage volume |

**Attributes**:
- `volume`: Storage volume name

| Volume | Path | Description |
|--------|------|-------------|
| `wal` | `{walDir}/` | WAL volume containing spool + WAL data |
| `data` | `{dataDir}/` | Data volume containing the Pebble database |

## Caching & Attributes Metrics

### Numscript Cache Metrics

The Numscript cache stores parsed Numscript programs to avoid re-parsing identical scripts.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `numscript.cache.size` | Gauge | 1 | Number of scripts currently in the cache |

### Attribute Cache Metrics

The attribute cache stores computed attribute values (volumes, metadata) in memory to avoid disk lookups.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `cache.rotations` | Counter | 1 | Number of cache generation rotations |
| `cache.generation` | Gauge | 1 | Current cache generation number |
| `cache.size` | Gauge | 1 | Number of entries in the cache by attribute type |

**Attributes**:
- `type`: Attribute type (`input`, `output`, `account_metadata`, `ledger_metadata`, `reversions`, `idempotency_keys`)

**Attribute Types**:
| Type | Description |
|------|-------------|
| `input` | Account input volumes (credits) |
| `output` | Account output volumes (debits) |
| `account_metadata` | Account metadata key/value pairs |
| `ledger_metadata` | Ledger metadata key/value pairs |
| `reversions` | Transaction reversion status |
| `idempotency_keys` | Idempotency key mappings |

**Cache Generations**: The cache uses a dual-generation system where old data is gradually evicted. Each "rotation" promotes Gen0 to Gen1 and discards the old Gen1, triggered by raft index thresholds.

### Bloom Filter Metrics

Bloom filters provide probabilistic key existence checks to avoid unnecessary Pebble Gets during preloading. They are configured per attribute type via `--bloom-*` flags.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `bloom.lookups` | Counter | 1 | Total bloom filter checks (MayContain calls) |
| `bloom.negatives` | Counter | 1 | Checks that returned definitely-not-present (Pebble Get avoided) |
| `bloom.false_positives` | Counter | 1 | Checks that returned maybe-present but Pebble Get found nothing |
| `bloom.adds` | Counter | 1 | Keys added to the bloom filter |
| `bloom.ready` | Gauge | 1 | Readiness state (1 = ready, 0 = populating) |

**Attributes**:
- `type`: Attribute type (`volumes`, `metadata`, `idempotency`, `references`, `ledgers`, `boundaries`, `transactions`)

**Key ratios**:
- **Negative rate** = `negatives / lookups` — fraction of lookups that avoided Pebble I/O. Higher is better.
- **False positive rate** = `false_positives / (lookups - negatives)` — fraction of Pebble Gets that were unnecessary. Should stay below the configured `fpRate` (default 1%).

**Lifecycle**: Bloom filters are never persisted in checkpoints. At startup, they are rebuilt from a full Pebble attribute scan in the background. During this scan (`bloom.ready = 0`), MayContain always returns true (no optimization, no false negatives).

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

Located at `misc/devenv/monitoring-dashboards/config/dashboards/ledger-metrics.json`

The dashboard is organized into the following sections:

**System Section**:
- Logs per Second
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
- Reception Queue Throughput (by priority)
- Reception Queue Load Heatmap (by priority)
- Reception Queue Full Counter
- Pending Send Queue Throughput
- Pending Send Queue Load Heatmap
- Pending Send Queue Full Counter
- Per-Peer Send Queue Throughput (by priority)
- Per-Peer Send Queue Load Heatmap (by priority)
- Per-Peer Send Queue Full Counter
- Unreachable Channel Throughput
- Unreachable Channel Load Heatmap
- Unreachable Channel Full Counter
- Propose Queue Throughput
- Propose Queue Load Heatmap
- Propose Queue Full Counter
- Ping Latency
- Pending Responses
- Snapshot Creation

**Ready Loop Section**:
- Process Ready Entry Time Passed
- Applying Entries Time Passed
- Append Entries Time Passed
- Batch Size Distribution
- Applying Entries Percentiles
- Applying Entries Rate
- WAL Cache Update Time
- WAL Save Time
- WAL Append Batch Size
- Gating Wait Duration
- Readies During Gating

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

**Caching & Attributes Section**:
- Numscript Cache Size
- Cache Generation & Rotations
- Cache Size by Type

**Admission Section**:
- Preload Duration (by type)
- Preload Rate (by type)
- Command Duration Percentiles
- Propose Duration Percentiles
- Command Size Distribution
- Preload Keys Needed Rate (by type)
- Preload Cache Hit Ratio (%)
- Preload Store Reads vs Cache Hits (by type)
- Propose Queue Load

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

## Continuous Profiling with Pyroscope

Ledger v3 POC supports continuous profiling with [Grafana Pyroscope](https://grafana.com/docs/pyroscope/latest/), enabling deep performance analysis and bottleneck identification.

### Overview

Pyroscope collects profiling data (CPU, memory, goroutines, etc.) continuously, allowing you to:
- Identify performance bottlenecks in production
- Analyze CPU and memory usage patterns
- Debug contention issues (mutex, blocking)
- Correlate profiles with traces and metrics

### Configuration

Enable Pyroscope profiling via environment variables or command-line flags:

```bash
# Enable Pyroscope profiling
export PYROSCOPE_ENABLED=true
export PYROSCOPE_SERVER_ADDRESS=http://pyroscope:4040
export PYROSCOPE_APPLICATION_NAME=ledger-v3-poc

# Optional: Authentication for Grafana Cloud
export PYROSCOPE_AUTH_TOKEN=your-grafana-cloud-token
export PYROSCOPE_TENANT_ID=your-tenant-id

# Optional: Basic auth
export PYROSCOPE_BASIC_AUTH_USER=user
export PYROSCOPE_BASIC_AUTH_PASSWORD=password

# Optional: Additional tags (can be specified multiple times)
export PYROSCOPE_TAGS=env=production,region=us-east-1

# Optional: Profile types (default: cpu,alloc_objects,alloc_space,inuse_objects,inuse_space)
export PYROSCOPE_PROFILE_TYPES=cpu,alloc_objects,alloc_space,inuse_objects,inuse_space,goroutines,mutex_count,mutex_duration,block_count,block_duration

# Optional: Upload rate (default: 15s)
export PYROSCOPE_UPLOAD_RATE=15s

# Optional: Mutex and block profiling rates (default: 5)
export PYROSCOPE_MUTEX_PROFILE_FRACTION=5
export PYROSCOPE_BLOCK_PROFILE_RATE=5

# Optional: Disable GC runs between heap profiles
export PYROSCOPE_DISABLE_GC_RUNS=false
```

### Command-Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--pyroscope-enabled` | Enable Pyroscope profiling | `false` |
| `--pyroscope-server-address` | Pyroscope server address | `http://localhost:4040` |
| `--pyroscope-application-name` | Application name in Pyroscope | Service name |
| `--pyroscope-auth-token` | Auth token for Grafana Cloud | - |
| `--pyroscope-tenant-id` | Tenant ID for multi-tenant Pyroscope | - |
| `--pyroscope-basic-auth-user` | Basic auth username | - |
| `--pyroscope-basic-auth-password` | Basic auth password | - |
| `--pyroscope-upload-rate` | Profile upload interval | `15s` |
| `--pyroscope-tags` | Additional tags (key=value, repeatable) | - |
| `--pyroscope-profile-types` | Profile types to enable (repeatable) | See below |
| `--pyroscope-mutex-profile-fraction` | Mutex profile fraction | `5` |
| `--pyroscope-block-profile-rate` | Block profile rate | `5` |
| `--pyroscope-disable-gc-runs` | Disable GC runs between heap profiles | `false` |

### Profile Types

Available profile types:
- `cpu` - CPU usage
- `alloc_objects` - Number of allocated objects
- `alloc_space` - Total allocated memory
- `inuse_objects` - Objects currently in use
- `inuse_space` - Memory currently in use
- `goroutines` - Goroutine stacks
- `mutex_count` - Mutex contention count
- `mutex_duration` - Mutex contention duration
- `block_count` - Blocking operations count
- `block_duration` - Blocking operations duration

### Kubernetes Deployment

Add Pyroscope configuration to your Helm values:

```yaml
config:
  pyroscope:
    enabled: true
    serverAddress: "http://pyroscope.monitoring.svc.cluster.local:4040"
    applicationName: "ledger-v3-poc"
    tags: "env=production"
    profileTypes: "cpu,alloc_objects,alloc_space,inuse_objects,inuse_space"
```

For Grafana Cloud:

```yaml
config:
  pyroscope:
    enabled: true
    serverAddress: "https://profiles-prod-001.grafana.net"
    authToken: "${GRAFANA_CLOUD_PYROSCOPE_TOKEN}"
    tenantId: "your-tenant-id"
    applicationName: "ledger-v3-poc"
```

### Automatic Tags

The following tags are automatically added to all profiles:
- `node_id` - The Raft node ID

### Best Practices

1. **Start with default profile types**: CPU and memory profiles provide the most value with minimal overhead.

2. **Enable mutex/block profiling selectively**: These profiles add overhead and should only be enabled when debugging contention issues.

3. **Use appropriate upload rate**: 15 seconds is a good default. Shorter intervals provide more granularity but increase overhead.

4. **Tag your profiles**: Use tags to differentiate between environments, regions, or versions.

5. **Monitor overhead**: Continuous profiling adds ~1-2% CPU overhead. Monitor your application's resource usage after enabling.

### Integration with Grafana

When using Grafana Cloud or self-hosted Grafana with Pyroscope:

1. Add Pyroscope as a data source in Grafana
2. Use the Profiles panel to view flame graphs
3. Correlate profiles with traces using the same service name
4. Use the "Profiles Drilldown" plugin for advanced analysis

### Troubleshooting

**Profiles not appearing in Pyroscope:**
- Verify `PYROSCOPE_ENABLED=true`
- Check network connectivity to Pyroscope server
- Verify authentication credentials if using Grafana Cloud
- Check application logs for Pyroscope-related errors

**High overhead:**
- Reduce the number of profile types
- Increase upload rate
- Disable mutex and block profiling

## Next Steps

- [Deployment](./deployment.md) - Configure observability stack
- [Architecture](../technical/architecture/core/architecture.md) - Understand system components
- [Storage](../technical/architecture/storage/storage.md) - Storage configuration and tuning
