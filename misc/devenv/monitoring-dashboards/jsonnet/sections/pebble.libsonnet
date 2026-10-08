// Pebble section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';
local queries = import '../lib/queries.libsonnet';

panels.row('Pebble', 165, [
  panels.timeseries(
    'Flush / second',
    { h: 8, w: 8, x: 0, y: 88 },
    [
      { expr: 'sum by (formance.ledger.node.id, status, reason) (rate({__name__="pebble.flushes", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval]))', legendFormat: 'Node {{formance.ledger.node.id}} {{status}} {{reason}}' },
    ],
    description=|||
      Number of Pebble flush operations per second. Flushes write data from memory (memtable) to disk (SSTable).
      
      Flushes are triggered when:
      - Memtable reaches capacity
      - Manual flush requested
      - Write stall prevention
      
      High flush rates indicate heavy write activity. Monitor flush duration for performance.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#flush-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Flush duration',
    { h: 8, w: 8, x: 8, y: 88 },
    [
      { expr: 'histogram_quantile(0.50, sum by (le, formance.ledger.node.id) (rate({__name__="pebble.flush.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}} p50' },
      { expr: 'histogram_quantile(0.95, sum by (le, formance.ledger.node.id) (rate({__name__="pebble.flush.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}} p95' },
      { expr: 'histogram_quantile(0.99, sum by (le, formance.ledger.node.id) (rate({__name__="pebble.flush.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}} p99' },
      { expr: queries.histogramAvg('pebble.flush.duration', by=['formance.ledger.node.id']), legendFormat: 'Node {{formance.ledger.node.id}} mean' },
    ], unit='s',
    description=|||
      Pebble flush duration percentiles (P50, P95, P99) in seconds.
      
      Flush duration measures how long it takes to write memtable contents to disk. High values indicate:
      - Slow disk I/O
      - Large memtables
      - Disk contention
      
      P99 spikes may correlate with write stalls. Consider NVMe storage for better performance.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#flush-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Flush input bytes / second',
    { h: 8, w: 8, x: 16, y: 88 },
    [
      { expr: queries.histogramSumRate('pebble.flush.input.size', by=['formance.ledger.node.id']), legendFormat: 'Node {{formance.ledger.node.id}}' },
    ], unit='Bps',
    description=|||
      Rate of bytes flushed from memtables to SSTables per second.
      
      This indicates write amplification and disk write throughput. High values mean:
      - Heavy write workload
      - Good throughput
      - High disk I/O utilization
      
      Compare with flush duration to understand I/O efficiency.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#flush-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Compactions / second',
    { h: 8, w: 8, x: 0, y: 96 },
    [
      { expr: 'sum by (formance.ledger.node.id, status, reason) (rate({__name__="pebble.compactions", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval]))', legendFormat: 'Node {{formance.ledger.node.id}}: {{status}} {{reason}}' },
    ],
    description=|||
      Number of Pebble compaction operations per second. Compactions merge and reorganize SSTables.
      
      Compaction purposes:
      - Merge overlapping keys
      - Reclaim deleted space
      - Optimize read performance
      - Level promotion
      
      High compaction rates indicate active data reorganization. Watch compaction duration for bottlenecks.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#compaction-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Compaction duration',
    { h: 8, w: 8, x: 8, y: 96 },
    [
      { expr: 'histogram_quantile(0.50, sum by (formance.ledger.node.id, le) (rate({__name__="pebble.compaction.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}}: p50' },
      { expr: 'histogram_quantile(0.95, sum by (formance.ledger.node.id, le) (rate({__name__="pebble.compaction.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}}: p95 ' },
      { expr: 'histogram_quantile(0.99, sum by (formance.ledger.node.id, le) (rate({__name__="pebble.compaction.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}}: p99' },
      { expr: queries.histogramAvg('pebble.compaction.duration', by=['formance.ledger.node.id']), legendFormat: 'Node {{formance.ledger.node.id}}: mean' },
    ], unit='s',
    description=|||
      Pebble compaction duration percentiles (P50, P95, P99) in seconds.
      
      Compaction duration depends on:
      - Amount of data being compacted
      - Disk I/O speed
      - CPU for decompression/compression
      
      Long compactions may temporarily impact read performance. Very high P99 values warrant investigation.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#compaction-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Compaction errors / second',
    { h: 8, w: 8, x: 16, y: 96 },
    [
      { expr: 'sum by (formance.ledger.node.id, reason) (rate({__name__="pebble.compactions", status="error", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval]))', legendFormat: 'Node {{formance.ledger.node.id}}: {{reason}}' },
    ],
    description=|||
      Rate of compaction errors per second.
      
      ALERT: Any non-zero value requires immediate investigation!
      
      Compaction errors may indicate:
      - Disk corruption
      - Out of disk space
      - Hardware failure
      - File system issues
      
      Check system logs and disk health immediately.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#compaction-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Write stall active (max)',
    { h: 8, w: 8, x: 0, y: 104 },
    [
      { expr: 'max by (formance.ledger.node.id, reason) ({__name__="pebble.write_stall.active", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"})', legendFormat: '{{formance.ledger.node.id}} / {{reason}}' },
    ],
    description=|||
      Shows if Pebble is currently stalling writes (1 = stalling, 0 = normal).
      
      CRITICAL ALERT: Value of 1 means transactions are being delayed!
      
      Write stalls occur when:
      - L0 has too many files
      - Memtable count too high
      - Compaction backlog too large
      
      Immediate actions:
      - Check disk I/O utilization
      - Consider faster storage (NVMe)
      - Review compaction settings
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#write-stall-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Write stalls / second',
    { h: 8, w: 8, x: 8, y: 104 },
    [
      { expr: 'sum by (formance.ledger.node.id, reason) (rate({__name__="pebble.write_stalls", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval]))', legendFormat: 'Node {{formance.ledger.node.id}} / {{reason}}' },
    ],
    description=|||
      Number of write stall events per second. Each stall temporarily blocks write operations.
      
      Write stalls happen when Pebble's internal queues fill up:
      - memtable: Too many memtables waiting to flush
      - l0: Too many L0 SSTables
      - flush_slowdown: Flush falling behind
      
      Frequent stalls indicate storage cannot keep up with write rate. Consider:
      - Faster storage (NVMe SSD)
      - Reduced write rate
      - Tuning Pebble settings
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#write-stall-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Write stall duration',
    { h: 8, w: 8, x: 16, y: 104 },
    [
      { expr: 'histogram_quantile(0.50, sum by (le, formance.ledger.node.id) (rate({__name__="pebble.write_stall.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}} p50' },
      { expr: 'histogram_quantile(0.95, sum by (le, formance.ledger.node.id) (rate({__name__="pebble.write_stall.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}} p95' },
      { expr: 'histogram_quantile(0.99, sum by (le, formance.ledger.node.id) (rate({__name__="pebble.write_stall.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}} p99' },
      { expr: queries.histogramAvg('pebble.write_stall.duration', by=['formance.ledger.node.id']), legendFormat: 'Node {{formance.ledger.node.id}} mean' },
    ], unit='s',
    description=|||
      Duration of write stalls (P50, P95, P99) in seconds. Shows how long writes are blocked.
      
      Stall duration directly impacts:
      - Transaction latency (blocked during stall)
      - Throughput (no progress during stall)
      - Client timeouts (if stall exceeds timeout)
      
      Long stalls (>1s) are critical. High P99 values indicate occasional severe blocking.
      
      See: https://github.com/formancehq/ledger/blob/release/v3.0/docs/ops/monitoring.md#write-stall-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'VFS IOPS (read / write)',
    { h: 8, w: 8, x: 0, y: 112 },
    [
      { expr: 'rate({__name__="pebble.vfs.read.ops", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])', legendFormat: 'Node {{formance.ledger.node.id}} — reads/s' },
      { expr: 'rate({__name__="pebble.vfs.write.ops", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])', legendFormat: 'Node {{formance.ledger.node.id}} — writes/s' },
    ], unit='ops',
    description=|||
      VFS-level read and write operations per second. Counted at the Pebble VFS layer — each Read()/ReadAt() or Write()/WriteAt() syscall increments the counter.
      
      This is the closest application-level approximation to disk IOPS.
   |||,
  ),

  panels.timeseries(
    'VFS Sync ops/s',
    { h: 8, w: 8, x: 8, y: 112 },
    [
      { expr: 'rate({__name__="pebble.vfs.sync.ops", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])', legendFormat: 'Node {{formance.ledger.node.id}} — syncs/s' },
    ], unit='ops',
    description=|||
      VFS-level sync (fsync) operations per second. Each Sync(), SyncTo(), or SyncData() call is counted.
      
      Sync operations are the most expensive I/O — they force data to stable storage. High sync rates indicate WAL syncs or flush finalization.
   |||,
  ),

  panels.timeseries(
    'VFS Total ops (cumulative)',
    { h: 8, w: 8, x: 16, y: 112 },
    [
      { expr: '{__name__="pebble.vfs.read.ops", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}} — reads' },
      { expr: '{__name__="pebble.vfs.write.ops", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}} — writes' },
      { expr: '{__name__="pebble.vfs.sync.ops", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}} — syncs' },
    ],
    description='Cumulative VFS read, write, and sync operations. Useful for comparing total I/O volume across nodes.',
  ),

  panels.timeseries(
    'Disk slow events / second',
    { h: 8, w: 12, x: 0, y: 120 },
    [
      { expr: 'sum by (formance.ledger.node.id, op) (rate({__name__="pebble.disk_slow.operations", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval]))', legendFormat: 'Node {{formance.ledger.node.id}} / {{op}}' },
    ],
    description=|||
      Number of slow disk operations detected by Pebble per second. A slow disk event fires when a write operation exceeds Pebble's disk slowness threshold.
      
      This is a leading indicator: disk_slow events typically precede write stalls. If this metric spikes, investigate disk I/O before it escalates.
      
      Consider enabling --pebble-wal-failover-dir to mitigate transient disk slowness.
   |||, opts={ showPoints: 'auto' },
  ),

  panels.timeseries(
    'Disk slow duration',
    { h: 8, w: 12, x: 12, y: 120 },
    [
      { expr: 'histogram_quantile(0.50, sum by (le, formance.ledger.node.id, op) (rate({__name__="pebble.disk_slow.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}} p50 {{op}}' },
      { expr: 'histogram_quantile(0.95, sum by (le, formance.ledger.node.id, op) (rate({__name__="pebble.disk_slow.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}} p95 {{op}}' },
      { expr: 'histogram_quantile(0.99, sum by (le, formance.ledger.node.id, op) (rate({__name__="pebble.disk_slow.duration_bucket", "k8s.namespace.name"=~"$namespace", "formance.ledger.cluster.name"=~"$cluster", "formance.ledger.node.id"=~"$node"}[$__rate_interval])))', legendFormat: 'Node {{formance.ledger.node.id}} p99 {{op}}' },
    ], unit='s',
    description=|||
      Duration of slow disk operations (P50, P95, P99). Shows how long disk operations have been stalled when Pebble detects slowness.
      
      High values (>1s) indicate severe disk issues. Correlate with write stall metrics to understand impact on transaction latency.
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),
])
