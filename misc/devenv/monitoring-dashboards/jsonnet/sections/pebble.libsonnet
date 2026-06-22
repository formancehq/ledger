// Pebble section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';
local queries = import '../lib/queries.libsonnet';

panels.row('Pebble', 165, [
  panels.timeseries(
    'Flush / second',
    { h: 8, w: 8, x: 0, y: 88 },
    [
      { expr: 'sum by (service.node_id, status, reason) (rate({__name__="pebble.flush.total", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}[$__rate_interval]))', legendFormat: 'Node {{service.node_id}} {{db}} {{status}} {{reason}}' },
    ],
    description=|||
      Number of Pebble flush operations per second. Flushes write data from memory (memtable) to disk (SSTable).
      
      Flushes are triggered when:
      - Memtable reaches capacity
      - Manual flush requested
      - Write stall prevention
      
      High flush rates indicate heavy write activity. Monitor flush duration for performance.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#flush-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Flush duration (ms)',
    { h: 8, w: 8, x: 8, y: 88 },
    [
      { expr: 'histogram_quantile(0.50, sum by (le, service.node_id) (rate({__name__="pebble.flush.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store"}[$__rate_interval])))', legendFormat: 'Node {{service.node_id}} p50 {{db}}' },
      { expr: 'histogram_quantile(0.95, sum by (le, service.node_id) (rate({__name__="pebble.flush.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store"}[$__rate_interval])))', legendFormat: 'Node {{service.node_id}} p95 {{db}}' },
      { expr: 'histogram_quantile(0.99, sum by (le, service.node_id) (rate({__name__="pebble.flush.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store"}[$__rate_interval])))', legendFormat: 'Node {{service.node_id}} p99 {{db}}' },
      { expr: queries.histogramAvg('pebble.flush.duration.milliseconds', by=['service.node_id'], selector='"scope.name"="pebble.runtime_store"'), legendFormat: 'Node {{service.node_id}} mean {{db}}' },
    ], unit='ms',
    description=|||
      Pebble flush duration percentiles (P50, P95, P99) in milliseconds.
      
      Flush duration measures how long it takes to write memtable contents to disk. High values indicate:
      - Slow disk I/O
      - Large memtables
      - Disk contention
      
      P99 spikes may correlate with write stalls. Consider NVMe storage for better performance.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#flush-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Flush input bytes / second',
    { h: 8, w: 8, x: 16, y: 88 },
    [
      { expr: 'sum by (service.node_id) (rate({__name__="pebble.flush.input.bytes_sum", "scope.name"="pebble.runtime_store"}[$__rate_interval]))', legendFormat: 'Node {{service.node_id}}' },
    ], unit='Bps',
    description=|||
      Rate of bytes flushed from memtables to SSTables per second.
      
      This indicates write amplification and disk write throughput. High values mean:
      - Heavy write workload
      - Good throughput
      - High disk I/O utilization
      
      Compare with flush duration to understand I/O efficiency.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#flush-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Compactions / second',
    { h: 8, w: 8, x: 0, y: 96 },
    [
      { expr: 'sum by (service.node_id, db, status, reason) (rate({__name__="pebble.compaction.total", "scope.name"="pebble.runtime_store"}[$__rate_interval]))', legendFormat: 'Node {{service.node_id}}: {{status}} {{reason}}' },
    ],
    description=|||
      Number of Pebble compaction operations per second. Compactions merge and reorganize SSTables.
      
      Compaction purposes:
      - Merge overlapping keys
      - Reclaim deleted space
      - Optimize read performance
      - Level promotion
      
      High compaction rates indicate active data reorganization. Watch compaction duration for bottlenecks.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#compaction-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Compaction duration (ms)',
    { h: 8, w: 8, x: 8, y: 96 },
    [
      { expr: 'histogram_quantile(0.50, sum by (service.node_id, le) (rate({__name__="pebble.compaction.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store"}[$__rate_interval])))', legendFormat: 'Node {{service.node_id}}: p50' },
      { expr: 'histogram_quantile(0.95, sum by (service.node_id, le) (rate({__name__="pebble.compaction.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store"}[$__rate_interval])))', legendFormat: 'Node {{service.node_id}}: p95 ' },
      { expr: 'histogram_quantile(0.99, sum by (service.node_id, le) (rate({__name__="pebble.compaction.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store"}[$__rate_interval])))', legendFormat: 'Node {{service.node_id}}: p99' },
      { expr: queries.histogramAvg('pebble.compaction.duration.milliseconds', by=['service.node_id'], selector='"scope.name"="pebble.runtime_store"'), legendFormat: 'Node {{service.node_id}}: mean' },
    ], unit='ms',
    description=|||
      Pebble compaction duration percentiles (P50, P95, P99) in milliseconds.
      
      Compaction duration depends on:
      - Amount of data being compacted
      - Disk I/O speed
      - CPU for decompression/compression
      
      Long compactions may temporarily impact read performance. Very high P99 values warrant investigation.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#compaction-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Compaction errors / second',
    { h: 8, w: 8, x: 16, y: 96 },
    [
      { expr: 'sum by (service.node_id, reason) (rate({__name__="pebble.compaction.total", status="error", "scope.name"="pebble.runtime_store"}[$__rate_interval]))', legendFormat: 'Node {{service.node_id}}: {{reason}}' },
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
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#compaction-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Write stall active (max)',
    { h: 8, w: 8, x: 0, y: 104 },
    [
      { expr: 'max by (service.node_id, reason) ({__name__="pebble.write_stall.active"})', legendFormat: '{{service.node_id}} / {{reason}}' },
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
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#write-stall-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Write stalls / second',
    { h: 8, w: 8, x: 8, y: 104 },
    [
      { expr: 'sum by (db, reason) (rate({__name__="pebble.write_stall.total"}[$__rate_interval]))', legendFormat: '{{db}} / {{reason}}' },
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
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#write-stall-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Write stall duration',
    { h: 8, w: 8, x: 16, y: 104 },
    [
      { expr: 'histogram_quantile(0.50, sum by (le, db) (rate({__name__="pebble.write_stall.duration.milliseconds_bucket"}[$__rate_interval])))', legendFormat: 'p50 {{db}}' },
      { expr: 'histogram_quantile(0.95, sum by (le, db) (rate({__name__="pebble.write_stall.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store"}[$__rate_interval])))', legendFormat: 'p95 {{db}}' },
      { expr: 'histogram_quantile(0.99, sum by (le, db) (rate({__name__="pebble.write_stall.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store"}[$__rate_interval])))', legendFormat: 'p99 {{db}}' },
      { expr: queries.histogramAvg('pebble.write_stall.duration.milliseconds', by=['service.node_id'], selector='"scope.name"="pebble.runtime_store"'), legendFormat: 'mean' },
    ], unit='ms',
    description=|||
      Duration of write stalls (P50, P95, P99) in seconds. Shows how long writes are blocked.
      
      Stall duration directly impacts:
      - Transaction latency (blocked during stall)
      - Throughput (no progress during stall)
      - Client timeouts (if stall exceeds timeout)
      
      Long stalls (>1s) are critical. High P99 values indicate occasional severe blocking.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#write-stall-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'VFS IOPS (read / write)',
    { h: 8, w: 8, x: 0, y: 112 },
    [
      { expr: 'rate({__name__="pebble.vfs.read.ops", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}} — reads/s' },
      { expr: 'rate({__name__="pebble.vfs.write.ops", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}} — writes/s' },
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
      { expr: 'rate({__name__="pebble.vfs.sync.ops", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}} — syncs/s' },
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
      { expr: '{__name__="pebble.vfs.read.ops", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}', legendFormat: 'Node {{service.node_id}} — reads' },
      { expr: '{__name__="pebble.vfs.write.ops", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}', legendFormat: 'Node {{service.node_id}} — writes' },
      { expr: '{__name__="pebble.vfs.sync.ops", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}', legendFormat: 'Node {{service.node_id}} — syncs' },
    ],
    description='Cumulative VFS read, write, and sync operations. Useful for comparing total I/O volume across nodes.',
  ),

  panels.timeseries(
    'Disk slow events / second',
    { h: 8, w: 12, x: 0, y: 120 },
    [
      { expr: 'sum by (op) (rate({__name__="pebble.disk_slow.total", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}[$__rate_interval]))', legendFormat: '{{op}}' },
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
      { expr: 'histogram_quantile(0.50, sum by (le, op) (rate({__name__="pebble.disk_slow.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}[$__rate_interval])))', legendFormat: 'p50 {{op}}' },
      { expr: 'histogram_quantile(0.95, sum by (le, op) (rate({__name__="pebble.disk_slow.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}[$__rate_interval])))', legendFormat: 'p95 {{op}}' },
      { expr: 'histogram_quantile(0.99, sum by (le, op) (rate({__name__="pebble.disk_slow.duration.milliseconds_bucket", "scope.name"="pebble.runtime_store", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}[$__rate_interval])))', legendFormat: 'p99 {{op}}' },
    ], unit='ms',
    description=|||
      Duration of slow disk operations (P50, P95, P99). Shows how long disk operations have been stalled when Pebble detects slowness.
      
      High values (>1s) indicate severe disk issues. Correlate with write stall metrics to understand impact on transaction latency.
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),
])
