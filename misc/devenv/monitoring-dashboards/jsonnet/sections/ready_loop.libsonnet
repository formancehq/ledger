// Ready Loop section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';
local queries = import '../lib/queries.libsonnet';

panels.row('Ready Loop', 2, [
  panels.timeseries(
    'Process ready entry time passed',
    { h: 8, w: 12, x: 0, y: 3 },
    [
      { expr: 'rate({"raft.process_entry_sum", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}}' },
    ], unit='µs',
    description=|||
      Time spent processing Raft 'Ready' state (in microseconds). The Ready loop is the main Raft processing cycle.
      
      This includes:
      - Sending messages to peers
      - Persisting entries to WAL
      - Applying committed entries
      - Advancing Raft state
      
      High values indicate the Raft loop is slow, which limits throughput.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#node-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Process ready entry rate',
    { h: 8, w: 12, x: 0, y: 11 },
    [
      { expr: 'sum(rate(raft.process_entry_count{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Node {{service.node_id}}' },
    ], unit='ops',
    description=|||
      Number of Raft ready entries processed per second.
      
      This metric shows the throughput of the Raft ready processing loop. Higher values indicate more activity in the Raft consensus layer.
      
      Correlate with 'Process ready entry time passed' to understand both throughput and latency.
   |||,
  ),

  panels.timeseries(
    'Process ready entry latency',
    { h: 8, w: 12, x: 12, y: 19 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(raft.process_entry_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P99' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.process_entry_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P95' },
      { expr: 'histogram_quantile(0.75, sum(rate(raft.process_entry_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P75' },
      { expr: queries.histogramAvg('raft.process_entry', by=['service.node_id']), legendFormat: 'Node {{service.node_id}}: Avg' },
    ], unit='µs',
    description=|||
      Percentile distribution of time spent processing Raft ready entries (in microseconds).
      
      Shows P99, P95, P75 and average latency for processing each ready batch from the Raft consensus layer.
      
      High latencies may indicate:
      - Heavy message processing load
      - Slow WAL writes
      - FSM apply bottlenecks
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Append entries time passed',
    { h: 8, w: 12, x: 0, y: 27 },
    [
      { expr: 'sum(rate(raft.append_entries_sum{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Node {{service.node_id}}' },
    ], unit='µs',
    description=|||
      Time spent appending entries to the Write-Ahead Log (WAL) in microseconds.
      
      WAL writes must complete before entries can be committed. High latency indicates:
      - Slow disk I/O
      - Disk saturation
      - Need for faster storage
      
      This directly impacts transaction latency and throughput.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#node-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Append entries latency',
    { h: 8, w: 12, x: 12, y: 35 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(raft.append_entries_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P99' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.append_entries_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P95' },
      { expr: 'histogram_quantile(0.75, sum(rate(raft.append_entries_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P75' },
      { expr: queries.histogramAvg('raft.append_entries', by=['service.node_id']), legendFormat: 'Node {{service.node_id}}: Avg' },
    ], unit='µs',
    description=|||
      Latency percentiles (P75, P95, P99) for appending entries to the WAL, in microseconds.
      
      This measures the time spent writing Raft log entries to durable storage:
      - P75: 75% of append operations complete within this time
      - P95: 95% of append operations complete within this time
      - P99: 99% of append operations complete within this time
      
      High latencies indicate slow disk I/O or storage bottlenecks.
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Committed entries per Ready',
    { h: 8, w: 12, x: 0, y: 43 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(raft.ready.committed_entries_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P99' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.ready.committed_entries_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P95' },
      { expr: 'histogram_quantile(0.75, sum(rate(raft.ready.committed_entries_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P75' },
      { expr: queries.histogramAvg('raft.ready.committed_entries', by=['service.node_id']), legendFormat: 'Node {{service.node_id}}: Avg' },
    ], unit='short',
    description=|||
      Number of committed entries per Raft Ready batch.
      
      Shows P99, P95, P75 percentiles and average count of committed entries received in each Ready from the Raft consensus layer.
      
      Higher values indicate more batching efficiency. Low values (close to 1) indicate entries are being processed one at a time.
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'WAL cache update time',
    { h: 8, w: 12, x: 0, y: 51 },
    [
      { expr: 'rate(wal.append.cache.duration_sum{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}}' },
    ], unit='µs',
    description=|||
      Time spent updating the in-memory cache (s.entries) during WAL append operations, in microseconds.
      
      This measures the time to update the in-memory entry cache before persisting to disk. Should be very fast as it's purely in-memory operations.
      
      High values may indicate:
      - Memory pressure
      - Large entry batches
      - GC pauses
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'WAL save time',
    { h: 8, w: 12, x: 12, y: 59 },
    [
      { expr: 'rate(wal.append.save.duration_sum{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}}' },
    ], unit='µs',
    description=|||
      Time spent saving entries to the Write-Ahead Log (WAL) on disk, in microseconds.
      
      This is the actual disk I/O time for persisting entries. High values indicate:
      - Slow disk I/O
      - Disk saturation
      - Need for faster storage (SSD/NVMe)
      
      This metric directly impacts transaction latency as WAL writes must complete before entries can be committed.
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'WAL append batch size',
    { h: 8, w: 12, x: 0, y: 67 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(wal.append.batch_size_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P99' },
      { expr: 'histogram_quantile(0.95, sum(rate(wal.append.batch_size_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P95' },
      { expr: 'histogram_quantile(0.75, sum(rate(wal.append.batch_size_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P75' },
      { expr: queries.histogramAvg('wal.append.batch_size', by=['service.node_id']), legendFormat: 'Node {{service.node_id}}: Avg' },
    ], unit='short',
    description=|||
      Distribution of batch sizes when appending entries to the WAL (P75, P95, P99 percentiles).
      
      This shows how many entries are appended at once:
      - Batch size 1: Single entry per append (less efficient)
      - Batch size >1: Multiple entries batched together (better throughput)
      
      Higher batch sizes indicate the system is efficiently batching entries under load.
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Ready wait duration',
    { h: 8, w: 12, x: 12, y: 75 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(raft.node.ready.wait_duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}} p99' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.node.ready.wait_duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}} p95' },
      { expr: 'histogram_quantile(0.50, sum(rate(raft.node.ready.wait_duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}} p50' },
      { expr: queries.histogramAvg('raft.node.ready.wait_duration', by=['service.node_id']), legendFormat: 'Node {{service.node_id}} avg' },
    ], unit='µs',
    description=|||
      Time spent waiting for a Ready from the Raft state machine. This measures the idle time between processing consecutive Ready batches.
      
      High wait times indicate:
      - Low activity (normal when idle)
      - System is not under load
      
      Low wait times indicate:
      - High throughput
      - System is processing many transactions
   |||,
  ),

  panels.timeseries(
    'Ready rate',
    { h: 8, w: 12, x: 0, y: 83 },
    [
      { expr: 'sum(rate(raft.node.ready.wait_duration_count{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Node {{service.node_id}}' },
    ], unit='ops',
    description=|||
      Number of Ready events processed per second. Each Ready contains a batch of entries to apply to the state machine.
      
      Higher rates indicate:
      - More transactions being processed
      - More Raft activity (heartbeats, elections, etc.)
   |||,
  ),

  panels.timeseries(
    'Ready wait cumulated',
    { h: 8, w: 12, x: 12, y: 91 },
    [
      { expr: 'rate({"raft.node.ready.wait_duration_sum", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}[$__rate_interval])', legendFormat: '__auto' },
    ], unit='µs', opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Ready terminated wait duration',
    { h: 8, w: 12, x: 12, y: 107 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(raft.node.ready_terminated.wait_duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}} p99' },
      { expr: 'histogram_quantile(0.50, sum(rate(raft.node.ready_terminated.wait_duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}} p50' },
    ], unit='µs',
    description=|||
      Time the processReadies goroutine spends waiting for the orchestrate loop to consume readyTerminated (in microseconds).
      
      High values indicate the orchestrate loop is slow to pick up completed Readies, which means the ready processing pipeline is stalled waiting for Advance.
   |||,
  ),

  panels.timeseries(
    'Unspool duration',
    { h: 7, w: 12, x: 0, y: 115 },
    [
      { expr: 'rate(raft.node.unspool.duration_sum{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}}' },
    ], unit='µs',
    description=|||
      Time spent in unspoolAndResume after a maintenance task (snapshot or checkpoint restore). During this time, spooled entries are replayed into the store.
      
      Long unspool times indicate many entries were spooled during the maintenance task, which means the system was under heavy write load when the snapshot was triggered.
   |||, opts={ drawStyle: 'bars', fillOpacity: 80 },
  ),

  panels.timeseries(
    'Gating wait duration',
    { h: 7, w: 12, x: 0, y: 129 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(raft.node.gating.wait_duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}} p99' },
      { expr: 'histogram_quantile(0.50, sum(rate(raft.node.gating.wait_duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}} p50' },
    ], unit='µs',
    description=|||
      Time spent waiting for gatingTerminated (maintenance task completion) in the processReadies goroutine.
      
      This measures how long the ready processing pipeline is stalled while a snapshot or checkpoint restore is in progress.
   |||,
  ),

  panels.timeseries(
    'Readies during gating',
    { h: 7, w: 12, x: 12, y: 136 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(raft.node.gating.readies_processed_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}} p99' },
      { expr: 'histogram_quantile(0.50, sum(rate(raft.node.gating.readies_processed_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}} p50' },
    ],
    description=|||
      Number of Raft Readies processed during each gating period (snapshot/checkpoint restore).
      
      Higher values indicate more Readies were spooled instead of applied directly to the FSM while the maintenance task was running.
   |||,
  ),

  panels.timeseries(
    'Gating timeline (snapshot creation vs replay spool)',
    { h: 7, w: 24, x: 0, y: 143 },
    [
      { expr: 'rate(raft.node.maintenance.snapshot_creation.duration_sum{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}}: Snapshot creation' },
      { expr: 'rate(raft.node.maintenance.replay_spool.duration_sum{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}}: Replay spool' },
    ], unit='µs',
    description=|||
      Gating timeline breakdown: snapshot creation time vs replay spool time (stacked bars).
      
      Shows how time is spent during each maintenance task (snapshot). The total of both series approximates the gating wait duration.
      
      - Snapshot creation (red): serializing FSM state + writing to WAL
      - Replay spool (orange): replaying entries that were spooled during the snapshot
      
      If snapshot creation dominates, the bottleneck is serialization/I/O.
      If replay spool dominates, many entries accumulated during the snapshot window.
   |||, opts={ stackMode: 'normal', drawStyle: 'bars', fillOpacity: 80 },
  ),

  panels.timeseries(
    'FSM rotation duration',
    { h: 7, w: 8, x: 0, y: 150 },
    [
      { expr: 'rate(raft.fsm.rotation.duration_sum{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}}' },
    ], unit='µs',
    description=|||
      Time spent in generation rotation (boundary flush) during ApplyEntries.
      
      Rotation happens when the raft index crosses a generation threshold. During rotation, dirty boundaries are flushed to PebbleDB inline in the critical path.
   |||, opts={ drawStyle: 'bars', fillOpacity: 80 },
  ),

  panels.timeseries(
    'FSM batch commit duration (p50, p99)',
    { h: 7, w: 8, x: 16, y: 157 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(raft.fsm.batch_commit.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}}: p99' },
      { expr: 'histogram_quantile(0.50, sum(rate(raft.fsm.batch_commit.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'Node {{service.node_id}}: p50' },
    ], unit='µs',
    description=|||
      Time spent in PebbleDB batch.Commit() during ApplyEntries (p50 and p99).
      
      Measures the final I/O cost of committing all accumulated writes to PebbleDB.
      
      Spikes correlate with large batches, PebbleDB compaction pressure, or disk I/O latency.
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),
])
