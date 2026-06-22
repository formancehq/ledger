// Applier section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';
local queries = import '../lib/queries.libsonnet';

panels.row('Applier', 164, [
  panels.timeseries(
    'Applying entries time passed',
    { h: 8, w: 12, x: 12, y: 1 },
    [
      { expr: 'sum(rate(raft.apply_entries.duration_sum{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Node {{service.node_id}}' },
    ], unit='µs',
    description=|||
      Total time spent applying committed entries to the FSM (in microseconds per second).
      
      This is where transactions are actually processed and balances updated. High values indicate:
      - Complex transactions taking longer
      - Storage (Pebble) write bottlenecks
      - High transaction volume
      
      Correlate with 'Applying entries rate' and Pebble metrics.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#node-metrics
   |||,
  ),

  panels.timeseries(
    'Apply entries Batch size distribution',
    { h: 8, w: 12, x: 12, y: 9 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(raft.apply_entries.batch_size_distribution_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P99' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.apply_entries.batch_size_distribution_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P95' },
      { expr: 'histogram_quantile(0.75, sum(rate(raft.apply_entries.batch_size_distribution_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P75' },
      { expr: queries.histogramAvg('raft.apply_entries.batch_size_distribution', by=['service.node_id']), legendFormat: 'Node {{service.node_id}}: Avg' },
    ],
    description=|||
      Distribution of batch sizes when applying entries (P75, P95, P99 percentiles).
      
      Larger batches are more efficient:
      - Batch size 1: Each entry applied individually (less efficient)
      - Batch size >1: Multiple entries applied together (better throughput)
      
      Higher percentiles indicate the system is effectively batching under load, which improves throughput.
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#node-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Applying entries percentiles',
    { h: 8, w: 12, x: 0, y: 17 },
    [
      { expr: 'histogram_quantile(0.99, sum(rate(raft.apply_entries.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P99' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.apply_entries.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P95' },
      { expr: 'histogram_quantile(0.75, sum(rate(raft.apply_entries.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'Node {{service.node_id}}: P75' },
      { expr: queries.histogramAvg('raft.apply_entries.duration', by=['service.node_id']), legendFormat: 'Node {{service.node_id}}: Avg' },
    ], unit='µs',
    description=|||
      Latency percentiles (P75, P95, P99) for applying committed entries to the FSM, in microseconds.
      
      This is the core transaction processing latency:
      - P75: 75% of batches complete within this time
      - P95: 95% of batches complete within this time
      - P99: 99% of batches complete within this time
      
      High P99 values may indicate occasional slow operations (GC pauses, disk flushes).
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#node-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Applying entries rate',
    { h: 8, w: 12, x: 12, y: 25 },
    [
      { expr: 'rate(raft.apply_entries.duration_count{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])', legendFormat: 'Node {{service.node_id}}' },
    ], unit='ops',
    description=|||
      Rate of apply operations per second (how often the FSM processes batches).
      
      Combine with 'Batch size distribution' to understand throughput:
      - High rate + low batch size = many small batches
      - Lower rate + high batch size = fewer but larger batches (more efficient)
      
      Total transactions/sec = rate × average batch size
      
      See: https://github.com/formancehq/ledger/v3/blob/master/docs/metrics.md#node-metrics
   |||, opts={ fillOpacity: 0, showPoints: 'auto' },
  ),

  panels.timeseries(
    'Applier Batch Wait (p50, p95, p99)',
    { h: 8, w: 24, x: 0, y: 109 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(raft.applier.batch_wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'p50 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.applier.batch_wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'p95 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(raft.applier.batch_wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'p99 - Node {{service.node_id}}' },
    ], unit='µs', opts={ showPoints: 'auto' },
  ),

  panels.timeseries(
    'Prepare Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 0, y: 117 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(raft.fsm.prepare.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p50' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.fsm.prepare.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p95' },
      { expr: 'histogram_quantile(0.99, sum(rate(raft.fsm.prepare.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p99' },
    ], unit='µs',
  ),

  panels.timeseries(
    'Commit Wait Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 12, y: 117 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(raft.applier.commit_wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p50' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.applier.commit_wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p95' },
      { expr: 'histogram_quantile(0.99, sum(rate(raft.applier.commit_wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p99' },
    ], unit='µs',
    description='Time spent waiting for the previous batch\'s async commit to finish before starting the next prepare. Near zero = pipelining is effective (commit finishes before next batch arrives). Near commit duration = fully I/O-bound, no pipelining benefit.',
  ),

  panels.timeseries(
    'Prepare Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 0, y: 125 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(raft.fsm.prepare.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p50' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.fsm.prepare.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p95' },
      { expr: 'histogram_quantile(0.99, sum(rate(raft.fsm.prepare.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p99' },
    ], unit='µs',
  ),

  panels.timeseries(
    'Commit Wait Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 12, y: 125 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(raft.applier.commit_wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p50' },
      { expr: 'histogram_quantile(0.95, sum(rate(raft.applier.commit_wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p95' },
      { expr: 'histogram_quantile(0.99, sum(rate(raft.applier.commit_wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p99' },
    ], unit='µs',
    description='Time spent waiting for the previous batch\'s async commit to finish. Near zero = pipelining effective.',
  ),
])
