// Mirror section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Mirror', 171, [
  panels.timeseries(
    'Logs Ingested/s',
    { h: 8, w: 12, x: 0, y: 107 },
    [
      { expr: 'sum(rate(mirror.logs.ingested{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (ledger)', legendFormat: '{{ledger}}' },
    ], unit='ops',
    description='Rate of v2 logs fetched and ingested by the mirror worker, broken down by ledger.',
  ),

  panels.timeseries(
    'Batch Rate',
    { h: 8, w: 12, x: 12, y: 107 },
    [
      { expr: 'sum(rate(mirror.batch.total{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (status)', legendFormat: '{{status}}' },
    ], unit='ops',
    description='Rate of mirror batches processed per second, broken down by status (success/error).', opts={ stackMode: 'normal' },
  ),

  panels.timeseries(
    'Batch Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 0, y: 115 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(mirror.batch.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p50' },
      { expr: 'histogram_quantile(0.95, sum(rate(mirror.batch.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p95' },
      { expr: 'histogram_quantile(0.99, sum(rate(mirror.batch.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p99' },
    ], unit='µs',
    description='End-to-end batch processing time from cursor read to FSM apply completion.',
  ),

  panels.timeseries(
    'Phase Breakdown (p95)',
    { h: 8, w: 12, x: 12, y: 115 },
    [
      { expr: 'histogram_quantile(0.95, sum(rate(mirror.fetch.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'fetch' },
      { expr: 'histogram_quantile(0.95, sum(rate(mirror.translate.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'translate' },
      { expr: 'histogram_quantile(0.95, sum(rate(mirror.preload.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'preload' },
      { expr: 'histogram_quantile(0.95, sum(rate(mirror.propose.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'propose' },
      { expr: 'histogram_quantile(0.95, sum(rate(mirror.fsm_wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'fsm_wait' },
    ], unit='µs',
    description='p95 duration of each phase within a mirror batch: fetch (HTTP/PG source), translate (v2→v3), preload (Pebble reads), propose (Raft), and fsm_wait (FSM apply).',
  ),

  panels.timeseries(
    'Command Size (p50, p95)',
    { h: 8, w: 12, x: 12, y: 123 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(mirror.command.size_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p50' },
      { expr: 'histogram_quantile(0.95, sum(rate(mirror.command.size_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p95' },
    ], unit='bytes',
    description='Size of the marshalled Raft proposal in bytes. Large commands increase WAL write amplification.',
  ),
])
