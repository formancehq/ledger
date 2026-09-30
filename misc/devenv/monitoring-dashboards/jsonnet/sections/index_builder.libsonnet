// Index Builder section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Index Builder', 173, [
  panels.gauge(
    'Index Builder Lag',
    { h: 8, w: 8, x: 0, y: 109 },
    'index.builder.lag{service.cluster=~"$cluster", service.node_id=~"$node"}', unit='none',
    description='Number of logs the index builder is behind the storage engine. A high lag means queries may return stale results. The builder batches logs to amortize write overhead.',
  ),

  panels.timeseries(
    'Indexing Rate',
    { h: 8, w: 8, x: 8, y: 109 },
    [
      { expr: 'rate(index.builder.logs_indexed_total{service.cluster=~"$cluster", service.node_id=~"$node"}[1m])', legendFormat: '{{service.node_id}}' },
    ], unit='ops',
    description='Rate of logs indexed per second. Higher values indicate faster catch-up.', opts={ showPoints: 'auto' },
  ),

  panels.timeseries(
    'Last Indexed vs Storage',
    { h: 8, w: 8, x: 16, y: 109 },
    [
      { expr: 'index.builder.last_indexed_sequence{service.cluster=~"$cluster", service.node_id=~"$node"}', legendFormat: 'Last Indexed ({{service.node_id}})' },
      { expr: 'index.builder.storage_last_sequence{service.cluster=~"$cluster", service.node_id=~"$node"}', legendFormat: 'Storage Last ({{service.node_id}})' },
    ], unit='none',
    description='Last indexed sequence vs storage last sequence. The gap between the two lines is the lag.', opts={ showPoints: 'auto' },
  ),
])
