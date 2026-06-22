// Caching & Attributes section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Caching & Attributes', 167, [
  panels.timeseries(
    'Cache Size by Type',
    { h: 8, w: 12, x: 0, y: 89 },
    [
      { expr: 'cache.size{service.cluster=~"$cluster", service.node_id=~"$node", type="volumes"}', legendFormat: 'Volumes (Node {{service.node_id}})' },
      { expr: 'cache.size{service.cluster=~"$cluster", service.node_id=~"$node", type="account_metadata"}', legendFormat: 'Account Metadata (Node {{service.node_id}})' },
      { expr: 'cache.size{service.cluster=~"$cluster", service.node_id=~"$node", type="idempotency_keys"}', legendFormat: 'Idempotency Keys (Node {{service.node_id}})' },
      { expr: 'cache.size{service.cluster=~"$cluster", service.node_id=~"$node", type="boundaries"}', legendFormat: 'Boundaries (Node {{service.node_id}})' },
    ], unit='none',
    description=|||
      Number of entries in the attribute cache by type.
      
      - volumes: Account volumes (input/output)
      - account_metadata: Account metadata
      - ledger_metadata: Ledger metadata
      - reversions: Transaction reversion status
      - idempotency_keys: Idempotency keys
      - references: Transaction references
      - ledgers: Ledger info
      - boundaries: Ledger boundaries
   |||,
  ),

  panels.timeseries(
    'Numscript Cache Size',
    { h: 8, w: 12, x: 12, y: 89 },
    [
      { expr: 'numscript.cache.size{service.cluster=~"$cluster", service.node_id=~"$node"}', legendFormat: 'Node {{service.node_id}}' },
    ], unit='none',
    description='Number of cached Numscript programs per node. Shows how many unique scripts are currently stored in the cache.',
  ),

  panels.timeseries(
    'Cache Generation & Rotations',
    { h: 8, w: 12, x: 0, y: 97 },
    [
      { expr: 'cache.generation{service.cluster=~"$cluster", service.node_id=~"$node"}', legendFormat: 'Node {{service.node_id}}: Generation' },
      { expr: 'sum(rate(cache.rotations{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Node {{service.node_id}}: Rotations/s' },
    ], unit='none',
    description=|||
      Number of cache generation rotations and current generation.
      
      Rotations occur when the raft index crosses a generation threshold, triggering cleanup of old cached data.
   |||,
  ),
])
