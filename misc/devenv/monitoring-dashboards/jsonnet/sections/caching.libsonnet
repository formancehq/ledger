// Caching & Attributes section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Caching & Attributes', 167, [
  panels.timeseries(
    'Cache Size by Type',
    { h: 8, w: 12, x: 0, y: 89 },
    [
      { expr: 'cache.size{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}', legendFormat: '{{type}} (Node {{formance.ledger.node.id}})' },
    ], unit='none',
    description=|||
      Number of entries in the attribute cache by type.
      
      The `type` label is displayed dynamically so newly introduced cache
      registries appear without requiring a dashboard update.
   |||,
  ),

  panels.timeseries(
    'Numscript Cache Size',
    { h: 8, w: 12, x: 12, y: 89 },
    [
      { expr: 'numscript.cache.size{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}}' },
    ], unit='none',
    description='Number of cached Numscript programs per node. Shows how many unique scripts are currently stored in the cache.',
  ),

  panels.timeseries(
    'Cache Generation & Rotations',
    { h: 8, w: 12, x: 0, y: 97 },
    [
      { expr: 'cache.generation{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}}: Generation' },
      { expr: 'sum(rate(cache.rotations{k8s.namespace.name=~"$namespace", formance.ledger.cluster.name=~"$cluster", formance.ledger.node.id=~"$node"}[$__rate_interval])) by (formance.ledger.node.id)', legendFormat: 'Node {{formance.ledger.node.id}}: Rotations/s' },
    ], unit='none',
    description=|||
      Number of cache generation rotations and current generation.
      
      Rotations occur when the raft index crosses a generation threshold, triggering cleanup of old cached data.
   |||,
  ),
])
