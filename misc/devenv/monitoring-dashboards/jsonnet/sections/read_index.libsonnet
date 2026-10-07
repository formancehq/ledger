// Read Index (Pebble) section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Read Index (Pebble)', 166, [
  panels.timeseries(
    'Level Sizes (stacked)',
    { h: 8, w: 12, x: 0, y: 1 },
    [
      { expr: 'readindex.level.bytes{formance.ledger.cluster.id=~"$cluster", formance.ledger.node.id=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}} — L{{level}}' },
    ], unit='bytes',
    description='Total bytes stored in each Pebble LSM level. Level 0 holds recently flushed memtable data; higher levels hold progressively older, compacted data.', opts={ stackMode: 'normal', fillOpacity: 20 },
  ),

  panels.timeseries(
    'Memtable Size',
    { h: 8, w: 12, x: 12, y: 1 },
    [
      { expr: 'readindex.memtable.bytes{formance.ledger.cluster.id=~"$cluster", formance.ledger.node.id=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}}' },
    ], unit='bytes',
    description='Current memtable size in bytes. The memtable absorbs writes before they are flushed to L0 SSTables.', opts={ fillOpacity: 15 },
  ),

  panels.timeseries(
    'Block Cache Hits / Misses',
    { h: 8, w: 12, x: 0, y: 10 },
    [
      { expr: 'readindex.cache.hits{formance.ledger.cluster.id=~"$cluster", formance.ledger.node.id=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}} — hits' },
      { expr: 'readindex.cache.misses{formance.ledger.cluster.id=~"$cluster", formance.ledger.node.id=~"$node"}', legendFormat: 'Node {{formance.ledger.node.id}} — misses' },
    ], unit='ops',
    description='Block cache hit and miss rates. A high hit rate indicates the working set fits in cache.',
  ),

  panels.stat(
    'Cache Hit Ratio',
    { h: 8, w: 12, x: 12, y: 10 },
    'readindex.cache.hits{formance.ledger.cluster.id=~"$cluster", formance.ledger.node.id=~"$node"} / (readindex.cache.hits{formance.ledger.cluster.id=~"$cluster", formance.ledger.node.id=~"$node"} + readindex.cache.misses{formance.ledger.cluster.id=~"$cluster", formance.ledger.node.id=~"$node"})', unit='percentunit',
    description='Block cache hit ratio: hits / (hits + misses). Values above 90% are good.', opts={ legendFormat: 'Node {{formance.ledger.node.id}}' },
  ),
])
