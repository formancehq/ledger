// Native RocksDB properties sampled by internal/storage/dal/metrics.go.
// These are gauges; no per-operation VFS counters or duration histograms are exported.
local panels = import '../lib/panels.libsonnet';

local property(title, metric, unit, description, x, y) = panels.timeseries(
  title,
  { h: 8, w: 8, x: x, y: y },
  [{
    expr: '{__name__="' + metric + '", "service.cluster"=~"$cluster", "service.node_id"=~"$node"}',
    legendFormat: 'Node {{service.node_id}}',
  }],
  unit=unit,
  description=description,
);

panels.row('RocksDB', 165, [
  property('Memtable bytes', 'rocksdb.memtable.bytes', 'bytes',
           'Approximate bytes in active memtables.', 0, 88),
  property('Immutable memtables', 'rocksdb.memtable.immutable.count', 'short',
           'Immutable memtables awaiting flush.', 8, 88),
  property('Flush pending', 'rocksdb.flush.pending', 'none',
           '1 when a memtable flush is pending; 0 otherwise.', 16, 88),
  property('Compaction pending', 'rocksdb.compaction.pending', 'none',
           '1 when a compaction is pending; 0 otherwise.', 0, 96),
  property('Compaction debt', 'rocksdb.compaction.debt.bytes', 'bytes',
           'Estimated bytes pending compaction.', 8, 96),
  property('Live SST bytes', 'rocksdb.sst.live.bytes', 'bytes',
           'Bytes in live SST files.', 16, 96),
  property('Block cache usage', 'rocksdb.cache.used.bytes', 'bytes',
           'Bytes used by the block cache.', 0, 104),
  property('Open snapshots', 'rocksdb.snapshots.count', 'short',
           'Unreleased RocksDB snapshots.', 8, 104),
  property('Writes stopped', 'rocksdb.write.stopped', 'none',
           '1 when RocksDB has stopped writes; 0 otherwise.', 16, 104),
  property('Background errors', 'rocksdb.background.errors', 'short',
           'Accumulated RocksDB background errors. This is a sampled property, not an error rate.', 0, 112),
])
