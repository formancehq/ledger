package dal

// RocksDB's DB properties describe engine state, not per-operation VFS IOPS.
// In particular, RocksDB does not expose Pebble's VFS read/write/sync counts.
// Only native properties with an unambiguous meaning are exported here.
var rocksDBProperties = []struct {
	name        string
	property    string
	description string
	unit        string
}{
	{"rocksdb.memtable.bytes", "rocksdb.cur-size-all-mem-tables", "Approximate bytes in active memtables", "By"},
	{"rocksdb.memtable.immutable.count", "rocksdb.num-immutable-mem-table", "Immutable memtables awaiting flush", "{table}"},
	{"rocksdb.flush.pending", "rocksdb.mem-table-flush-pending", "Whether a memtable flush is pending", "1"},
	{"rocksdb.compaction.pending", "rocksdb.compaction-pending", "Whether a compaction is pending", "1"},
	{"rocksdb.compaction.debt.bytes", "rocksdb.estimate-pending-compaction-bytes", "Estimated bytes pending compaction", "By"},
	{"rocksdb.sst.live.bytes", "rocksdb.live-sst-files-size", "Bytes in live SST files", "By"},
	{"rocksdb.cache.used.bytes", "rocksdb.block-cache-usage", "Bytes used by the block cache", "By"},
	{"rocksdb.snapshots.count", "rocksdb.num-snapshots", "Unreleased RocksDB snapshots", "{snapshot}"},
	{"rocksdb.write.stopped", "rocksdb.is-write-stopped", "Whether RocksDB has stopped writes", "1"},
	{"rocksdb.background.errors", "rocksdb.background-errors", "Accumulated RocksDB background errors", "{error}"},
}
