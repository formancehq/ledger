package dal

import (
	"maps"
	"math"
	"slices"

	"github.com/linxGnu/grocksdb"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// GetMetrics keeps the existing proto envelope for the primary store. Only
// fields with a direct RocksDB equivalent are populated; absent fields are
// unavailable Pebble metrics and must not be interpreted as RocksDB zeroes.
func (s *Store) GetMetrics() any {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return nil
	}

	return rocksDBProtoMetrics(db.Raw())
}

func rocksDBProtoMetrics(db *grocksdb.DB) *servicepb.PebbleMetrics {
	result := &servicepb.PebbleMetrics{}
	if size, ok := db.GetIntProperty("rocksdb.block-cache-usage"); ok && size <= math.MaxInt64 {
		result.BlockCache = &servicepb.BlockCacheMetrics{Size: int64(size)}
	}
	if debt, ok := db.GetIntProperty("rocksdb.estimate-pending-compaction-bytes"); ok {
		result.Compact = &servicepb.CompactMetrics{EstimatedDebt: debt}
	}
	if size, ok := db.GetIntProperty("rocksdb.cur-size-all-mem-tables"); ok {
		result.MemTable = &servicepb.MemTableMetrics{Size: size}
	}
	if count, ok := db.GetIntProperty("rocksdb.num-snapshots"); ok && count <= math.MaxInt32 {
		result.Snapshots = &servicepb.SnapshotsMetrics{Count: int32(count)}
	}
	// Live SST files do not include WALs or obsolete SSTs, so they cannot
	// truthfully populate Pebble's DiskSpaceUsage field.
	levels := map[int]*servicepb.LevelMetrics{}
	for _, file := range db.GetLiveFilesMetaData() {
		if file.Level < 0 || file.Level > math.MaxInt32 {
			continue
		}
		level := levels[file.Level]
		if level == nil {
			level = &servicepb.LevelMetrics{Level: int32(file.Level)}
			levels[file.Level] = level
		}
		level.NumFiles++
		level.Size += file.Size
	}
	for _, level := range slices.Sorted(maps.Keys(levels)) {
		result.Levels = append(result.Levels, levels[level])
	}

	return result
}
