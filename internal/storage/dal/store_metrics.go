package dal

import (
	"maps"
	"math"
	"slices"

	"github.com/linxGnu/grocksdb"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// GetMetrics returns the properties RocksDB makes available for the primary
// store. A nil optional field means the corresponding property was unavailable.
func (s *Store) GetMetrics() *servicepb.StorageMetrics {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return nil
	}

	return rocksDBProtoMetrics(db.Raw())
}

func rocksDBProtoMetrics(db *grocksdb.DB) *servicepb.StorageMetrics {
	result := &servicepb.StorageMetrics{}
	if size, ok := db.GetIntProperty("rocksdb.block-cache-usage"); ok {
		result.BlockCacheUsageBytes = &size
	}
	if debt, ok := db.GetIntProperty("rocksdb.estimate-pending-compaction-bytes"); ok {
		result.PendingCompactionBytes = &debt
	}
	if size, ok := db.GetIntProperty("rocksdb.cur-size-all-mem-tables"); ok {
		result.MemtableSizeBytes = &size
	}
	if count, ok := db.GetIntProperty("rocksdb.num-snapshots"); ok {
		result.SnapshotCount = &count
	}

	levels := map[int]*servicepb.StorageLevelMetrics{}
	for _, file := range db.GetLiveFilesMetaData() {
		if file.Level < 0 || file.Level > math.MaxInt32 || file.Size < 0 {
			continue
		}
		level := levels[file.Level]
		if level == nil {
			level = &servicepb.StorageLevelMetrics{Level: int32(file.Level)}
			levels[file.Level] = level
		}
		level.NumFiles++
		level.SizeBytes += uint64(file.Size)
	}
	for _, level := range slices.Sorted(maps.Keys(levels)) {
		result.Levels = append(result.Levels, levels[level])
	}

	return result
}
