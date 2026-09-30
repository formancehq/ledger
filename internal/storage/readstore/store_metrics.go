package readstore

import (
	"maps"
	"math"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// GetMetrics returns the RocksDB properties available for the read index.
func (s *Store) GetMetrics() *servicepb.StorageMetrics {
	raw := s.db.Raw()
	result := &servicepb.StorageMetrics{}
	if size, ok := raw.GetIntProperty("rocksdb.block-cache-usage"); ok {
		result.BlockCacheUsageBytes = &size
	}
	if debt, ok := raw.GetIntProperty("rocksdb.estimate-pending-compaction-bytes"); ok {
		result.PendingCompactionBytes = &debt
	}
	if size, ok := raw.GetIntProperty("rocksdb.cur-size-all-mem-tables"); ok {
		result.MemtableSizeBytes = &size
	}
	if count, ok := raw.GetIntProperty("rocksdb.num-snapshots"); ok {
		result.SnapshotCount = &count
	}

	levels := map[int]*servicepb.StorageLevelMetrics{}
	for _, file := range raw.GetLiveFilesMetaData() {
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
