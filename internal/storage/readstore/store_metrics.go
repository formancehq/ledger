package readstore

import (
	"maps"
	"math"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// GetMetrics retains the existing wire envelope for the read index. Only
// fields with a direct RocksDB equivalent are populated.
func (s *Store) GetMetrics() *servicepb.PebbleMetrics {
	raw := s.db.Raw()
	result := &servicepb.PebbleMetrics{}
	if size, ok := raw.GetIntProperty("rocksdb.block-cache-usage"); ok && size <= math.MaxInt64 {
		result.BlockCache = &servicepb.BlockCacheMetrics{Size: int64(size)}
	}
	if debt, ok := raw.GetIntProperty("rocksdb.estimate-pending-compaction-bytes"); ok {
		result.Compact = &servicepb.CompactMetrics{EstimatedDebt: debt}
	}
	if size, ok := raw.GetIntProperty("rocksdb.cur-size-all-mem-tables"); ok {
		result.MemTable = &servicepb.MemTableMetrics{Size: size}
	}
	if count, ok := raw.GetIntProperty("rocksdb.num-snapshots"); ok && count <= math.MaxInt32 {
		result.Snapshots = &servicepb.SnapshotsMetrics{Count: int32(count)}
	}
	levels := map[int]*servicepb.LevelMetrics{}
	for _, file := range raw.GetLiveFilesMetaData() {
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
