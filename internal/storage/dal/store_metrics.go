package dal

import (
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// GetMetrics returns the current metrics from the Pebble database as proto message.
// Implements store.MetricsProvider interface.
func (s *Store) GetMetrics() any {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return nil
	}

	m := db.Metrics()

	result := &servicepb.PebbleMetrics{
		BlockCache: &servicepb.BlockCacheMetrics{
			Size:   m.BlockCache.Size,
			Count:  m.BlockCache.Count,
			Hits:   m.BlockCache.Hits,
			Misses: m.BlockCache.Misses,
		},
		Compact: &servicepb.CompactMetrics{
			Count:            m.Compact.Count,
			DefaultCount:     m.Compact.DefaultCount,
			DeleteOnlyCount:  m.Compact.DeleteOnlyCount,
			ElisionOnlyCount: m.Compact.ElisionOnlyCount,
			MoveCount:        m.Compact.MoveCount,
			ReadCount:        m.Compact.ReadCount,
			RewriteCount:     m.Compact.RewriteCount,
			MultiLevelCount:  m.Compact.MultiLevelCount,
			EstimatedDebt:    m.Compact.EstimatedDebt,
			InProgressBytes:  m.Compact.InProgressBytes,
			NumInProgress:    m.Compact.NumInProgress,
			MarkedFiles:      int32(m.Compact.MarkedFiles),
		},
		Flush: &servicepb.FlushMetrics{
			Count:              m.Flush.Count,
			NumInProgress:      m.Flush.NumInProgress,
			AsIngestCount:      m.Flush.AsIngestCount,
			AsIngestTableCount: m.Flush.AsIngestTableCount,
			AsIngestBytes:      m.Flush.AsIngestBytes,
		},
		MemTable: &servicepb.MemTableMetrics{
			Size:        m.MemTable.Size,
			Count:       m.MemTable.Count,
			ZombieSize:  m.MemTable.ZombieSize,
			ZombieCount: m.MemTable.ZombieCount,
		},
		Snapshots: &servicepb.SnapshotsMetrics{
			Count:          int32(m.Snapshots.Count),
			EarliestSeqNum: uint64(m.Snapshots.EarliestSeqNum),
			PinnedKeys:     m.Snapshots.PinnedKeys,
			PinnedSize:     m.Snapshots.PinnedSize,
		},
		Table: &servicepb.TableMetrics{
			ZombieSize:  m.Table.ZombieSize,
			ZombieCount: m.Table.ZombieCount,
		},
		TableCache: &servicepb.TableCacheMetrics{
			Size:   m.FileCache.Size,
			Count:  m.FileCache.TableCount,
			Hits:   m.FileCache.Hits,
			Misses: m.FileCache.Misses,
		},
		Wal: &servicepb.WALMetrics{
			Files:         m.WAL.Files,
			ObsoleteFiles: m.WAL.ObsoleteFiles,
			Size:          m.WAL.Size,
			BytesIn:       m.WAL.BytesIn,
			BytesWritten:  m.WAL.BytesWritten,
		},
		Keys: &servicepb.KeysMetrics{
			RangeKeySetsCount: m.Keys.RangeKeySetsCount,
			TombstoneCount:    m.Keys.TombstoneCount,
		},
		DiskSpaceUsage: m.DiskSpaceUsage(),
	}

	// Convert level metrics
	for i, level := range m.Levels {
		result.Levels = append(result.Levels, &servicepb.LevelMetrics{
			Level:           int32(i),
			NumFiles:        level.TablesCount,
			Size:            level.TablesSize,
			Score:           level.Score,
			BytesIn:         level.TableBytesIn,
			BytesIngested:   level.TableBytesIngested,
			BytesMoved:      level.TableBytesMoved,
			BytesRead:       level.TableBytesRead,
			BytesCompacted:  level.TableBytesCompacted,
			BytesFlushed:    level.TableBytesFlushed,
			TablesCompacted: level.TablesCompacted,
			TablesFlushed:   level.TablesFlushed,
			TablesIngested:  level.TablesIngested,
			TablesMoved:     level.TablesMoved,
		})
	}

	return result
}
