package readstore

import (
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// GetMetrics returns the current Pebble metrics for the read index as a proto message.
func (s *Store) GetMetrics() *servicepb.PebbleMetrics {
	m := s.db.Metrics()
	cacheHits, cacheMisses := m.BlockCache.HitsAndMisses.Aggregate()
	tableZombie := m.Table.Physical.Zombie.Total()

	result := &servicepb.PebbleMetrics{
		BlockCache: &servicepb.BlockCacheMetrics{
			Size:   m.BlockCache.Size,
			Count:  m.BlockCache.Count,
			Hits:   cacheHits,
			Misses: cacheMisses,
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
			ZombieSize:  tableZombie.Bytes,
			ZombieCount: int64(tableZombie.Count),
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

	for i, level := range m.Levels {
		result.Levels = append(result.Levels, &servicepb.LevelMetrics{
			Level:           int32(i),
			NumFiles:        int64(level.Tables.Count),
			Size:            int64(level.Tables.Bytes),
			Score:           level.Score,
			BytesIn:         level.TableBytesIn,
			BytesIngested:   level.TablesIngested.Bytes,
			BytesMoved:      level.TablesMoved.Bytes,
			BytesRead:       level.TableBytesRead,
			BytesCompacted:  level.TablesCompacted.Bytes,
			BytesFlushed:    level.TablesFlushed.Bytes,
			TablesCompacted: level.TablesCompacted.Count,
			TablesFlushed:   level.TablesFlushed.Count,
			TablesIngested:  level.TablesIngested.Count,
			TablesMoved:     level.TablesMoved.Count,
		})
	}

	return result
}
