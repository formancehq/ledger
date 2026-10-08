package readstore

import (
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// GetMetrics returns the current Pebble metrics for the read index as a proto message.
func (s *Store) GetMetrics() *ledgerpb.PebbleMetrics {
	m := s.db.Metrics()

	result := &ledgerpb.PebbleMetrics{
		BlockCache: &ledgerpb.BlockCacheMetrics{
			Size:   m.BlockCache.Size,
			Count:  m.BlockCache.Count,
			Hits:   m.BlockCache.Hits,
			Misses: m.BlockCache.Misses,
		},
		Compact: &ledgerpb.CompactMetrics{
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
		Flush: &ledgerpb.FlushMetrics{
			Count:              m.Flush.Count,
			NumInProgress:      m.Flush.NumInProgress,
			AsIngestCount:      m.Flush.AsIngestCount,
			AsIngestTableCount: m.Flush.AsIngestTableCount,
			AsIngestBytes:      m.Flush.AsIngestBytes,
		},
		MemTable: &ledgerpb.MemTableMetrics{
			Size:        m.MemTable.Size,
			Count:       m.MemTable.Count,
			ZombieSize:  m.MemTable.ZombieSize,
			ZombieCount: m.MemTable.ZombieCount,
		},
		Snapshots: &ledgerpb.SnapshotsMetrics{
			Count:          int32(m.Snapshots.Count),
			EarliestSeqNum: uint64(m.Snapshots.EarliestSeqNum),
			PinnedKeys:     m.Snapshots.PinnedKeys,
			PinnedSize:     m.Snapshots.PinnedSize,
		},
		Table: &ledgerpb.TableMetrics{
			ZombieSize:  m.Table.ZombieSize,
			ZombieCount: m.Table.ZombieCount,
		},
		TableCache: &ledgerpb.TableCacheMetrics{
			Size:   m.FileCache.Size,
			Count:  m.FileCache.TableCount,
			Hits:   m.FileCache.Hits,
			Misses: m.FileCache.Misses,
		},
		Wal: &ledgerpb.WALMetrics{
			Files:         m.WAL.Files,
			ObsoleteFiles: m.WAL.ObsoleteFiles,
			Size:          m.WAL.Size,
			BytesIn:       m.WAL.BytesIn,
			BytesWritten:  m.WAL.BytesWritten,
		},
		Keys: &ledgerpb.KeysMetrics{
			RangeKeySetsCount: m.Keys.RangeKeySetsCount,
			TombstoneCount:    m.Keys.TombstoneCount,
		},
		DiskSpaceUsage: m.DiskSpaceUsage(),
	}

	for i, level := range m.Levels {
		result.Levels = append(result.Levels, &ledgerpb.LevelMetrics{
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
