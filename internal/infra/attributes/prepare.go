package attributes

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// PrepareForBackup makes a checkpoint portable and restartable on a fresh
// cluster. It resets cluster-local and checkpoint-era zones and does NOT
// touch the attribute zone.
//
// There is no attribute compaction to do: since the raft-index suffix was
// removed from attribute keys (commit e752437eb), each canonical key holds
// exactly one Pebble entry that Set overwrites in place, so there are no
// versions to fold. The attribute zone is left byte-for-byte intact.
//
// The six resets are:
//  1. lastAppliedIndex -> 1, the index the restored FSM genesis occupies in
//     the new raft log (see below), fresh of any source-cluster raft index.
//  2. persisted config (nodeId, clusterId) deleted, so the backup is portable
//     to any cluster.
//  3. ZoneClusterTransient wiped — in-flight-only tracking (backup jobs) has
//     no meaning on the restored cluster.
//  4. persisted bloom blocks dropped, so the booting node rebuilds the bloom
//     from a full attribute scan using its own config.
//  5. persisted Raft peers dropped (EN-1413), so the restored cluster does
//     not dial the source cluster's pods. NewNode reseeds [ZoneGlobal]
//     [SubGlobPeers] from cfg.Peers + self on the next boot.
//  6. cache zone (ZoneCache) cleared, so the restored node boots with a cold
//     cache and re-seeds from the rebuilt attribute zone on first touch.
//
// The caller must ensure all in-memory state has been flushed to Pebble before
// the checkpoint was taken. The backup flow achieves this by running the flush
// and checkpoint atomically on the Raft loop.
func PrepareForBackup(s *dal.Store) error {
	batch := s.OpenWriteSession()

	// Pin lastAppliedIndex to 1: the restored store is the FSM state the new
	// raft log builds on, and the RESTORED bootstrap plants its WAL snapshot
	// at this index, so the log starts at 2 and raft must route any fresh
	// peer through the snapshot → checkpoint-sync path. At 0 the snapshot is
	// empty and the log claims completeness from index 1 — a learner joining
	// before the first post-restore raft snapshot is then "caught up" by
	// plain log replay onto an empty store, missing every restored row.
	appliedIndex := make([]byte, 8)
	binary.BigEndian.PutUint64(appliedIndex, 1)

	if err := batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobLastAppliedIndex}, appliedIndex); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("pinning applied index: %w", err)
	}

	// Remove persisted config (nodeId, clusterId) so the backup is portable to any cluster.
	if err := batch.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig}); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting persisted config: %w", err)
	}

	// Wipe ZoneClusterTransient — backup-job state and any other
	// in-flight-only tracking has no meaning on the restored cluster.
	// A backup taken while a job was RUNNING would otherwise carry that
	// entry through the snapshot, locking the destination on the
	// restored cluster until cleanup eventually fails the orphan.
	// Clearing the whole zone here gives the contract a single
	// enforcement point and matches the zone's documented intent (see
	// dal.ZoneClusterTransient).
	if err := batch.DeleteRange(
		[]byte{dal.ZoneClusterTransient},
		[]byte{dal.ZoneClusterTransient + 1},
		pebble.NoSync,
	); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting cluster-transient zone: %w", err)
	}

	// Drop persisted bloom blocks. After an incremental restore they are stale
	// (they predate the logs RebuildDelta replayed into the attribute zone, so
	// they lack any post-checkpoint account), and their block layout is tied to
	// the source's bloom config — not necessarily the config of the cluster that
	// boots this data. Clearing them forces the booting node to rebuild the
	// bloom from a full attribute scan using its own config; otherwise
	// RestoreFromStore loads the stale blocks and post-checkpoint accounts get
	// bloom-false-negatived (read as {0,0}) on the apply path.
	if err := batch.DeleteRange(
		[]byte{dal.ZoneGlobal, dal.SubGlobBloom},
		[]byte{dal.ZoneGlobal, dal.SubGlobBloom + 1},
		pebble.NoSync,
	); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting persisted bloom blocks: %w", err)
	}

	// Drop persisted Raft peers (EN-1413). Cluster membership is local to
	// the source cluster; carrying it over would make a restored node dial
	// the source pods. The booting node reseeds membership from cfg.Peers
	// + self via n.RegisterPeer in the bootstrap module.
	if err := batch.DeleteRange(
		[]byte{dal.ZoneGlobal, dal.SubGlobPeers},
		[]byte{dal.ZoneGlobal, dal.SubGlobPeers + 1},
		pebble.NoSync,
	); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting persisted Raft peers: %w", err)
	}

	// Clear the cache zone (per-entry cache rows + rotation metadata). Same
	// rationale as the bloom blocks above: these rows predate the logs
	// RebuildDelta replayed into the attribute zone, so a key modified
	// post-checkpoint still carries its checkpoint-era value here while the
	// attribute zone holds the fresh one. RestoreFromStore would load the
	// stale entries and the FSM would serve them as CacheHits — and
	// MirrorPreload's existing-entry-wins seeding means even a fresh Pebble
	// reload cannot displace them.
	if err := batch.DeleteRange(
		[]byte{dal.ZoneCache},
		[]byte{dal.ZoneCache + 1},
		pebble.NoSync,
	); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting cache zone: %w", err)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("committing backup preparation: %w", err)
	}

	// Force a Pebble flush to ensure the resets are written to SSTs.
	// todo: directly commit with NoSync
	if err := s.Flush(); err != nil {
		return fmt.Errorf("flushing backup preparation: %w", err)
	}

	return nil
}
