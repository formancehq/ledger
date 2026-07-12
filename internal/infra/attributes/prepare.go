package attributes

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// PrepareForBackup makes a checkpoint portable and restartable on a fresh
// cluster. It performs five Global-zone resets and does NOT touch the
// attribute zone.
//
// There is no attribute compaction to do: since the raft-index suffix was
// removed from attribute keys (commit e752437eb), each canonical key holds
// exactly one Pebble entry that Set overwrites in place, so there are no
// versions to fold. The attribute zone is left byte-for-byte intact.
//
// The resets are:
//  1. lastAppliedIndex -> 0, so the restored cluster starts fresh without
//     raft-index conflicts.
//  2. rolling FSM digest dropped, so the restored cluster starts its
//     cross-node digest chain from (appliedIndex=0, ZeroFSMDigest). The
//     source record is keyed to the source's applied index N; keeping it
//     while lastAppliedIndex is reset to 0 would leave the digest baseline
//     internally inconsistent (chain hash for index N, but the store is at
//     index 0) and would trip the cross-node (applied, hash) equality check
//     the moment a peer joins. LoadFSMDigest reads a missing key as
//     (0, ZeroFSMDigest), so deleting is the clean baseline.
//  3. persisted config (nodeId, clusterId) deleted, so the backup is portable
//     to any cluster.
//  4. persisted bloom blocks dropped, so the booting node rebuilds the bloom
//     from a full attribute scan using its own config.
//  5. persisted Raft peers dropped (EN-1413), so the restored cluster does
//     not dial the source cluster's pods. NewNode reseeds [ZoneGlobal]
//     [SubGlobPeers] from cfg.Peers + self on the next boot.
//
// The caller must ensure all in-memory state has been flushed to Pebble before
// the checkpoint was taken. The backup flow achieves this by running the flush
// and checkpoint atomically on the Raft loop.
func PrepareForBackup(s *dal.Store) error {
	batch := s.OpenWriteSession()

	// Reset lastAppliedIndex to 0 so the restored cluster starts fresh.
	if err := batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobLastAppliedIndex}, make([]byte, 8)); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("resetting applied index: %w", err)
	}

	// Drop the rolling FSM digest so the restored store re-baselines the
	// cross-node digest chain from (appliedIndex=0, ZeroFSMDigest). The source
	// record is keyed to the source's applied index; preserving it while
	// lastAppliedIndex is reset above would seed a digest at index N onto a
	// store now at index 0 — an inconsistent baseline that trips the oracle's
	// (applied, hash) equality check as soon as a peer joins.
	if err := batch.DeleteKey(dal.FSMDigestKey()); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting rolling fsm digest: %w", err)
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
