package attributes

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
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
// Restore preparation retains business rows, marks query checkpoints as
// restored, discards both cluster-local zones and the cache, then writes the
// restored genesis boundary into the new cluster's durable zone. The applied
// index is read before the local zone is cleared; index 0 maps to boundary 1.
// The destination reseeds its persisted identity, peers and bloom filters.
// The removed-member registry is intentionally discarded with the source peers.
//
// The caller must ensure all in-memory state has been flushed to Pebble before
// the checkpoint was taken. The backup flow achieves this by running the flush
// and checkpoint atomically on the Raft loop.
func PrepareForBackup(s *dal.Store) error {
	// The checkpoint's applied index becomes the restored genesis boundary:
	// the RESTORED bootstrap plants its WAL snapshot at this index, so the
	// new log starts at boundary+1 and raft must route any fresh peer
	// through the snapshot → checkpoint-sync path. At 0 the snapshot would
	// be empty and the log would claim completeness from index 1 — a learner
	// joining before the first post-restore raft snapshot would then be
	// "caught up" by plain log replay onto an empty store, missing every
	// restored row — so a genesis checkpoint gets the fallback boundary 1.
	//
	// The boundary is a label for the new log's start, NOT the state's
	// source-cluster provenance: incremental exports are sequence-keyed and
	// never advance this key, so after a full + incremental restore the
	// state is newer than the boundary.
	// Read the raw key: only genuine absence may fall back — a present but
	// malformed value is a corrupt checkpoint and must fail closed, not be
	// silently rewritten as the genesis fallback.
	var genesisBoundary uint64

	switch val, closer, err := s.Get([]byte{dal.ZoneClusterPersistent, dal.SubGlobLastAppliedIndex}); {
	case err == nil:
		if len(val) != 8 {
			_ = closer.Close()

			return fmt.Errorf("invariant: checkpoint applied index has %d bytes (expected 8) — corrupt checkpoint", len(val))
		}

		genesisBoundary = binary.BigEndian.Uint64(val)
		if err := closer.Close(); err != nil {
			return fmt.Errorf("closing applied index read: %w", err)
		}
	case errors.Is(err, pebble.ErrNotFound):
		// Genesis checkpoint: the key has never been written.
	default:
		return fmt.Errorf("reading checkpoint applied index: %w", err)
	}

	// Several raft paths compute boundary+1 (first entry, FSM gap check).
	if genesisBoundary == math.MaxUint64 {
		return errors.New("invariant: checkpoint applied index is MaxUint64 — corrupt checkpoint")
	}

	if genesisBoundary == 0 {
		genesisBoundary = 1
	}

	batch := s.OpenWriteSession()

	// Query-checkpoint metadata survives in the primary Pebble store, but the
	// physical main/read-index checkpoint directories do not. Mark every live
	// row before the restored node starts so the asynchronous read-index builder
	// never interprets a source-cluster applied index as progress in the new Raft
	// domain. RebuildDelta applies the same marker to post-checkpoint rows.
	checkpointReader, err := s.NewDirectReadHandle()
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("opening query checkpoints for restore preparation: %w", err)
	}
	checkpoints, err := dal.CollectZone[*raftcmdpb.QueryCheckpointState](checkpointReader, dal.ZoneGlobal, dal.SubGlobQueryCheckpoint)
	closeErr := checkpointReader.Close()
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("reading query checkpoints for restore preparation: %w", err)
	}
	if closeErr != nil {
		_ = batch.Cancel()

		return fmt.Errorf("closing query checkpoints after restore preparation: %w", closeErr)
	}
	for _, checkpoint := range checkpoints {
		checkpoint.RestoredFromBackup = true
		key := dal.NewKeyBuilder().
			PutZonePrefix(dal.ZoneGlobal, dal.SubGlobQueryCheckpoint).
			PutUint64(checkpoint.GetCheckpointId()).
			Build()
		if err := batch.SetProto(key, checkpoint); err != nil {
			_ = batch.Cancel()

			return fmt.Errorf("marking query checkpoint %d as restored: %w", checkpoint.GetCheckpointId(), err)
		}
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

	// Drop all durable cluster-local rows in one range tombstone. This includes
	// identity, topology, membership, removal tombstones and bloom blocks.
	// New local prefixes inherit the same restore contract automatically.
	if err := batch.DeleteRange(
		[]byte{dal.ZoneClusterPersistent},
		[]byte{dal.ZoneClusterPersistent + 1},
		pebble.NoSync,
	); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting cluster-persistent zone: %w", err)
	}

	appliedIndex := make([]byte, 8)
	binary.BigEndian.PutUint64(appliedIndex, genesisBoundary)

	if err := batch.SetBytes([]byte{dal.ZoneClusterPersistent, dal.SubGlobLastAppliedIndex}, appliedIndex); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("writing genesis boundary: %w", err)
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
