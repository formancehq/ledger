package state

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Synchronizer owns the multi-step follower-sync coordination: install a
// checkpoint published by a leader, then re-hydrate the FSM. It is the only
// type that retains a dal.IncomingRestoreFactory, so the hot-path Machine
// receiver has no way to invoke the prepare/activate/restore primitives
// individually.
//
// The Synchronizer also wraps the in-memory state-machine operations the node
// triggers around Raft snapshot install (InstallSnapshot, IsStoreUpToDate) so
// the applier and node receive a single coherent surface for "follower-sync"
// concerns. All method bodies live here, not on Machine.
type Synchronizer struct {
	apply           *Machine
	recovery        *Recovery
	incomingRestore dal.IncomingRestoreFactory
}

// NewSynchronizer wires a Synchronizer onto an existing Machine and Recovery.
// The IncomingRestoreFactory is held only here; the Machine cannot invoke it
// directly.
func NewSynchronizer(apply *Machine, recovery *Recovery, incomingRestore dal.IncomingRestoreFactory) *Synchronizer {
	return &Synchronizer{
		apply:           apply,
		recovery:        recovery,
		incomingRestore: incomingRestore,
	}
}

// SynchronizeWithLeader fetches a fresh checkpoint from the leader, installs
// it (which closes and reopens Pebble), then re-hydrates the FSM via Recovery.
// Returns the last applied index after the resync.
func (s *Synchronizer) SynchronizeWithLeader(ctx context.Context, snapshotFetcher SnapshotFetcher, progress *SyncProgress) (uint64, error) {
	// Stop background tasks (bloom restore, etc.) that may hold Pebble iterators.
	// RestoreCheckpoint closes and reopens the DB — outstanding references cause a panic.
	s.apply.StopBackgroundTasks()

	if err := s.restoreCheckpoint(ctx, snapshotFetcher, progress, s.apply.snapshotIndex); err != nil {
		return 0, fmt.Errorf("restoring checkpoint from leader: %w", err)
	}

	// Restore cache from Pebble (the checkpoint contains the leader's cache data)
	if err := s.recovery.RestoreCacheFromStore(); err != nil {
		return 0, fmt.Errorf("restoring cache after sync: %w", err)
	}

	// Reload all FSM state from Pebble (the checkpoint contains the leader's state).
	// This also recovers lastAppliedIndex from the restored Pebble — the fresh
	// checkpoint is at an index >= snapshotIndex, so spool replay correctly
	// skips entries already in the checkpoint.
	// Hold mu because concurrent readers (e.g. QueryCheckpointScheduler) access
	// fields like queryCheckpointSchedule under the same lock.
	s.apply.mu.Lock()
	err := s.recovery.RecoverState()
	s.apply.mu.Unlock()

	if err != nil {
		return 0, fmt.Errorf("recovering state after sync: %w", err)
	}

	lifecycle.SendEvent("sync_with_leader_complete", map[string]any{
		"lastAppliedIndex": s.apply.lastAppliedIndex,
		"snapshotIndex":    s.apply.snapshotIndex,
	})

	return s.apply.lastAppliedIndex, nil
}

// restoreCheckpoint fetches a fresh checkpoint from the leader and restores
// it via the IncomingRestoreFactory.Run callback. The 3-step
// prepare → fn(staging) → activate → restore sequence is encapsulated; the
// Synchronizer does not see the primitives individually.
func (s *Synchronizer) restoreCheckpoint(ctx context.Context, snapshotFetcher SnapshotFetcher, progress *SyncProgress, minAppliedIndex uint64) error {
	s.apply.logger.Infof("Fetching fresh checkpoint from leader")

	checkpointID, err := s.incomingRestore.Run(func(stagingDir string) error {
		size, fetchErr := snapshotFetcher.FetchSnapshot(ctx, stagingDir, progress, minAppliedIndex)
		if fetchErr != nil {
			return fmt.Errorf("fetching snapshot from leader: %w", fetchErr)
		}

		s.apply.logger.WithFields(map[string]any{
			"size": size,
		}).Infof("Checkpoint fetched from leader")

		return nil
	})
	if err != nil {
		return err
	}

	s.apply.logger.WithFields(map[string]any{
		"checkpointId": checkpointID,
	}).Infof("Checkpoint restored successfully")

	return nil
}

// InstallSnapshot updates in-memory snapshot bookkeeping on the Machine. The
// underlying Pebble state is not changed here — the actual restore (when
// needed) happens via SynchronizeWithLeader.
func (s *Synchronizer) InstallSnapshot(_ context.Context, snapshot raftpb.Snapshot) error {
	s.apply.snapshotIndex = snapshot.Metadata.Index

	// Reset the cache — it will be restored from Pebble later:
	// - On restart: after InstallSnapshot, via RestoreCacheFromStore
	// - On follower sync: after restoreCheckpoint, via RestoreCacheFromStore
	s.apply.Registry.Cache.Reset()
	s.apply.Registry.Idempotency.Reset()

	s.apply.logger.WithFields(map[string]any{
		"snapshotIndex": snapshot.Metadata.Index,
	}).Infof("InstallSnapshot complete")

	lifecycle.SendEvent("install_snapshot", map[string]any{
		"snapshotIndex": snapshot.Metadata.Index,
	})

	return nil
}

// IsStoreUpToDate reports whether the in-memory applied index has caught up
// to the latest received snapshot index. Used by the applier to decide
// whether SynchronizeWithLeader is needed.
func (s *Synchronizer) IsStoreUpToDate(_ context.Context) (bool, error) {
	return s.apply.lastAppliedIndex >= s.apply.snapshotIndex, nil
}
