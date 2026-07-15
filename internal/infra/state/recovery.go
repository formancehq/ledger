package state

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Recovery owns the Pebble read capability used to hydrate / re-hydrate a
// Machine from the main store (boot, post-follower-sync, leadership-acquired
// dispatch). It is the only type in this package that retains a
// dal.RecoveryReader, so the hot-path Machine receiver has no field or method
// through which it can read Pebble.
//
// The bodies of RecoverState / RestoreCacheFromStore live here rather than
// as private methods on Machine: the code is colocated with the capability
// it uses, and a future contributor reading machine.go won't find any read
// primitive to pull from.
type Recovery struct {
	apply  *Machine
	reader dal.RecoveryReader
}

// NewRecovery wires a Recovery onto an existing Machine. The reader is held
// only on the Recovery, never on the Machine.
func NewRecovery(apply *Machine, reader dal.RecoveryReader) *Recovery {
	return &Recovery{apply: apply, reader: reader}
}

// RecoverState rebuilds the Machine from the Pebble data store, in three
// visible phases:
//
//  1. Load: read every FSMState field into a fresh struct via
//     LoadFSMStateFromStore. Any failure returns before the Machine is
//     mutated — the previous state stays intact.
//  2. Swap: atomically install the loaded state into the Machine via
//     RestoreState, preserving SnapshotIndex (in-memory only, carried from
//     the previous value — set by InstallSnapshot on followers, zero at
//     boot).
//  3. Reset sub-trackers: Chapters, Registry.Reversions, KeyStore,
//     SharedState, Registry.Cache settings, Registry.Idempotency. These
//     are not part of FSMState; they are reset from the same handle so the
//     Machine ends up coherent.
//
// Called on restart and after follower sync (Synchronizer.SynchronizeWithLeader).
func (r *Recovery) RecoverState() error {
	// Create a ReadHandle for functions that need iterator access (PebbleReader).
	// Get-only calls use r.reader directly (PebbleGetter).
	handle, err := r.reader.NewDirectReadHandle()
	if err != nil {
		return fmt.Errorf("creating read handle for recovery: %w", err)
	}

	defer func() { _ = handle.Close() }()

	// Phase 1: load a fresh FSMState in its entirety. Any error returns
	// before we touch the Machine.
	newState, err := LoadFSMStateFromStore(r.reader, handle, r.apply.State.ClusterID)
	if err != nil {
		return err
	}

	// SnapshotIndex is in-memory only and must survive the swap (set by
	// InstallSnapshot on follower sync; zero at fresh boot).
	newState.SnapshotIndex = r.apply.State.SnapshotIndex

	// Phase 2: atomic swap.
	r.apply.RestoreState(newState)

	// Route through publishApplied rather than a bare Store: RecoverState also
	// runs at runtime (SynchronizeWithLeader), where a silent advance of
	// lastPersistedIndex would strand any blocked WaitForApplied caller — the
	// lost-wakeup shape of #327.
	r.apply.publishApplied(newState.LastAppliedIndex)

	// Phase 3: rebuild sub-trackers (not part of FSMState).
	chaptersCursor, err := query.ReadChapters(context.Background(), handle)
	if err != nil {
		return fmt.Errorf("recovering chapters: %w", err)
	}

	chaptersFromStore, err := cursor.Collect(chaptersCursor)
	if err != nil {
		return fmt.Errorf("collecting chapters: %w", err)
	}

	allChapters := make(map[uint64]*commonpb.Chapter, len(chaptersFromStore))

	var currentOpenChapter *commonpb.Chapter

	var closingChapters []*commonpb.Chapter

	for _, p := range chaptersFromStore {
		allChapters[p.GetId()] = p

		switch p.GetStatus() {
		case commonpb.ChapterStatus_CHAPTER_OPEN:
			currentOpenChapter = p
		case commonpb.ChapterStatus_CHAPTER_CLOSING:
			closingChapters = append(closingChapters, p)
		}
	}

	nextChapterID, err := query.ReadNextChapterID(r.reader)
	if err != nil {
		return fmt.Errorf("recovering next chapter ID: %w", err)
	}

	r.apply.Chapters.Reset(allChapters, currentOpenChapter, closingChapters, nextChapterID)

	chapterSchedule, err := query.ReadChapterSchedule(r.reader)
	if err != nil {
		return fmt.Errorf("recovering chapter schedule: %w", err)
	}

	r.apply.Chapters.SetSchedule(chapterSchedule)

	reversions, malformedReversions, err := query.ReadReversions(handle)
	if err != nil {
		return fmt.Errorf("recovering reversions: %w", err)
	}

	// Only SaveReversionWord produces these rows, so a malformed one means
	// corruption or tampering. Boot proceeds on the decodable words — the
	// checker reports malformed rows as integrity events — but never silently.
	for _, m := range malformedReversions {
		r.apply.logger.Errorf("malformed reversion row at key %x skipped during recovery: %s", m.Key, m.Reason)
	}

	r.apply.Registry.Reversions = reversions

	if r.apply.keyStore != nil {
		r.apply.keyStore.Reset()

		signingKeys, err := query.ReadSigningKeys(handle)
		if err != nil {
			return fmt.Errorf("loading signing keys: %w", err)
		}

		for keyID, entry := range signingKeys {
			r.apply.keyStore.AddPublicKey(keyID, entry.PublicKey, entry.ParentKeyID)
		}
	}

	r.apply.sharedState.Reset()

	requireSig, err := query.ReadSigningConfig(r.reader)
	if err != nil {
		return fmt.Errorf("loading signing config: %w", err)
	}

	r.apply.sharedState.SetRequireSignatures(requireSig)

	maintenanceMode, err := query.ReadMaintenanceMode(r.reader)
	if err != nil {
		return fmt.Errorf("loading maintenance mode: %w", err)
	}

	r.apply.sharedState.SetMaintenanceMode(maintenanceMode)

	if newState.LastClusterConfig != nil {
		r.apply.Registry.Cache.SetGenerationThreshold(newState.LastClusterConfig.GetRotationThreshold())
		// Epoch is never 0 in the running cache (cache.New initializes it to 1).
		// Persisted clusterState from before that change may still carry 0 —
		// bump it up so the staleness check never sees a zero live epoch (#302).
		persistedEpoch := newState.CacheEpoch
		if persistedEpoch == 0 {
			persistedEpoch = 1
		}

		r.apply.Registry.Cache.SetEpoch(persistedEpoch)
	}

	// Rebuild the idempotency bridge from Pebble. Without this, a node that
	// restarts loses every idempotency key whose surrounding proposal already
	// landed in Pebble — replays would then be accepted as new work until the
	// in-memory bridge naturally refilled. See issue #300.
	r.apply.Registry.Idempotency.Reset()

	if err := r.apply.Registry.Idempotency.RestoreFromStore(handle); err != nil {
		return fmt.Errorf("recovering idempotency bridge: %w", err)
	}

	r.apply.logger.WithFields(map[string]any{
		"nextSequenceID":        newState.NextSequenceID,
		"nextAuditSequenceID":   newState.NextAuditSequenceID,
		"nextQueryCheckpointID": newState.NextQueryCheckpointID,
		"hasAuditHash":          len(newState.LastAuditHash) > 0,
		"chapterCount":          len(allChapters),
		"reversionLedgers":      len(reversions),
		"pendingCleanups":       len(newState.PendingLedgerCleanups),
	}).Infof("Recovered FSM state from store")

	return nil
}

// RestoreCacheFromStore re-hydrates the in-memory cache from the 0xFF zone in
// Pebble. The cache snapshotter holds its own read capability scoped to
// restore.
func (r *Recovery) RestoreCacheFromStore() error {
	if err := r.apply.cacheSnapshotter.RestoreFromStore(r.reader); err != nil {
		return err
	}

	if r.apply.sentinelMode {
		r.apply.cacheSnapshotter.verifyCacheRestoreCoherence()
	}

	return nil
}

// OnLeadershipAcquired is called when this node becomes the Raft leader. It
// re-dispatches archive requests from durable state, allowing the new leader
// to retry work that may have been in flight when the previous leader
// crashed.
func (r *Recovery) OnLeadershipAcquired(stop <-chan struct{}) {
	go r.apply.DispatchArchiveRequests(stop)
}

// DispatchBloomRebuilds consumes the Machine's bloom-rebuild signal channel
// and triggers an async bloom populate using Recovery's reader. The Machine
// hot path emits on the channel when a cluster-config change requires a
// rebuild; it does not invoke StartAsyncBloomPopulate directly because it
// does not hold a Pebble reader.
//
// Returns when stop is closed; intended to be run as a goroutine by the
// bootstrap lifecycle (similar to other background dispatchers).
func (r *Recovery) DispatchBloomRebuilds(stop <-chan struct{}) {
	ch := r.apply.BloomRebuildCh()
	for {
		select {
		case <-stop:
			return
		case reason, ok := <-ch:
			if !ok {
				return
			}

			r.apply.cacheSnapshotter.StartAsyncBloomPopulate(r.reader, reason)
		}
	}
}
