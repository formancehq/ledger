package node

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/futures"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/spool"
	"github.com/formancehq/ledger-v3-poc/internal/storage/wal"
)

// applyWork represents a unit of work for the Applier goroutine.
// Either entries are non-nil (normal work) or barrier is non-nil (drain request).
type applyWork struct {
	entries   []raftpb.Entry
	confState *raftpb.ConfState
	barrier   chan struct{} // non-nil = drain; closed when processed
}

// Applier owns all FSM application logic and gating/spool lifecycle.
// It runs as a dedicated goroutine, decoupling WAL writes (processReadies)
// from FSM application so they can overlap across consecutive Ready cycles.
type Applier struct {
	fsm                     *state.Machine
	spool                   spool.Spool
	store                   *dal.Store
	wal                     wal.WAL
	futures                 *SyncMap[uint64, *futures.Future[state.ApplyResult]]
	taskExecutor            *singleTaskExecutor
	logger                  logging.Logger
	snapshotThreshold       uint64
	compactionMargin        uint64
	replayBatchSize         int
	snapshotFetcherProvider state.SnapshotFetcherProvider

	status           *atomic.Int32
	syncProgress     atomic.Pointer[state.SyncProgress]
	gatingTerminated chan struct{}
	ch               chan applyWork // buffered(1)
	snapshotWrapper  func([]byte) ([]byte, error)

	// Metrics
	applyEntriesHistogram           metric.Int64Histogram
	applyEntriesBatchSizeCounter    metric.Int64Counter
	applyEntriesBatchSizeHistogram  metric.Int64Histogram
	createSnapshotHistogram         metric.Float64Histogram
	snapshotTriggeredCounter        metric.Int64Counter
	unspoolDurationHistogram        metric.Float64Histogram
	gatingWaitDurationHistogram     metric.Int64Histogram
	readiesDuringGatingHistogram    metric.Int64Histogram
	maintenanceSnapshotHistogram    metric.Float64Histogram
	maintenanceReplaySpoolHistogram metric.Float64Histogram
}

// NewApplier creates a new Applier with all metrics registered on the provided meter.
func NewApplier(
	fsm *state.Machine,
	spool spool.Spool,
	store *dal.Store,
	wal wal.WAL,
	logger logging.Logger,
	meter metric.Meter,
	snapshotThreshold uint64,
	compactionMargin uint64,
	replayBatchSize int,
	snapshotFetcherProvider state.SnapshotFetcherProvider,
) (*Applier, error) {
	initialStatus := atomic.Int32{}
	initialStatus.Store(statusNormal)

	a := &Applier{
		fsm:                     fsm,
		spool:                   spool,
		store:                   store,
		wal:                     wal,
		futures:                 &SyncMap[uint64, *futures.Future[state.ApplyResult]]{},
		taskExecutor:            newSingleTaskExecutor(logger),
		logger:                  logger,
		snapshotThreshold:       snapshotThreshold,
		compactionMargin:        compactionMargin,
		replayBatchSize:         replayBatchSize,
		snapshotFetcherProvider: snapshotFetcherProvider,
		status:                  &initialStatus,
		ch:                      make(chan applyWork, 1),
	}

	var err error

	a.applyEntriesHistogram, err = meter.Int64Histogram("raft.apply_entries.duration",
		metric.WithDescription("Time spent applying entries to Machine"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 5000, 10000, 20000, 50000, 100000, 150000, 200000, 300000, 500000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating apply_entries histogram: %w", err)
	}

	a.applyEntriesBatchSizeCounter, err = meter.Int64Counter("raft.apply_entries.batch_size",
		metric.WithDescription("Size of batches passed to ApplyEntries"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating batch_size counter: %w", err)
	}

	a.applyEntriesBatchSizeHistogram, err = meter.Int64Histogram("raft.apply_entries.batch_size_distribution",
		metric.WithDescription("Distribution of batch sizes passed to ApplyEntries"),
		metric.WithUnit("1"),
		metric.WithExplicitBucketBoundaries(
			1, 2, 3, 4, 5, 10, 20, 50, 100, 200, 500, 1000, 2000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating batch_size_distribution histogram: %w", err)
	}

	a.createSnapshotHistogram, err = meter.Float64Histogram("raft.syncer.create_snapshot.duration",
		metric.WithDescription("Time spent creating snapshot in syncer"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(
			0, 5, 10, 25, 50, 100, 250, 500, 1000, 5000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating create_snapshot histogram: %w", err)
	}

	a.snapshotTriggeredCounter, err = meter.Int64Counter("raft.snapshot.triggered",
		metric.WithDescription("Number of snapshots triggered"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating snapshot_triggered counter: %w", err)
	}

	a.unspoolDurationHistogram, err = meter.Float64Histogram(
		"raft.node.unspool.duration",
		metric.WithDescription("Time spent in unspoolAndResume after a maintenance task (snapshot/checkpoint)"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 5000, 10000, 20000, 50000, 100000, 250000, 500000, 1000000, 2000000, 5000000, 10000000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating unspool_duration histogram: %w", err)
	}

	a.gatingWaitDurationHistogram, err = meter.Int64Histogram(
		"raft.node.gating.wait_duration",
		metric.WithDescription("Time spent waiting for gatingTerminated (maintenance task completion)"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating gating_wait_duration histogram: %w", err)
	}

	a.readiesDuringGatingHistogram, err = meter.Int64Histogram(
		"raft.node.gating.readies_processed",
		metric.WithDescription("Number of Readies processed during each gating period"),
		metric.WithUnit("1"),
		metric.WithExplicitBucketBoundaries(
			0, 1, 2, 3, 5, 10, 20, 50, 100, 200,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readies_processed histogram: %w", err)
	}

	a.maintenanceSnapshotHistogram, err = meter.Float64Histogram(
		"raft.node.maintenance.snapshot_creation.duration",
		metric.WithDescription("Time spent creating the snapshot during a maintenance task (excluding replay spool)"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 5000000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating maintenance_snapshot histogram: %w", err)
	}

	a.maintenanceReplaySpoolHistogram, err = meter.Float64Histogram(
		"raft.node.maintenance.replay_spool.duration",
		metric.WithDescription("Time spent replaying spooled entries after snapshot creation in a maintenance task"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 5000000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating maintenance_replay_spool histogram: %w", err)
	}

	return a, nil
}

// SnapshotThreshold returns the configured snapshot threshold.
func (a *Applier) SnapshotThreshold() uint64 {
	return a.snapshotThreshold
}

// CompactionMargin returns the configured compaction margin.
func (a *Applier) CompactionMargin() uint64 {
	return a.compactionMargin
}

// Store returns the underlying DAL store.
func (a *Applier) Store() *dal.Store {
	return a.store
}

// StoreFuture registers a future for a given command ID.
func (a *Applier) StoreFuture(commandID uint64, future *futures.Future[state.ApplyResult]) {
	a.futures.Store(commandID, future)
}

// DeleteFuture removes the future for a given command ID.
func (a *Applier) DeleteFuture(commandID uint64) {
	a.futures.Delete(commandID)
}

// Interrupt interrupts any running maintenance task.
func (a *Applier) Interrupt() {
	a.taskExecutor.interrupt()
}

// TaskError returns the channel on which task executor errors are reported.
func (a *Applier) TaskError() <-chan error {
	return a.taskExecutor.error()
}

// SetSnapshotWrapper sets the function used to wrap FSM snapshots with cluster metadata.
func (a *Applier) SetSnapshotWrapper(fn func([]byte) ([]byte, error)) {
	a.snapshotWrapper = fn
}

// SetOutOfSync transitions the applier to the out-of-sync status.
func (a *Applier) SetOutOfSync() {
	a.status.Store(statusOutOfSync)
}

// RecoverAndReplay checks whether the store is up to date with the FSM and,
// if so, recovers any incomplete seal checkpoint, replays WAL entries and
// spooled entries to bring the store fully up to date. When the store is not
// up to date (snapshot was installed), it marks the applier as out-of-sync.
// Returns true when the store was up to date and replay succeeded.
func (a *Applier) RecoverAndReplay(ctx context.Context) (bool, error) {
	isStoreUpToDate, err := a.fsm.IsStoreUpToDate(ctx)
	if err != nil {
		return false, fmt.Errorf("checking if store is up to date: %w", err)
	}

	if !isStoreUpToDate {
		a.logger.Infof("Store is not up to date, resuming from snapshot and tagging node as out of sync")
		a.SetOutOfSync()

		return false, nil
	}

	// Restore cache from Pebble (store is up to date, checkpoint has cache data)
	if err := a.fsm.RestoreCacheFromStore(); err != nil {
		return false, fmt.Errorf("restoring cache from store on restart: %w", err)
	}

	// Recovery: if a period is in CLOSING state but no seal checkpoint exists,
	// the node crashed after ClosePeriod batch.Commit() but before checkpoint creation.
	// Pebble state is exactly at the ClosePeriod boundary right now (spool replay hasn't run).
	if period := a.fsm.ClosingPeriod(); period != nil {
		if _, exists := a.store.TemporaryCheckpointPath("seal"); !exists {
			a.logger.Infof("Recovering: creating seal checkpoint for closing period %d", period.GetId())

			checkpointPath, err := a.store.CreateTemporaryCheckpoint("seal")
			if err != nil {
				return false, fmt.Errorf("creating recovery seal checkpoint: %w", err)
			}

			req := state.SealRequestFromPeriod(period)

			req.CheckpointPath = checkpointPath
			select {
			case a.fsm.SealRequestCh() <- *req:
			default:
			}
		}
	}

	storeLastAppliedIndex, err := query.ReadLastAppliedIndex(a.store)
	if err != nil {
		return false, fmt.Errorf("getting store last applied index: %w", err)
	}

	replayStart := time.Now()

	// Replay WAL entries that were applied to the live Pebble DB but not
	// captured in the last Raft snapshot checkpoint. At startup the store
	// is restored from the checkpoint, so entries between the checkpoint
	// index and the spool start may be missing. The WAL always has them.
	if err := a.replayWAL(ctx, storeLastAppliedIndex); err != nil {
		return false, fmt.Errorf("replaying WAL: %w", err)
	}

	// Re-read after WAL replay — it may have advanced the index.
	storeLastAppliedIndex, err = query.ReadLastAppliedIndex(a.store)
	if err != nil {
		return false, fmt.Errorf("getting store last applied index after WAL replay: %w", err)
	}

	a.logger.WithFields(map[string]any{
		"fromIndex": storeLastAppliedIndex,
	}).Infof("Starting spool replay")

	// todo: is it necessary to replay spool since we re read from the wal?
	// why don't we just need to replay the wal then clear the spool?
	if err := a.replaySpool(ctx, storeLastAppliedIndex); err != nil {
		return false, fmt.Errorf("replaying spool: %w", err)
	}

	a.logger.WithFields(map[string]any{
		"duration": time.Since(replayStart).String(),
	}).Infof("Spool replay complete")

	return true, nil
}

// Submit sends committed entries to the Applier goroutine for asynchronous
// FSM application (or spooling if the node is in a non-normal state).
func (a *Applier) Submit(entries []raftpb.Entry, confState *raftpb.ConfState, stop chan struct{}) {
	select {
	case a.ch <- applyWork{entries: entries, confState: confState}:
	case <-stop:
	}
}

// Drain blocks until all previously submitted work has been processed.
// Used before operations that require the FSM to be idle (snapshot install,
// leadership acquisition).
func (a *Applier) Drain(stop chan struct{}) {
	barrier := make(chan struct{})
	select {
	case a.ch <- applyWork{barrier: barrier}:
		select {
		case <-barrier:
		case <-stop:
		}
	case <-stop:
	}
}

// Status returns the current applier status (statusNormal, statusSyncing, etc.).
func (a *Applier) Status() int32 {
	return a.status.Load()
}

// Run is the Applier goroutine. It processes submitted work items and
// handles gating termination (unspool after maintenance tasks).
func (a *Applier) Run(ctx context.Context, stop chan struct{}) error {
	var (
		readiesDuringGating int64
		gatingStart         time.Time
	)

	for {
		select {
		case work := <-a.ch:
			if work.barrier != nil {
				close(work.barrier)

				continue
			}

			if a.gatingTerminated != nil && gatingStart.IsZero() {
				gatingStart = time.Now()
			}

			if !gatingStart.IsZero() {
				readiesDuringGating++
			}

			switch a.status.Load() {
			case statusNormal:
				err := a.applyEntriesToFSM(ctx, work.confState, work.entries...)
				if err != nil {
					return err
				}
			default:
				a.logger.Debugf("Spool committed entries")

				err := a.spool.AppendCommittedEntries(ctx, work.entries...)
				if err != nil {
					return fmt.Errorf("spooling committed entries: %w", err)
				}
			}
		case <-a.gatingTerminated:
			a.gatingWaitDurationHistogram.Record(context.Background(), time.Since(gatingStart).Microseconds())
			a.readiesDuringGatingHistogram.Record(context.Background(), readiesDuringGating)
			readiesDuringGating = 0
			gatingStart = time.Time{}
			unspoolStart := time.Now()

			err := a.unspoolAndResume(ctx)
			if err != nil {
				return err
			}

			a.unspoolDurationHistogram.Record(context.Background(), float64(time.Since(unspoolStart).Microseconds()))
			a.gatingTerminated = nil
		case <-stop:
			a.taskExecutor.interrupt()

			return nil
		}
	}
}

// SyncSnapshot starts a background synchronization with the leader.
// On failure the node transitions to statusOutOfSync so that new entries
// are spooled and a retry is triggered when a leader reappears in SoftState.
func (a *Applier) SyncSnapshot(ctx context.Context, leader uint64) {
	a.logger.
		WithFields(map[string]any{
			"leader": leader,
		}).
		Infof("Syncing snapshot from leader")

	a.status.Store(statusSyncing)

	progress := state.NewSyncProgress()
	a.syncProgress.Store(progress)

	a.runMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
		snapshotFetcher, err := a.snapshotFetcherProvider.GetForPeer(leader)
		if err != nil {
			a.logger.WithFields(map[string]any{
				"leader": leader,
				"error":  err,
			}).Errorf("Failed to get snapshot fetcher, marking node as out of sync")
			a.syncProgress.Store(nil)
			a.status.Store(statusOutOfSync)

			return 0, nil
		}

		if _, err := a.fsm.SynchronizeWithLeader(ctx, snapshotFetcher, progress); err != nil {
			a.logger.WithFields(map[string]any{
				"leader": leader,
				"error":  err,
			}).Errorf("Failed to synchronize with leader, marking node as out of sync")
			a.syncProgress.Store(nil)
			a.status.Store(statusOutOfSync)

			return 0, nil
		}

		a.syncProgress.Store(nil)

		return 0, nil
	}, nil)
}

// StatusString returns the current applier status as a human-readable string.
func (a *Applier) StatusString() string {
	switch a.status.Load() {
	case statusNormal:
		return "normal"
	case statusSyncing:
		return "syncing"
	case statusSnapshotting:
		return "snapshotting"
	case statusOutOfSync:
		return "out_of_sync"
	default:
		return "unknown"
	}
}

// GetSyncProgress returns the current sync progress, or nil if not syncing.
func (a *Applier) GetSyncProgress() *state.SyncProgress {
	return a.syncProgress.Load()
}

func (a *Applier) applyEntriesAndResolveCommands(ctx context.Context, entries ...raftpb.Entry) (*state.ApplyEntriesResult, error) {
	start := time.Now()

	result, err := a.fsm.ApplyEntries(ctx, entries...)
	if err != nil {
		return nil, fmt.Errorf("applying entries to Machine: %w", err)
	}

	a.applyEntriesHistogram.Record(ctx, time.Since(start).Microseconds())
	a.applyEntriesBatchSizeCounter.Add(ctx, int64(len(result.Results)))
	a.applyEntriesBatchSizeHistogram.Record(ctx, int64(len(result.Results)))

	// Resolve all proposal futures. When CheckpointRequired, the last result
	// is the checkpoint-triggering entry -- its future is resolved later by
	// handleCheckpointRequired once the checkpoint path is known.
	resolveCount := len(result.Results)
	if result.CheckpointRequired && resolveCount > 0 {
		resolveCount--
	}

	for _, r := range result.Results[:resolveCount] {
		future, exists := a.futures.Load(r.ProposalID)
		if !exists {
			continue
		}

		future.Resolve(r, r.Error)
		a.futures.Delete(r.ProposalID)
	}

	return result, nil
}

// applyEntriesToFSM applies entries directly to the Machine.
func (a *Applier) applyEntriesToFSM(ctx context.Context, confState *raftpb.ConfState, entries ...raftpb.Entry) error {
	result, err := a.applyEntriesAndResolveCommands(ctx, entries...)
	if err != nil {
		return err
	}

	// If Machine stopped at a checkpoint boundary (ClosePeriod),
	// enter maintenance mode and create the checkpoint off the Raft hot path.
	if result.CheckpointRequired {
		return a.handleCheckpointRequired(ctx, entries, result)
	}

	lastSnapshot, err := a.wal.Snapshot()
	if err != nil {
		panic(fmt.Errorf("getting last snapshot: %w", err))
	}

	lastEntryIndex := entries[len(entries)-1].Index
	thresholdReached := lastEntryIndex-lastSnapshot.Metadata.Index >= a.snapshotThreshold

	// Force a snapshot when cluster membership has changed since the last
	// snapshot. Without this, a newly added learner/voter would receive
	// a snapshot whose ConfState doesn't include it, causing etcd/raft
	// on the new node to reject the snapshot.
	//
	// Skip joint consensus states (VotersOutgoing non-empty): V2 conf
	// changes like learner-to-voter promotion go through a transient joint
	// state before auto-leaving. Persisting such a state causes newRaft
	// to panic on restore. The auto-leave ConfChange follows immediately
	// and will trigger the snapshot with a clean ConfState.
	confStateChanged := confState != nil &&
		confStateIsClean(confState) &&
		!confStatesEqual(confState, &lastSnapshot.Metadata.ConfState)

	if thresholdReached || confStateChanged {
		a.triggerSnapshot(ctx, confState, lastEntryIndex, lastSnapshot.Metadata.Index)
	}

	return nil
}

// handleCheckpointRequired enters maintenance mode to create a checkpoint off
// the Raft hot path for ClosePeriod (seal checkpoint). While the checkpoint is
// being created, new committed entries are spooled and replayed afterward.
func (a *Applier) handleCheckpointRequired(
	ctx context.Context,
	entries []raftpb.Entry,
	applyResult *state.ApplyEntriesResult,
) error {
	// Spool remaining entries -- they'll be replayed after the maintenance task
	if len(applyResult.RemainingEntries) > 0 {
		err := a.spool.AppendCommittedEntries(ctx, applyResult.RemainingEntries...)
		if err != nil {
			return fmt.Errorf("spooling remaining entries: %w", err)
		}
	}

	// Last applied index: the boundary entry (before any remaining entries)
	var frozenAtIndex uint64
	if len(applyResult.RemainingEntries) > 0 {
		frozenAtIndex = applyResult.RemainingEntries[0].Index - 1
	} else {
		frozenAtIndex = entries[len(entries)-1].Index
	}

	a.status.Store(statusSnapshotting)

	// Resolve the deferred future for the checkpoint-triggering proposal.
	// The last result in applyResult.Results is the entry that set CheckpointRequired.
	var (
		deferredResult *state.ApplyResult
		deferredFuture *futures.Future[state.ApplyResult]
	)

	if len(applyResult.Results) > 0 {
		deferredResult = &applyResult.Results[len(applyResult.Results)-1]
		if f, ok := a.futures.Load(deferredResult.ProposalID); ok {
			deferredFuture = f

			a.futures.Delete(deferredResult.ProposalID)
		}
	}

	a.runMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
		path, err := a.store.CreateTemporaryCheckpoint("checkpoint")
		if err != nil {
			if deferredFuture != nil {
				deferredFuture.Resolve(state.ApplyResult{}, err)
			}

			return 0, fmt.Errorf("creating checkpoint: %w", err)
		}

		if applyResult.OnCheckpointDone != nil {
			applyResult.OnCheckpointDone(path)
		}

		// Create compact baseline snapshot for the checker (non-fatal on error).
		if err := a.createBaselineSnapshot(); err != nil {
			a.logger.WithFields(map[string]any{"error": err}).
				Errorf("Failed to create baseline snapshot (checker will degrade gracefully)")
		}

		if deferredFuture != nil {
			deferredResult.CheckpointPath = path
			deferredFuture.Resolve(*deferredResult, nil)
		}

		return frozenAtIndex, nil
	}, nil)

	return nil
}

// confStateIsClean returns false when the ConfState represents a joint
// consensus transition (VotersOutgoing is non-empty). Joint states are
// transient — etcd/raft auto-leaves them immediately — and must not be
// persisted in snapshots because newRaft panics when restoring from one.
func confStateIsClean(cs *raftpb.ConfState) bool {
	return len(cs.VotersOutgoing) == 0
}

// confStatesEqual returns true when two ConfStates have identical membership.
func confStatesEqual(a, b *raftpb.ConfState) bool {
	return slicesEqual(a.Voters, b.Voters) &&
		slicesEqual(a.Learners, b.Learners) &&
		slicesEqual(a.VotersOutgoing, b.VotersOutgoing) &&
		slicesEqual(a.LearnersNext, b.LearnersNext)
}

func slicesEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// triggerSnapshot creates a Raft snapshot when the threshold is reached.
func (a *Applier) triggerSnapshot(ctx context.Context, confState *raftpb.ConfState, lastEntryIndex, lastSnapshotIndex uint64) {
	a.snapshotTriggeredCounter.Add(ctx, 1)
	a.status.Store(statusSnapshotting)

	a.runMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
		a.logger.WithFields(map[string]any{
			"applied":           lastEntryIndex,
			"lastSnapshotIndex": lastSnapshotIndex,
			"snapshotThreshold": a.snapshotThreshold,
			"compactionMargin":  a.compactionMargin,
		}).Infof("Creating new snapshot")

		startTime := time.Now()

		snapshotData, err := a.fsm.CreateSnapshot(ctx)
		if err != nil {
			return 0, err
		}

		// Wrap FSM data with cluster-level metadata (peer addresses) if wrapper is set.
		if a.snapshotWrapper != nil {
			snapshotData, err = a.snapshotWrapper(snapshotData)
			if err != nil {
				return 0, fmt.Errorf("wrapping snapshot: %w", err)
			}
		}

		if err := a.wal.CreateSnapshot(lastEntryIndex, confState, snapshotData); err != nil {
			return 0, err
		}

		a.createSnapshotHistogram.Record(ctx, float64(time.Since(startTime).Milliseconds()))

		return lastEntryIndex, nil
	}, func(ctx context.Context) {
		// WAL compaction runs after gating ends to avoid holding the WAL
		// mutex during the spooling window, which would block wal.Append
		// and stall the Ready pipeline.
		if lastEntryIndex > a.compactionMargin {
			err := a.wal.Compact(lastEntryIndex - a.compactionMargin)
			if err != nil && !errors.Is(err, raft.ErrCompacted) {
				a.logger.WithFields(map[string]any{
					"error": err,
				}).Errorf("Failed to compact WAL")
			}
		}
	})
}

func (a *Applier) runMaintenanceTask(
	ctx context.Context,
	task func(ctx context.Context) (uint64, error),
	postGating func(ctx context.Context),
) {
	gatingTerminated := make(chan struct{})
	a.gatingTerminated = gatingTerminated

	a.taskExecutor.interrupt()
	a.taskExecutor.run(ctx, func(ctx context.Context) error {
		var closeOnce sync.Once

		closeGating := func() { closeOnce.Do(func() { close(gatingTerminated) }) }
		defer closeGating()

		snapshotStart := time.Now()

		frozenAtIndex, err := task(ctx)
		if err != nil {
			return err
		}

		a.maintenanceSnapshotHistogram.Record(context.Background(), float64(time.Since(snapshotStart).Microseconds()))

		replayStart := time.Now()

		if err := a.replaySpool(ctx, frozenAtIndex); err != nil {
			return err
		}

		a.maintenanceReplaySpoolHistogram.Record(context.Background(), float64(time.Since(replayStart).Microseconds()))

		// End gating before post-gating work (e.g. WAL compaction).
		// Post-gating work doesn't need the FSM to be frozen and would
		// unnecessarily extend the spooling window, increasing latency.
		closeGating()

		if postGating != nil {
			postGating(ctx)
		}

		return nil
	})
}

func (a *Applier) unspoolAndResume(ctx context.Context) error {
	a.logger.Infof("Background operation terminated, applying spooled entries before resuming...")

	lastAppliedIndex, err := query.ReadLastAppliedIndex(a.store)
	if err != nil {
		return fmt.Errorf("getting last applied index: %w", err)
	}

	if err := a.replaySpool(ctx, lastAppliedIndex); err != nil {
		return fmt.Errorf("replaying spool: %w", err)
	}

	a.status.Store(statusNormal)

	lastAppliedIndex, err = query.ReadLastAppliedIndex(a.store)
	if err != nil {
		return fmt.Errorf("getting last applied index: %w", err)
	}

	if err := a.spool.Prune(lastAppliedIndex); err != nil {
		return fmt.Errorf("pruning spool: %w", err)
	}

	a.logger.Infof("Unspooling operation terminated, resuming...")

	return nil
}

// replayWAL replays committed Raft WAL entries from afterIndex+1 up to the
// HardState commit index. This bridges the gap between the Pebble checkpoint
// (restored at startup) and the spool start, since entries applied after the
// last Raft snapshot but before a maintenance window are in the WAL but not
// in the spool. We cap at HardState.Commit (not WAL LastIndex) to avoid
// advancing applied past committed, which would cause raft.NewRawNode to panic.
func (a *Applier) replayWAL(ctx context.Context, afterIndex uint64) error {
	hardState, _, err := a.wal.InitialState()
	if err != nil {
		return fmt.Errorf("reading WAL initial state: %w", err)
	}

	commitIndex := hardState.Commit
	if commitIndex <= afterIndex {
		return nil
	}

	lo := afterIndex + 1

	a.logger.WithFields(map[string]any{
		"from": lo,
		"to":   commitIndex,
	}).Infof("Replaying WAL entries before spool")

	entries, err := a.wal.Entries(lo, commitIndex+1, math.MaxUint64)
	if err != nil {
		return fmt.Errorf("reading WAL entries [%d, %d): %w", lo, commitIndex+1, err)
	}

	if len(entries) == 0 {
		return nil
	}

	for i := 0; i < len(entries); i += a.replayBatchSize {
		end := min(i+a.replayBatchSize, len(entries))

		result, err := a.applyEntriesAndResolveCommands(ctx, entries[i:end]...)
		if err != nil {
			return fmt.Errorf("applying WAL entries: %w", err)
		}

		if result.CheckpointRequired {
			if err := a.handleCheckpointDuringReplay(ctx, result); err != nil {
				return fmt.Errorf("handling checkpoint during WAL replay: %w", err)
			}
		}
	}

	a.logger.WithFields(map[string]any{
		"count": len(entries),
	}).Infof("WAL replay complete")

	return nil
}

func (a *Applier) replaySpool(ctx context.Context, fromIndex uint64) error {
	a.logger.WithFields(map[string]any{
		"fromIndex": fromIndex,
	}).Infof("Replaying spool")

	until, err := a.spool.End()
	if err != nil {
		return fmt.Errorf("getting spool end position: %w", err)
	}

	count := 0
	batchSize := a.replayBatchSize
	batch := make([]raftpb.Entry, 0, batchSize)
	logFields := map[string]any{}

	var lastEntry *raftpb.Entry

	if err := a.spool.ReplayUntil(ctx, *until, fromIndex, func(entry raftpb.Entry) error {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			result, err := a.applyEntriesAndResolveCommands(ctx, batch...)
			if err != nil {
				return err
			}

			count += len(batch)
			batch = batch[:0]
			lastEntry = &entry

			// Handle checkpoint during replay (ClosePeriod)
			if result.CheckpointRequired {
				err := a.handleCheckpointDuringReplay(ctx, result)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("replaying spool: %w", err)
	}

	if len(batch) > 0 {
		count += len(batch)

		result, err := a.applyEntriesAndResolveCommands(ctx, batch...)
		if err != nil {
			return err
		}

		lastEntry = new(batch[len(batch)-1])

		// Handle checkpoint during replay (ClosePeriod or CreateCheckpoint)
		if result.CheckpointRequired {
			err := a.handleCheckpointDuringReplay(ctx, result)
			if err != nil {
				return err
			}
		}
	}

	if lastEntry != nil {
		logFields["last_entry_index"] = lastEntry.Index
	}

	logFields["count"] = count
	a.logger.
		WithFields(logFields).
		WithField("count", count).
		Infof("Replayed spool")

	return nil
}

// handleCheckpointDuringReplay creates a temporary checkpoint and calls the
// FSM-provided callback when a checkpoint-requiring entry (ClosePeriod)
// is encountered during spool replay.
// Unlike handleCheckpointRequired, this does not enter maintenance mode -- the
// checkpoint is created synchronously (acceptable since we're already off
// the hot path) and remaining entries are applied directly.
func (a *Applier) handleCheckpointDuringReplay(ctx context.Context, applyResult *state.ApplyEntriesResult) error {
	checkpointPath, err := a.store.CreateTemporaryCheckpoint("replay")
	if err != nil {
		return fmt.Errorf("creating checkpoint during replay: %w", err)
	}

	if applyResult.OnCheckpointDone != nil {
		applyResult.OnCheckpointDone(checkpointPath)
	}

	// Create compact baseline snapshot for the checker (non-fatal on error).
	if err := a.createBaselineSnapshot(); err != nil {
		a.logger.WithFields(map[string]any{"error": err}).
			Errorf("Failed to create baseline snapshot during replay (checker will degrade gracefully)")
	}

	// Resolve the deferred future for the checkpoint-triggering entry.
	// During replay, applyEntriesAndResolveCommands skips this future
	// (resolveCount-- when CheckpointRequired is true).
	if len(applyResult.Results) > 0 {
		lastResult := &applyResult.Results[len(applyResult.Results)-1]
		if f, ok := a.futures.Load(lastResult.ProposalID); ok {
			lastResult.CheckpointPath = checkpointPath
			f.Resolve(*lastResult, nil)
			a.futures.Delete(lastResult.ProposalID)
		}
	}

	// Apply remaining entries directly (no re-spool needed since we're replaying)
	if len(applyResult.RemainingEntries) > 0 {
		_, err := a.applyEntriesAndResolveCommands(ctx, applyResult.RemainingEntries...)
		if err != nil {
			return fmt.Errorf("applying remaining entries after checkpoint during replay: %w", err)
		}
	}

	return nil
}

// createBaselineSnapshot creates a compact attribute-only snapshot for the checker.
// Unlike a full Pebble checkpoint, this contains only computed attribute values
// (volumes, metadata, transactions), making it orders of magnitude smaller.
func (a *Applier) createBaselineSnapshot() error {
	destPath, err := a.store.BaselineSnapshotDir()
	if err != nil {
		return err
	}

	return attributes.CreateBaselineSnapshot(a.store, a.fsm.Registry.Attrs, destPath)
}
