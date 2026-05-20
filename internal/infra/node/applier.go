package node

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/futures"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/spool"
	"github.com/formancehq/ledger-v3-poc/internal/storage/wal"
)

// applyWork represents a unit of work for the Applier goroutine.
// Exactly one of (entries, barrier, syncLeader) is set per work item.
type applyWork struct {
	entries    []raftpb.Entry
	confState  *raftpb.ConfState
	barrier    chan struct{} // non-nil = drain; closed when processed
	syncLeader uint64        // non-zero = trigger SyncSnapshot from Run goroutine
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
	taskExecutor            *worker.SingleTaskExecutor
	logger                  logging.Logger
	compactionMargin        uint64
	replayBatchSize         int
	snapshotFetcherProvider state.SnapshotFetcherProvider

	status           *atomic.Int32
	syncProgress     atomic.Pointer[state.SyncProgress]
	gatingTerminated chan struct{}
	ch               chan applyWork  // buffered(1)
	commitCh         chan commitWork // buffered(1), read by the committer goroutine

	// pending holds the in-flight commit result channel, if any.
	// At most one at a time. Drained before barriers, checkpoints, and shutdown.
	pending *pendingCommit

	// Metrics
	applyEntriesHistogram           metric.Int64Histogram
	applyEntriesBatchSizeCounter    metric.Int64Counter
	applyEntriesBatchSizeHistogram  metric.Int64Histogram
	unspoolDurationHistogram        metric.Float64Histogram
	gatingWaitDurationHistogram     metric.Int64Histogram
	readiesDuringGatingHistogram    metric.Int64Histogram
	maintenanceSnapshotHistogram    metric.Float64Histogram
	maintenanceReplaySpoolHistogram metric.Float64Histogram
	batchWaitDurationHistogram      metric.Int64Histogram
	commitWaitHistogram             metric.Int64Histogram
	prepareDurationHistogram        metric.Int64Histogram
}

type pendingFuture struct {
	proposalID uint64
	result     state.ApplyResult
	future     *futures.Future[state.ApplyResult]
}

type commitWork struct {
	prepared *state.PreparedBatch
	futures  []pendingFuture
	done     chan error
}

type pendingCommit struct {
	done chan error // signaled when the committer goroutine finishes this work
}

// NewApplier creates a new Applier with all metrics registered on the provided meter.
func NewApplier(
	fsm *state.Machine,
	spool spool.Spool,
	store *dal.Store,
	wal wal.WAL,
	logger logging.Logger,
	meter metric.Meter,
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
		taskExecutor:            worker.NewSingleTaskExecutor(logger),
		logger:                  logger,
		compactionMargin:        compactionMargin,
		replayBatchSize:         replayBatchSize,
		snapshotFetcherProvider: snapshotFetcherProvider,
		status:                  &initialStatus,
		ch:                      make(chan applyWork, 1),
		commitCh:                make(chan commitWork, 1),
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

	a.batchWaitDurationHistogram, err = meter.Int64Histogram(
		"raft.applier.batch_wait.duration",
		metric.WithDescription("Time the applier spends idle waiting for the next batch of entries"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating batch_wait histogram: %w", err)
	}

	a.commitWaitHistogram, err = meter.Int64Histogram(
		"raft.applier.commit_wait.duration",
		metric.WithDescription("Time spent waiting for the previous batch's commit to finish before starting the next prepare"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating commit_wait histogram: %w", err)
	}

	a.prepareDurationHistogram, err = meter.Int64Histogram(
		"raft.fsm.prepare.duration",
		metric.WithDescription("Time spent in PrepareEntries (processing + merge, without commit)"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating prepare_duration histogram: %w", err)
	}

	return a, nil
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
	a.taskExecutor.Interrupt()
}

// TaskError returns the channel on which task executor errors are reported.
func (a *Applier) TaskError() <-chan error {
	return a.taskExecutor.Error()
}

// SetSnapshotWrapper sets the function used to wrap FSM snapshots with cluster metadata.
// SetOutOfSync transitions the applier to the out-of-sync status.
func (a *Applier) SetOutOfSync() {
	a.status.Store(statusOutOfSync)
}

// RecoverAndReplay checks whether the store is up to date with the FSM and,
// if so, recovers the in-memory cache, replays Raft WAL entries and spooled
// entries to bring the node fully up to date. When the store is not up to date
// (e.g. the node crashed after receiving a leader snapshot but before syncing),
// it marks the applier as out-of-sync so SynchronizeWithLeader runs.
// Returns true when the store was up to date and replay succeeded.
func (a *Applier) RecoverAndReplay(ctx context.Context) (bool, error) {
	isStoreUpToDate, err := a.fsm.IsStoreUpToDate(ctx)
	if err != nil {
		return false, fmt.Errorf("checking if store is up to date: %w", err)
	}

	if !isStoreUpToDate {
		if a.logger.Enabled(logging.DebugLevel) {
			a.logger.Debugf("Store is not up to date, resuming from snapshot and tagging node as out of sync")
		}
		a.SetOutOfSync()

		return false, nil
	}

	// Restore cache from Pebble (store is up to date, checkpoint has cache data).
	// FSM counters (sequences, periods, reversions, etc.) are already loaded by
	// NewMachine → RecoverState in the constructor and Pebble has not changed
	// since (InstallSnapshot only touches in-memory state, and the
	// SynchronizeWithLeader path exits earlier via SetOutOfSync).
	if err := a.fsm.RestoreCacheFromStore(); err != nil {
		return false, fmt.Errorf("restoring cache from store on restart: %w", err)
	}

	// Recovery: if periods are in CLOSING state but no seal checkpoint exists,
	// the node crashed after ClosePeriod batch.Commit() but before checkpoint creation.
	// Pebble state is exactly at the ClosePeriod boundary right now (spool replay hasn't run).
	for _, period := range a.fsm.ClosingPeriods() {
		name := state.SealCheckpointName(period.GetId())
		if _, exists := a.store.TemporaryCheckpointPath(name); !exists {
			if a.logger.Enabled(logging.DebugLevel) {
				a.logger.Debugf("Recovering: creating seal checkpoint for closing period %d", period.GetId())
			}

			checkpointPath, err := a.store.CreateTemporaryCheckpoint(name)
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

	// Read HardState to know the committed index — we must never apply past it.
	hardState, _, err := a.wal.InitialState()
	if err != nil {
		return false, fmt.Errorf("reading WAL initial state: %w", err)
	}

	// Replay Raft WAL entries that were committed but not yet applied
	// (e.g. the node crashed between Raft commit and FSM apply).
	if err := a.replayWAL(ctx, storeLastAppliedIndex); err != nil {
		return false, fmt.Errorf("replaying WAL: %w", err)
	}

	// Re-read after WAL replay — it may have advanced the index.
	storeLastAppliedIndex, err = query.ReadLastAppliedIndex(a.store)
	if err != nil {
		return false, fmt.Errorf("getting store last applied index after WAL replay: %w", err)
	}

	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.WithFields(map[string]any{
			"fromIndex":   storeLastAppliedIndex,
			"commitIndex": hardState.Commit,
		}).Debugf("Starting spool replay")
	}

	// Replay spooled entries up to the committed index only.
	// The spool may contain uncommitted entries (received from the leader
	// before the node crashed). Applying past committed would cause
	// etcd-raft to panic with "applied is out of range".
	if err := a.replaySpoolUntil(ctx, storeLastAppliedIndex, hardState.Commit); err != nil {
		return false, fmt.Errorf("replaying spool: %w", err)
	}

	assert.Reachable("startup recovery completed", map[string]any{
		"duration": time.Since(replayStart).String(),
	})
	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.WithFields(map[string]any{
			"duration": time.Since(replayStart).String(),
		}).Debugf("Spool replay complete")
	}

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

// InterruptMaintenance cancels any in-flight maintenance task (sync, checkpoint
// creation, etc.) and blocks until the task goroutine has exited. Must be
// called before mutating FSM state that a running maintenance task may read
// (e.g. before InstallSnapshot, which writes fsm.lastCheckpointID /
// fsm.snapshotIndex that SynchronizeWithLeader reads).
func (a *Applier) InterruptMaintenance() {
	a.taskExecutor.Interrupt()
}

// Status returns the current applier status (statusNormal, statusSyncing, etc.).
func (a *Applier) Status() int32 {
	return a.status.Load()
}

// SetStatus stores the applier status.
func (a *Applier) SetStatus(s int32) {
	a.status.Store(s)
}

// Run is the Applier goroutine. It processes submitted work items and
// handles gating termination (unspool after maintenance tasks).
func (a *Applier) Run(ctx context.Context, stop chan struct{}) error {
	// Start the dedicated committer goroutine. It exits when commitCh is closed.
	committerDone := make(chan struct{})

	go func() {
		a.runCommitter(ctx)
		close(committerDone)
	}()

	defer func() {
		// Drain any pending commit, then shut down the committer.
		if a.pending != nil {
			_ = a.waitPendingCommit(ctx)
		}

		close(a.commitCh)
		<-committerDone
	}()

	var (
		readiesDuringGating int64
		gatingStart         time.Time
		waitStart           = time.Now()
	)

	for {
		select {
		case work := <-a.ch:
			a.batchWaitDurationHistogram.Record(ctx, time.Since(waitStart).Microseconds())

			if work.barrier != nil {
				// Drain pending commit before signaling barrier completion.
				if err := a.waitPendingCommit(ctx); err != nil {
					return err
				}

				close(work.barrier)
				waitStart = time.Now()

				continue
			}

			if work.syncLeader != 0 {
				// Drain pending commit before starting sync.
				if err := a.waitPendingCommit(ctx); err != nil {
					return err
				}

				a.startSyncSnapshot(ctx, work.syncLeader)
				waitStart = time.Now()

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
				// Drain pending commit before switching to spool mode.
				if err := a.waitPendingCommit(ctx); err != nil {
					return err
				}

				if a.logger.Enabled(logging.DebugLevel) {
					a.logger.Debugf("Spool committed entries")
				}

				err := a.spool.AppendCommittedEntries(ctx, work.entries...)
				if err != nil {
					return fmt.Errorf("spooling committed entries: %w", err)
				}
			}

			waitStart = time.Now()
		case <-a.gatingTerminated:
			a.gatingWaitDurationHistogram.Record(context.Background(), time.Since(gatingStart).Microseconds())
			a.readiesDuringGatingHistogram.Record(context.Background(), readiesDuringGating)
			readiesDuringGating = 0
			gatingStart = time.Time{}
			a.gatingTerminated = nil

			switch a.status.Load() {
			case statusOutOfSync:
				if a.logger.Enabled(logging.DebugLevel) {
					a.logger.Debugf("Background operation failed, node is out of sync — waiting for next sync")
				}
				waitStart = time.Now()

				continue
			case statusInstallingSnapshot:
				// processReadies set this status before interrupting the
				// previous maintenance task. InstallSnapshot is about to (or
				// already has) write fsm.snapshotIndex on the processReadies
				// goroutine; calling unspoolAndResume here would race.
				if a.logger.Enabled(logging.DebugLevel) {
					a.logger.Debugf("Skipping unspoolAndResume: leader snapshot installation in progress")
				}
				waitStart = time.Now()

				continue
			}

			unspoolStart := time.Now()

			err := a.unspoolAndResume(ctx)
			if err != nil {
				return err
			}

			a.unspoolDurationHistogram.Record(context.Background(), float64(time.Since(unspoolStart).Microseconds()))
			waitStart = time.Now()
		case <-stop:
			a.taskExecutor.Interrupt()

			return nil // defer handles pending commit drain and committer shutdown
		}
	}
}

// SyncSnapshot enqueues a snapshot synchronization with the leader for
// processing by the Run goroutine. This ensures that gatingTerminated is
// always assigned from Run, avoiding data races.
func (a *Applier) SyncSnapshot(leader uint64, stop chan struct{}) {
	a.status.Store(statusSyncing)

	select {
	case a.ch <- applyWork{syncLeader: leader}:
	case <-stop:
	}
}

// startSyncSnapshot is called from Run to perform the actual sync.
func (a *Applier) startSyncSnapshot(ctx context.Context, leader uint64) {
	syncDetails := map[string]any{
		"leader": leader,
	}
	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.WithFields(syncDetails).Debugf("Syncing snapshot from leader")
	}
	lifecycle.SendEvent("sync_snapshot_started", syncDetails)

	progress := state.NewSyncProgress()
	a.syncProgress.Store(progress)

	a.gatingTerminated = a.startMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
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

		syncedIndex, err := a.fsm.SynchronizeWithLeader(ctx, snapshotFetcher, progress)
		if err != nil {
			a.logger.WithFields(map[string]any{
				"leader": leader,
				"error":  err,
			}).Errorf("Failed to synchronize with leader, marking node as out of sync")
			a.syncProgress.Store(nil)
			a.status.Store(statusOutOfSync)

			return 0, nil
		}

		a.syncProgress.Store(nil)

		return syncedIndex, nil
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
	case statusInstallingSnapshot:
		return "installing_snapshot"
	default:
		return "unknown"
	}
}

// GetSyncProgress returns the current sync progress, or nil if not syncing.
func (a *Applier) GetSyncProgress() *state.SyncProgress {
	return a.syncProgress.Load()
}

// applyEntriesAndResolveCommands applies entries synchronously and resolves
// futures. Used by replay paths (spool, WAL) that do not need pipelining.
func (a *Applier) applyEntriesAndResolveCommands(ctx context.Context, entries ...raftpb.Entry) (*state.ApplyEntriesResult, error) {
	start := time.Now()

	result, err := a.fsm.ApplyEntries(ctx, entries...)
	if err != nil {
		return nil, fmt.Errorf("applying entries to Machine: %w", err)
	}

	a.applyEntriesHistogram.Record(ctx, time.Since(start).Microseconds())
	a.applyEntriesBatchSizeCounter.Add(ctx, int64(len(result.Results)))
	a.applyEntriesBatchSizeHistogram.Record(ctx, int64(len(result.Results)))

	a.resolveFutures(result)

	return result, nil
}

// waitPendingCommit blocks until the in-flight commit goroutine finishes.
// Returns the commit error, if any.
func (a *Applier) waitPendingCommit(ctx context.Context) error {
	if a.pending == nil {
		return nil
	}

	waitStart := time.Now()
	err := <-a.pending.done
	a.commitWaitHistogram.Record(ctx, time.Since(waitStart).Microseconds())

	a.pending = nil

	return err
}

// startAsyncCommit launches a goroutine that commits the batch and resolves
// futures. The caller must call waitPendingCommit before starting another.
// submitAsyncCommit sends a commit to the dedicated committer goroutine.
func (a *Applier) submitAsyncCommit(pb *state.PreparedBatch, pfs []pendingFuture) {
	done := make(chan error, 1)
	a.pending = &pendingCommit{done: done}
	a.commitCh <- commitWork{prepared: pb, futures: pfs, done: done}
}

// runCommitter is the dedicated goroutine that processes commits sequentially.
// It reads from commitCh and commits each batch, resolving futures on success.
// Exits when commitCh is closed.
func (a *Applier) runCommitter(ctx context.Context) {
	for work := range a.commitCh {
		err := a.fsm.CommitPreparedBatch(ctx, work.prepared)
		if err == nil {
			for _, pf := range work.futures {
				pf.future.Resolve(pf.result, pf.result.Error)
				a.futures.Delete(pf.proposalID)
			}
		}

		work.done <- err
	}
}

// applyEntriesPipelined prepares entries (CPU-bound) and starts the commit
// asynchronously. The previous batch's commit runs concurrently with this
// batch's prepare. Used by the hot path in Run (statusNormal).
func (a *Applier) applyEntriesPipelined(ctx context.Context, entries ...raftpb.Entry) (*state.ApplyEntriesResult, error) {
	prepareStart := time.Now()

	pb, err := a.fsm.PrepareEntries(ctx, entries...)
	if err != nil {
		_ = a.waitPendingCommit(ctx)

		return nil, fmt.Errorf("preparing entries: %w", err)
	}

	a.prepareDurationHistogram.Record(ctx, time.Since(prepareStart).Microseconds())
	a.applyEntriesBatchSizeCounter.Add(ctx, int64(len(pb.Result.Results)))
	a.applyEntriesBatchSizeHistogram.Record(ctx, int64(len(pb.Result.Results)))

	// Wait for the PREVIOUS batch's commit to finish before launching a new one.
	if err := a.waitPendingCommit(ctx); err != nil {
		pb.Close()

		return nil, fmt.Errorf("waiting for previous commit: %w", err)
	}

	// Collect futures for THIS batch.
	resolveCount := len(pb.Result.Results)
	if pb.Result.CheckpointRequired && resolveCount > 0 {
		resolveCount--
	}

	var pfs []pendingFuture
	for _, r := range pb.Result.Results[:resolveCount] {
		future, exists := a.futures.Load(r.ProposalID)
		if !exists {
			continue
		}

		pfs = append(pfs, pendingFuture{
			proposalID: r.ProposalID,
			result:     r,
			future:     future,
		})
	}

	// Checkpoint boundaries are already committed synchronously inside
	// PrepareEntries (via commitAndRequestCheckpoint). Resolve futures now.
	if pb.Result.CheckpointRequired {
		for _, pf := range pfs {
			pf.future.Resolve(pf.result, pf.result.Error)
			a.futures.Delete(pf.proposalID)
		}

		return pb.Result, nil
	}

	// Send to the committer goroutine. Futures are resolved when the
	// commit completes. No need to wait for the next batch.
	a.submitAsyncCommit(pb, pfs)

	return pb.Result, nil
}

// resolveFutures resolves proposal futures from an ApplyEntriesResult.
// When CheckpointRequired, the last result is deferred (resolved later
// by handleCheckpointRequired).
func (a *Applier) resolveFutures(result *state.ApplyEntriesResult) {
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
}

// applyEntriesToFSM applies entries to the Machine using pipelined commits.
// The prepare phase (CPU-bound) runs immediately while the previous batch's
// commit may still be in-flight. The commit is deferred until the next batch
// arrives or a drain is required.
func (a *Applier) applyEntriesToFSM(ctx context.Context, confState *raftpb.ConfState, entries ...raftpb.Entry) error {
	result, err := a.applyEntriesPipelined(ctx, entries...)
	if err != nil {
		return err
	}

	// If Machine stopped at a checkpoint boundary (ClosePeriod or CreateQueryCheckpoint),
	// enter maintenance mode and create the checkpoint off the Raft hot path.
	// The pipelined path already committed this batch synchronously.
	if result.CheckpointRequired {
		if result.QueryCheckpointID > 0 {
			return a.handleQueryCheckpointRequired(ctx, entries, result)
		}

		return a.handleCheckpointRequired(ctx, entries, result)
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

	// Called from Run goroutine — assign gating directly.
	a.gatingTerminated = a.startMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
		path, err := a.store.CreateTemporaryCheckpoint(fmt.Sprintf("checkpoint-%d", applyResult.CheckpointPeriodID))
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

// handleQueryCheckpointRequired enters maintenance mode to create the main store
// checkpoint. The read index checkpoint is created separately by the index builder
// when it processes the CreatedQueryCheckpoint log. While the checkpoint is being
// created, new committed entries are spooled and replayed afterward.
func (a *Applier) handleQueryCheckpointRequired(
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

	checkpointID := applyResult.QueryCheckpointID

	// Called from Run goroutine — assign gating directly.
	a.gatingTerminated = a.startMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
		if err := a.createMainStoreCheckpoint(checkpointID); err != nil {
			if deferredFuture != nil {
				deferredFuture.Resolve(state.ApplyResult{}, err)
			}

			return 0, err
		}

		if deferredFuture != nil {
			deferredFuture.Resolve(*deferredResult, nil)
		}

		return frozenAtIndex, nil
	}, nil)

	return nil
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

// startMaintenanceTask creates a gating channel and runs the maintenance task
// in the background. Returns the gating channel so the caller can deliver it
// to Run — either by direct assignment (when called from Run itself) or via
// a.gatingCh (when called from another goroutine like processReadies).
func (a *Applier) startMaintenanceTask(
	ctx context.Context,
	task func(ctx context.Context) (uint64, error),
	postGating func(ctx context.Context),
) chan struct{} {
	gatingTerminated := make(chan struct{})

	a.taskExecutor.Interrupt()
	a.taskExecutor.Run(ctx, func(ctx context.Context) error {
		var closeOnce sync.Once

		closeGating := func() { closeOnce.Do(func() { close(gatingTerminated) }) }
		defer closeGating()

		snapshotStart := time.Now()

		frozenAtIndex, err := task(ctx)
		if err != nil {
			return err
		}

		a.maintenanceSnapshotHistogram.Record(context.Background(), float64(time.Since(snapshotStart).Microseconds()))

		// If the task marked the node as out of sync (e.g. failed sync with leader),
		// skip spool replay — the node must wait for a new sync attempt.
		if a.status.Load() == statusOutOfSync {
			return nil
		}

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

	return gatingTerminated
}

func (a *Applier) unspoolAndResume(ctx context.Context) error {
	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.Debugf("Background operation terminated, applying spooled entries before resuming...")
	}

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

	lifecycle.SendEvent("spool replay completed", nil)
	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.Debugf("Unspooling operation terminated, resuming...")
	}

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

	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.WithFields(map[string]any{
			"from": lo,
			"to":   commitIndex,
		}).Debugf("Replaying WAL entries before spool")
	}

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

	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.WithFields(map[string]any{
			"count": len(entries),
		}).Debugf("WAL replay complete")
	}

	return nil
}

// replaySpoolUntil replays spooled entries from fromIndex up to maxIndex (inclusive).
// Entries beyond maxIndex are skipped. This prevents applying uncommitted entries
// that may be in the spool after a crash.
func (a *Applier) replaySpoolUntil(ctx context.Context, fromIndex uint64, maxIndex uint64) error {
	return a.replaySpoolImpl(ctx, fromIndex, maxIndex)
}

func (a *Applier) replaySpool(ctx context.Context, fromIndex uint64) error {
	return a.replaySpoolImpl(ctx, fromIndex, 0)
}

func (a *Applier) replaySpoolImpl(ctx context.Context, fromIndex uint64, maxIndex uint64) error {
	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.WithFields(map[string]any{
			"fromIndex": fromIndex,
			"maxIndex":  maxIndex,
		}).Debugf("Replaying spool")
	}

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
		// Skip uncommitted entries beyond the commit boundary.
		if maxIndex > 0 && entry.Index > maxIndex {
			return nil
		}

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

// handleCheckpointDuringReplay creates a checkpoint synchronously when a
// checkpoint-requiring entry (ClosePeriod or CreateQueryCheckpoint) is
// encountered during spool/WAL replay.
// Unlike handleCheckpointRequired, this does not enter maintenance mode -- the
// checkpoint is created synchronously (acceptable since we're already off
// the hot path) and remaining entries are applied directly.
func (a *Applier) handleCheckpointDuringReplay(ctx context.Context, applyResult *state.ApplyEntriesResult) error {
	if cpID := applyResult.QueryCheckpointID; cpID > 0 {
		return a.handleQueryCheckpointDuringReplay(ctx, applyResult)
	}

	if err := a.createReplayCheckpoint(applyResult); err != nil {
		return err
	}

	// Apply remaining entries. Loop to handle cascading checkpoints
	// (multiple ClosePeriod entries in the same spool batch).
	remaining := applyResult.RemainingEntries

	for len(remaining) > 0 {
		remainResult, err := a.applyEntriesAndResolveCommands(ctx, remaining...)
		if err != nil {
			return fmt.Errorf("applying remaining entries after checkpoint during replay: %w", err)
		}

		if !remainResult.CheckpointRequired {
			break
		}

		if err := a.createReplayCheckpoint(remainResult); err != nil {
			return err
		}

		remaining = remainResult.RemainingEntries
	}

	return nil
}

// createReplayCheckpoint creates a checkpoint for a ClosePeriod entry encountered
// during spool replay and resolves the deferred future.
func (a *Applier) createReplayCheckpoint(result *state.ApplyEntriesResult) error {
	checkpointPath, err := a.store.CreateTemporaryCheckpoint(fmt.Sprintf("replay-%d", result.CheckpointPeriodID))
	if err != nil {
		return fmt.Errorf("creating checkpoint during replay: %w", err)
	}

	if result.OnCheckpointDone != nil {
		result.OnCheckpointDone(checkpointPath)
	}

	if err := a.createBaselineSnapshot(); err != nil {
		a.logger.WithFields(map[string]any{"error": err}).
			Errorf("Failed to create baseline snapshot during replay (checker will degrade gracefully)")
	}

	if len(result.Results) > 0 {
		lastResult := &result.Results[len(result.Results)-1]
		if f, ok := a.futures.Load(lastResult.ProposalID); ok {
			lastResult.CheckpointPath = checkpointPath
			f.Resolve(*lastResult, nil)
			a.futures.Delete(lastResult.ProposalID)
		}
	}

	return nil
}

// handleQueryCheckpointDuringReplay creates query checkpoint stores synchronously
// during spool/WAL replay.
func (a *Applier) handleQueryCheckpointDuringReplay(ctx context.Context, applyResult *state.ApplyEntriesResult) error {
	if err := a.createMainStoreCheckpoint(applyResult.QueryCheckpointID); err != nil {
		return fmt.Errorf("during replay: %w", err)
	}

	// Resolve the deferred future.
	if len(applyResult.Results) > 0 {
		lastResult := &applyResult.Results[len(applyResult.Results)-1]
		if f, ok := a.futures.Load(lastResult.ProposalID); ok {
			f.Resolve(*lastResult, nil)
			a.futures.Delete(lastResult.ProposalID)
		}
	}

	// Apply remaining entries directly.
	if len(applyResult.RemainingEntries) > 0 {
		_, err := a.applyEntriesAndResolveCommands(ctx, applyResult.RemainingEntries...)
		if err != nil {
			return fmt.Errorf("applying remaining entries after query checkpoint replay: %w", err)
		}
	}

	return nil
}

// createMainStoreCheckpoint creates a physical Pebble checkpoint of the main store.
// The read index checkpoint is created separately by the index builder when it
// processes the CreatedQueryCheckpoint log.
func (a *Applier) createMainStoreCheckpoint(checkpointID uint64) error {
	if _, err := a.store.CreateQueryCheckpoint(checkpointID); err != nil {
		return fmt.Errorf("creating main store query checkpoint %d: %w", checkpointID, err)
	}

	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.WithFields(map[string]any{
			"checkpointID": checkpointID,
		}).Debugf("Created main store query checkpoint")
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
