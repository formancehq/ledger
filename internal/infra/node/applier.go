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
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/spool"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// applyWork represents a unit of work for the Applier goroutine.
// Exactly one of (entries, barrier, syncLeader, installSnapshot) is set per work item.
type applyWork struct {
	entries         []raftpb.Entry
	confState       *raftpb.ConfState
	barrier         chan struct{} // non-nil = drain; closed when processed
	syncLeader      uint64        // non-zero = trigger SyncSnapshot from Run goroutine
	installSnapshot chan struct{} // non-nil = prepare for snapshot install; closed when ready
	// responses are MsgStorageApplyResp messages attached to the
	// MsgStorageApply that carried these entries. They must be Step()-ed back
	// into rawNode AFTER the entries are applied to the FSM — that is what
	// bumps raft.Applied and aligns it with FSM-applied. Used only when
	// raft.Config.AsyncStorageWrites is true.
	responses []raftpb.Message
}

// gatingResult carries the outcome of a maintenance task back to the Run
// goroutine so it can decide whether to unspool or mark the node out-of-sync.
type gatingResult struct {
	syncFailed bool // true if the sync task failed, node should be marked out-of-sync
	taskFailed bool // true if the task returned an error, node should stay gated until shutdown
}

// Applier owns all FSM application logic and gating/spool lifecycle.
// It runs as a dedicated goroutine, decoupling WAL writes (processReadies)
// from FSM application so they can overlap across consecutive Ready cycles.
type Applier struct {
	fsm                     *state.Machine
	recovery                *state.Recovery
	synchronizer            *state.Synchronizer
	spool                   spool.Spool
	store                   *dal.Store
	wal                     wal.WAL
	futures                 *SyncMap[uint64, *pendingApplyFuture]
	taskExecutor            *worker.SingleTaskExecutor
	logger                  logging.Logger
	compactionMargin        uint64
	replayBatchSize         int
	snapshotFetcherProvider state.SnapshotFetcherProvider

	status           *atomic.Int32
	gatingReason     atomic.Int32 // gatingReason* constants — for observability
	syncProgress     atomic.Pointer[state.SyncProgress]
	gatingTerminated chan gatingResult
	ch               chan applyWork  // buffered(1)
	commitCh         chan commitWork // buffered(1), read by the committer goroutine

	// pending holds the in-flight commit result channel, if any.
	// At most one at a time. Drained before barriers, checkpoints, and shutdown.
	pending *pendingCommit

	// responseSink receives MsgStorageApplyResp messages after the apply (or
	// spool append) completes for a batch. Same channel instance the Node
	// drains in its orchestrate loop. Nil-safe (no-op when the channel is
	// unwired in tests).
	responseSink LocalResponses

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
	// term is the Raft term observed at Propose time (copied from
	// pendingApplyFuture). It is a lower bound on the term the entry was
	// actually appended under; runCommitter uses it to detect the
	// "old-term entry committed indirectly" coverage condition.
	term uint64
}

// pendingApplyFuture pairs the FSM-apply future with the Raft term observed
// at Propose time. The term lets FailFuturesBelowTerm fail futures whose
// entries were truncated by a new leader (issue #172): once any entry at a
// higher term is applied locally, Raft leader-completeness guarantees that
// any uncommitted local proposal from an older term was overwritten.
type pendingApplyFuture struct {
	term   uint64
	future *futures.Future[state.ApplyResult]
}

type commitWork struct {
	prepared *state.PreparedBatch
	futures  []pendingFuture
	// maxTerm is the highest term seen in the entries that produced this
	// commit batch. After resolving per-commandID futures, the committer
	// uses it to fail any pending future whose stored term is strictly
	// smaller — those proposals were truncated.
	maxTerm uint64
	// responses are MsgStorageApplyResp messages tied to this commit. On
	// commit success, runCommitter sends them on Applier.responseSink so
	// raft.Applied advances only after CommitPreparedBatch has landed.
	// Empty on all but the last sub-batch of an applyEntriesToFSM call,
	// and always empty for the replay path (applyEntriesAndResolveCommands).
	responses []raftpb.Message
	done      chan error
}

type pendingCommit struct {
	done chan error // signaled when the committer goroutine finishes this work
}

// NewApplier creates a new Applier with all metrics registered on the provided meter.
func NewApplier(
	fsm *state.Machine,
	recovery *state.Recovery,
	synchronizer *state.Synchronizer,
	spool spool.Spool,
	store *dal.Store,
	wal wal.WAL,
	logger logging.Logger,
	meter metric.Meter,
	compactionMargin uint64,
	replayBatchSize int,
	snapshotFetcherProvider state.SnapshotFetcherProvider,
	responseSink LocalResponses,
) (*Applier, error) {
	initialStatus := atomic.Int32{}
	initialStatus.Store(statusNormal)

	a := &Applier{
		fsm:                     fsm,
		recovery:                recovery,
		synchronizer:            synchronizer,
		spool:                   spool,
		store:                   store,
		wal:                     wal,
		futures:                 &SyncMap[uint64, *pendingApplyFuture]{},
		taskExecutor:            worker.NewSingleTaskExecutor(logger),
		logger:                  logger,
		compactionMargin:        compactionMargin,
		replayBatchSize:         replayBatchSize,
		snapshotFetcherProvider: snapshotFetcherProvider,
		responseSink:            responseSink,
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

// StoreFuture registers a future for a given command ID and the Raft term
// the proposer observed at Propose time. The term is later used to fail the
// future if a higher-term entry is applied locally (issue #172).
func (a *Applier) StoreFuture(commandID, term uint64, future *futures.Future[state.ApplyResult]) {
	a.futures.Store(commandID, &pendingApplyFuture{term: term, future: future})
}

// DeleteFuture removes the future for a given command ID.
func (a *Applier) DeleteFuture(commandID uint64) {
	a.futures.Delete(commandID)
}

// ResolveDroppedFuture is called from handleProposal when rawNode rejects a
// proposal (e.g. ErrProposalDropped after leadership loss). It atomically
// removes the registered FSM future and resolves it with the dropping error,
// so the caller awaiting fsmFuture.Wait() unblocks immediately rather than
// hanging until a term advance.
func (a *Applier) ResolveDroppedFuture(commandID uint64, err error) {
	if paf, ok := a.futures.LoadAndDelete(commandID); ok {
		paf.future.Resolve(state.ApplyResult{}, err)
	}
}

// FailFuturesBelowTerm resolves every pending future whose stored term is
// strictly below threshold. Used after applying a batch whose max term is
// `threshold`: per Raft leader-completeness, any committed older-term entry
// has already been applied (resolving its future by commandID) before this
// sweep runs, so anything left at term < threshold is a truncated local
// proposal that will never apply.
//
// LoadAndDelete (with an identity check) makes the resolve race-free: if
// another path has taken ownership of the future since the Range loop
// captured it (extractBatchFutures, extractDeferredFuture, or
// ResolveDroppedFuture all use LoadAndDelete for the same reason), the
// second LoadAndDelete returns false and we skip it. This prevents a
// double Resolve on a futures.Future, which is not idempotent.
func (a *Applier) FailFuturesBelowTerm(threshold uint64, err error) {
	resolved := 0

	a.futures.Range(func(commandID uint64, paf *pendingApplyFuture) bool {
		if paf.term >= threshold {
			return true
		}

		cur, ok := a.futures.LoadAndDelete(commandID)
		if !ok || cur != paf {
			return true
		}

		paf.future.Resolve(state.ApplyResult{}, err)
		resolved++

		// Coverage anchor for the leadership-lost error taxonomy: proves the
		// below-term resolve path is actually exercised under fault injection
		// (without it, workload-side absence checks could pass vacuously).
		assert.Reachable("future failed by below-term sweep", map[string]any{
			"commandID": commandID,
			"term":      paf.term,
			"threshold": threshold,
		})

		return true
	})

	// This sweep runs after every committed batch with maxTerm > 0, so the
	// condition — not the call — carries the signal: it is true only when an
	// actual straggler (truncated lower-term proposal) was swept.
	assert.Sometimes(resolved > 0, "FailFuturesBelowTerm resolved at least one future", map[string]any{
		"threshold": threshold,
		"resolved":  resolved,
	})
}

// batchMaxTerm returns the highest Raft term in entries.
//
// Within any batch of committed entries delivered by raft, terms are
// monotonically non-decreasing: raft delivers entries in log order, and a
// new leader can only append at a term strictly higher than any preceding
// entry's. The last entry's term is therefore the max. Returns 0 for an
// empty slice — callers skip the sweep when maxTerm == 0.
func batchMaxTerm(entries []raftpb.Entry) uint64 {
	if len(entries) == 0 {
		return 0
	}

	return entries[len(entries)-1].Term
}

// batchMaxTermDecoded is the DecodedEntry variant of batchMaxTerm.
func batchMaxTermDecoded(decoded []state.DecodedEntry) uint64 {
	if len(decoded) == 0 {
		return 0
	}

	return decoded[len(decoded)-1].Entry.Term
}

// sweepBelowTerm fails every pending future whose stored term is strictly
// smaller than the max term in entries. Callers should invoke this AFTER
// per-commandID resolution for the same batch, so committed older-term
// entries get their futures resolved first.
func (a *Applier) sweepBelowTerm(entries []raftpb.Entry) {
	maxTerm := batchMaxTerm(entries)

	if maxTerm > 0 {
		a.FailFuturesBelowTerm(maxTerm, ErrLeadershipLost)
	}
}

// extractDeferredFuture pulls the future for the LAST entry in result (the
// entry that triggered CheckpointRequired). Callers attach a CheckpointPath
// to lastResult before resolving the future. Both return values may be nil:
// if there are no results, both are nil; if the proposalID has no registered
// future, only lastResult is non-nil.
//
// Centralizes the LoadAndDelete + "last result is the deferred one"
// convention used by every checkpoint flow, so adding a new checkpoint
// trigger only needs to call this helper.
func (a *Applier) extractDeferredFuture(result *state.ApplyEntriesResult) (
	*state.ApplyResult,
	*futures.Future[state.ApplyResult],
) {
	if len(result.Results) == 0 {
		return nil, nil
	}

	lastResult := &result.Results[len(result.Results)-1]
	if paf, ok := a.futures.LoadAndDelete(lastResult.ProposalID); ok {
		return lastResult, paf.future
	}

	return lastResult, nil
}

// extractBatchFutures pulls the futures for every result in the batch,
// except the deferred-checkpoint one when result.CheckpointRequired is
// set (callers pair this with extractDeferredFuture). Uses LoadAndDelete
// for atomic ownership transfer — see FailFuturesBelowTerm for the race
// rationale.
func (a *Applier) extractBatchFutures(result *state.ApplyEntriesResult) []pendingFuture {
	n := len(result.Results)
	if result.CheckpointRequired && n > 0 {
		n--
	}

	pfs := make([]pendingFuture, 0, n)

	for _, r := range result.Results[:n] {
		paf, ok := a.futures.LoadAndDelete(r.ProposalID)
		if !ok {
			continue
		}

		pfs = append(pfs, pendingFuture{
			proposalID: r.ProposalID,
			result:     r,
			future:     paf.future,
			term:       paf.term,
		})
	}

	return pfs
}

// Interrupt interrupts any running maintenance task.
func (a *Applier) Interrupt() {
	a.taskExecutor.Interrupt()
}

// TaskError returns the channel on which task executor errors are reported.
func (a *Applier) TaskError() <-chan error {
	return a.taskExecutor.Error()
}

// setOutOfSync transitions the applier to the out-of-sync status.
// Only called from RecoverAndReplay (startup, before Run starts).
func (a *Applier) setOutOfSync() {
	a.status.Store(statusOutOfSync)
}

// RecoverAndReplay checks whether the store is up to date with the FSM and,
// if so, recovers the in-memory cache, replays Raft WAL entries and spooled
// entries to bring the node fully up to date. When the store is not up to date
// (e.g. the node crashed after receiving a leader snapshot but before syncing),
// it marks the applier as out-of-sync so SynchronizeWithLeader runs.
// Returns true when the store was up to date and replay succeeded.
func (a *Applier) RecoverAndReplay(ctx context.Context) (bool, error) {
	// Reset the spool unconditionally: any data from a previous run is stale.
	// A SIGKILL can leave a partially-written spool that causes corrupt record
	// errors and entry index gaps during post-sync replay.
	if err := a.spool.Reset(); err != nil {
		return false, fmt.Errorf("resetting spool: %w", err)
	}

	isStoreUpToDate, err := a.synchronizer.IsStoreUpToDate(ctx)
	if err != nil {
		return false, fmt.Errorf("checking if store is up to date: %w", err)
	}

	if !isStoreUpToDate {
		if a.logger.Enabled(logging.DebugLevel) {
			a.logger.Debugf("Store is not up to date, resuming from snapshot and tagging node as out of sync")
		}
		a.setOutOfSync()

		return false, nil
	}

	// Restore cache from Pebble (store is up to date, checkpoint has cache data).
	// FSM counters (sequences, chapters, reversions, etc.) are already loaded by
	// NewMachine → RecoverState in the constructor and Pebble has not changed
	// since (InstallSnapshot only touches in-memory state, and the
	// SynchronizeWithLeader path exits earlier via setOutOfSync).
	if err := a.recovery.RestoreCacheFromStore(); err != nil {
		return false, fmt.Errorf("restoring cache from store on restart: %w", err)
	}

	// Recovery: if chapters are in CLOSING state but no seal checkpoint exists,
	// the node crashed after CloseChapter batch.Commit() but before checkpoint creation.
	// Pebble state is exactly at the CloseChapter boundary right now (spool replay hasn't run).
	for _, chapter := range a.fsm.ClosingChapters() {
		name := state.SealCheckpointName(chapter.GetId())
		if _, exists := a.store.TemporaryCheckpointPath(name); !exists {
			if a.logger.Enabled(logging.DebugLevel) {
				a.logger.Debugf("Recovering: creating seal checkpoint for closing chapter %d", chapter.GetId())
			}

			_, err := a.store.CreateTemporaryCheckpoint(name)
			if err != nil {
				return false, fmt.Errorf("creating recovery seal checkpoint: %w", err)
			}

			// The checkpoint is now on disk. The Sealer's recoverPendingSeal
			// and periodic reconciliation will pick it up once started.
		}
	}

	storeLastAppliedIndex, err := query.ReadLastAppliedIndex(a.store)
	if err != nil {
		return false, fmt.Errorf("getting store last applied index: %w", err)
	}

	replayStart := time.Now()

	// Replay Raft WAL entries that were committed but not yet applied
	// (e.g. the node crashed between Raft commit and FSM apply).
	if err := a.replayWAL(ctx, storeLastAppliedIndex); err != nil {
		return false, fmt.Errorf("replaying WAL: %w", err)
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
//
// responses (AsyncStorageWrites only) are MsgStorageApplyResp messages that
// must be Step()-ed back into rawNode after the apply completes for this
// batch. Pass nil in tests / sync mode.
func (a *Applier) Submit(entries []raftpb.Entry, confState *raftpb.ConfState, responses []raftpb.Message, stop chan struct{}) {
	select {
	case a.ch <- applyWork{entries: entries, confState: confState, responses: responses}:
	case <-stop:
	}
}

// Drain blocks until all previously submitted work has been processed.
// Used before operations that require the FSM to be idle (leadership acquisition).
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

// PrepareForSnapshotInstall asks the Run goroutine to drain pending work,
// interrupt any maintenance task, and set statusInstallingSnapshot. Blocks
// until Run has completed all of this. After this returns, the caller
// (processReadies) can safely call InstallSnapshot on the FSM.
//
// This is the single-writer protocol: only Run writes status, so there is
// no race between status writes and entry processing.
func (a *Applier) PrepareForSnapshotInstall(stop chan struct{}) {
	ack := make(chan struct{})
	select {
	case a.ch <- applyWork{installSnapshot: ack}:
		select {
		case <-ack:
		case <-stop:
		}
	case <-stop:
	}
}

// Status returns the current applier status.
func (a *Applier) Status() int32 {
	return a.status.Load()
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

				a.status.Store(statusGated)
				a.gatingReason.Store(gatingReasonSyncing)
				a.startSyncSnapshot(ctx, work.syncLeader)
				waitStart = time.Now()

				continue
			}

			if work.installSnapshot != nil {
				// Drain pending commit and interrupt any maintenance task,
				// then mark the node as installing a snapshot. Only after
				// all of this is done do we signal processReadies (via ack)
				// that it is safe to call InstallSnapshot on the FSM.
				//
				// Because this runs in the Run goroutine AFTER all prior
				// work items (including entries that may trigger
				// handleCheckpointRequired), there is no status race.
				if err := a.waitPendingCommit(ctx); err != nil {
					return err
				}

				a.taskExecutor.Interrupt()
				a.status.Store(statusInstallingSnapshot)
				close(work.installSnapshot)
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
				// applyEntriesToFSM threads work.responses down to the last
				// applyEntriesPipelined call, which attaches them to the
				// commitWork sent to runCommitter. runCommitter fires the
				// responses AFTER CommitPreparedBatch succeeds, so
				// raft.Applied advances in lockstep with FSM-applied without
				// serializing prepare-N+1 against commit-N (the
				// applier→committer pipeline is preserved).
				err := a.applyEntriesToFSM(ctx, work.confState, work.responses, work.entries...)
				if err != nil {
					return err
				}
			default:
				// Drain pending commit before switching to spool mode.
				if err := a.waitPendingCommit(ctx); err != nil {
					return err
				}

				if a.logger.Enabled(logging.TraceLevel) {
					a.logger.Tracef("Spool committed entries")
				}

				err := a.spool.AppendCommittedEntries(ctx, work.entries...)
				if err != nil {
					return fmt.Errorf("spooling committed entries: %w", err)
				}

				// Entries are durably staged in the spool and will be
				// re-applied via unspool without further raft involvement,
				// so acknowledging Applied now is safe.
				if len(work.responses) > 0 && a.responseSink != nil {
					select {
					case a.responseSink <- work.responses:
					case <-stop:
						return nil
					}
				}
			}

			waitStart = time.Now()
		case result := <-a.gatingTerminated:
			a.gatingWaitDurationHistogram.Record(context.Background(), time.Since(gatingStart).Microseconds())
			a.readiesDuringGatingHistogram.Record(context.Background(), readiesDuringGating)
			readiesDuringGating = 0
			gatingStart = time.Time{}
			a.gatingTerminated = nil

			if a.status.Load() == statusInstallingSnapshot {
				// Run set this status via the installSnapshot command.
				// processReadies is about to (or already has) called
				// InstallSnapshot on the FSM — skip unspoolAndResume.
				if a.logger.Enabled(logging.DebugLevel) {
					a.logger.Debugf("Skipping unspoolAndResume: leader snapshot installation in progress")
				}
				waitStart = time.Now()

				continue
			}

			if result.syncFailed {
				a.status.Store(statusOutOfSync)
				if a.logger.Enabled(logging.DebugLevel) {
					a.logger.Debugf("Background operation failed, node is out of sync — waiting for next sync")
				}
				waitStart = time.Now()

				continue
			}

			if result.taskFailed {
				if a.logger.Enabled(logging.DebugLevel) {
					a.logger.Debugf("Background operation failed, keeping node gated until task error is handled")
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
// processing by the Run goroutine. Run sets the status to statusGated
// when it processes this command — no cross-goroutine status write needed.
func (a *Applier) SyncSnapshot(leader uint64, stop chan struct{}) {
	select {
	case a.ch <- applyWork{syncLeader: leader}:
	case <-stop:
	}
}

// TrySyncSnapshot is a non-blocking variant of SyncSnapshot: returns true
// if the sync request was enqueued, false if the work channel is full
// (another item is already pending). Callers that fire from a per-Ready
// polling loop (e.g. the out-of-sync fallback in processReady) MUST use
// this variant — blocking would stall the raft-ready goroutine and enqueuing
// duplicate syncLeader items causes startMaintenanceTask to interrupt its
// own in-flight fetch, restarting checkpoint download from scratch.
func (a *Applier) TrySyncSnapshot(leader uint64) bool {
	select {
	case a.ch <- applyWork{syncLeader: leader}:
		return true
	default:
		return false
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

	a.gatingTerminated = a.startMaintenanceTask(ctx, func(ctx context.Context) (maintenanceTaskResult, error) {
		snapshotFetcher, err := a.snapshotFetcherProvider.GetForPeer(leader)
		if err != nil {
			a.logger.WithFields(map[string]any{
				"leader": leader,
				"error":  err,
			}).Errorf("Failed to get snapshot fetcher, marking node as out of sync")
			a.syncProgress.Store(nil)

			return maintenanceTaskResult{syncFailed: true}, nil
		}

		syncedIndex, err := a.synchronizer.SynchronizeWithLeader(ctx, snapshotFetcher, progress)
		if err != nil {
			a.logger.WithFields(map[string]any{
				"leader": leader,
				"error":  err,
			}).Errorf("Failed to synchronize with leader, marking node as out of sync")
			a.syncProgress.Store(nil)

			return maintenanceTaskResult{syncFailed: true}, nil
		}

		a.syncProgress.Store(nil)

		return maintenanceTaskResult{frozenAtIndex: syncedIndex}, nil
	}, nil)
}

// StatusString returns the current applier status as a human-readable string.
func (a *Applier) StatusString() string {
	switch a.status.Load() {
	case statusNormal:
		return "normal"
	case statusGated:
		switch a.gatingReason.Load() {
		case gatingReasonSyncing:
			return "syncing"
		case gatingReasonSnapshotting:
			return "snapshotting"
		case gatingReasonQueryCheckpoint:
			return "query-checkpoint"
		default:
			return "gated"
		}
	case statusOutOfSync:
		return "out_of_sync"
	case statusInstallingSnapshot:
		return "installing_snapshot"
	default:
		return "unknown"
	}
}

// IsSyncing returns true if the applier is gated due to a leader sync.
func (a *Applier) IsSyncing() bool {
	return a.status.Load() == statusGated && a.gatingReason.Load() == gatingReasonSyncing
}

// GetSyncProgress returns the current sync progress, or nil if not syncing.
func (a *Applier) GetSyncProgress() *state.SyncProgress {
	return a.syncProgress.Load()
}

// applyEntriesAndResolveCommands applies entries synchronously and resolves
// futures. Used by replay paths (spool, WAL) that do not need pipelining.
//
// Callers must ensure no checkpoint trigger appears mid-slice; use
// applyReplayEntries when iterating an arbitrary slice. The caller owns
// the lifetime of decoded[].Proposal — this method does not release them.
func (a *Applier) applyEntriesAndResolveCommands(ctx context.Context, decoded ...state.DecodedEntry) (*state.ApplyEntriesResult, error) {
	start := time.Now()

	pb, err := a.fsm.PrepareDecodedEntries(ctx, a.store, decoded...)
	if err != nil {
		return nil, fmt.Errorf("preparing entries on Machine: %w", err)
	}

	if err := a.fsm.CommitPreparedBatch(ctx, pb); err != nil {
		return nil, fmt.Errorf("committing entries on Machine: %w", err)
	}

	result := pb.Result

	a.applyEntriesHistogram.Record(ctx, time.Since(start).Microseconds())
	a.applyEntriesBatchSizeCounter.Add(ctx, int64(len(result.Results)))
	a.applyEntriesBatchSizeHistogram.Record(ctx, int64(len(result.Results)))

	a.resolveFutures(result)

	if maxTerm := batchMaxTermDecoded(decoded); maxTerm > 0 {
		a.FailFuturesBelowTerm(maxTerm, ErrLeadershipLost)
	}

	return result, nil
}

// applyReplayEntries applies a slice of entries during replay (spool or WAL),
// splitting on checkpoint boundaries so each FSM batch contains at most one
// trigger entry (always last). Each checkpoint is handled synchronously
// (createReplayCheckpoint / createMainStoreCheckpoint) before continuing.
// Cascading checkpoints in the same slice are handled by the loop.
func (a *Applier) applyReplayEntries(ctx context.Context, entries []raftpb.Entry) error {
	decoded, err := state.DecodeEntries(entries)
	if err != nil {
		return fmt.Errorf("decoding replay entries: %w", err)
	}

	defer state.ReleaseDecodedEntries(decoded)

	for offset := 0; offset < len(decoded); {
		boundary := findCheckpointBoundaryDecoded(decoded[offset:])

		end := offset + boundary

		result, err := a.applyEntriesAndResolveCommands(ctx, decoded[offset:end]...)
		if err != nil {
			return err
		}

		if result.CheckpointRequired {
			if err := a.handleCheckpointDuringReplay(ctx, result); err != nil {
				return err
			}
		}

		offset = end
	}

	return nil
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

// submitAsyncCommit sends a commit to the dedicated committer goroutine.
// maxTerm carries the highest term seen in the source entries so the
// committer can fail older-term pending futures after resolving this batch.
// responses (AsyncStorageWrites only) are MsgStorageApplyResp messages to
// fire on the response sink AFTER commit completes; pass nil to skip.
func (a *Applier) submitAsyncCommit(pb *state.PreparedBatch, pfs []pendingFuture, maxTerm uint64, responses []raftpb.Message) {
	done := make(chan error, 1)
	a.pending = &pendingCommit{done: done}
	a.commitCh <- commitWork{prepared: pb, futures: pfs, maxTerm: maxTerm, responses: responses, done: done}
}

// runCommitter is the dedicated goroutine that processes commits sequentially.
// It reads from commitCh and commits each batch, resolving futures on success
// or failure. Exits when commitCh is closed.
func (a *Applier) runCommitter(ctx context.Context) {
	for work := range a.commitCh {
		err := a.fsm.CommitPreparedBatch(ctx, work.prepared)

		// Fire MsgStorageApplyResp responses BEFORE resolving futures /
		// failing older-term stragglers, and only on success. This is what
		// bumps raft.Applied in lockstep with FSM-applied under
		// AsyncStorageWrites. On commit failure we intentionally don't fire:
		// raft.Applied stays behind, the committed entries will be re-emitted
		// on the next boot (raft.Config.Applied re-reads from Pebble), and
		// re-apply is idempotent via the applied-index guard.
		//
		// On ctx.Done we drop the response and fall through — we MUST NOT
		// return here, otherwise the trailing `work.done <- err` never fires
		// and Applier.Run's deferred `waitPendingCommit` deadlocks on
		// `<-a.pending.done` during shutdown (finding 34540caa/9047f08a).
		if err == nil && len(work.responses) > 0 && a.responseSink != nil {
			select {
			case a.responseSink <- work.responses:
			case <-ctx.Done():
			}
		}

		if err == nil {
			// 1. Resolve futures owned by THIS batch. Ownership was taken
			//    via extractBatchFutures in applyEntriesPipelined (which
			//    uses LoadAndDelete), so the futures are no longer in the
			//    map — no per-future Delete needed here.
			//
			// oldTermResolved counts futures tagged below this batch's max
			// term that nevertheless resolve with their real apply result:
			// the entry committed (possibly indirectly, under a newer
			// leader) and per-commandID resolution won over the sweep — the
			// resolve-before-sweep ordering this loop exists to preserve.
			// The tag is the propose-time observed term (a lower bound), so
			// the count can also include the benign lastObservedTerm-lag
			// window; both variants exercise the same ordering.
			oldTermResolved := 0

			for _, pf := range work.futures {
				if work.maxTerm > 0 && pf.term < work.maxTerm {
					oldTermResolved++
				}

				pf.future.Resolve(pf.result, pf.result.Error)
			}

			// 2. Sweep stragglers from older terms (issue #172).
			if work.maxTerm > 0 {
				a.FailFuturesBelowTerm(work.maxTerm, ErrLeadershipLost)
			}

			// Runs on every committed batch: the condition is the signal —
			// true only when a below-maxTerm future got its real result in
			// the same batch that triggers a sweep.
			assert.Sometimes(oldTermResolved > 0,
				"old-term entry committed and resolved in same batch as a sweep",
				map[string]any{
					"maxTerm":         work.maxTerm,
					"oldTermResolved": oldTermResolved,
					"batchFutures":    len(work.futures),
				})
		} else {
			// Fail fast: ownership was already taken via LoadAndDelete, so no
			// other path (term sweep, dropped-proposal resolution) can ever
			// reach these futures — leaving them unresolved would hang every
			// client in the batch until its own deadline while the commit
			// error sits unobserved in work.done until the next work item.
			// The storage error maps to gRPC Unknown (ambiguous by contract):
			// the entry is Raft-committed and will reapply on restart, so a
			// client retry resolves through the idempotency/reference checks.
			for _, pf := range work.futures {
				pf.future.Resolve(state.ApplyResult{}, err)
			}

			// Sweep older-term stragglers here too: term-truncated entries
			// never apply regardless of this batch's commit outcome, and the
			// deferred fatal error may sit unobserved on a quiet node.
			if work.maxTerm > 0 {
				a.FailFuturesBelowTerm(work.maxTerm, ErrLeadershipLost)
			}
		}

		work.done <- err
	}
}

// applyEntriesPipelined prepares entries (CPU-bound) and starts the commit
// asynchronously. The previous batch's commit runs concurrently with this
// batch's prepare. Used by the hot path in Run (statusNormal).
//
// The caller (applyEntriesToFSM) owns the lifetime of decoded[].Proposal
// pointers — this method does not release them.
func (a *Applier) applyEntriesPipelined(ctx context.Context, responses []raftpb.Message, decoded ...state.DecodedEntry) (*state.ApplyEntriesResult, error) {
	prepareStart := time.Now()

	pb, err := a.fsm.PrepareDecodedEntries(ctx, a.store, decoded...)
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

	// Collect futures for THIS batch. extractBatchFutures uses LoadAndDelete
	// for atomic ownership so a concurrent FailFuturesBelowTerm can't race
	// to double-resolve them, and skips the deferred (last) entry when
	// CheckpointRequired is set — that one is handled downstream by
	// gateAndLaunchMaintenance for the hot path, or by
	// handleCheckpointDuringReplay on the replay path.
	pfs := a.extractBatchFutures(pb.Result)

	// Highest term seen in this batch, used by the post-resolve sweep
	// (issue #172). See batchMaxTermDecoded for the monotonic-term shortcut.
	maxTerm := batchMaxTermDecoded(decoded)

	// Send to the committer goroutine. Futures are resolved when the
	// commit completes. No need to wait for the next batch. responses (may be
	// nil for non-last sub-batches of the current applyEntriesToFSM call, or
	// when AsyncStorageWrites is off) rides with the commit and is fired by
	// runCommitter on success.
	a.submitAsyncCommit(pb, pfs, maxTerm, responses)

	// Checkpoint batches must be drained synchronously so the caller can
	// safely create the Pebble checkpoint on a fully-committed store. Without
	// this drain, handleCheckpointRequired/handleQueryCheckpointRequired would
	// race the in-flight commit; with it, all commits go through runCommitter
	// in a strictly serialized order (one batch = one commit).
	if pb.Result.CheckpointRequired {
		if err := a.waitPendingCommit(ctx); err != nil {
			return nil, fmt.Errorf("draining commit before checkpoint: %w", err)
		}
	}

	return pb.Result, nil
}

// resolveFutures resolves proposal futures from an ApplyEntriesResult.
// Used by the replay path (applyEntriesAndResolveCommands). When
// CheckpointRequired is set the last result is deferred — extracted by
// extractBatchFutures and resolved later by handleCheckpointDuringReplay.
func (a *Applier) resolveFutures(result *state.ApplyEntriesResult) {
	for _, pf := range a.extractBatchFutures(result) {
		pf.future.Resolve(pf.result, pf.result.Error)
	}
}

// applyEntriesToFSM applies entries to the Machine using pipelined commits.
// The prepare phase (CPU-bound) runs immediately while the previous batch's
// commit may still be in-flight. The commit is deferred until the next batch
// arrives or a drain is required.
//
// To preserve the "one PrepareEntries call = one commit through runCommitter"
// invariant, callers must never mix a checkpoint-trigger entry with later
// entries in the same FSM batch. We split the input slice at each checkpoint
// trigger and apply each prefix as its own batch; entries past a trigger that
// actually fires (CheckpointRequired) are spooled before we hand control to
// the maintenance task so the spool replay picks them up once gating clears.
//
// findCheckpointBoundary classifies entries STRUCTURALLY (the proposal's last
// order is a checkpoint trigger), but the trigger may not actually fire at
// apply time — e.g. checkStaleProposal rejects the whole proposal after a
// leadership transition, so no order runs and CheckpointRequired stays false.
// When that happens we MUST continue processing the tail in subsequent FSM
// batches; dropping it produces an "entry index gap detected" panic in the
// next PrepareEntries call. Hence the loop.
func (a *Applier) applyEntriesToFSM(ctx context.Context, confState *raftpb.ConfState, responses []raftpb.Message, entries ...raftpb.Entry) error {
	// Decode once at the applier boundary so every downstream stage
	// (checkpoint boundary scan, FSM apply, position validation) reads the
	// already-decoded proposal instead of re-unmarshalling the raw payload.
	decoded, err := state.DecodeEntries(entries)
	if err != nil {
		return fmt.Errorf("decoding entries: %w", err)
	}

	defer state.ReleaseDecodedEntries(decoded)

	// responsesAttached tracks whether the async-storage response batch has
	// been handed off to a submitAsyncCommit (via a sub-batch whose end
	// reaches len(decoded) — the only sub-batch whose commit completion
	// means "everything in this applyWork is durably in the FSM"). If a
	// checkpoint fires on a sub-batch that ends short of len(decoded),
	// nothing was attached and we fall back to eager delivery after
	// handleCheckpointRequired has drained the tail through the spool.
	responsesAttached := false

	for offset := 0; offset < len(decoded); {
		boundary := findCheckpointBoundaryDecoded(decoded[offset:])

		end := offset + boundary
		head := decoded[offset:end]
		tail := decoded[end:]

		var subResponses []raftpb.Message
		if end == len(decoded) {
			subResponses = responses
			responsesAttached = true
		}

		result, err := a.applyEntriesPipelined(ctx, subResponses, head...)
		if err != nil {
			return err
		}

		if !result.CheckpointRequired {
			// Either head carried no trigger, or the trigger was rejected
			// (e.g. stale proposal). Continue with the next FSM batch — the
			// tail must not be dropped.
			offset = end

			continue
		}

		// Spool entries past the checkpoint trigger so the spool replay picks
		// them up after the maintenance window clears. Doing this here (instead
		// of inside gateAndLaunchMaintenance) keeps the maintenance helper
		// agnostic of "leftover entries" and makes the pre-split contract
		// explicit at the call site.
		if len(tail) > 0 {
			if err := a.spool.AppendCommittedEntries(ctx, rawEntriesFromDecoded(tail)...); err != nil {
				return fmt.Errorf("spooling entries past checkpoint trigger: %w", err)
			}
		}

		headRaw := rawEntriesFromDecoded(head)

		var checkpointErr error
		if result.QueryCheckpointID > 0 {
			checkpointErr = a.handleQueryCheckpointRequired(ctx, headRaw, result)
		} else {
			checkpointErr = a.handleCheckpointRequired(ctx, headRaw, result)
		}

		// Checkpoint returned. If responses weren't attached to any commit
		// above (checkpoint triggered on a sub-batch that ended short of
		// len(decoded)), the tail is durable in the spool and will apply via
		// unspool without further raft involvement — safe to acknowledge
		// Applied now.
		if !responsesAttached && len(responses) > 0 && a.responseSink != nil {
			select {
			case a.responseSink <- responses:
			case <-ctx.Done():
				if checkpointErr != nil {
					return checkpointErr
				}

				return ctx.Err()
			}
		}

		return checkpointErr
	}

	return nil
}

// rawEntriesFromDecoded extracts the underlying raftpb.Entry values from a
// slice of DecodedEntry for APIs that take raw entries (spool, replay
// fallbacks, maintenance handoff). The returned slice is freshly allocated
// and does not share storage with the input.
func rawEntriesFromDecoded(decoded []state.DecodedEntry) []raftpb.Entry {
	out := make([]raftpb.Entry, len(decoded))
	for i := range decoded {
		out[i] = decoded[i].Entry
	}

	return out
}

// findCheckpointBoundary returns the length of the prefix of entries that
// should be applied as a single FSM batch — i.e. up to and including the
// first entry whose proposal contains a checkpoint-trigger order. Entries
// without a proposal payload (config-change entries, empty entries) cannot
// contain triggers and are scanned through cheaply.
//
// Returns len(entries) when no trigger is present (apply the whole slice as
// one batch).
//
// Convenience wrapper around findCheckpointBoundaryDecoded: decodes
// entries, scans, and releases. Hot-path callers should decode once at the
// applier boundary and use findCheckpointBoundaryDecoded directly so the
// pipeline never re-unmarshals the same payload.
func findCheckpointBoundary(entries []raftpb.Entry) (int, error) {
	decoded, err := state.DecodeEntries(entries)
	if err != nil {
		return 0, err
	}

	defer state.ReleaseDecodedEntries(decoded)

	return findCheckpointBoundaryDecoded(decoded), nil
}

// findCheckpointBoundaryDecoded is the no-unmarshal variant used on the
// hot path. It mirrors findCheckpointBoundary but operates on entries that
// have already been decoded once at the applier boundary.
func findCheckpointBoundaryDecoded(decoded []state.DecodedEntry) int {
	for i := range decoded {
		if state.DecodedEntryRequiresCheckpoint(decoded[i]) {
			return i + 1
		}
	}

	return len(decoded)
}

// maintenanceTask is the body of a gated maintenance task. It receives the
// deferred future + result extracted from the entry that triggered gating
// (both may be nil if there was no future for that proposalID) and the
// frozenAtIndex (last applied entry before the spool replay starts).
type maintenanceTask func(
	ctx context.Context,
	deferredResult *state.ApplyResult,
	deferredFuture *futures.Future[state.ApplyResult],
	frozenAtIndex uint64,
) (maintenanceTaskResult, error)

// gateAndLaunchMaintenance is the shared setup for every "checkpoint
// required during apply" flow:
//  1. Compute frozenAtIndex (the last applied index — the trigger entry is
//     the last in the slice by construction; see applyEntriesToFSM).
//  2. Mark the applier as gated with the given gatingReason.
//  3. Extract the deferred future (the proposal that triggered gating).
//  4. Sweep stale-term futures so they don't survive the gating window
//     (issue #172).
//  5. Launch the maintenance task asynchronously.
//
// Callers supply only the gating reason and the task body. The task body
// receives the deferred result/future and the frozen index so it can
// resolve the future with the checkpoint path on success or with the
// error on failure.
//
// There are no remaining entries to spool here: applyEntriesToFSM pre-splits
// the slice so the checkpoint trigger is always the last entry it applies.
// Any entries received from raft AFTER gating starts go to the spool via the
// normal hot-path routing.
func (a *Applier) gateAndLaunchMaintenance(
	ctx context.Context,
	entries []raftpb.Entry,
	applyResult *state.ApplyEntriesResult,
	gatingReason int32,
	task maintenanceTask,
) error {
	frozenAtIndex := entries[len(entries)-1].Index

	a.status.Store(statusGated)
	a.gatingReason.Store(gatingReason)

	deferredResult, deferredFuture := a.extractDeferredFuture(applyResult)

	// Sweep stale-term futures before launching — pending older-term
	// proposals shouldn't survive the gating window.
	a.sweepBelowTerm(entries)

	a.gatingTerminated = a.startMaintenanceTask(ctx, func(ctx context.Context) (maintenanceTaskResult, error) {
		return task(ctx, deferredResult, deferredFuture, frozenAtIndex)
	}, nil)

	return nil
}

// handleCheckpointRequired enters maintenance mode to create a checkpoint off
// the Raft hot path for CloseChapter (seal checkpoint). While the checkpoint is
// being created, new committed entries are spooled and replayed afterward.
func (a *Applier) handleCheckpointRequired(
	ctx context.Context,
	entries []raftpb.Entry,
	applyResult *state.ApplyEntriesResult,
) error {
	return a.gateAndLaunchMaintenance(ctx, entries, applyResult, gatingReasonSnapshotting,
		func(ctx context.Context, deferredResult *state.ApplyResult, deferredFuture *futures.Future[state.ApplyResult], frozenAtIndex uint64) (maintenanceTaskResult, error) {
			path, err := a.store.CreateTemporaryCheckpoint(fmt.Sprintf("checkpoint-%d", applyResult.CheckpointChapterID))
			if err != nil {
				if deferredFuture != nil {
					deferredFuture.Resolve(state.ApplyResult{}, err)
				}

				return maintenanceTaskResult{}, fmt.Errorf("creating checkpoint: %w", err)
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

			return maintenanceTaskResult{frozenAtIndex: frozenAtIndex}, nil
		})
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
	checkpointID := applyResult.QueryCheckpointID

	return a.gateAndLaunchMaintenance(ctx, entries, applyResult, gatingReasonQueryCheckpoint,
		func(ctx context.Context, deferredResult *state.ApplyResult, deferredFuture *futures.Future[state.ApplyResult], frozenAtIndex uint64) (maintenanceTaskResult, error) {
			if err := a.createMainStoreCheckpoint(checkpointID); err != nil {
				if deferredFuture != nil {
					deferredFuture.Resolve(state.ApplyResult{}, err)
				}

				return maintenanceTaskResult{}, err
			}

			if deferredFuture != nil {
				deferredFuture.Resolve(*deferredResult, nil)
			}

			return maintenanceTaskResult{frozenAtIndex: frozenAtIndex}, nil
		})
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

// maintenanceTaskResult is returned by the task function to indicate the outcome.
type maintenanceTaskResult struct {
	frozenAtIndex uint64
	syncFailed    bool // if true, skip spool replay and mark node out-of-sync
}

// startMaintenanceTask creates a gating channel and runs the maintenance task
// in the background. Returns the gating channel so the caller can deliver it
// to Run — either by direct assignment (when called from Run itself) or via
// a.gatingCh (when called from another goroutine like processReadies).
func (a *Applier) startMaintenanceTask(
	ctx context.Context,
	task func(ctx context.Context) (maintenanceTaskResult, error),
	postGating func(ctx context.Context),
) chan gatingResult {
	gatingTerminated := make(chan gatingResult, 1)

	a.taskExecutor.Interrupt()
	a.taskExecutor.Run(ctx, func(ctx context.Context) error {
		var sendOnce sync.Once

		sendResult := func(result gatingResult) {
			sendOnce.Do(func() {
				gatingTerminated <- result
				close(gatingTerminated)
			})
		}
		defer sendResult(gatingResult{taskFailed: true})

		snapshotStart := time.Now()

		taskResult, err := task(ctx)
		if err != nil {
			return err
		}

		a.maintenanceSnapshotHistogram.Record(context.Background(), float64(time.Since(snapshotStart).Microseconds()))

		// If the task failed (e.g. failed sync with leader), signal Run to
		// mark the node out-of-sync. Don't replay the spool.
		if taskResult.syncFailed {
			sendResult(gatingResult{syncFailed: true})

			return nil
		}

		replayStart := time.Now()

		if _, err := a.replaySpool(ctx, taskResult.frozenAtIndex); err != nil {
			return err
		}

		a.maintenanceReplaySpoolHistogram.Record(context.Background(), float64(time.Since(replayStart).Microseconds()))

		// End gating before post-gating work (e.g. WAL compaction).
		// Post-gating work doesn't need the FSM to be frozen and would
		// unnecessarily extend the spooling window, increasing latency.
		sendResult(gatingResult{})

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

	replayed, err := a.replaySpool(ctx, lastAppliedIndex)
	if err != nil {
		return fmt.Errorf("replaying spool: %w", err)
	}

	// Non-vacuity probe: proves the gated-window -> unspool path is exercised
	// with a non-empty spool under faults (otherwise the spool corruption
	// guardrail is never meaningfully covered).
	assert.Sometimes(replayed > 0, "spool replay after gated window applied at least one entry", map[string]any{
		"entriesApplied": replayed,
		"fromIndex":      lastAppliedIndex,
	})

	a.status.Store(statusNormal)

	lastAppliedIndex, err = query.ReadLastAppliedIndex(a.store)
	if err != nil {
		return fmt.Errorf("getting last applied index: %w", err)
	}

	pruneStats, err := a.spool.Prune(lastAppliedIndex)
	if err != nil {
		return fmt.Errorf("pruning spool: %w", err)
	}

	// Reclamation probe: prune actually had fully-applied segments to delete
	// after a gated window (spool disk usage is bounded in practice).
	assert.Sometimes(pruneStats.SegmentsRemoved > 0, "spool pruned after resync", map[string]any{
		"segmentsRemoved":   pruneStats.SegmentsRemoved,
		"bytesRemoved":      pruneStats.BytesRemoved,
		"segmentsRemaining": pruneStats.SegmentsRemaining,
		"lastAppliedIndex":  lastAppliedIndex,
	})

	// Prune effectiveness: no segment known to be sealed (trailer-bearing)
	// and fully applied (maxIndex <= lastApplied) may survive Prune. The
	// trailer-less ACTIVE segment is exempt by design and never counted;
	// unreadable segments are reported separately (state unknown) and do not
	// fail the condition.
	assert.Always(pruneStats.SealedFullyAppliedRemaining == 0, "spool prune leaves no fully-applied sealed segment behind", map[string]any{
		"sealedFullyAppliedRemaining": pruneStats.SealedFullyAppliedRemaining,
		"segmentsUnreadable":          pruneStats.SegmentsUnreadable,
		"segmentsRemoved":             pruneStats.SegmentsRemoved,
		"segmentsRemaining":           pruneStats.SegmentsRemaining,
		"lastAppliedIndex":            lastAppliedIndex,
	})

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

		if err := a.applyReplayEntries(ctx, entries[i:end]); err != nil {
			return fmt.Errorf("applying WAL entries: %w", err)
		}
	}

	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.WithFields(map[string]any{
			"count": len(entries),
		}).Debugf("WAL replay complete")
	}

	return nil
}

// replaySpool replays spooled entries with Index > fromIndex into the FSM.
// It returns the number of entries handed to the FSM apply path.
func (a *Applier) replaySpool(ctx context.Context, fromIndex uint64) (int, error) {
	return a.replaySpoolImpl(ctx, fromIndex, 0)
}

func (a *Applier) replaySpoolImpl(ctx context.Context, fromIndex uint64, maxIndex uint64) (int, error) {
	if a.logger.Enabled(logging.DebugLevel) {
		a.logger.WithFields(map[string]any{
			"fromIndex": fromIndex,
			"maxIndex":  maxIndex,
		}).Debugf("Replaying spool")
	}

	until, err := a.spool.End()
	if err != nil {
		return 0, fmt.Errorf("getting spool end position: %w", err)
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
			if err := a.applyReplayEntries(ctx, batch); err != nil {
				return err
			}

			count += len(batch)
			batch = batch[:0]
			lastEntry = &entry
		}

		return nil
	}); err != nil {
		return count, fmt.Errorf("replaying spool: %w", err)
	}

	if len(batch) > 0 {
		count += len(batch)

		if err := a.applyReplayEntries(ctx, batch); err != nil {
			return count, err
		}

		lastEntry = new(batch[len(batch)-1])
	}

	if lastEntry != nil {
		logFields["last_entry_index"] = lastEntry.Index
	}

	logFields["count"] = count
	a.logger.
		WithFields(logFields).
		WithField("count", count).
		Infof("Replayed spool")

	return count, nil
}

// handleCheckpointDuringReplay creates a checkpoint synchronously when a
// checkpoint-requiring entry (CloseChapter or CreateQueryCheckpoint) is the
// last entry of a replay batch. Unlike handleCheckpointRequired, this does
// not enter maintenance mode — we are already off the hot path. Callers
// (replayWAL, replaySpoolImpl) pre-split so the checkpoint trigger is always
// the last applied entry; cascading checkpoints in the same source slice are
// handled by the caller's outer loop.
func (a *Applier) handleCheckpointDuringReplay(_ context.Context, applyResult *state.ApplyEntriesResult) error {
	if applyResult.QueryCheckpointID > 0 {
		if err := a.createMainStoreCheckpoint(applyResult.QueryCheckpointID); err != nil {
			return fmt.Errorf("during replay: %w", err)
		}

		if lastResult, deferred := a.extractDeferredFuture(applyResult); deferred != nil {
			deferred.Resolve(*lastResult, nil)
		}

		return nil
	}

	return a.createReplayCheckpoint(applyResult)
}

// createReplayCheckpoint creates a checkpoint for a CloseChapter entry encountered
// during spool replay and resolves the deferred future.
func (a *Applier) createReplayCheckpoint(result *state.ApplyEntriesResult) error {
	checkpointPath, err := a.store.CreateTemporaryCheckpoint(fmt.Sprintf("replay-%d", result.CheckpointChapterID))
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

	if lastResult, deferred := a.extractDeferredFuture(result); deferred != nil {
		lastResult.CheckpointPath = checkpointPath
		deferred.Resolve(*lastResult, nil)
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

	handle, handleErr := a.store.NewDirectReadHandle()
	if handleErr != nil {
		return fmt.Errorf("creating read handle for baseline snapshot: %w", handleErr)
	}
	defer func() { _ = handle.Close() }()

	return attributes.CreateBaselineSnapshot(handle, a.fsm.Registry.Attrs, destPath)
}
