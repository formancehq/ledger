package state

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source machine.go -destination notifier_generated_test.go -typed -package state . Notifier

// Notifier is notified by the FSM when logs are committed or config changes.
// Used by the events Manager and mirror Manager.
type Notifier interface {
	NotifyLogsCommitted(lastSeq uint64)
	NotifyConfigChanged()
}

type Machine struct {
	logger logging.Logger

	// The Machine is the hot-path apply receiver: it never holds a Pebble
	// read capability, never holds the incoming-restore primitives, and never
	// holds a raw *dal.Store. Boot/recovery reads and follower-sync coordination
	// live on the separate Recovery / Synchronizer types defined alongside this
	// one. The mutable FSM state shared between apply and recovery is grouped
	// under Machine.State (see fsmstate.go). The hot path receives a
	// WriteSessionFactory as a parameter to PrepareEntries / ApplyEntries, not
	// as a field.

	// queryCheckpoints lets the FSM delete query-checkpoint files when an
	// apply removes their metadata. Two methods only: create + delete. The
	// surface is not a read capability and does not give access to Pebble
	// contents.
	queryCheckpoints dal.QueryCheckpoints

	// sentinel runs scoped post-commit reader-based checks in debug mode.
	// The Reader never escapes the callback, so even in sentinelMode the
	// hot path apply does not hold a long-lived read capability.
	sentinel dal.SentinelFactory

	mu sync.Mutex

	// Composed subsystems
	Registry *StateRegistry // KeyStores + Cache + Attrs
	Periods  *PeriodTracker // Period lifecycle

	// State groups the FSM-level mutable state (counters, timestamps, audit
	// chain, cluster config, pending ledger cleanups). It is the explicit
	// shared surface between the apply path and Recovery: Machine writes
	// fsm.State.X on apply, Recovery writes r.apply.State.X on hydrate.
	State *FSMState

	queryCheckpointScheduleChanged signal.Signal
	auditHashBuf                   []byte

	// KeyStore holds registered signing keys (updated after proposal apply)
	keyStore *keystore.KeyStore

	// sharedState holds maintenance mode and require-signatures flags
	sharedState *SharedState

	// RequestProcessor handles business logic
	processor *processing.RequestProcessor

	// sealRequestCh receives seal requests when a ClosePeriod log is applied.
	// The Sealer reads from this channel to perform background sealing.
	sealRequestCh *worker.Channel[SealRequest]

	// archiveRequestCh receives archive requests when an ArchivePeriod order is applied.
	// The Archiver reads from this channel to perform background archival to cold storage.
	archiveRequestCh *worker.Channel[ArchiveRequest]

	// metadataConvertRequestCh receives conversion requests when a SetMetadataFieldType
	// log is applied. The MetadataConverter reads from this channel to perform
	// background conversion of existing account metadata values.
	metadataConvertRequestCh *worker.Channel[MetadataConvertRequest]

	// coldCompactionCh signals the SmartCompactor that a period purge has been applied,
	// meaning the cold zone [0x01, 0xF1) contains fresh tombstones that benefit from compaction.
	coldCompactionCh chan struct{}

	// bloomRebuildCh signals an external consumer (Recovery) that the bloom
	// filters must be rebuilt asynchronously. The hot path cannot trigger the
	// rebuild directly because StartAsyncBloomPopulate needs a Pebble reader
	// and the Machine deliberately does not hold one — the Recovery consumer
	// runs StartAsyncBloomPopulate with its own reader. Capacity 1 + TrySend
	// gives "coalesce: keep latest reason" semantics, matching coldCompactionCh.
	bloomRebuildCh chan string

	// cacheSnapshotter handles persisting/restoring cache, reversions, and bloom
	// filters to/from Pebble (0xFF prefix).
	cacheSnapshotter *CacheSnapshotter

	// BloomFilters holds per-attribute-type bloom filters for key existence checks.
	// Updated during FSM apply, read during preload building.
	BloomFilters *bloom.FilterSet

	// Metrics
	logsAppendedCounter       metric.Int64Counter
	rotationDurationHistogram metric.Int64Histogram
	batchCommitHistogram      metric.Int64Histogram

	// lastPersistedIndex is the highest Raft index whose FSM batch has been
	// committed to Pebble's memtable and WAL with pebble.NoSync. It is NOT
	// guaranteed durable on disk by itself: a power loss between commit and
	// Pebble's async WAL flush would lose the corresponding entries. After
	// recovery, fsm.LastAppliedIndex (loaded from Pebble) is the true durable
	// applied index — it may be less than lastPersistedIndex captured before
	// the crash, and Raft is expected to redeliver the missing entries.
	lastPersistedIndex atomic.Uint64

	// writeSet is a reusable WriteSet for applyProposal. Since the FSM is
	// single-goroutine, we can avoid per-proposal allocations by resetting
	// and reusing the same WriteSet across proposals.
	writeSet *WriteSet

	// sentinelMode enables runtime volume consistency checks
	// (monotonicity, delta/posting cross-check, post-commit cache/Pebble verification).
	sentinelMode bool

	// sentinelTracer points to the tracer scoped to the in-flight PrepareEntries.
	// A fresh tracer is allocated at the start of every PrepareEntries call when
	// sentinelMode is on. The pointer is captured by the PreparedBatch so a
	// later pipelined CommitPreparedBatch can Dump() the right batch's trace,
	// even after the next PrepareEntries has started populating a NEW tracer
	// behind the same field. Only ever read/written under fsm.mu.
	sentinelTracer *SentinelTracer

	// notifier is notified after new logs are committed and when configuration
	// changes. A single notifier decouples the FSM from individual consumers;
	// the bootstrap layer fans out to events Manager, mirror Manager, and index
	// builder via signal.FanOut.
	notifier Notifier

	// appliedMu and appliedCond are used to notify waiters when lastPersistedIndex advances.
	// This enables ReadIndex-based linearizable reads: callers wait until the FSM has caught up
	// to a target commit index before reading local state.
	appliedMu   sync.Mutex
	appliedCond *sync.Cond

	// publishSeq counts publishApplied calls; guarded by appliedMu. It feeds
	// the lost-wakeup detector in WaitForApplied: an index advance observed
	// without a sequence increment can only come from a bare Store that
	// bypassed the wake protocol (#327 class).
	publishSeq uint64
}

// NewMachine constructs the hot-path FSM. It composes pre-built sub-objects
// (Registry, CacheSnapshotter) so the constructor reads no Pebble and keeps
// the surface focused on hot-path concerns. The bootstrap is responsible for
// wiring those sub-objects up-front. NewMachine does NOT perform RecoverState;
// callers must invoke Recovery.RecoverState() before the Machine applies
// entries.
func NewMachine(logger logging.Logger, registry *StateRegistry, cacheSnapshotter *CacheSnapshotter, queryCheckpoints dal.QueryCheckpoints, sentinel dal.SentinelFactory, meter metric.Meter, ks *keystore.KeyStore, sharedState *SharedState, notifier Notifier, bloomFilters *bloom.FilterSet, clusterID string, numscriptCacheSize int) (*Machine, error) {
	sentinelMode := sentinel.IsEnabled()
	logsAppendedCounter, err := meter.Int64Counter(
		"raft.fsm.logs_appended",
		metric.WithDescription("Total number of logs appended to the store. Use rate() to get logs per second."),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating logs_appended counter: %w", err)
	}

	rotationDurationHistogram, err := meter.Int64Histogram(
		"raft.fsm.rotation.duration",
		metric.WithDescription("Time spent in generation rotation (volume compaction) during ApplyEntries"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating rotation_duration histogram: %w", err)
	}

	batchCommitHistogram, err := meter.Int64Histogram(
		"raft.fsm.batch_commit.duration",
		metric.WithDescription("Time spent in PebbleDB batch.Commit() during ApplyEntries"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating batch_commit_duration histogram: %w", err)
	}

	processor, err := processing.NewRequestProcessor(meter, numscriptCacheSize)
	if err != nil {
		return nil, fmt.Errorf("creating request processor: %w", err)
	}

	fsm := &Machine{
		logger:                         logger,
		queryCheckpoints:               queryCheckpoints,
		sentinel:                       sentinel,
		BloomFilters:                   bloomFilters,
		sentinelMode:                   sentinelMode,
		logsAppendedCounter:            logsAppendedCounter,
		rotationDurationHistogram:      rotationDurationHistogram,
		batchCommitHistogram:           batchCommitHistogram,
		processor:                      processor,
		notifier:                       notifier,
		keyStore:                       ks,
		sharedState:                    sharedState,
		Registry:                       registry,
		Periods:                        NewPeriodTracker(nil, nil, nil, 0, ""),
		State:                          NewFSMState(clusterID),
		queryCheckpointScheduleChanged: signal.New(),
		sealRequestCh:                  worker.NewChannel[SealRequest](logger, "seal request", 10),
		archiveRequestCh:               worker.NewChannel[ArchiveRequest](logger, "archive request", 1),
		metadataConvertRequestCh:       worker.NewChannel[MetadataConvertRequest](logger, "metadata conversion request", 16),
		coldCompactionCh:               make(chan struct{}, 1),
		bloomRebuildCh:                 make(chan string, 1),
		auditHashBuf:                   make([]byte, 0, 4096),
	}
	fsm.appliedCond = sync.NewCond(&fsm.appliedMu)
	fsm.cacheSnapshotter = cacheSnapshotter
	fsm.writeSet = NewWriteSet(fsm)

	// Recovery from Pebble is not performed here on purpose: NewMachine is the
	// hot-path FSM constructor and must not read from Pebble. The caller (the
	// bootstrap, or a test helper) is responsible for wiring a Recovery and
	// invoking recovery.RecoverState() before the Machine is asked to apply
	// any entries.

	return fsm, nil
}

// recoverState loads all FSM in-memory state from the Pebble data store.
// Called on restart (via RecoverAndReplay) and after follower sync
// (via reloadStateFromStore). The reader is supplied by the caller (Recovery
// owns it) so the Machine itself does not hold a Pebble read capability and
// no hot-path method can accidentally invoke this without a reader argument.

func (fsm *Machine) LastPersistedIndex() uint64 {
	return fsm.lastPersistedIndex.Load()
}

// LastAppliedIndex returns the last applied Raft index as read from the data
// store at construction time. It is NOT updated during Apply — use
// LastPersistedIndex for the live value. This is intended for raft.Config.Applied
// so that the first Ready does not re-emit already-applied entries.
func (fsm *Machine) LastAppliedIndex() uint64 {
	return fsm.State.LastAppliedIndex
}

// RestoreState atomically replaces the FSM-level state. The intended callers
// are Recovery (at boot) and Synchronizer (after install-snapshot), which
// build a fresh FSMState from Pebble via LoadFSMStateFromStore and swap it
// in here. Sub-trackers (Periods, Registry.Reversions, KeyStore,
// SharedState, Registry.Cache settings, Registry.Idempotency) are NOT
// touched by this method — they have their own lifecycles and the caller
// is responsible for resetting them in the same critical section.
//
// Concurrency: the swap is a single pointer assignment, but readers of
// fsm.State.X racing with RestoreState would see the new struct's
// zero/transitional values. Callers must ensure no apply is in flight:
// at boot there is no apply yet; on follower sync the Applier gates the
// apply pipeline before invoking Synchronizer (waitPendingCommit drains
// the in-flight commit and statusGated blocks further entries) — see
// internal/infra/node/applier.go.
func (fsm *Machine) RestoreState(s *FSMState) {
	fsm.State = s
}

// publishApplied advances lastPersistedIndex and wakes WaitForApplied
// callers, holding appliedMu so the Store + Broadcast pair cannot
// interleave with a waiter's Load() / Wait() sequence. Without the lock
// the writer can complete Store + Broadcast in the window between the
// waiter's lastPersistedIndex.Load() check and its appliedCond.Wait()
// call — the broadcast lands on an empty wait queue and the waiter
// sleeps until ctx cancellation. On an idle cluster (no further
// publishes) the wake-up never arrives and node.Run's startup gate or
// ReadIndex stalls until the context times out (#327).
//
// lastPersistedIndex stays atomic for the fast-path callers that only
// need a one-shot read with no readiness wait (e.g. PrepareEntries).
func (fsm *Machine) publishApplied(idx uint64) {
	fsm.appliedMu.Lock()
	fsm.lastPersistedIndex.Store(idx)
	fsm.publishSeq++
	fsm.appliedCond.Broadcast()
	fsm.appliedMu.Unlock()
}

// WaitForApplied blocks until the FSM has applied entries up to (and including) targetIndex,
// or the context is cancelled. Used by ReadIndex to ensure local state is fresh enough
// for linearizable reads.
func (fsm *Machine) WaitForApplied(ctx context.Context, targetIndex uint64) error {
	// Fast path: already caught up.
	if fsm.lastPersistedIndex.Load() >= targetIndex {
		return nil
	}

	// Spawn a goroutine that broadcasts on the cond when the context is cancelled.
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			fsm.appliedCond.Broadcast()
		case <-done:
		}
	}()

	fsm.appliedMu.Lock()

	blocked := false

	for fsm.lastPersistedIndex.Load() < targetIndex {
		if ctx.Err() != nil {
			fsm.appliedMu.Unlock()

			return ctx.Err()
		}

		blocked = true
		seqBefore := fsm.publishSeq

		fsm.appliedCond.Wait()

		// Lost-wakeup detector (#327 class): the target index became visible
		// while we slept, yet publishSeq did not move — the index was advanced
		// by a bare Store that bypassed the wake protocol. Sound under
		// arbitrary pauses and wake sources: a legitimate publish always
		// increments publishSeq under appliedMu before its index is
		// observable from this loop.
		if fsm.lastPersistedIndex.Load() >= targetIndex && fsm.publishSeq == seqBefore {
			assert.Unreachable("WaitForApplied observed an index advance without a publish", map[string]any{
				"targetIndex":        targetIndex,
				"lastPersistedIndex": fsm.lastPersistedIndex.Load(),
			})
		}
	}

	fsm.appliedMu.Unlock()

	if blocked {
		assert.Reachable("WaitForApplied woke after blocking", map[string]any{
			"targetIndex": targetIndex,
		})
	}

	return nil
}

// PrepareEntries processes Raft entries and builds a Pebble batch without
// committing it. All in-memory state (cache, KeyStore, counters) is updated.
// The caller must either call CommitPreparedBatch or PreparedBatch.Close.
//
// This is the first half of the pipelining split: PrepareEntries is CPU-bound
// and can run while a previous batch's commit is in-flight.
func (fsm *Machine) PrepareEntries(ctx context.Context, sessions dal.WriteSessionFactory, entries ...raftpb.Entry) (*PreparedBatch, error) {
	// Validate checkpoint trigger positions BEFORE taking the lock or mutating
	// any in-memory state. A malformed batch (trigger not last) is rejected
	// here so the FSM cannot be left with lastAppliedIndex bumped and proposal
	// side effects applied for an entry that will never be committed.
	if err := ValidateCheckpointEntryPositions(entries); err != nil {
		return nil, err
	}

	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	// Allocate a fresh tracer for this PrepareEntries call. The pointer is
	// captured by the PreparedBatch below; once we return, the next
	// PrepareEntries reassigns fsm.sentinelTracer to a new instance — the old
	// one stays referenced by the in-flight PreparedBatch's pb.sentinelTracer
	// so the pipelined committer can Dump() the correct batch's trace on a
	// sentinel failure.
	if fsm.sentinelMode {
		fsm.sentinelTracer = NewSentinelTracer(fsm.logger)
	}

	// With pipelining, lastPersistedIndex may lag lastAppliedIndex by one batch
	// (the pending commit). This is expected and safe.
	persistedIdx := fsm.lastPersistedIndex.Load()
	if persistedIdx != fsm.State.LastAppliedIndex && len(entries) > 0 && fsm.logger.Enabled(logging.TraceLevel) {
		fsm.logger.WithFields(map[string]any{
			"lastPersistedIndex": persistedIdx,
			"lastAppliedIndex":   fsm.State.LastAppliedIndex,
			"snapshotIndex":      fsm.State.SnapshotIndex,
			"entryCount":         len(entries),
			"firstEntryIndex":    entries[0].Index,
			"gen0":               fsm.Registry.Cache.BaseIndex.Gen0,
			"gen1":               fsm.Registry.Cache.BaseIndex.Gen1,
			"currentGeneration":  fsm.Registry.Cache.CurrentGeneration(),
		}).Tracef("PrepareEntries: lastPersistedIndex lags (pending commit in-flight)")
	}

	if fsm.State.SnapshotIndex > fsm.State.LastAppliedIndex {
		assert.Unreachable("node out of sync during apply", map[string]any{
			"snapshotIndex":    fsm.State.SnapshotIndex,
			"lastAppliedIndex": fsm.State.LastAppliedIndex,
		})

		return nil, &ErrNodeOutOfSync{
			SnapshotIndex:    fsm.State.SnapshotIndex,
			LastAppliedIndex: fsm.State.LastAppliedIndex,
		}
	}

	batch := sessions.OpenWriteSession()

	cmd := raftcmdpb.ProposalFromVTPool()
	defer func() { cmd.ReturnToVTPool() }()

	ret := &ApplyEntriesResult{
		Results: make([]ApplyResult, 0, len(entries)),
	}
	eventsConfigChanged := false
	mirrorConfigChanged := false
	needsArchiveDispatch := false
	needsColdCompaction := false

	var pendingConvertRequests []MetadataConvertRequest

	for i, entry := range entries {
		if entry.Index <= fsm.State.LastAppliedIndex {
			ret.Results = append(ret.Results, ApplyResult{})

			continue
		}

		if entry.Index > fsm.State.LastAppliedIndex+1 {
			assert.Unreachable("entry index gap detected", map[string]any{
				"receivedIndex": entry.Index,
				"expectedIndex": fsm.State.LastAppliedIndex + 1,
			})

			_ = batch.Cancel()

			return nil, &ErrInvalidEntryIndex{
				ReceivedIndex: entry.Index,
				ExpectedIndex: fsm.State.LastAppliedIndex + 1,
			}
		}

		preRotationPQGen0 := fsm.Registry.Cache.PreparedQueries.Gen0().Size()
		preRotationPQGen1 := fsm.Registry.Cache.PreparedQueries.Gen1().Size()

		if rotated, _ := fsm.Registry.Cache.CheckRotationNeeded(entry.Index); rotated {
			if fsm.sentinelMode {
				fsm.sentinelTracer.SetCacheRotated()
			}
			lifecycle.SendEvent("cache_rotation", map[string]any{
				"preRotationPQGen0": preRotationPQGen0,
				"preRotationPQGen1": preRotationPQGen1,
				"entryIndex":        entry.Index,
				"currentGeneration": fsm.Registry.Cache.CurrentGeneration(),
				"gen0Base":          fsm.Registry.Cache.BaseIndex.Gen0,
				"gen1Base":          fsm.Registry.Cache.BaseIndex.Gen1,
				"volumeGen0Size":    fsm.Registry.Cache.Volumes.Gen0().Size(),
				"volumeGen1Size":    fsm.Registry.Cache.Volumes.Gen1().Size(),
				"ledgerGen0Size":    fsm.Registry.Cache.Ledgers.Gen0().Size(),
				"ledgerGen1Size":    fsm.Registry.Cache.Ledgers.Gen1().Size(),
				"prepQueryGen0Size": fsm.Registry.Cache.PreparedQueries.Gen0().Size(),
				"prepQueryGen1Size": fsm.Registry.Cache.PreparedQueries.Gen1().Size(),
				"boundaryGen0Size":  fsm.Registry.Cache.Boundaries.Gen0().Size(),
				"boundaryGen1Size":  fsm.Registry.Cache.Boundaries.Gen1().Size(),
			})
			if fsm.logger.Enabled(logging.DebugLevel) {
				fsm.logger.WithFields(map[string]any{
					"entryIndex":        entry.Index,
					"currentGeneration": fsm.Registry.Cache.CurrentGeneration(),
					"gen0":              fsm.Registry.Cache.BaseIndex.Gen0,
					"gen1":              fsm.Registry.Cache.BaseIndex.Gen1,
				}).Debugf("Cache generation rotated")
			}
			rotationStart := time.Now()

			if err := writeCacheRotation(
				batch,
				fsm.Registry.Cache.CurrentGeneration(),
				fsm.Registry.Cache.BaseIndex.Gen0,
				fsm.Registry.Cache.BaseIndex.Gen1,
			); err != nil {
				_ = batch.Cancel()

				return nil, fmt.Errorf("writing cache rotation: %w", err)
			}

			if fsm.BloomFilters != nil && fsm.BloomFilters.IsReady() {
				if err := fsm.BloomFilters.PersistDirtyBlocks(batch); err != nil {
					_ = batch.Cancel()

					return nil, fmt.Errorf("persisting bloom dirty blocks: %w", err)
				}
			}

			fsm.rotationDurationHistogram.Record(context.Background(), time.Since(rotationStart).Microseconds())
		}

		fsm.State.LastAppliedIndex++

		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			if fsm.sentinelMode {
				fsm.sentinelTracer.SkipEntry(entry.Index, entry.Type.String(), len(entry.Data))
			}

			continue
		}

		cmd.ReturnToVTPool()
		cmd = raftcmdpb.ProposalFromVTPool()

		if err := cmd.UnmarshalVT(entry.Data); err != nil {
			_ = batch.Cancel()

			return nil, err
		}

		if len(cmd.GetOrders()) == 0 && len(cmd.GetMirrorSyncUpdates()) == 0 && len(cmd.GetEventsSinkUpdates()) == 0 && len(cmd.GetMetadataConversionBatches()) == 0 && len(cmd.GetMetadataConversionsComplete()) == 0 && len(cmd.GetIndexReadyUpdates()) == 0 && cmd.GetIdempotencyEviction() == nil && cmd.GetClusterConfig() == nil {
			if fsm.sentinelMode && fsm.logger.Enabled(logging.TraceLevel) {
				fsm.logger.WithFields(map[string]any{
					"raftIndex":  entry.Index,
					"proposalID": cmd.GetId(),
				}).Tracef("SENTINEL: skipping no-op proposal")
			}

			ret.Results = append(ret.Results, ApplyResult{ProposalID: cmd.GetId(), AppliedIndex: entry.Index})

			continue
		}

		if fsm.sentinelMode {
			fsm.sentinelTracer.StartEntry(entry.Index, cmd.GetId(), len(cmd.GetOrders()))
		}

		result, err := fsm.applyProposal(ctx, entry.Index, batch, cmd)
		if err != nil {
			_ = batch.Cancel()

			return nil, err
		}

		result.AppliedIndex = entry.Index

		if fsm.sentinelMode {
			if result.Error != nil {
				fsm.sentinelTracer.RecordRejected(result.Error.Error())
			} else {
				fsm.sentinelTracer.RecordApplied(
					result.ledgerIDs, len(result.createdLogs),
					len(result.volumeUpdates), len(result.purgedVolumeKeys),
				)
			}
		}

		if result.ConfigChanged {
			eventsConfigChanged = true
		}

		if result.MirrorConfigChanged {
			mirrorConfigChanged = true
		}

		if result.HasArchiveRequests {
			needsArchiveDispatch = true
		}

		if result.HasPurges {
			needsColdCompaction = true
		}

		ret.Results = append(ret.Results, *result)
		pendingConvertRequests = append(pendingConvertRequests, result.MetadataConvertRequests...)

		// Checkpoint-trigger detection. The applier is responsible for pre-
		// splitting the entries slice so any trigger entry is the last one in
		// this batch; the FSM only records the flag here and lets the batch
		// commit through the normal pipelined path (via CommitPreparedBatch
		// → runCommitter). This restores the "one batch = one commit" invariant
		// that mid-batch synchronous commits used to violate.
		//
		// The position invariant is enforced upfront by
		// ValidateCheckpointEntryPositions at the start of PrepareEntries — by
		// the time we reach this point any trigger entry is guaranteed to be
		// the last one. The assert below is a belt-and-braces check kept in
		// place because reaching it would mean either the upfront validator and
		// this dynamic detection disagree, or applyProposal produced a trigger
		// effect from a proposal whose orders did not carry a trigger.
		sealReqBase := fsm.checkClosePeriod(result)
		queryCheckpointCreated := result.QueryCheckpointCreated > 0

		if (sealReqBase != nil || queryCheckpointCreated) && i != len(entries)-1 {
			assert.Unreachable("checkpoint trigger entry not last in PrepareEntries batch", map[string]any{
				"raftIndex":  entry.Index,
				"proposalID": cmd.GetId(),
				"position":   i,
				"entryCount": len(entries),
			})

			_ = batch.Cancel()

			return nil, fmt.Errorf(
				"checkpoint trigger entry at position %d/%d in PrepareEntries batch (raft index %d) — applier must pre-split",
				i, len(entries), entry.Index,
			)
		}

		if sealReqBase != nil {
			ret.CheckpointRequired = true
			ret.CheckpointPeriodID = sealReqBase.PeriodID
			ret.OnCheckpointDone = func(checkpointPath string) {
				sealReqBase.CheckpointPath = checkpointPath
				fsm.sealRequestCh.TrySend(*sealReqBase, fmt.Sprintf("period %d", sealReqBase.PeriodID))
			}
		}

		if queryCheckpointCreated {
			ret.CheckpointRequired = true
			ret.QueryCheckpointID = result.QueryCheckpointCreated
		}
	}

	err := SetAppliedIndex(batch, fsm.State.LastAppliedIndex)
	if err != nil {
		_ = batch.Cancel()

		return nil, fmt.Errorf("setting applied index: %w", err)
	}

	err = setLastAppliedTimestamp(batch, fsm.State.LastAppliedTimestamp)
	if err != nil {
		_ = batch.Cancel()

		return nil, fmt.Errorf("setting last applied timestamp: %w", err)
	}

	// Capture all post-commit data now, so CommitPreparedBatch does not
	// need to read mutable fsm fields.
	pb := &PreparedBatch{
		batch:                batch,
		Result:               ret,
		lastAppliedIndex:     fsm.State.LastAppliedIndex,
		lastSequenceID:       fsm.State.NextSequenceID - 1,
		needsArchiveDispatch: needsArchiveDispatch,
		needsColdCompaction:  needsColdCompaction,
		eventsConfigChanged:  eventsConfigChanged,
		mirrorConfigChanged:  mirrorConfigChanged,
		convertRequests:      pendingConvertRequests,
		entryCount:           len(entries),
	}

	// Capture sentinel data before releasing the lock.
	if fsm.sentinelMode {
		pb.sentinelMode = true
		pb.sentinelUpdates = deduplicateVolumeUpdates(ret.Results)
		pb.sentinelLedgerIDs = collectLedgerIDsFromResults(ret.Results)
		pb.sentinelTracer = fsm.sentinelTracer
	}

	// Capture archive requests from current period state.
	if needsArchiveDispatch {
		for _, p := range fsm.Periods.AllPeriods() {
			if p.GetStatus() == commonpb.PeriodStatus_PERIOD_ARCHIVING {
				pb.archiveRequests = append(pb.archiveRequests, ArchiveRequest{
					PeriodID:           p.GetId(),
					StartSequence:      p.GetStartSequence(),
					CloseSequence:      p.GetCloseSequence(),
					StartAuditSequence: p.GetStartAuditSequence(),
					CloseAuditSequence: p.GetCloseAuditSequence(),
				})
			}
		}
	}

	// Capture query checkpoint deletions.
	for _, r := range ret.Results {
		if cpID := r.QueryCheckpointDeleted; cpID > 0 {
			pb.checkpointDeletes = append(pb.checkpointDeletes, cpID)
		}
	}

	return pb, nil
}

// CommitPreparedBatch commits the Pebble batch from a PreparedBatch and runs
// all post-commit side effects (sentinel checks, notifications, dispatches).
//
// This is the second half of the pipelining split. It does NOT hold fsm.mu
// and only uses atomic fields, captured data from PreparedBatch, and
// thread-safe notifier/channel operations.
func (fsm *Machine) CommitPreparedBatch(ctx context.Context, pb *PreparedBatch) error {
	commitStart := time.Now()

	err := pb.batch.Commit()
	if err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	pb.batch = nil // committed, prevent double-close

	fsm.batchCommitHistogram.Record(ctx, time.Since(commitStart).Microseconds())

	lifecycle.SendEvent("batch_committed", map[string]any{
		"lastAppliedIndex": pb.lastAppliedIndex,
		"entryCount":       pb.entryCount,
		"volumeUpdates":    len(pb.sentinelUpdates),
	})

	// Post-commit sentinel checks. The Reader is materialised inside the
	// callback only — it has no field on Machine and never escapes this scope,
	// which keeps the hot path structurally read-free outside of this block.
	// When sentinelMode is off, sentinel.Run is a no-op. We also skip the
	// snapshot open when both update sets are empty (typical for batches with
	// no volume mutations) — avoids the snapshot/close cost in the empty case.
	//
	// The factory uses a snapshot read handle (not a direct one) so the view
	// is pinned to the DB state right after this batch's commit. With the
	// fix/checkpoint-commit-race refactor, all main-store commits go through
	// runCommitter so no other write should race with this read, but the
	// snapshot is cheap and ensures the sentinel keeps the right invariant
	// even if a future change reintroduces a synchronous commit path.
	if len(pb.sentinelUpdates) == 0 && len(pb.sentinelLedgerIDs) == 0 {
		// Nothing to verify — skip the snapshot open entirely.
	} else if err := fsm.sentinel.Run(func(sentinelHandle dal.PebbleReader) error {
		if len(pb.sentinelUpdates) > 0 {
			if err := verifyPostCommitVolumes(
				sentinelHandle, fsm.Registry.Attrs.Volume,
				pb.sentinelUpdates, pb.lastAppliedIndex, fsm.logger,
			); err != nil {
				fsm.logger.Errorf("POST-COMMIT VOLUME ASSERTION FAILED: %v", err)
				pb.sentinelTracer.Dump(fsm.logger)

				return fmt.Errorf("post-commit volume assertion failed: %w", err)
			}
		}

		if len(pb.sentinelLedgerIDs) > 0 {
			if fsm.logger.Enabled(logging.TraceLevel) {
				fsm.logger.Tracef("Verifying aggregated volume balance for %d ledgers at raft index %d", len(pb.sentinelLedgerIDs), pb.lastAppliedIndex)
			}

			if err := verifyAggregatedVolumesBalanced(
				sentinelHandle, fsm.Registry.Attrs.Volume, pb.sentinelLedgerIDs, pb.lastAppliedIndex, fsm.logger,
			); err != nil {
				fsm.logger.Errorf("AGGREGATED VOLUME BALANCE CHECK FAILED: %v", err)
				dumpCacheVsPebbleCoherence(sentinelHandle, fsm.Registry.Cache, pb.lastAppliedIndex, fsm.logger)
				pb.sentinelTracer.Dump(fsm.logger)

				return fmt.Errorf("aggregated volume balance check failed: %w", err)
			}
		}

		return nil
	}); err != nil {
		return err
	}

	previousPersisted := fsm.lastPersistedIndex.Load()
	fsm.publishApplied(pb.lastAppliedIndex)

	if pb.lastAppliedIndex != previousPersisted+uint64(pb.entryCount) {
		if fsm.logger.Enabled(logging.DebugLevel) {
			fsm.logger.WithFields(map[string]any{
				"previousPersisted": previousPersisted,
				"newPersisted":      pb.lastAppliedIndex,
				"entryCount":        pb.entryCount,
			}).Debugf("lastPersistedIndex updated (non-trivial jump)")
		}
	}

	// Dispatch post-commit side effects using captured data.
	for _, req := range pb.archiveRequests {
		fsm.archiveRequestCh.TrySend(req, fmt.Sprintf("period %d", req.PeriodID))
	}

	for _, cpID := range pb.checkpointDeletes {
		fsm.deleteQueryCheckpointFiles(cpID)
	}

	if pb.needsColdCompaction {
		select {
		case fsm.coldCompactionCh <- struct{}{}:
		default:
			// Coalescent signal — safe to drop, next purge will re-signal.
		}
	}

	for _, req := range pb.convertRequests {
		fsm.metadataConvertRequestCh.TrySend(req, fmt.Sprintf("%s/%s", req.LedgerName, req.Key))
	}

	fsm.notifier.NotifyLogsCommitted(pb.lastSequenceID)

	if pb.eventsConfigChanged || pb.mirrorConfigChanged {
		fsm.notifier.NotifyConfigChanged()
	}

	return nil
}

// ApplyEntries processes entries and commits synchronously. This is a
// convenience wrapper around PrepareEntries + CommitPreparedBatch for
// callers that do not need pipelining (spool replay, WAL replay).
//
// Callers must pre-split entries so that any checkpoint-triggering entry is
// the last in the slice (see state.ClassifyCheckpointOrderPosition); the FSM
// enforces this with a defensive check inside PrepareEntries.
func (fsm *Machine) ApplyEntries(ctx context.Context, sessions dal.WriteSessionFactory, entries ...raftpb.Entry) (*ApplyEntriesResult, error) {
	pb, err := fsm.PrepareEntries(ctx, sessions, entries...)
	if err != nil {
		return nil, err
	}

	if err := fsm.CommitPreparedBatch(ctx, pb); err != nil {
		return nil, err
	}

	return pb.Result, nil
}

// deleteQueryCheckpointFiles removes the physical files for a deleted checkpoint.
// Called after the batch containing the DeleteQueryCheckpoint metadata removal is committed.
func (fsm *Machine) deleteQueryCheckpointFiles(checkpointID uint64) {
	if err := fsm.queryCheckpoints.DeleteQueryCheckpointFiles(checkpointID); err != nil {
		if fsm.logger.Enabled(logging.DebugLevel) {
			fsm.logger.WithFields(map[string]any{
				"error":        err,
				"checkpointID": checkpointID,
			}).Debugf("Failed to delete query checkpoint files (may not exist on this node)")
		}
	}
}

// Preload applies preloaded data to the Machine's volatile state.
// batch and genByte are used for incremental 0xFF persistence of NumscriptParsed entries.
func (fsm *Machine) Preload(preloadSet *raftcmdpb.PreloadSet, batch *dal.WriteSession, genByte byte) error {
	if preloadSet == nil || (len(preloadSet.GetPreloads()) == 0 && len(preloadSet.GetTouches()) == 0) {
		return nil
	}

	// The preloads must target gen0 or gen1. The admission uses the
	// IndexTracker to predict the next Raft index and compute the boundary.
	// A mismatch here indicates a bug in the preload/cache coordination.
	switch preloadSet.GetLastPersistedIndex() {
	case fsm.Registry.Cache.BaseIndex.Gen0:
		if fsm.logger.Enabled(logging.DebugLevel) {
			fsm.logger.Debug("Selecting cache generation 0")
		}
	case fsm.Registry.Cache.BaseIndex.Gen1:
		if fsm.logger.Enabled(logging.DebugLevel) {
			fsm.logger.Debug("Selecting cache generation 1")
		}
	default:
		details := map[string]any{
			"lastPersistedIndex":  preloadSet.GetLastPersistedIndex(),
			"gen0":                fsm.Registry.Cache.BaseIndex.Gen0,
			"gen1":                fsm.Registry.Cache.BaseIndex.Gen1,
			"currentGeneration":   fsm.Registry.Cache.CurrentGeneration(),
			"generationThreshold": fsm.Registry.Cache.GenerationThreshold(),
			"lastAppliedIndex":    fsm.State.LastAppliedIndex,
			"preloadCount":        len(preloadSet.GetPreloads()),
			"touchCount":          len(preloadSet.GetTouches()),
		}
		fsm.logger.WithFields(details).Errorf("Preload boundary mismatch: LastPersistedIndex does not match Gen0 or Gen1")
		assert.Unreachable("preload boundary mismatch should be prevented by predicted_index check", details)
		lifecycle.SendEvent("preload_boundary_mismatch", details)

		return fmt.Errorf("preloading preloaded index is invalid: lastPersistedIndex=%d gen0=%d gen1=%d currentGen=%d lastApplied=%d",
			preloadSet.GetLastPersistedIndex(),
			fsm.Registry.Cache.BaseIndex.Gen0,
			fsm.Registry.Cache.BaseIndex.Gen1,
			fsm.Registry.Cache.CurrentGeneration(),
			fsm.State.LastAppliedIndex,
		)
	}

	gen1Byte := genByte ^ 1
	for _, preload := range preloadSet.GetPreloads() {
		// Handle idempotency keys separately — they use the dedicated store, not the cache.
		if ik, ok := preload.GetType().(*raftcmdpb.Preload_IdempotencyKey); ok {
			ikData := ik.IdempotencyKey
			if ikData.GetValue() != nil && ikData.GetValue().GetLogSequence() > 0 {
				fsm.Registry.Idempotency.Put(ikData.GetKey(), ikData.GetValue())
			}

			continue
		}

		if err := fsm.cacheSnapshotter.MirrorPreload(batch, genByte, gen1Byte, preload); err != nil {
			return err
		}
	}

	for _, touch := range preloadSet.GetTouches() {
		id := attributes.U128FromBytes(touch.GetId())
		if err := fsm.cacheSnapshotter.MirrorTouch(batch, byte(touch.GetAttrType()), genByte, id); err != nil {
			return err
		}
	}

	return nil
}

// authorizedInMaintenanceMode returns true if every order in the batch is a SetMaintenanceMode order.
func authorizedInMaintenanceMode(orders []*raftcmdpb.Order) bool {
	for _, order := range orders {
		if _, ok := order.GetType().(*raftcmdpb.Order_SetMaintenanceMode); !ok {
			return false
		}
	}

	return true
}

// checkStaleProposal rejects proposals whose predicted index or cache epoch
// doesn't match the current state. This detects stale proposals admitted with
// an inflated IndexTracker or before a cache reset.
func (fsm *Machine) checkStaleProposal(raftIndex uint64, proposal *raftcmdpb.Proposal) domain.Describable {
	if predicted := proposal.GetPredictedIndex(); predicted != 0 && predicted != raftIndex {
		if fsm.logger.Enabled(logging.TraceLevel) {
			fsm.logger.WithFields(map[string]any{
				"predictedIndex": predicted,
				"actualIndex":    raftIndex,
				"proposalID":     proposal.GetId(),
			}).Tracef("Rejecting proposal: predicted index mismatch (stale tracker)")
		}

		assert.Reachable("stale proposal rejected: predicted index mismatch", map[string]any{
			"predictedIndex": predicted,
			"actualIndex":    raftIndex,
			"proposalID":     proposal.GetId(),
		})

		lifecycle.SendEvent("stale_proposal_rejected", map[string]any{
			"predictedIndex": predicted,
			"actualIndex":    raftIndex,
		})

		return domain.ErrStaleProposal
	}

	if preloadEpoch := proposal.GetPreload().GetCacheEpoch(); preloadEpoch != 0 && preloadEpoch != fsm.Registry.Cache.Epoch() {
		if fsm.logger.Enabled(logging.TraceLevel) {
			fsm.logger.WithFields(map[string]any{
				"preloadEpoch": preloadEpoch,
				"cacheEpoch":   fsm.Registry.Cache.Epoch(),
				"proposalID":   proposal.GetId(),
				"raftIndex":    raftIndex,
			}).Tracef("Rejecting proposal: cache epoch mismatch (cache was reset)")
		}

		assert.Reachable("stale proposal rejected: cache epoch mismatch", map[string]any{
			"preloadEpoch": preloadEpoch,
			"cacheEpoch":   fsm.Registry.Cache.Epoch(),
			"proposalID":   proposal.GetId(),
			"raftIndex":    raftIndex,
		})

		return domain.ErrStaleProposal
	}

	return nil
}

// applyProposal processes all orders in a proposal atomically.
// Uses RequestProcessor which handles rollback internally via WriteSet.
//
// Phase ordering matters: checkStaleProposal and Preload run BEFORE
// applyTechnicalUpdates so that technical updates that need to consult
// the cache (notably applyMetadataConversionBatch's compare-and-set
// against ExpectedValue) see the values the leader captured in the
// preload. Previously these two ran only on orders-bearing proposals,
// so technical-only proposals (the converter, cluster config,
// idempotency eviction, index-ready) silently ignored any PredictedIndex
// or Preload they carried.
func (fsm *Machine) applyProposal(ctx context.Context, raftIndex uint64, batch *dal.WriteSession, proposal *raftcmdpb.Proposal) (*ApplyResult, error) {
	// FSM-level safety net mirroring the admission check: a checkpoint trigger
	// (CreateQueryCheckpoint or ClosePeriod) must be the last order. The
	// applier relies on this so it can place a Pebble-batch boundary at this
	// proposal — a trigger that is not last would force a mid-batch commit
	// and race the pipelined committer. Any violation here means the
	// admission path was bypassed (replay of a pre-fix proposal, or a future
	// bug); refuse to apply rather than corrupt state.
	if ClassifyCheckpointOrderPosition(proposal.GetOrders()) == CheckpointOrderInvalid {
		assert.Unreachable("checkpoint trigger order not last in proposal", map[string]any{
			"raftIndex":  raftIndex,
			"proposalID": proposal.GetId(),
			"orderCount": len(proposal.GetOrders()),
		})

		return nil, fmt.Errorf("checkpoint trigger order not last in proposal id=%d at raft index %d", proposal.GetId(), raftIndex)
	}

	// checkStaleProposal is a no-op when PredictedIndex and CacheEpoch
	// are both unset, so legacy technical-only proposals (none today
	// set them) are unaffected.
	if err := fsm.checkStaleProposal(raftIndex, proposal); err != nil {
		return &ApplyResult{
			ProposalID: proposal.GetId(),
			Error:      &domain.BusinessError{Err: err},
		}, nil
	}

	// Preload is a no-op when the proposal carries no PreloadSet.
	genByte := byte(fsm.Registry.Cache.CurrentGeneration() % 2)
	if err := fsm.Preload(proposal.GetPreload(), batch, genByte); err != nil {
		return nil, fmt.Errorf("raftIndex=%d: %w", raftIndex, err)
	}

	if err := fsm.applyTechnicalUpdates(batch, raftIndex, proposal); err != nil {
		return nil, err
	}

	if len(proposal.GetOrders()) == 0 {
		return &ApplyResult{ProposalID: proposal.GetId()}, nil
	}

	// FSM-level maintenance mode check: reject proposals containing non-maintenance
	// orders that were admitted before maintenance mode was enabled but batched into
	// a Raft entry applied after the maintenance mode flag was set.
	if fsm.sharedState.MaintenanceMode() && !authorizedInMaintenanceMode(proposal.GetOrders()) {
		// Proves the admitted-before/applied-after race is actually exercised:
		// this branch only fires when a write passed admission before the
		// maintenance flag flipped on but applies after.
		assert.Reachable("write rejected by FSM maintenance gate after flag flip", map[string]any{
			"proposalID": proposal.GetId(),
			"orderCount": len(proposal.GetOrders()),
			"raftIndex":  raftIndex,
		})

		return &ApplyResult{
			ProposalID: proposal.GetId(),
			Error:      &domain.BusinessError{Err: domain.ErrMaintenanceMode},
		}, nil
	}

	// Compute the effective date using the HLC to guarantee monotonicity
	effectiveDate := &commonpb.Timestamp{Data: fsm.State.AdvanceHLC(proposal.GetDate().GetData())}

	if err := fsm.ensurePeriodBootstrapped(effectiveDate, batch); err != nil {
		return nil, err
	}

	// Reset the reusable WriteSet for this proposal.
	fsm.writeSet.Reset(effectiveDate)
	buffer := fsm.writeSet

	orders := proposal.GetOrders()

	// The maintenance gate above is the only barrier between an
	// admitted-before/applied-after write and the ledger state. Re-check the
	// inverse immediately before orders mutate state: reaching this point
	// with maintenance active and a non-SetMaintenanceMode order means the
	// gate was bypassed. The flag is FSM-owned (write_set.go) and only
	// mutates when a WriteSet commits, so it cannot change between the gate
	// and here within the same sequential apply — this never fires on the
	// legitimate carve-out (an all-SetMaintenanceMode batch, e.g. flipping
	// the flag off while maintenance is active).
	if fsm.sharedState.MaintenanceMode() && !authorizedInMaintenanceMode(orders) {
		assert.Unreachable("non-maintenance order applied while maintenance mode active", map[string]any{
			"proposalID": proposal.GetId(),
			"orderCount": len(orders),
			"raftIndex":  raftIndex,
		})
	}

	// Process the proposal
	logs, err := fsm.processor.ProcessOrders(orders, buffer)

	// Pre-marshal each order once. The same byte slices are (a) bound in
	// the audit hash chain via buildPerItemPayload and (b) persisted to
	// AuditItem.SerializedOrder, so verifiers re-hash the exact bytes
	// captured at apply time instead of re-marshalling an Order proto.
	// This decouples chain verification from vtprotobuf and from the
	// Order proto schema.
	serializedOrders := marshalOrdersForAudit(orders)

	// Helper to build, hash and write the audit entry (shared by success
	// and failure paths). The caller passes an AuditEntry with `Outcome`
	// already set; every other field is filled here BEFORE the hash is
	// computed, because BuildHashedHeaderPayload binds them all into the
	// chain. Tampering with any AuditEntry or AuditItem field on disk
	// after this commit is detected by checker.verifyAuditHashChain.
	writeAuditEntry := func(entry *auditpb.AuditEntry, logs []*raftcmdpb.CreatedLogOrReference, label string) error {
		// Sequence must be set BEFORE the header payload is built —
		// BuildHashedHeaderPayload binds entry.Sequence, and the
		// verifier rebuilds the payload from the persisted (non-zero)
		// sequence. Peek NextAuditSequenceID without mutating state;
		// the actual advance happens in AppendAuditEntry below, after
		// the hash is computed, so a partial failure in this closure
		// does not leak a phantom sequence into FSMState.
		entry.Sequence = fsm.State.NextAuditSequenceID
		entry.Timestamp = effectiveDate
		entry.ProposalId = proposal.GetId()
		entry.OrderCount = uint32(len(orders))
		entry.Ledgers = extractLedgers(orders)
		entry.HashVersion = uint32(fsm.State.HashGenerator.Algorithm())
		entry.CallerSnapshot = proposal.GetCallerSnapshot()

		items := buildAuditItems(serializedOrders, logs)

		// Hash pre-image: [header_payload || per_item_0 || ... ||
		// per_item_N-1] hashed alongside lastAuditHash. The verifier
		// rebuilds the same slices from the stored AuditEntry +
		// AuditItem fields via the same builders — never marshalling
		// anything, and never relying on a separately-persisted blob
		// (so a tampered typed field is structurally detectable).
		headerPayload, headerErr := BuildHashedHeaderPayload(entry)
		if headerErr != nil {
			return fmt.Errorf("building hashed header for %s: %w", label, headerErr)
		}

		hashSlices := make([][]byte, 0, 1+len(items))
		hashSlices = append(hashSlices, headerPayload)

		for _, item := range items {
			hashSlices = append(hashSlices, BuildPerItemPayload(item))
		}

		var auditHash []byte
		fsm.auditHashBuf, auditHash = fsm.State.HashGenerator.Compute(fsm.auditHashBuf, fsm.State.LastAuditHash, hashSlices)
		entry.Hash = auditHash

		// AppendAuditEntry validates that the peeked sequence matches
		// the actual next one (no concurrent mutation) and advances
		// LastAuditHash for the next entry.
		committedSeq := fsm.State.AppendAuditEntry(auditHash)
		if committedSeq != entry.GetSequence() {
			return fmt.Errorf("audit sequence race for %s: peeked %d, got %d", label, entry.GetSequence(), committedSeq)
		}

		// Items live under their own Pebble keys (SubColdAuditItem). The
		// AuditEntry value must never carry an embedded items list: the
		// chain does not hash entry.Items (each item is hashed via its
		// per-item payload from the separate keys), so a non-empty
		// embedded list would be a smuggling vector for content
		// returned through ListAuditEntries / GetAuditEntry without
		// being bound by the chain. Force it nil before persistence;
		// the checker also flags any non-empty value on read as a
		// defence-in-depth backstop.
		entry.Items = nil

		if appendErr := appendAuditEntries(batch, entry); appendErr != nil {
			return fmt.Errorf("appending audit entry for %s: %w", label, appendErr)
		}

		if appendErr := appendAuditItems(batch, entry.GetSequence(), items...); appendErr != nil {
			return fmt.Errorf("appending audit items for %s: %w", label, appendErr)
		}

		return nil
	}

	if err != nil {
		// FAILURE: write audit entry and return business error. `err` is
		// produced by processor.ProcessOrders which now returns Describable
		// directly — no boundary cast, no fallback path.
		if appendErr := writeAuditEntry(&auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Failure{Failure: buildAuditFailure(err)}}, nil, "failure"); appendErr != nil {
			return nil, appendErr
		}

		return &ApplyResult{
			ProposalID: proposal.GetId(),
			Error:      &domain.BusinessError{Err: err},
		}, nil
	}

	// Validate transient volumes have zero balance. This is a business error
	// (rejected proposal), not a fatal FSM error, so it must be checked before Commit.
	if err := buffer.ValidateTransientVolumes(); err != nil {
		if appendErr := writeAuditEntry(&auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Failure{Failure: buildAuditFailure(err)}}, nil, "transient validation failure"); appendErr != nil {
			return nil, appendErr
		}

		return &ApplyResult{
			ProposalID: proposal.GetId(),
			Error:      &domain.BusinessError{Err: err},
		}, nil
	}

	// Extract created logs (reference sequences are idempotent responses
	// that don't produce new logs).
	var createdLogs []*commonpb.Log

	for _, logOrRef := range logs {
		if created := logOrRef.GetCreatedLog(); created != nil {
			createdLogs = append(createdLogs, created)
		}
	}

	configChanged := buffer.HasPendingSinkChanges()
	mirrorConfigChanged := hasMirrorConfigChange(proposal)
	hasArchiveRequests := len(buffer.pendingArchives) > 0
	hasPurges := buffer.HasPurges()

	if err := buffer.Merge(batch, createdLogs); err != nil {
		return nil, err
	}

	// Update bloom filters with newly written keys (before batch.Commit).
	if fsm.BloomFilters != nil {
		fsm.BloomFilters.AddCanonicalKeys(buffer.BloomUpdates())
	}

	// Cross-check: volume deltas must match postings in the committed logs.
	if fsm.sentinelMode {
		// Build ledger name → ID map for sentinel cross-check.
		sentinelNameToID := make(map[string]uint32)
		for _, log := range createdLogs {
			if apply, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply); ok && apply.Apply != nil {
				name := apply.Apply.GetLedgerName()
				if _, exists := sentinelNameToID[name]; !exists {
					if info, ok := buffer.GetLedger(name); ok {
						sentinelNameToID[name] = info.GetId()
					}
				}
			}
		}

		if err := verifyVolumeDeltasMatchPostings(buffer.AllVolumeUpdates(), createdLogs, sentinelNameToID); err != nil {
			return nil, fmt.Errorf("volume delta/posting cross-check failed at raft index %d: %w", raftIndex, err)
		}
	}

	// Collect ledger IDs for post-commit aggregated balance verification.
	// The check must run after batch.Commit() — reading from the sentinel reader
	// before commit sees stale committed state that excludes the current batch,
	// which produces false positives when batch boundaries differ across nodes.
	ledgerIDs := collectLedgerIDs(proposal.GetOrders(), buffer)

	// Capture the audit hash BEFORE writing this proposal's audit entry.
	// This is the hash of the predecessor — used as LastAuditHash on the
	// period so the checker can chain-verify from the first non-purged entry.
	preAuditHash := make([]byte, len(fsm.State.LastAuditHash))
	copy(preAuditHash, fsm.State.LastAuditHash)

	// SUCCESS: write audit entry with batch-level side effects.
	minLogSeq, maxLogSeq := extractLogSequenceRange(logs)
	auditSuccess := &auditpb.AuditSuccess{
		MinLogSequence: minLogSeq,
		MaxLogSequence: maxLogSeq,
	}

	if ta := buffer.TransientAccounts(); len(ta) > 0 {
		auditSuccess.TransientAccounts = make(map[string]*auditpb.AccountList, len(ta))
		for ledgerID, accounts := range ta {
			ledgerName := strconv.FormatUint(uint64(ledgerID), 10)
			if info, ok := buffer.GetLedgerByID(ledgerID); ok {
				ledgerName = info.GetName()
			}

			auditSuccess.TransientAccounts[ledgerName] = &auditpb.AccountList{Accounts: accounts}
		}
	}

	if pa := buffer.PurgedAccounts(); len(pa) > 0 {
		auditSuccess.PurgedAccounts = make(map[string]*auditpb.AccountList, len(pa))
		for ledgerID, accounts := range pa {
			ledgerName := strconv.FormatUint(uint64(ledgerID), 10)
			if info, ok := buffer.GetLedgerByID(ledgerID); ok {
				ledgerName = info.GetName()
			}

			auditSuccess.PurgedAccounts[ledgerName] = &auditpb.AccountList{Accounts: accounts}
		}
	}

	if appendErr := writeAuditEntry(&auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Success{Success: auditSuccess}}, logs, "success"); appendErr != nil {
		return nil, appendErr
	}

	// Update closing period's LastAuditHash if this batch contains a ClosePeriod.
	// We use preAuditHash (the hash before this proposal's audit entry) so the
	// checker can use it as the chain input when verifying the first non-purged
	// audit entry after archive.
	for _, logOrRef := range logs {
		if created := logOrRef.GetCreatedLog(); created != nil {
			if cp := created.GetPayload().GetClosePeriod(); cp != nil {
				if closingPeriod := fsm.Periods.LatestClosingPeriod(); closingPeriod != nil {
					closingPeriod.LastAuditHash = preAuditHash
				}
			}
		}
	}

	fsm.logsAppendedCounter.Add(ctx, int64(len(createdLogs)))

	// Detect query checkpoint create/delete for gating.
	// The checkpoint ID is assigned by the processor, so read from pending saves.
	var queryCheckpointCreated, queryCheckpointDeleted uint64

	for _, cp := range buffer.pendingQueryCheckpointSaves {
		queryCheckpointCreated = cp.GetCheckpointId()
	}

	for _, order := range proposal.GetOrders() {
		if cp := order.GetDeleteQueryCheckpoint(); cp != nil {
			queryCheckpointDeleted = cp.GetCheckpointId()
		}
	}

	return &ApplyResult{
		ProposalID:              proposal.GetId(),
		Logs:                    logs,
		ConfigChanged:           configChanged,
		MirrorConfigChanged:     mirrorConfigChanged,
		HasArchiveRequests:      hasArchiveRequests,
		HasPurges:               hasPurges,
		QueryCheckpointCreated:  queryCheckpointCreated,
		QueryCheckpointDeleted:  queryCheckpointDeleted,
		MetadataConvertRequests: buffer.MetadataConvertRequests(),
		volumeUpdates:           buffer.KeptVolumeUpdates(),
		purgedVolumeKeys:        buffer.PurgedVolumeKeys(),
		createdLogs:             createdLogs,
		ledgerIDs:               ledgerIDs,
	}, nil
}

// hasMirrorConfigChange returns true if any order in the proposal creates or promotes a mirror ledger.
func hasMirrorConfigChange(proposal *raftcmdpb.Proposal) bool {
	for _, order := range proposal.GetOrders() {
		switch o := order.GetType().(type) {
		case *raftcmdpb.Order_CreateLedger:
			if o.CreateLedger.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
				return true
			}
		case *raftcmdpb.Order_PromoteLedger:
			return true
		}
	}

	return false
}

// Close stops background work owned by the Machine (e.g. bloom populate).
func (fsm *Machine) Close() {
	fsm.cacheSnapshotter.Stop()
}

// SealRequestCh returns the channel used to communicate seal requests between
// the Machine (writer, on ClosePeriod) and the Sealer (reader).
// Both sides need send access (Machine for normal flow, Sealer/Node for recovery).
func (fsm *Machine) SealRequestCh() *worker.Channel[SealRequest] {
	return fsm.sealRequestCh
}

// StopBackgroundTasks interrupts background tasks (bloom restore) that may hold
// Pebble iterators. Must be called during shutdown, after the Raft node tasks
// are stopped and before the Pebble store is closed.
func (fsm *Machine) StopBackgroundTasks() {
	fsm.cacheSnapshotter.Stop()
}

// DrainBackgroundChannels empties every background-request channel without
// blocking. Called by Synchronizer.SynchronizeWithLeader before the leader's
// checkpoint is installed: messages enqueued by the FSM hot path pre-sync
// reference period IDs, sequence ranges and checkpoint paths that may no
// longer line up with the post-sync FSMState. Fresh requests are re-pushed
// from durable state by Recovery.DispatchArchiveRequests /
// DispatchMetadataConversionRequests when leadership is (re)acquired, and by
// the per-worker reconciliation tickers in the meantime.
func (fsm *Machine) DrainBackgroundChannels() {
	fsm.sealRequestCh.Drain()
	fsm.archiveRequestCh.Drain()
	fsm.metadataConvertRequestCh.Drain()
	drainSignalChan(fsm.coldCompactionCh)
	drainSignalChan(fsm.bloomRebuildCh)
}

// drainSignalChan is the plain-channel counterpart of worker.Channel.Drain.
func drainSignalChan[T any](ch chan T) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// ArchiveRequestCh returns the channel used to dispatch archive requests to the Archiver.
func (fsm *Machine) ArchiveRequestCh() *worker.Channel[ArchiveRequest] {
	return fsm.archiveRequestCh
}

// MetadataConvertRequestCh returns the channel used to dispatch metadata
// conversion requests to the MetadataConverter.
func (fsm *Machine) MetadataConvertRequestCh() *worker.Channel[MetadataConvertRequest] {
	return fsm.metadataConvertRequestCh
}

// ColdCompactionCh returns the channel that signals the SmartCompactor when
// cold zone compaction is needed (after period purges).
func (fsm *Machine) ColdCompactionCh() <-chan struct{} {
	return fsm.coldCompactionCh
}

// BloomRebuildCh returns the channel signalled when a bloom rebuild is
// required (e.g. cluster config change). The consumer (Recovery) owns the
// Pebble reader and invokes StartAsyncBloomPopulate; the Machine itself
// holds no reader and cannot trigger the rebuild directly.
func (fsm *Machine) BloomRebuildCh() <-chan string {
	return fsm.bloomRebuildCh
}

// dispatchArchiveRequests sends archive requests for all ARCHIVING periods
// to the archiver channel.
//
// When stop is non-nil (recovery/reconciliation paths), sends block until
// the worker drains or stop is closed.
// When stop is nil (FSM apply path), sends are non-blocking with drop logging.
func (fsm *Machine) DispatchArchiveRequests(stop <-chan struct{}) {
	for _, p := range fsm.Periods.AllPeriods() {
		if p.GetStatus() == commonpb.PeriodStatus_PERIOD_ARCHIVING {
			req := ArchiveRequest{
				PeriodID:           p.GetId(),
				StartSequence:      p.GetStartSequence(),
				CloseSequence:      p.GetCloseSequence(),
				StartAuditSequence: p.GetStartAuditSequence(),
				CloseAuditSequence: p.GetCloseAuditSequence(),
			}

			if stop != nil {
				if !fsm.archiveRequestCh.Send(req, stop) {
					return
				}
			} else {
				fsm.archiveRequestCh.TrySend(req, fmt.Sprintf("period %d", req.PeriodID))
			}
		}
	}
}

// dispatchMetadataConversionRequests iterates all ledgers and dispatches
// conversion requests for metadata fields still in CONVERTING status. Called
// from recovery paths; the reader is supplied by Recovery so the Machine
// itself does not hold a Pebble read capability.

func (fsm *Machine) dispatchConvertingFields(stop <-chan struct{}, info *commonpb.LedgerInfo, targetType commonpb.TargetType, fields map[string]*commonpb.MetadataFieldSchema) {
	for key, field := range fields {
		if field.GetStatus() == commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING {
			if !fsm.metadataConvertRequestCh.Send(MetadataConvertRequest{
				LedgerName: info.GetName(),
				TargetType: targetType,
				Key:        key,
				Type:       field.GetType(),
			}, stop) {
				return
			}
		}
	}
}

// OnLeadershipAcquired lives on Recovery (it needs the Pebble reader for
// metadata-conversion dispatch). Callers should invoke Recovery.OnLeadershipAcquired.

// ensurePeriodBootstrapped creates the first period deterministically at the
// first proposal. The period start timestamp is derived from the proposal's
// effective date so that all nodes produce the same deterministic state.
func (fsm *Machine) ensurePeriodBootstrapped(effectiveDate *commonpb.Timestamp, batch *dal.WriteSession) error {
	if fsm.Periods.CurrentOpenPeriod() != nil {
		return nil
	}

	p := &commonpb.Period{
		Id:            1,
		Start:         effectiveDate,
		Status:        commonpb.PeriodStatus_PERIOD_OPEN,
		StartSequence: 1,
	}
	fsm.Periods.SetCurrentOpenPeriod(p)
	fsm.Periods.SetNextPeriodID(2)

	if err := StorePeriod(batch, p); err != nil {
		return fmt.Errorf("storing bootstrapped period: %w", err)
	}

	if err := StoreNextPeriodID(batch, fsm.Periods.NextPeriodID()); err != nil {
		return fmt.Errorf("storing next period ID: %w", err)
	}

	return nil
}

// AllPeriods returns all non-purged periods kept in memory.
func (fsm *Machine) AllPeriods() []*commonpb.Period {
	return fsm.Periods.AllPeriods()
}

// ClosingPeriods returns all periods currently in CLOSING state.
// Used for crash recovery on startup.
func (fsm *Machine) ClosingPeriods() []*commonpb.Period {
	return fsm.Periods.ClosingPeriods()
}

// ClosingPeriodByID returns the closing period with the given ID, if any.
func (fsm *Machine) ClosingPeriodByID(id uint64) (*commonpb.Period, bool) {
	return fsm.Periods.ClosingPeriodByID(id)
}

// ArchivingPeriodByID returns the period with the given ID if it is currently
// in ARCHIVING status. Used by the Archiver to gate consumption of stale
// requests after a follower sync: if the leader has already advanced the
// period to ARCHIVED (or further), the request must not produce a cold-storage
// write — the data ranges it carries no longer exist in the restored Pebble.
func (fsm *Machine) ArchivingPeriodByID(id uint64) (*commonpb.Period, bool) {
	p, ok := fsm.Periods.GetPeriodByID(id)
	if !ok || p.GetStatus() != commonpb.PeriodStatus_PERIOD_ARCHIVING {
		return nil, false
	}

	return p, true
}

// PeriodSchedule returns the current period schedule cron expression.
// Empty string means the schedule is disabled.
func (fsm *Machine) PeriodSchedule() string {
	return fsm.Periods.Schedule()
}

// ScheduleChanged returns the Signal that fires when the period schedule changes.
func (fsm *Machine) ScheduleChanged() signal.Signal {
	return fsm.Periods.ScheduleChanged()
}

// QueryCheckpointSchedule returns the current query checkpoint schedule cron expression.
// Empty string means the schedule is disabled.
func (fsm *Machine) QueryCheckpointSchedule() string {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	return fsm.State.QueryCheckpointSchedule
}

// SetQueryCheckpointSchedule updates the query checkpoint schedule and fires the changed signal.
// Must not be called from within ApplyEntries (use setQueryCheckpointSchedule instead).
func (fsm *Machine) SetQueryCheckpointSchedule(s string) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.setQueryCheckpointSchedule(s)
}

// setQueryCheckpointSchedule is the lock-free version for use within ApplyEntries
// where fsm.mu is already held.
func (fsm *Machine) setQueryCheckpointSchedule(s string) {
	fsm.State.QueryCheckpointSchedule = s
	fsm.queryCheckpointScheduleChanged.Notify()
}

// QueryCheckpointScheduleChanged returns the Signal that fires when the query checkpoint schedule changes.
func (fsm *Machine) QueryCheckpointScheduleChanged() signal.Signal {
	return fsm.queryCheckpointScheduleChanged
}

// checkClosePeriod checks if the apply result contains a ClosePeriod log
// and returns a SealRequest if the sealer should be triggered.
// Only created logs are checked since reference sequences are idempotent
// responses that already triggered sealing when first applied.
//
// Uses the FSM state's closing period (not the log payload snapshot) because
// applyProposal updates closingPeriod.LastAuditHash after computing the batch
// audit hash. The sealer must use the same value for the sealing hash to be
// verifiable by the checker.
func (fsm *Machine) checkClosePeriod(result *ApplyResult) *SealRequest {
	if result == nil {
		return nil
	}

	for _, logOrRef := range result.Logs {
		if created := logOrRef.GetCreatedLog(); created != nil {
			if created.GetPayload().GetClosePeriod() != nil {
				closingPeriod := fsm.Periods.LatestClosingPeriod()
				if closingPeriod != nil {
					return SealRequestFromPeriod(closingPeriod)
				}
			}
		}
	}

	return nil
}

func SealRequestFromPeriod(period *commonpb.Period) *SealRequest {
	return &SealRequest{
		PeriodID:      period.GetId(),
		CloseSequence: period.GetCloseSequence(),
		LastAuditHash: period.GetLastAuditHash(),
	}
}

// PreparedBatch holds an uncommitted Pebble batch and all data needed for
// the post-commit phase. Created by PrepareEntries, consumed by CommitPreparedBatch.
// This separation enables pipelining: the applier can process batch N+1 while
// batch N's commit is in-flight.
type PreparedBatch struct {
	batch *dal.WriteSession
	// Result holds the per-entry apply results. Exported for the applier to read.
	Result *ApplyEntriesResult

	// State captured during prepare for post-commit use.
	lastAppliedIndex     uint64
	lastSequenceID       uint64
	needsArchiveDispatch bool
	needsColdCompaction  bool
	eventsConfigChanged  bool
	mirrorConfigChanged  bool
	convertRequests      []MetadataConvertRequest
	checkpointDeletes    []uint64

	// Sentinel data (captured during prepare, validated after commit).
	sentinelMode      bool
	sentinelUpdates   []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]
	sentinelLedgerIDs []uint32
	sentinelTracer    *SentinelTracer

	// archiveRequests is captured during prepare so CommitPreparedBatch
	// does not need to read fsm.Periods (which may be mutated by the next prepare).
	archiveRequests []ArchiveRequest

	entryCount int
}

// Close cancels the uncommitted batch. Must be called if CommitPreparedBatch
// will not be called (e.g. on error in the caller).
func (pb *PreparedBatch) Close() {
	if pb.batch != nil {
		_ = pb.batch.Cancel()
		pb.batch = nil
	}
}

type ApplyResult struct {
	ProposalID              uint64
	AppliedIndex            uint64 // Raft index at which this entry was applied
	Logs                    []*raftcmdpb.CreatedLogOrReference
	Error                   error
	CheckpointPath          string // Set by Node after checkpoint creation (ClosePeriod proposals)
	ConfigChanged           bool   // True when sink configuration changed
	MirrorConfigChanged     bool   // True when mirror ledger configuration changed
	HasArchiveRequests      bool   // True when there are pending archive requests
	HasPurges               bool   // True when cold zone data was purged (triggers cold compaction)
	MetadataConvertRequests []MetadataConvertRequest

	// QueryCheckpointCreated holds the checkpoint ID when a CreateQueryCheckpoint
	// order was processed. Signals ApplyEntries to split the batch and create
	// physical Pebble checkpoints before continuing with remaining entries.
	QueryCheckpointCreated uint64

	// QueryCheckpointDeleted holds the checkpoint ID when a DeleteQueryCheckpoint
	// order was processed. Signals ApplyEntries to clean up physical files after commit.
	QueryCheckpointDeleted uint64

	// volumeUpdates and createdLogs are captured for post-commit verification.
	// Not exported because they are only used internally by ApplyEntries.
	volumeUpdates    []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]
	purgedVolumeKeys []domain.VolumeKey // keys removed by ephemeral purge
	createdLogs      []*commonpb.Log
	ledgerIDs        []uint32 // ledger IDs touched by this proposal (for post-commit balance check)
}

type ApplyEntriesResult struct {
	// Results contains one ApplyResult per processed entry that carried a proposal.
	Results []ApplyResult

	// CheckpointRequired is true when the caller must create a Pebble checkpoint
	// before resuming entry processing (e.g. after a ClosePeriod or
	// CreateQueryCheckpoint). The triggering entry is always the last in the
	// slice that produced this result — callers must pre-split to maintain that
	// invariant (see state.ClassifyCheckpointOrderPosition).
	CheckpointRequired bool

	// CheckpointPeriodID is the period ID that triggered the checkpoint.
	// Used by the Applier to name the checkpoint uniquely per period.
	CheckpointPeriodID uint64

	// OnCheckpointDone is called by Node once the Pebble checkpoint has been created.
	// It forges a SealRequest and sends it to the sealer.  Nil when CheckpointRequired is false.
	OnCheckpointDone func(checkpointPath string)

	// QueryCheckpointID is set when the checkpoint was triggered by a
	// CreateQueryCheckpointOrder (not a ClosePeriod). The Applier uses this
	// to create the main store checkpoint. The read index checkpoint is
	// created separately by the index builder when it processes the log.
	QueryCheckpointID uint64
}
