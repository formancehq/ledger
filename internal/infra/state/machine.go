package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
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
	Registry *StateRegistry  // KeyStores + Cache + Attrs
	Chapters *ChapterTracker // Chapter lifecycle

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

	// sealRequestCh receives seal requests when a CloseChapter log is applied.
	// The Sealer reads from this channel to perform background sealing.
	sealRequestCh *worker.Channel[SealRequest]

	// archiveRequestCh receives archive requests when an ArchiveChapter order is applied.
	// The Archiver reads from this channel to perform background archival to cold storage.
	archiveRequestCh *worker.Channel[ArchiveRequest]

	// coldCompactionCh signals the SmartCompactor that a chapter purge has been applied,
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
	preloadMissCounter        metric.Int64Counter

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

	// digestChain is the rolling cross-node FSM digest chain. The FSM
	// attaches it to every WriteSession opened on the hot path via
	// WriteSessionFactory.OpenFSMWriteSession, and folds one chain link
	// per applied Raft entry via WriteSession.AdvanceDigest. The chain
	// itself is a thin wrapper around an XXH3 HashGenerator (per-cluster
	// keyed) — see processing.NewFSMDigestChain. Stored as a single
	// pointer because the FSM is single-goroutine: no concurrent advance.
	digestChain dal.FSMDigestChain
}

// NewMachine constructs the hot-path FSM. It composes pre-built sub-objects
// (Registry, CacheSnapshotter) so the constructor reads no Pebble and keeps
// the surface focused on hot-path concerns. The bootstrap is responsible for
// wiring those sub-objects up-front. NewMachine does NOT perform RecoverState;
// callers must invoke Recovery.RecoverState() before the Machine applies
// entries.
func NewMachine(logger logging.Logger, registry *StateRegistry, cacheSnapshotter *CacheSnapshotter, queryCheckpoints dal.QueryCheckpoints, sentinel dal.SentinelFactory, meterProvider metric.MeterProvider, ks *keystore.KeyStore, sharedState *SharedState, notifier Notifier, bloomFilters *bloom.FilterSet, clusterID string, numscriptCacheSize int) (*Machine, error) {
	sentinelMode := sentinel.IsEnabled()
	// raft.* metrics describe the consensus engine and follow the
	// upstream etcd-raft naming convention; numscript.* metrics are
	// application-specific so they live on a separate meter, which
	// the metric-naming factory may prefix in `prom` mode.
	raftMeter := meterProvider.Meter("raft.node")
	numscriptMeter := meterProvider.Meter("numscript")
	logsAppendedCounter, err := raftMeter.Int64Counter(
		"raft.fsm.logs_appended",
		metric.WithDescription("Total number of logs appended to the store. Use rate() to get logs per second."),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating logs_appended counter: %w", err)
	}

	rotationDurationHistogram, err := raftMeter.Int64Histogram(
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

	batchCommitHistogram, err := raftMeter.Int64Histogram(
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

	preloadMissCounter, err := raftMeter.Int64Counter(
		"ledger.preload.coverage_miss",
		metric.WithDescription("Reads on the FSM hot path of keys not declared in the proposal's ExecutionPlan. Labeled by attribute kind. The order observing the miss is rejected with a business error."),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating preload_coverage_miss counter: %w", err)
	}

	processor, err := processing.NewRequestProcessor(numscriptMeter, numscriptCacheSize)
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
		preloadMissCounter:             preloadMissCounter,
		processor:                      processor,
		notifier:                       notifier,
		keyStore:                       ks,
		sharedState:                    sharedState,
		Registry:                       registry,
		Chapters:                       NewChapterTracker(nil, nil, nil, 0, ""),
		State:                          NewFSMState(clusterID),
		queryCheckpointScheduleChanged: signal.New(),
		sealRequestCh:                  worker.NewChannel[SealRequest](logger, "seal request", 10),
		archiveRequestCh:               worker.NewChannel[ArchiveRequest](logger, "archive request", 1),
		coldCompactionCh:               make(chan struct{}, 1),
		bloomRebuildCh:                 make(chan string, 1),
		auditHashBuf:                   make([]byte, 0, 4096),
		digestChain:                    processing.NewFSMDigestChain(clusterID),
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

// LastAppliedIndex returns the FSM's in-memory "currently being applied"
// cursor. It is seeded from Pebble in RecoverState at boot and advanced by
// `fsm.State.LastAppliedIndex++` during PrepareEntries (under fsm.mu),
// BEFORE the matching pb.batch.Commit() persists the increment. It serves
// three internal roles, all on the apply hot path: as the dedup watermark
// (`entry.Index <= LastAppliedIndex → skip`), as the gap detector
// (`entry.Index > LastAppliedIndex+1 → error`), and as the value
// SetAppliedIndex writes into the apply batch so the next boot reads it
// back from Pebble.
//
// This exported getter has a SINGLE production caller: node.Run's startup
// path, which reads it once (before node.Run starts) to seed
// raft.Config.Applied so etcd-raft does not re-emit already-applied
// entries on the first Ready. Other callers should prefer
// LastPersistedIndex (the post-commit durable cursor) — there is exactly
// one apply-cycle window during which LastPersistedIndex < LastAppliedIndex
// (between PrepareEntries bumping the in-memory value and publishApplied
// firing after Commit), so the two are NOT interchangeable for live
// "is this index durable?" checks.
func (fsm *Machine) LastAppliedIndex() uint64 {
	return fsm.State.LastAppliedIndex
}

// RestoreState atomically replaces the FSM-level state. The intended callers
// are Recovery (at boot) and Synchronizer (after install-snapshot), which
// build a fresh FSMState from Pebble via LoadFSMStateFromStore and swap it
// in here. Sub-trackers (Chapters, Registry.Reversions, KeyStore,
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

	batch := sessions.OpenFSMWriteSession(fsm.digestChain)

	cmd := raftcmdpb.ProposalFromVTPool()
	defer func() { cmd.ReturnToVTPool() }()

	ret := &ApplyEntriesResult{
		Results: make([]ApplyResult, 0, len(entries)),
	}
	sinkConfigChanged := false
	mirrorConfigChanged := false
	needsArchiveDispatch := false
	needsColdCompaction := false

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

			// Advance the rolling cross-node FSM digest chain even for
			// non-Normal entries (ConfChange) and empty-payload entries.
			// They bump LastAppliedIndex on every node identically, so the
			// chain must take a (deterministic, empty-ops) step to stay in
			// sync with peers. Skipping the advance would leave the chain
			// frozen at the last applyProposal entry while LastAppliedIndex
			// keeps advancing — and two nodes committing at different
			// lastAppliedIndex values would persist the same hash under
			// different applied indices, tripping the workload's
			// (applied, hash) equality check.
			batch.AdvanceDigest()

			continue
		}

		cmd.ReturnToVTPool()
		cmd = raftcmdpb.ProposalFromVTPool()

		if err := cmd.UnmarshalVT(entry.Data); err != nil {
			_ = batch.Cancel()

			return nil, err
		}

		if len(cmd.GetOrders()) == 0 && len(cmd.GetTechnicalUpdates()) == 0 {
			if fsm.sentinelMode && fsm.logger.Enabled(logging.TraceLevel) {
				fsm.logger.WithFields(map[string]any{
					"raftIndex":  entry.Index,
					"proposalID": cmd.GetId(),
				}).Tracef("SENTINEL: skipping no-op proposal")
			}

			ret.Results = append(ret.Results, ApplyResult{ProposalID: cmd.GetId(), AppliedIndex: entry.Index})

			// See the matching comment on the EntryConfChange / empty-data
			// branch above: a no-op proposal also bumps LastAppliedIndex on
			// every node, so the chain must take its empty-ops step here too.
			batch.AdvanceDigest()

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

		// Fold this entry's filtered op stream into the rolling cross-node
		// FSM digest chain. One chain link per Raft entry is what makes the
		// digest cross-node-equal regardless of how Raft batches entries
		// into MsgApps. No-op when deterministic encoding is off (the
		// WriteSession has no chain attached).
		batch.AdvanceDigest()

		result.AppliedIndex = entry.Index

		if fsm.sentinelMode {
			if result.Error != nil {
				fsm.sentinelTracer.RecordRejected(result.Error.Error())
			} else {
				fsm.sentinelTracer.RecordApplied(
					result.ledgerNames, len(result.createdLogs),
					len(result.volumeUpdates), len(result.purgedVolumeKeys),
				)
			}
		}

		if result.SinkConfigChanged {
			sinkConfigChanged = true
		}

		if result.MirrorConfigChanged {
			mirrorConfigChanged = true
		}

		if result.ArchiveRequested {
			needsArchiveDispatch = true
		}

		if result.ChaptersPurged {
			needsColdCompaction = true
		}

		ret.Results = append(ret.Results, *result)

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
		sealReqBase := fsm.checkCloseChapter(result)
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
			ret.CheckpointChapterID = sealReqBase.ChapterID
			ret.OnCheckpointDone = func(checkpointPath string) {
				sealReqBase.CheckpointPath = checkpointPath
				fsm.sealRequestCh.TrySend(*sealReqBase, fmt.Sprintf("chapter %d", sealReqBase.ChapterID))
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
		sinkConfigChanged:    sinkConfigChanged,
		mirrorConfigChanged:  mirrorConfigChanged,
		entryCount:           len(entries),
	}

	// Capture sentinel data before releasing the lock.
	if fsm.sentinelMode {
		pb.sentinelMode = true
		pb.sentinelUpdates = deduplicateVolumeUpdates(ret.Results)
		pb.sentinelLedgerNames = collectLedgerNamesFromResults(ret.Results)
		pb.sentinelTracer = fsm.sentinelTracer
	}

	// Capture archive requests from current chapter state.
	if needsArchiveDispatch {
		for _, p := range fsm.Chapters.AllChapters() {
			if p.GetStatus() == commonpb.ChapterStatus_CHAPTER_ARCHIVING {
				pb.archiveRequests = append(pb.archiveRequests, ArchiveRequest{
					ChapterID:          p.GetId(),
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

	// CommitWithRollingDigest persists the cross-node FSM digest tuple
	// (lastAppliedIndex, hash) into the same Pebble batch as the rest of
	// the writes, then commits. When deterministic encoding is off the
	// session has no chain attached and this degenerates to a plain
	// commit — same path, no overhead. On success the store's in-memory
	// rolling-digest cache is updated so the next session's chain seed
	// stays current.
	if _, err := pb.batch.CommitWithRollingDigest(pb.lastAppliedIndex); err != nil {
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
	if len(pb.sentinelUpdates) == 0 && len(pb.sentinelLedgerNames) == 0 {
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

		if len(pb.sentinelLedgerNames) > 0 {
			if fsm.logger.Enabled(logging.TraceLevel) {
				fsm.logger.Tracef("Verifying aggregated volume balance for %d ledgers at raft index %d", len(pb.sentinelLedgerNames), pb.lastAppliedIndex)
			}

			if err := verifyAggregatedVolumesBalanced(
				sentinelHandle, fsm.Registry.Attrs.Volume, pb.sentinelLedgerNames, pb.lastAppliedIndex, fsm.logger,
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
		fsm.archiveRequestCh.TrySend(req, fmt.Sprintf("chapter %d", req.ChapterID))
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

	fsm.notifier.NotifyLogsCommitted(pb.lastSequenceID)

	if pb.sinkConfigChanged || pb.mirrorConfigChanged {
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
func (fsm *Machine) Preload(executionPlan *raftcmdpb.ExecutionPlan, batch *dal.WriteSession, genByte byte) error {
	if executionPlan == nil {
		return nil
	}

	// Idempotency keys live outside the AttributePlan stream — they are not
	// a cache attribute (the FSM applies them to the dedicated Idempotency-
	// Store, not the per-kind cache). Apply them first and unconditionally:
	// a proposal carrying only idempotency keys (idempotent maintenance /
	// signature orders with no attribute needs) must still restore the
	// IdempotencyStore, otherwise at-most-once breaks on replay.
	for _, ik := range executionPlan.GetIdempotencyKeys() {
		// Install any value carrying an outcome — a committed log sequence or a
		// frozen business failure. Both must restore so a duplicate replays its
		// stored outcome instead of re-executing.
		v := ik.GetValue()
		if v != nil && (v.GetFirstLogSequence() > 0 || v.GetFailure() != nil) {
			fsm.Registry.Idempotency.Put(ik.GetKey(), v)
		}
	}

	if len(executionPlan.GetAttributes()) == 0 {
		return nil
	}

	// Pre-validate every AttributePlan envelope before touching the
	// cache. Without this, a forged plan with a nil/short AttributeID
	// or no intent would silently zero-pad through MirrorTouch /
	// MirrorPreload, mutating both the in-memory cache and the 0xFF
	// Pebble writes. A later business rejection from the scope path
	// commits its failure audit batch — and the cache mutations would
	// commit with it. Run the same validation the scope path uses
	// here, so a malformed plan is caught before the first MirrorTouch.
	for i, plan := range executionPlan.GetAttributes() {
		if err := validatePlan(plan, i); err != nil {
			return err
		}
	}

	// The preloads must target gen0 or gen1. The admission uses the
	// IndexTracker to predict the next Raft index and compute the boundary.
	// A mismatch here indicates a bug in the preload/cache coordination.
	switch executionPlan.GetLastPersistedIndex() {
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
			"lastPersistedIndex":  executionPlan.GetLastPersistedIndex(),
			"gen0":                fsm.Registry.Cache.BaseIndex.Gen0,
			"gen1":                fsm.Registry.Cache.BaseIndex.Gen1,
			"currentGeneration":   fsm.Registry.Cache.CurrentGeneration(),
			"generationThreshold": fsm.Registry.Cache.GenerationThreshold(),
			"lastAppliedIndex":    fsm.State.LastAppliedIndex,
			"attributeCount":      len(executionPlan.GetAttributes()),
		}
		fsm.logger.WithFields(details).Errorf("Preload boundary mismatch: LastPersistedIndex does not match Gen0 or Gen1")
		assert.Unreachable("preload boundary mismatch should be prevented by predicted_index check", details)
		lifecycle.SendEvent("preload_boundary_mismatch", details)

		return fmt.Errorf("preloading preloaded index is invalid: lastPersistedIndex=%d gen0=%d gen1=%d currentGen=%d lastApplied=%d",
			executionPlan.GetLastPersistedIndex(),
			fsm.Registry.Cache.BaseIndex.Gen0,
			fsm.Registry.Cache.BaseIndex.Gen1,
			fsm.Registry.Cache.CurrentGeneration(),
			fsm.State.LastAppliedIndex,
		)
	}

	gen1Byte := genByte ^ 1
	for _, plan := range executionPlan.GetAttributes() {
		switch intent := plan.GetIntent().(type) {
		case *raftcmdpb.AttributePlan_Declare:
			// Pure coverage declaration: the value is already in Gen0 on
			// every node. No FSM-side mutation; the Plan consumes
			// the declaration separately.

		case *raftcmdpb.AttributePlan_Touch:
			id := attributes.U128FromBytes(plan.GetId().GetId())
			if err := fsm.cacheSnapshotter.MirrorTouch(batch, byte(plan.GetAttrCode()), genByte, id); err != nil {
				return err
			}

		case *raftcmdpb.AttributePlan_Value:
			if err := fsm.cacheSnapshotter.MirrorPreload(batch, genByte, gen1Byte, plan.GetId(), byte(plan.GetAttrCode()), intent.Value); err != nil {
				return err
			}
		}
	}

	return nil
}

// authorizedInMaintenanceMode returns true if every order in the batch is a SetMaintenanceMode order.
func authorizedInMaintenanceMode(orders []*raftcmdpb.Order) bool {
	for _, order := range orders {
		system := order.GetSystemScoped()
		if system == nil {
			return false
		}

		if _, ok := system.GetPayload().(*raftcmdpb.SystemScopedOrder_SetMaintenanceMode); !ok {
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

	if preloadEpoch := proposal.GetExecutionPlan().GetCacheEpoch(); preloadEpoch != 0 && preloadEpoch != fsm.Registry.Cache.Epoch() {
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
// the cache see the values the leader captured in the preload.
// Previously these two ran only on orders-bearing proposals, so
// technical-only proposals (cluster config, idempotency eviction,
// index-ready) silently ignored any PredictedIndex or Preload they
// carried.
// planInvariantDescribable extracts the Describable wrapped in err when
// it is a coverage / execution-plan invariant violation. Returns nil
// when err is some other kind of error (Pebble write failure, etc.) so
// the caller can fall through to the FSM-killing path.
//
// Admission ships bits that don't match the AttributePlan slice →
// *ErrCoverageMiss or *domain.ErrInvalidExecutionPlan. Both implement
// Describable with KindInternal. Surfacing them via ApplyResult.Error
// rejects the proposal as a business error instead of wedging the FSM
// apply loop; the proposal is malformed, but the FSM state is not (no
// cache mutation lands before Merge).
func planInvariantDescribable(err error) domain.Describable {
	var miss *ErrCoverageMiss
	if errors.As(err, &miss) {
		return miss
	}

	var invalid *domain.ErrInvalidExecutionPlan
	if errors.As(err, &invalid) {
		return invalid
	}

	return nil
}

func (fsm *Machine) applyProposal(ctx context.Context, raftIndex uint64, batch *dal.WriteSession, proposal *raftcmdpb.Proposal) (*ApplyResult, error) {
	// FSM-level safety net mirroring the admission check: a checkpoint trigger
	// (CreateQueryCheckpoint or CloseChapter) must be the last order. The
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

	// Build the result up-front so every business-error branch below can
	// stash its rejection on the same ApplyResult — including the per-order
	// rejections (e.g. ErrBackupInProgress) that applyTechnicalUpdates
	// writes. For technical-only proposals the result is the only thing
	// returned anyway.
	result := &ApplyResult{ProposalID: proposal.GetId()}

	// checkStaleProposal is a no-op when PredictedIndex and CacheEpoch
	// are both unset, so legacy technical-only proposals (none today
	// set them) are unaffected.
	if err := fsm.checkStaleProposal(raftIndex, proposal); err != nil {
		result.Error = &domain.BusinessError{Err: err}

		return result, nil
	}

	// Preload is a no-op when the proposal carries no ExecutionPlan.
	genByte := byte(fsm.Registry.Cache.CurrentGeneration() % 2)
	if err := fsm.Preload(proposal.GetExecutionPlan(), batch, genByte); err != nil {
		if invariant := planInvariantDescribable(err); invariant != nil {
			// Malformed AttributePlan caught before any MirrorTouch /
			// MirrorPreload — no cache mutation landed. Surface as a
			// business rejection in the same shape as scope-level plan
			// invariants so the admission side can diagnose its bug.
			return &ApplyResult{
				ProposalID: proposal.GetId(),
				Error:      &domain.BusinessError{Err: invariant},
			}, nil
		}

		return nil, fmt.Errorf("raftIndex=%d: %w", raftIndex, err)
	}

	// Reset the reusable WriteSet up-front with proposal.GetDate() so the
	// technical-update phase queues its writes through the same overlay
	// the order phase will eventually drain via Merge. The Date will be
	// re-pointed to the HLC-advanced effective date below before
	// ProcessOrders runs (idempotency reads/writes use it; tech updates
	// do not).
	fsm.writeSet.Reset(proposal.GetDate())
	buffer := fsm.writeSet

	// scopeFactory is the Scope factory used by both ProcessOrders (one scope
	// per order, narrowed by Order.coverage_bits) and applyTechnicalUpdates
	// (one scope per TechnicalUpdate, narrowed by tu.coverage_bits). The
	// post-orders ValidateTransientVolumes uses scopeFactory(nil, nil) — full
	// proposal coverage — since the validation is cross-order by nature.
	scopeFactory := NewScopeFactory(buffer, proposal.GetExecutionPlan(), fsm.logger, fsm.preloadMissCounter, raftIndex)

	if err := fsm.applyTechnicalUpdates(scopeFactory, batch, raftIndex, proposal); err != nil {
		if invariant := planInvariantDescribable(err); invariant != nil {
			// Coverage miss or malformed execution plan in a TU handler
			// — same model as orders: surface as a business rejection,
			// not as an FSM-killing error. The overlay accumulated by
			// earlier TUs is discarded with the WriteSet when the
			// caller cancels the batch, so cache and Pebble stay in
			// lockstep for the next proposal. The audit chain is not
			// extended here: TU-only proposals don't appear in the
			// audit log at all, and a mixed proposal that fails before
			// reaching the orders phase already has its preload-miss
			// recorded by the gatedScope counter + structured log.
			return &ApplyResult{
				ProposalID: proposal.GetId(),
				Error:      &domain.BusinessError{Err: invariant},
			}, nil
		}

		// Backup lifecycle handlers (BackupOrder / IncrementalBackupOrder)
		// surface per-job rejections (ErrBackupInProgress, ErrBackupJobNotFound,
		// ErrBackupJobIDCollision) as the typed sentinels. The caller wants
		// these back as proposal-level errors (so the orchestrator can decide
		// whether to retry, abort, etc.) — same model as planInvariantDescribable
		// above. Anything else returned from a TU handler is FSM-fatal.
		if errors.Is(err, ErrBackupInProgress) ||
			errors.Is(err, ErrBackupJobIDCollision) ||
			errors.Is(err, ErrBackupJobNotFound) {
			return &ApplyResult{
				ProposalID: proposal.GetId(),
				Error:      err,
			}, nil
		}

		return nil, err
	}

	if len(proposal.GetOrders()) == 0 {
		// Technical-only proposal: still drain the overlay through Merge so
		// any tech-update writes (PutLedger, PutAccountMetadata, …) reach
		// the cache + Pebble. With no orders there is no log to append; the
		// audit-entry path is skipped entirely.
		if err := buffer.Merge(batch, nil); err != nil {
			return nil, fmt.Errorf("merging technical-update writes: %w", err)
		}

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

		result.Error = &domain.BusinessError{Err: domain.ErrMaintenanceMode}

		return result, nil
	}

	// Compute the effective date using the HLC to guarantee monotonicity
	effectiveDate := &commonpb.Timestamp{Data: fsm.State.AdvanceHLC(proposal.GetDate().GetData())}

	if err := fsm.ensureChapterBootstrapped(effectiveDate, batch); err != nil {
		return nil, err
	}

	// Re-point the WriteSet at the HLC-advanced effective date. The overlay
	// (Derived) populated by the technical-update phase is preserved — only
	// the timestamp field is rewired so order handlers see the monotonic
	// effective date (used by idempotency TTL checks and CreatedAt stamping).
	buffer.SetDate(effectiveDate)

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

	// Per-proposal idempotency: the whole atomic batch dedups under one key
	// (proposal.Idempotency), computed over the ordered orders so a retry of the
	// SAME batch replays while a differently-composed batch re-executes. A
	// duplicate replays the first outcome — reference logs for a success, the
	// original error for a frozen failure — instead of re-processing.
	idempotencyKey := proposal.GetIdempotency().GetKey()

	var (
		proposalHash []byte
		logs         []*raftcmdpb.CreatedLogOrReference
		err          domain.Describable
		replayed     bool
	)

	if idempotencyKey != "" {
		proposalHash = fsm.processor.HashProposal(proposal)

		if stored, ok := fsm.Registry.Idempotency.Get(idempotencyKey); ok &&
			!fsm.Registry.Idempotency.IsExpired(stored, effectiveDate.GetData()) {
			switch {
			case !bytes.Equal(proposalHash, stored.GetHash()):
				err = &domain.ErrIdempotencyKeyConflict{Key: idempotencyKey}
			case stored.GetFailure() != nil:
				replayed = true
				fr := stored.GetFailure()
				err = &domain.ReplayedFailure{
					ErrReason: domain.ReasonString(fr.GetReason()),
					Msg:       fr.GetMessage(),
					Meta:      fr.GetMetadata(),
				}
			default:
				replayed = true
				logs = make([]*raftcmdpb.CreatedLogOrReference, stored.GetLogCount())
				for i := range logs {
					logs[i] = &raftcmdpb.CreatedLogOrReference{
						Type: &raftcmdpb.CreatedLogOrReference_ReferenceSequence{
							ReferenceSequence: stored.GetFirstLogSequence() + uint64(i),
						},
					}
				}
			}
		}
	}

	// A replay of a recorded outcome returns it verbatim — no pipeline, no new
	// log, no audit entry. The original apply already committed its logs and
	// wrote its audit entry; re-recording would stretch the hash chain and
	// duplicate the order bytes on every retry. A conflict (same key, different
	// orders) is a fresh rejection, not a replay, so it still audits below.
	if replayed {
		if err != nil {
			result.Error = &domain.BusinessError{Err: err}
		} else {
			result.Logs = logs
		}

		return result, nil
	}

	// Process the proposal unless the dedup gate rejected it as a conflict
	// (err set). Handlers see the gated Scope facade for state reads/writes;
	// cross-order signals (purge, archive, schedule, sink config, cleanup,
	// chapter closing, ...) are derived from the per-order log payloads via
	// WriteSet.Absorb. OrdersResult carries the per-order log slice plus the
	// derivations (createdLogs filter, min/max sequence) so applyProposal
	// never re-walks `logs` to rebuild them.
	var ordersResult *processing.OrdersResult
	if err == nil {
		ordersResult, err = fsm.processor.ProcessOrders(orders, scopeFactory, buffer)
		if ordersResult != nil {
			logs = ordersResult.Logs
		}
	}

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
	//
	// IMPORTANT — write failure convention: `appendAuditEntries`,
	// `appendAuditItems`, and `appendAppliedProposal` (called by the
	// success path right after this) write into the Pebble batch. If
	// any of them returns an error AFTER `State.AppendAuditEntry` has
	// advanced the in-memory audit state, the in-memory state is ahead
	// of what will land on disk (the caller cancels the batch). The
	// project convention is: any such error MUST propagate out of
	// `Applier.Run()` and crash the process — a restart reloads
	// `LastAppliedIndex` from Pebble and Raft redelivers the missing
	// entries (cf. the lastPersistedIndex comment above). Properly-
	// configured nodes have no realistic way to fail here (the writes
	// go to an in-memory memtable), so we accept this in lieu of a
	// transactional stage/commit rework. Tracked in EN-1330.
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
		// Batch identity, bound into the hash chain. Shared (not cloned) like
		// CallerSnapshot: ResetVT only nils these pointers, never returns the
		// proposal's sub-messages to a pool.
		entry.Idempotency = proposal.GetIdempotency()
		entry.Signature = proposal.GetSignature()

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
		failureEntry := auditpb.AuditEntryFromVTPool()
		failureEntry.Outcome = &auditpb.AuditEntry_Failure{Failure: buildAuditFailure(err)}
		appendErr := writeAuditEntry(failureEntry, nil, "failure")
		failureEntry.ReturnToVTPool()
		if appendErr != nil {
			return nil, appendErr
		}

		if recErr := fsm.recordIdempotencyFailure(batch, idempotencyKey, proposalHash, err, effectiveDate.GetData()); recErr != nil {
			return nil, recErr
		}

		result.Error = &domain.BusinessError{Err: err}

		return result, nil
	}

	// ValidateTransientVolumes runs after the per-order RestrictTo passes
	// ProcessOrders ran with per-order scopes; the proposal `scope` itself
	// was never narrowed, so the cross-order ledger probe below goes
	// through the proposal-wide coverage the proposer shipped.
	//
	// Validate transient volumes have zero balance. This is a business
	// error (rejected proposal), not a fatal FSM error, so it must be
	// checked before Commit.
	validateScope, scopeErr := scopeFactory.NewProposalScope()
	if scopeErr != nil {
		// Building the proposal-wide scope failed only when the
		// ExecutionPlan is malformed (unknown attr_code). Treat as
		// business rejection — same model as orders/TU coverage misses.
		// NewScope's contract guarantees the error is
		// *domain.ErrInvalidExecutionPlan.
		invariant := planInvariantDescribable(scopeErr)
		if invariant == nil {
			invariant = &domain.ErrInvalidExecutionPlan{Reason_: scopeErr.Error()}
		}

		scopeFailureEntry := auditpb.AuditEntryFromVTPool()
		scopeFailureEntry.Outcome = &auditpb.AuditEntry_Failure{Failure: buildAuditFailure(scopeErr)}
		appendErr := writeAuditEntry(scopeFailureEntry, nil, "validate-scope construction failure")
		scopeFailureEntry.ReturnToVTPool()
		if appendErr != nil {
			return nil, appendErr
		}

		return &ApplyResult{
			ProposalID: proposal.GetId(),
			Error:      &domain.BusinessError{Err: invariant},
		}, nil
	}

	transientErr := buffer.ValidateTransientVolumes(validateScope)

	if err := transientErr; err != nil {
		transientFailureEntry := auditpb.AuditEntryFromVTPool()
		transientFailureEntry.Outcome = &auditpb.AuditEntry_Failure{Failure: buildAuditFailure(err)}
		appendErr := writeAuditEntry(transientFailureEntry, nil, "transient validation failure")
		transientFailureEntry.ReturnToVTPool()
		if appendErr != nil {
			return nil, appendErr
		}

		if recErr := fsm.recordIdempotencyFailure(batch, idempotencyKey, proposalHash, err, effectiveDate.GetData()); recErr != nil {
			return nil, recErr
		}

		result.Error = &domain.BusinessError{Err: err}

		return result, nil
	}

	sinkConfigChanged := buffer.SinkConfigChanged()
	mirrorConfigChanged := buffer.MirrorConfigChanged()
	archiveRequested := len(buffer.archiveRequests) > 0
	chaptersPurged := len(buffer.purgeRanges) > 0

	// Merge consumes the per-order log slice (CreatedLog or ReferenceSequence)
	// so it can inject Log.purged_volumes using per-order tracking before
	// AppendLogs runs.
	if err := buffer.Merge(batch, logs); err != nil {
		return nil, err
	}

	// CreatedLogs is accumulated during ProcessOrders' single pass — no
	// second walk over `logs` is needed.
	createdLogs := ordersResult.CreatedLogs

	// Freeze the proposal's success outcome under its idempotency key so a
	// duplicate replays the same committed logs instead of re-executing.
	// Sequences are contiguous, so (first, count) reconstructs every reference.
	if idempotencyKey != "" && len(createdLogs) > 0 {
		value := &commonpb.IdempotencyKeyValue{
			FirstLogSequence: createdLogs[0].GetSequence(),
			LogCount:         uint32(len(createdLogs)),
			Hash:             proposalHash,
			CreatedAt:        effectiveDate.GetData(),
		}

		if saveErr := saveIdempotencyKey(batch, idempotencyKey, value); saveErr != nil {
			return nil, fmt.Errorf("storing idempotency outcome: %w", saveErr)
		}

		fsm.Registry.Idempotency.Put(idempotencyKey, value)
	}

	// Update bloom filters with newly written keys (before batch.Commit).
	if fsm.BloomFilters != nil {
		fsm.BloomFilters.AddCanonicalKeys(buffer.BloomUpdates())
	}

	// Cross-check: volume deltas must match postings in the committed logs.
	if fsm.sentinelMode {
		if err := verifyVolumeDeltasMatchPostings(buffer.AllVolumeUpdates(), createdLogs); err != nil {
			return nil, fmt.Errorf("volume delta/posting cross-check failed at raft index %d: %w", raftIndex, err)
		}
	}

	// Collect ledger IDs for post-commit aggregated balance verification.
	// The check must run after batch.Commit() — reading from the sentinel reader
	// before commit sees stale committed state that excludes the current batch,
	// which produces false positives when batch boundaries differ across nodes.
	ledgerNames := collectLedgerNames(proposal.GetOrders())

	// Capture the audit hash BEFORE writing this proposal's audit entry.
	// This is the hash of the predecessor — used as LastAuditHash on the
	// chapter so the checker can chain-verify from the first non-purged entry.
	preAuditHash := make([]byte, len(fsm.State.LastAuditHash))
	copy(preAuditHash, fsm.State.LastAuditHash)

	// SUCCESS: emit batch-level side effects. The min/max sequence range
	// was accumulated during ProcessOrders — no second walk over `logs`.
	minLogSeq, maxLogSeq := ordersResult.MinLogSequence, ordersResult.MaxLogSequence

	auditSuccess := &auditpb.AuditSuccess{
		MinLogSequence: minLogSeq,
		MaxLogSequence: maxLogSeq,
	}

	// SUCCESS: write the audit entry first (advances State + writes audit
	// + audit items to the batch), then write the AppliedProposal record
	// (SubColdAppliedProposal = 0x04, after the audit entries at 0x02 /
	// 0x03 so the ZoneCold Pebble writes stay monotonically increasing
	// on the sub-prefix dimension — preferred by the memtable skiplist).
	//
	// `auditEntry.GetSequence()` is set by writeAuditEntry to the
	// peeked NextAuditSequenceID, preserving the 1:1
	// AppliedProposal.sequence == AuditEntry.sequence invariant on the
	// success path. Failure paths write no AppliedProposal — the
	// sequence space has gaps that index builder cursors tolerate.
	//
	// Write-failure convention applies here too: if appendAppliedProposal
	// returns an error, the audit state has already advanced in-memory
	// (see writeAuditEntry). The convention is to let that error
	// propagate out of Run() and crash the process; a restart reloads
	// from Pebble and Raft redelivers. Tracked for hardening in EN-1330.
	auditEntry := auditpb.AuditEntryFromVTPool()
	auditEntry.Outcome = &auditpb.AuditEntry_Success{Success: auditSuccess}
	appendErr := writeAuditEntry(auditEntry, logs, "success")
	auditSequence := auditEntry.GetSequence()
	auditEntry.ReturnToVTPool()
	if appendErr != nil {
		return nil, appendErr
	}

	applied := proposalpb.AppliedProposalFromVTPool()
	applied.Sequence = auditSequence
	applied.MinLogSequence = minLogSeq
	applied.MaxLogSequence = maxLogSeq
	if tv := buffer.TransientVolumes(); len(tv) > 0 {
		applied.TransientVolumes = make(map[string]*proposalpb.TouchedVolumeList, len(tv))
		for ledgerName, volumes := range tv {
			applied.TransientVolumes[ledgerName] = &proposalpb.TouchedVolumeList{Volumes: volumes}
		}
	}
	appendErr = appendAppliedProposal(batch, applied)
	applied.ReturnToVTPool()
	if appendErr != nil {
		return nil, fmt.Errorf("appending applied proposal: %w", appendErr)
	}

	// Update closing chapter's LastAuditHash if this batch contains a CloseChapter.
	// We use preAuditHash (the hash before this proposal's audit entry) so the
	// checker can use it as the chain input when verifying the first non-purged
	// audit entry after archive. The CloseChapter processor flips an O(1) flag
	// on the buffer; previously this was reconstructed by walking the log slice.
	if buffer.ChapterClosing() {
		if closingChapter := fsm.Chapters.LatestClosingChapter(); closingChapter != nil {
			closingChapter.LastAuditHash = preAuditHash
		}
	}

	fsm.logsAppendedCounter.Add(ctx, int64(len(createdLogs)))

	// Detect query checkpoint create/delete for gating.
	// The processor that created / deleted the checkpoint also emits the id;
	// previously this was reconstructed by walking pendingQueryCheckpointSaves
	// and proposal.GetOrders().
	queryCheckpointCreated := buffer.QueryCheckpointCreated()
	queryCheckpointDeleted := buffer.QueryCheckpointDeleted()

	return &ApplyResult{
		ProposalID:             proposal.GetId(),
		Logs:                   logs,
		SinkConfigChanged:      sinkConfigChanged,
		MirrorConfigChanged:    mirrorConfigChanged,
		ArchiveRequested:       archiveRequested,
		ChaptersPurged:         chaptersPurged,
		QueryCheckpointCreated: queryCheckpointCreated,
		QueryCheckpointDeleted: queryCheckpointDeleted,
		volumeUpdates:          buffer.KeptVolumeUpdates(),
		purgedVolumeKeys:       buffer.PurgedVolumeKeys(),
		createdLogs:            createdLogs,
		ledgerNames:            ledgerNames,
	}, nil
}

// recordIdempotencyFailure freezes a definitive business rejection against the
// proposal's idempotency key, so a duplicate (e.g. a retry across a leadership
// change) replays the same error instead of re-executing in a changed context.
// Written directly to the idempotency store + batch because the proposal's
// WriteSet is rolled back on failure; the batch is still committed (it carries
// the audit-failure entry). createdAt feeds the TTL time index so frozen
// failures expire like successes. proposalHash is the batch dedup hash, so a
// replay matches.
//
// No-op when there is no key, for non-business / retryable failures, for an
// already-replayed failure (re-recording would reset its TTL), and over a live
// (non-expired) prior outcome.
func (fsm *Machine) recordIdempotencyFailure(batch *dal.WriteSession, key string, proposalHash []byte, bizErr error, createdAt uint64) error {
	if key == "" {
		return nil
	}

	var replayed *domain.ReplayedFailure
	if errors.As(bizErr, &replayed) {
		return nil
	}

	var d domain.Describable
	if !errors.As(bizErr, &d) || !domain.IsFreezableFailure(domain.Kind(d)) {
		return nil
	}

	// Skip only when a live (non-expired) outcome already exists — never
	// overwrite a real result (a conflict holds the real outcome). An expired
	// entry not yet reclaimed by eviction counts as absent here, matching the
	// dedup gate, so a post-expiration failure still freezes.
	if existing, exists := fsm.Registry.Idempotency.Get(key); exists &&
		!fsm.Registry.Idempotency.IsExpired(existing, createdAt) {
		return nil
	}

	value := &commonpb.IdempotencyKeyValue{
		Hash:      proposalHash,
		CreatedAt: createdAt,
		Failure: &commonpb.IdempotencyFailure{
			Reason:   domain.ReasonCode(d.Reason()),
			Message:  d.Error(),
			Metadata: d.Metadata(),
		},
	}

	if err := saveIdempotencyKey(batch, key, value); err != nil {
		return err
	}

	fsm.Registry.Idempotency.Put(key, value)

	return nil
}

// Close stops background work owned by the Machine (e.g. bloom populate).
func (fsm *Machine) Close() {
	fsm.cacheSnapshotter.Stop()
}

// SealRequestCh returns the channel used to communicate seal requests between
// the Machine (writer, on CloseChapter) and the Sealer (reader).
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
// reference chapter IDs, sequence ranges and checkpoint paths that may no
// longer line up with the post-sync FSMState. Fresh requests are re-pushed
// from durable state by Recovery.DispatchArchiveRequests when leadership
// is (re)acquired, and by the per-worker reconciliation tickers in the
// meantime.
func (fsm *Machine) DrainBackgroundChannels() {
	fsm.sealRequestCh.Drain()
	fsm.archiveRequestCh.Drain()
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

// ColdCompactionCh returns the channel that signals the SmartCompactor when
// cold zone compaction is needed (after chapter purges).
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

// dispatchArchiveRequests sends archive requests for all ARCHIVING chapters
// to the archiver channel.
//
// When stop is non-nil (recovery/reconciliation paths), sends block until
// the worker drains or stop is closed.
// When stop is nil (FSM apply path), sends are non-blocking with drop logging.
func (fsm *Machine) DispatchArchiveRequests(stop <-chan struct{}) {
	for _, p := range fsm.Chapters.AllChapters() {
		if p.GetStatus() == commonpb.ChapterStatus_CHAPTER_ARCHIVING {
			req := ArchiveRequest{
				ChapterID:          p.GetId(),
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
				fsm.archiveRequestCh.TrySend(req, fmt.Sprintf("chapter %d", req.ChapterID))
			}
		}
	}
}

// OnLeadershipAcquired lives on Recovery (it needs the Pebble reader for
// metadata-conversion dispatch). Callers should invoke Recovery.OnLeadershipAcquired.

// ensureChapterBootstrapped creates the first chapter deterministically at the
// first proposal. The chapter start timestamp is derived from the proposal's
// effective date so that all nodes produce the same deterministic state.
func (fsm *Machine) ensureChapterBootstrapped(effectiveDate *commonpb.Timestamp, batch *dal.WriteSession) error {
	if fsm.Chapters.CurrentOpenChapter() != nil {
		return nil
	}

	p := &commonpb.Chapter{
		Id:            1,
		Start:         effectiveDate,
		Status:        commonpb.ChapterStatus_CHAPTER_OPEN,
		StartSequence: 1,
	}
	fsm.Chapters.SetCurrentOpenChapter(p)
	fsm.Chapters.SetNextChapterID(2)

	if err := StoreChapter(batch, p); err != nil {
		return fmt.Errorf("storing bootstrapped chapter: %w", err)
	}

	if err := StoreNextChapterID(batch, fsm.Chapters.NextChapterID()); err != nil {
		return fmt.Errorf("storing next chapter ID: %w", err)
	}

	return nil
}

// AllChapters returns all non-purged chapters kept in memory.
func (fsm *Machine) AllChapters() []*commonpb.Chapter {
	return fsm.Chapters.AllChapters()
}

// ClosingChapters returns all chapters currently in CLOSING state.
// Used for crash recovery on startup.
func (fsm *Machine) ClosingChapters() []*commonpb.Chapter {
	return fsm.Chapters.ClosingChapters()
}

// ClosingChapterByID returns the closing chapter with the given ID, if any.
func (fsm *Machine) ClosingChapterByID(id uint64) (*commonpb.Chapter, bool) {
	return fsm.Chapters.ClosingChapterByID(id)
}

// ArchivingChapterByID returns the chapter with the given ID if it is currently
// in ARCHIVING status. Used by the Archiver to gate consumption of stale
// requests after a follower sync: if the leader has already advanced the
// chapter to ARCHIVED (or further), the request must not produce a cold-storage
// write — the data ranges it carries no longer exist in the restored Pebble.
func (fsm *Machine) ArchivingChapterByID(id uint64) (*commonpb.Chapter, bool) {
	p, ok := fsm.Chapters.GetChapterByID(id)
	if !ok || p.GetStatus() != commonpb.ChapterStatus_CHAPTER_ARCHIVING {
		return nil, false
	}

	return p, true
}

// ChapterSchedule returns the current chapter schedule cron expression.
// Empty string means the schedule is disabled.
func (fsm *Machine) ChapterSchedule() string {
	return fsm.Chapters.Schedule()
}

// ScheduleChanged returns the Signal that fires when the chapter schedule changes.
func (fsm *Machine) ScheduleChanged() signal.Signal {
	return fsm.Chapters.ScheduleChanged()
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

// checkCloseChapter checks if the apply result contains a CloseChapter log
// and returns a SealRequest if the sealer should be triggered.
// Only created logs are checked since reference sequences are idempotent
// responses that already triggered sealing when first applied.
//
// Uses the FSM state's closing chapter (not the log payload snapshot) because
// applyProposal updates closingChapter.LastAuditHash after computing the batch
// audit hash. The sealer must use the same value for the sealing hash to be
// verifiable by the checker.
func (fsm *Machine) checkCloseChapter(result *ApplyResult) *SealRequest {
	if result == nil {
		return nil
	}

	for _, logOrRef := range result.Logs {
		if created := logOrRef.GetCreatedLog(); created != nil {
			if created.GetPayload().GetCloseChapter() != nil {
				closingChapter := fsm.Chapters.LatestClosingChapter()
				if closingChapter != nil {
					return SealRequestFromChapter(closingChapter)
				}
			}
		}
	}

	return nil
}

func SealRequestFromChapter(chapter *commonpb.Chapter) *SealRequest {
	return &SealRequest{
		ChapterID:     chapter.GetId(),
		CloseSequence: chapter.GetCloseSequence(),
		LastAuditHash: chapter.GetLastAuditHash(),
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
	sinkConfigChanged    bool
	mirrorConfigChanged  bool
	checkpointDeletes    []uint64

	// Sentinel data (captured during prepare, validated after commit).
	sentinelMode        bool
	sentinelUpdates     []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]
	sentinelLedgerNames []string
	sentinelTracer      *SentinelTracer

	// archiveRequests is captured during prepare so CommitPreparedBatch
	// does not need to read fsm.Chapters (which may be mutated by the next prepare).
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
	ProposalID          uint64
	AppliedIndex        uint64 // Raft index at which this entry was applied
	Logs                []*raftcmdpb.CreatedLogOrReference
	Error               error
	CheckpointPath      string // Set by Node after checkpoint creation (CloseChapter proposals)
	SinkConfigChanged   bool   // True when events sink configuration changed
	MirrorConfigChanged bool   // True when mirror ledger configuration changed
	ArchiveRequested    bool   // True when at least one chapter archive was requested
	ChaptersPurged      bool   // True when cold zone data was purged (triggers cold compaction)

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
	ledgerNames      []string // ledger names touched by this proposal (for post-commit balance check)
}

type ApplyEntriesResult struct {
	// Results contains one ApplyResult per processed entry that carried a proposal.
	Results []ApplyResult

	// CheckpointRequired is true when the caller must create a Pebble checkpoint
	// before resuming entry processing (e.g. after a CloseChapter or
	// CreateQueryCheckpoint). The triggering entry is always the last in the
	// slice that produced this result — callers must pre-split to maintain that
	// invariant (see state.ClassifyCheckpointOrderPosition).
	CheckpointRequired bool

	// CheckpointChapterID is the chapter ID that triggered the checkpoint.
	// Used by the Applier to name the checkpoint uniquely per chapter.
	CheckpointChapterID uint64

	// OnCheckpointDone is called by Node once the Pebble checkpoint has been created.
	// It forges a SealRequest and sends it to the sealer.  Nil when CheckpointRequired is false.
	OnCheckpointDone func(checkpointPath string)

	// QueryCheckpointID is set when the checkpoint was triggered by a
	// CreateQueryCheckpointOrder (not a CloseChapter). The Applier uses this
	// to create the main store checkpoint. The read index checkpoint is
	// created separately by the index builder when it processes the log.
	QueryCheckpointID uint64
}
