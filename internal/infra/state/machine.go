package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/cockroachdb/pebble"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/keystore"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/kv"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Notifier is notified by the FSM when logs are committed or config changes.
// Used by the events Manager and mirror Manager.
type Notifier interface {
	NotifyLogsCommitted(lastSeq uint64)
	NotifyConfigChanged()
}

// NoopNotifier is a no-op implementation of Notifier for use in tests.
type NoopNotifier struct{}

func (NoopNotifier) NotifyLogsCommitted(uint64) {}
func (NoopNotifier) NotifyConfigChanged()       {}

type Machine struct {
	logger    logging.Logger
	dataStore *dal.Store

	mu sync.Mutex

	// Composed subsystems
	Registry *StateRegistry // 10 KeyStores + Cache + Attrs (includes NumscriptVersions/NumscriptEntries)
	Periods  *PeriodTracker // Period lifecycle

	// pendingLedgerCleanups tracks deleted ledgers whose Pebble data has not
	// yet been purged. Map key is the ledger name; value is the sequence number
	// of the DeleteLedger log. Data is cleaned up when the purge range covers
	// the delete sequence.
	pendingLedgerCleanups map[string]uint64

	// FSM mechanics
	nextSequenceID      uint64
	nextAuditSequenceID uint64
	lastLogHash         []byte
	lastCheckpointID    uint64

	lastAppliedIndex     uint64
	lastAppliedTimestamp uint64
	snapshotIndex        uint64

	// KeyStore holds registered signing keys (updated after proposal apply)
	keyStore *keystore.KeyStore

	// sharedState holds maintenance mode and require-signatures flags
	sharedState *SharedState

	// RequestProcessor handles business logic
	processor *processing.RequestProcessor

	// sealRequestCh receives seal requests when a ClosePeriod log is applied.
	// The Sealer reads from this channel to perform background sealing.
	sealRequestCh chan SealRequest

	// archiveRequestCh receives archive requests when an ArchivePeriod order is applied.
	// The Archiver reads from this channel to perform background archival to cold storage.
	archiveRequestCh chan ArchiveRequest

	// metadataConvertRequestCh receives conversion requests when a SetMetadataFieldType
	// log is applied. The MetadataConverter reads from this channel to perform
	// background conversion of existing account metadata values.
	metadataConvertRequestCh chan MetadataConvertRequest

	// coldCompactionCh signals the SmartCompactor that a period purge has been applied,
	// meaning the cold zone [0x01, 0xF1) contains fresh tombstones that benefit from compaction.
	coldCompactionCh chan struct{}

	// Metrics
	logsAppendedCounter       metric.Int64Counter
	rotationDurationHistogram metric.Int64Histogram
	batchCommitHistogram      metric.Int64Histogram
	lastPersistedIndex        atomic.Uint64

	// volumeAssertions enables runtime volume consistency checks
	// (monotonicity, delta/posting cross-check, post-commit cache/Pebble verification).
	volumeAssertions bool

	// eventNotifier is notified after new logs are committed and when events
	// config changes. Used by the event Manager.
	eventNotifier Notifier

	// appliedMu and appliedCond are used to notify waiters when lastPersistedIndex advances.
	// This enables ReadIndex-based linearizable reads: callers wait until the FSM has caught up
	// to a target commit index before reading local state.
	appliedMu   sync.Mutex
	appliedCond *sync.Cond

	// mirrorNotifier is notified after new logs are committed and when mirror
	// ledger config changes. Used by the mirror Manager.
	mirrorNotifier Notifier

	// indexNotifier is notified after new logs are committed.
	// Used by the index builder to update the read store.
	indexNotifier Notifier

	// snapshotBuf is a reusable buffer for snapshot serialization to avoid
	// repeated allocations (snapshots can be large).
	snapshotBuf []byte
}

func NewMachine(logger logging.Logger, dataStore *dal.Store, meter metric.Meter, cache *cache.Cache, attrs *attributes.Attributes, ks *keystore.KeyStore, sharedState *SharedState, eventNotifier Notifier, mirrorNotifier Notifier, indexNotifier Notifier, numscriptCacheSize int, volumeAssertions bool) (*Machine, error) {
	stepStart := time.Now()

	lastAppliedIndex, err := query.ReadLastAppliedIndex(dataStore)
	if err != nil {
		return nil, err
	}

	logger.WithFields(map[string]any{"duration": time.Since(stepStart).String()}).Infof("FSM: ReadLastAppliedIndex done")

	stepStart = time.Now()

	lastAppliedTimestamp, err := query.ReadLastAppliedTimestamp(dataStore)
	if err != nil {
		return nil, err
	}

	logger.WithFields(map[string]any{"duration": time.Since(stepStart).String()}).Infof("FSM: ReadLastAppliedTimestamp done")

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

	stepStart = time.Now()

	periodsCursor, err := query.ReadPeriods(context.Background(), dataStore)
	if err != nil {
		return nil, fmt.Errorf("loading periods from store: %w", err)
	}

	periodsFromStore, err := dal.Collect(periodsCursor)
	if err != nil {
		return nil, fmt.Errorf("collecting periods: %w", err)
	}

	logger.WithFields(map[string]any{"duration": time.Since(stepStart).String(), "count": len(periodsFromStore)}).Infof("FSM: ReadPeriods done")

	allPeriods := make(map[uint64]*commonpb.Period, len(periodsFromStore))

	var currentOpenPeriod *commonpb.Period

	var closingPeriods []*commonpb.Period

	for _, p := range periodsFromStore {
		allPeriods[p.GetId()] = p

		switch p.GetStatus() {
		case commonpb.PeriodStatus_PERIOD_OPEN:
			currentOpenPeriod = p
		case commonpb.PeriodStatus_PERIOD_CLOSING:
			closingPeriods = append(closingPeriods, p)
		}
	}

	stepStart = time.Now()

	nextPeriodID, err := query.ReadNextPeriodID(dataStore)
	if err != nil {
		return nil, fmt.Errorf("loading next period ID from store: %w", err)
	}

	logger.WithFields(map[string]any{"duration": time.Since(stepStart).String()}).Infof("FSM: ReadNextPeriodID done")

	stepStart = time.Now()

	periodSchedule, err := query.ReadPeriodSchedule(dataStore)
	if err != nil {
		return nil, fmt.Errorf("loading period schedule from store: %w", err)
	}

	logger.WithFields(map[string]any{"duration": time.Since(stepStart).String()}).Infof("FSM: ReadPeriodSchedule done")

	stepStart = time.Now()

	processor, err := processing.NewRequestProcessor(meter, numscriptCacheSize)
	if err != nil {
		return nil, fmt.Errorf("creating request processor: %w", err)
	}

	logger.WithFields(map[string]any{"duration": time.Since(stepStart).String()}).Infof("FSM: NewRequestProcessor done")

	// Load signing keys from Pebble on startup
	stepStart = time.Now()

	if ks != nil {
		signingKeys, err := query.ReadSigningKeys(dataStore)
		if err != nil {
			return nil, fmt.Errorf("loading signing keys from store: %w", err)
		}

		for keyID, entry := range signingKeys {
			ks.AddPublicKey(keyID, entry.PublicKey, entry.ParentKeyID)
		}
	}

	logger.WithFields(map[string]any{"duration": time.Since(stepStart).String()}).Infof("FSM: ReadSigningKeys done")

	// Load shared runtime flags from Pebble on startup
	stepStart = time.Now()

	requireSig, err := query.ReadSigningConfig(dataStore)
	if err != nil {
		return nil, fmt.Errorf("loading signing config from store: %w", err)
	}

	sharedState.SetRequireSignatures(requireSig)

	maintenanceMode, err := query.ReadMaintenanceMode(dataStore)
	if err != nil {
		return nil, fmt.Errorf("loading maintenance mode from store: %w", err)
	}

	sharedState.SetMaintenanceMode(maintenanceMode)

	auditEnabled, err := query.ReadAuditConfig(dataStore)
	if err != nil {
		return nil, fmt.Errorf("loading audit config from store: %w", err)
	}

	sharedState.SetAuditEnabled(auditEnabled)
	logger.WithFields(map[string]any{"duration": time.Since(stepStart).String()}).Infof("FSM: ReadSharedState done")

	stepStart = time.Now()

	pendingCleanups, err := query.ReadPendingLedgerCleanups(dataStore)
	if err != nil {
		return nil, fmt.Errorf("loading pending ledger cleanups from store: %w", err)
	}

	logger.WithFields(map[string]any{"duration": time.Since(stepStart).String(), "count": len(pendingCleanups)}).Infof("FSM: ReadPendingLedgerCleanups done")

	fsm := &Machine{
		logger:                    logger,
		dataStore:                 dataStore,
		lastAppliedIndex:          lastAppliedIndex,
		lastAppliedTimestamp:      lastAppliedTimestamp,
		volumeAssertions:          volumeAssertions,
		logsAppendedCounter:       logsAppendedCounter,
		rotationDurationHistogram: rotationDurationHistogram,
		batchCommitHistogram:      batchCommitHistogram,
		processor:                 processor,
		eventNotifier:             eventNotifier,
		mirrorNotifier:            mirrorNotifier,
		indexNotifier:             indexNotifier,
		keyStore:                  ks,
		sharedState:               sharedState,
		Registry:                  NewStateRegistry(cache, attrs),
		Periods:                   NewPeriodTracker(allPeriods, currentOpenPeriod, closingPeriods, nextPeriodID, periodSchedule),
		nextSequenceID:            1,
		nextAuditSequenceID:       1,
		sealRequestCh:             make(chan SealRequest, 10),
		archiveRequestCh:          make(chan ArchiveRequest, 1),
		metadataConvertRequestCh:  make(chan MetadataConvertRequest, 16),
		coldCompactionCh:          make(chan struct{}, 1),
		pendingLedgerCleanups:     pendingCleanups,
	}
	fsm.appliedCond = sync.NewCond(&fsm.appliedMu)

	return fsm, nil
}

// RecoverState recovers the FSM's in-memory counters from the Pebble data store.
// This is called during restore bootstrap when the WAL snapshot doesn't carry
// the FSM memory state (nextSequenceID, etc.).
// After calling this method, CreateSnapshot will serialize the correct state.
func (fsm *Machine) RecoverState() error {
	// Recover nextSequenceID from last log sequence
	lastSeq, err := query.ReadLastSequence(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering last sequence: %w", err)
	}

	if lastSeq > 0 {
		fsm.nextSequenceID = lastSeq + 1
	}

	// Recover lastLogHash from the last log entry
	lastLog, err := query.ReadLastLog(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering last log: %w", err)
	}

	if lastLog != nil {
		fsm.lastLogHash = lastLog.GetHash()
	}

	// Recover nextAuditSequenceID from last audit entry
	lastAuditSeq, err := query.ReadLastAuditSequence(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering last audit sequence: %w", err)
	}

	if lastAuditSeq > 0 {
		fsm.nextAuditSequenceID = lastAuditSeq + 1
	}

	fsm.logger.WithFields(map[string]any{
		"nextSequenceID":      fsm.nextSequenceID,
		"nextAuditSequenceID": fsm.nextAuditSequenceID,
		"hasLogHash":          len(fsm.lastLogHash) > 0,
	}).Infof("Recovered FSM state from store")

	return nil
}

func (fsm *Machine) LastPersistedIndex() uint64 {
	return fsm.lastPersistedIndex.Load()
}

// LastAppliedIndex returns the last applied Raft index as read from the data
// store at construction time. It is NOT updated during Apply — use
// LastPersistedIndex for the live value. This is intended for raft.Config.Applied
// so that the first Ready does not re-emit already-applied entries.
func (fsm *Machine) LastAppliedIndex() uint64 {
	return fsm.lastAppliedIndex
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
	for fsm.lastPersistedIndex.Load() < targetIndex {
		if ctx.Err() != nil {
			fsm.appliedMu.Unlock()

			return ctx.Err()
		}

		fsm.appliedCond.Wait()
	}
	fsm.appliedMu.Unlock()

	return nil
}

func (fsm *Machine) ApplyEntries(ctx context.Context, entries ...raftpb.Entry) (*ApplyEntriesResult, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if fsm.snapshotIndex > fsm.lastAppliedIndex {
		assert.Unreachable("node out of sync during apply", map[string]any{
			"snapshotIndex":    fsm.snapshotIndex,
			"lastAppliedIndex": fsm.lastAppliedIndex,
		})

		return nil, &ErrNodeOutOfSync{
			SnapshotIndex:    fsm.snapshotIndex,
			LastAppliedIndex: fsm.lastAppliedIndex,
		}
	}

	batch := fsm.dataStore.NewBatch()

	defer func() {
		_ = batch.Cancel()
	}()

	cmd := &raftcmdpb.Proposal{}
	ret := &ApplyEntriesResult{
		Results: make([]ApplyResult, 0, len(entries)),
	}
	eventsConfigChanged := false
	mirrorConfigChanged := false
	needsArchiveDispatch := false
	needsColdCompaction := false

	var pendingConvertRequests []MetadataConvertRequest

	for i, entry := range entries {
		if entry.Index <= fsm.lastAppliedIndex {
			ret.Results = append(ret.Results, ApplyResult{})

			continue
		}

		if entry.Index > fsm.lastAppliedIndex+1 {
			assert.Unreachable("entry index gap detected", map[string]any{
				"receivedIndex": entry.Index,
				"expectedIndex": fsm.lastAppliedIndex + 1,
			})

			return nil, &ErrInvalidEntryIndex{
				ReceivedIndex: entry.Index,
				ExpectedIndex: fsm.lastAppliedIndex + 1,
			}
		}

		if rotated, _ := fsm.Registry.Cache.CheckRotationNeeded(entry.Index); rotated {
			lifecycle.SendEvent("spool replay completed", map[string]any{
				"entryIndex": entry.Index,
			})
			rotationStart := time.Now()
			fsm.rotationDurationHistogram.Record(context.Background(), time.Since(rotationStart).Microseconds())
		}

		fsm.lastAppliedIndex++

		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}

		cmd.Reset()

		if err := cmd.UnmarshalVT(entry.Data); err != nil {
			return nil, err
		}

		// Skip applyProposal for system-only proposals with no orders AND
		// no sink/mirror updates. Proposals carrying MirrorSyncUpdates or
		// EventsSinkUpdates must still go through applyProposal so that
		// cursor and status writes reach the Pebble batch.
		if len(cmd.GetOrders()) == 0 && len(cmd.GetMirrorSyncUpdates()) == 0 && len(cmd.GetEventsSinkUpdates()) == 0 {
			ret.Results = append(ret.Results, ApplyResult{ProposalID: cmd.GetId()})

			continue
		}

		result, err := fsm.applyProposal(ctx, entry.Index, batch, cmd)
		if err != nil {
			return nil, err
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

		// If ClosePeriod was detected, stop processing immediately.
		// Commit the batch so the ClosePeriod state is persisted,
		// then signal the caller to create a Pebble checkpoint.
		if sealReqBase := fsm.checkClosePeriod(result); sealReqBase != nil {
			return fsm.commitAndRequestCheckpoint(batch, ret, entries[i+1:], needsArchiveDispatch, sealReqBase.PeriodID, func(checkpointPath string) {
				sealReqBase.CheckpointPath = checkpointPath
				select {
				case fsm.sealRequestCh <- *sealReqBase:
				default:
				}
			})
		}
	}

	err := SetAppliedIndex(batch, fsm.lastAppliedIndex)
	if err != nil {
		return nil, fmt.Errorf("setting applied index: %w", err)
	}

	err = SetLastAppliedTimestamp(batch, fsm.lastAppliedTimestamp)
	if err != nil {
		return nil, fmt.Errorf("setting last applied timestamp: %w", err)
	}

	commitStart := time.Now()

	err = batch.Commit()
	if err != nil {
		return nil, fmt.Errorf("committing batch: %w", err)
	}

	fsm.batchCommitHistogram.Record(context.Background(), time.Since(commitStart).Microseconds())

	// Post-commit assertion: verify cache/Pebble volume consistency.
	// This catches bugs where the cache diverges from Pebble (stale preloads,
	// lost updates, snapshot issues). Runs only when there are volume updates.
	//
	// When multiple entries in the same ApplyEntries batch touch the same volume
	// key, only the last entry's value survives in Pebble (earlier entries are
	// deleted by mergeSimple's DeleteAt). We must deduplicate by canonical key,
	// keeping only the latest update, before comparing with Pebble.
	if fsm.volumeAssertions {
		finalUpdates := deduplicateVolumeUpdates(ret.Results)
		if len(finalUpdates) > 0 {
			if err := verifyPostCommitVolumes(
				fsm.dataStore, fsm.Registry.Attrs.Volume,
				finalUpdates, fsm.lastAppliedIndex,
			); err != nil {
				fsm.logger.Errorf("POST-COMMIT VOLUME ASSERTION FAILED: %v", err)

				return nil, fmt.Errorf("post-commit volume assertion failed: %w", err)
			}
		}
	}

	fsm.lastPersistedIndex.Store(fsm.lastAppliedIndex)
	fsm.appliedCond.Broadcast()

	if needsArchiveDispatch {
		fsm.dispatchArchiveRequests()
	}

	if needsColdCompaction {
		select {
		case fsm.coldCompactionCh <- struct{}{}:
		default:
		}
	}

	for _, req := range pendingConvertRequests {
		select {
		case fsm.metadataConvertRequestCh <- req:
		default:
		}
	}

	// Notify event Manager that new logs are available.
	lastSeq := fsm.nextSequenceID - 1
	fsm.eventNotifier.NotifyLogsCommitted(lastSeq)

	if eventsConfigChanged {
		fsm.eventNotifier.NotifyConfigChanged()
	}

	// Notify mirror Manager that new logs are available.
	fsm.mirrorNotifier.NotifyLogsCommitted(lastSeq)

	// Notify index builder that new logs are available.
	fsm.indexNotifier.NotifyLogsCommitted(lastSeq)

	if mirrorConfigChanged {
		fsm.mirrorNotifier.NotifyConfigChanged()
	}

	return ret, nil
}

// commitAndRequestCheckpoint commits the current batch, stores remaining entries,
// and returns with CheckpointRequired = true. Used by ClosePeriod (seal checkpoint).
func (fsm *Machine) commitAndRequestCheckpoint(
	batch *dal.Batch,
	ret *ApplyEntriesResult,
	remaining []raftpb.Entry,
	needsArchiveDispatch bool,
	checkpointPeriodID uint64,
	onCheckpointDone func(checkpointPath string),
) (*ApplyEntriesResult, error) {
	err := SetAppliedIndex(batch, fsm.lastAppliedIndex)
	if err != nil {
		return nil, fmt.Errorf("setting applied index: %w", err)
	}

	err = SetLastAppliedTimestamp(batch, fsm.lastAppliedTimestamp)
	if err != nil {
		return nil, fmt.Errorf("setting last applied timestamp: %w", err)
	}

	err = batch.Commit()
	if err != nil {
		return nil, fmt.Errorf("committing batch for checkpoint: %w", err)
	}

	fsm.lastPersistedIndex.Store(fsm.lastAppliedIndex)
	fsm.appliedCond.Broadcast()

	if needsArchiveDispatch {
		fsm.dispatchArchiveRequests()
	}

	ret.CheckpointRequired = true
	ret.CheckpointPeriodID = checkpointPeriodID

	if len(remaining) > 0 {
		ret.RemainingEntries = make([]raftpb.Entry, len(remaining))
		copy(ret.RemainingEntries, remaining)
	}

	ret.OnCheckpointDone = onCheckpointDone

	return ret, nil
}

// Preload applies preloaded data to the Machine's volatile state.
func (fsm *Machine) Preload(preloadSet *raftcmdpb.PreloadSet) error {
	if preloadSet == nil || (len(preloadSet.GetPreloads()) == 0 && len(preloadSet.GetTouches()) == 0) {
		return nil
	}

	// The preloads should target gen0 or gen1. The admission uses the
	// IndexTracker to predict the next Raft index and compute the boundary,
	// but the prediction can be off by ±1 due to the race between the tracker
	// and rawNode.Propose() (non-proposal entries like leader no-ops can
	// intercalate). When the off-by-one crosses a generation boundary, the
	// preload targets a boundary that doesn't match either gen. This is safe
	// because preloaded values only seed keys not already in cache, and a
	// forward-mismatch (preload ahead) means the data is at least as fresh.
	switch preloadSet.GetLastPersistedIndex() {
	case fsm.Registry.Cache.BaseIndex.Gen0:
		fsm.logger.Debug("Selecting cache generation 0")
	case fsm.Registry.Cache.BaseIndex.Gen1:
		fsm.logger.Debug("Selecting cache generation 1")
	default:
		return errors.New("preloading preloaded index is invalid")
	}

	// Helper function to put a preloaded volume pair into a cache generation.
	// Since volumes are always preloaded with absolute Known values,
	// if already in cache we keep the existing value (already up to date).
	putInCacheVolumePair := func(
		kv kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]],
		attrID *raftcmdpb.AttributeID,
		pair *raftcmdpb.VolumePair,
	) *raftcmdpb.VolumePair {
		id := attributes.U128FromBytes(attrID.GetId())

		value, ok := kv.Get(id)
		if ok {
			// Assertion: the preloaded value should never be greater than the
			// cached value. If it is, something went wrong with the preload
			// (the admission layer loaded a value that's ahead of the cache).
			// The reverse (preload < cache) is expected: the cache was updated
			// by a later proposal after this preload was computed at admission.
			cacheInput := value.Data.GetInput().ToBigInt()
			cacheOutput := value.Data.GetOutput().ToBigInt()
			preloadInput := pair.GetInput().ToBigInt()
			preloadOutput := pair.GetOutput().ToBigInt()

			if preloadInput.Cmp(cacheInput) > 0 || preloadOutput.Cmp(cacheOutput) > 0 {
				// Todo: return a true error
				fsm.logger.WithFields(map[string]any{
					"id":             id.Hex(),
					"cache_input":    cacheInput.String(),
					"cache_output":   cacheOutput.String(),
					"preload_input":  preloadInput.String(),
					"preload_output": preloadOutput.String(),
				}).Errorf("PRELOAD AHEAD OF CACHE: preloaded volume is greater than cached value — this should not happen")
			}

			return value.Data
		}

		kv.Put(id, attributes.Entry[*raftcmdpb.VolumePair]{
			Tag:       attrID.GetTag(),
			Data:      pair,
			BaseIndex: attrID.GetBaseIndex(),
		})

		return pair
	}

	// Helper function to put a preloaded idempotency value into a cache generation
	putInCacheIdempotencyValue := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.IdempotencyKeyValue]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.IdempotencyKeyValue,
	) *commonpb.IdempotencyKeyValue {
		id := attributes.U128FromBytes(attrID.GetId())

		// fsm.logger.WithFields(map[string]any{
		//	"id":           id.Hex(),
		//	"log_sequence": value.GetLogSequence(),
		//	"hash":         value.GetHash(),
		// }).Debugf("Preload idempotency value")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.IdempotencyKeyValue]{
			Tag:       attrID.GetTag(),
			Data:      value,
			BaseIndex: attrID.GetBaseIndex(),
		})

		return value
	}

	// Helper function to put a preloaded transaction reference value into a cache generation
	putInCacheReferenceValue := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.TransactionReferenceValue,
	) *commonpb.TransactionReferenceValue {
		id := attributes.U128FromBytes(attrID.GetId())

		// fsm.logger.WithFields(map[string]any{
		//	"id":             id.Hex(),
		//	"transaction_id": value.GetTransactionId(),
		// }).Debugf("Preload transaction reference value")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.TransactionReferenceValue]{
			Tag:       attrID.GetTag(),
			Data:      value,
			BaseIndex: attrID.GetBaseIndex(),
		})

		return value
	}

	// Helper function to put a preloaded ledger info into a cache generation
	putInCacheLedger := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.LedgerInfo,
	) *commonpb.LedgerInfo {
		id := attributes.U128FromBytes(attrID.GetId())

		// fsm.logger.WithFields(map[string]any{
		//	"id":   id.Hex(),
		//	"name": value.GetName(),
		// }).Debugf("Preload ledger")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.LedgerInfo]{
			Tag:       attrID.GetTag(),
			Data:      value,
			BaseIndex: attrID.GetBaseIndex(),
		})

		return value
	}

	// Helper function to put a preloaded boundary into a cache generation
	putInCacheBoundary := func(
		kv kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]],
		attrID *raftcmdpb.AttributeID,
		value *raftcmdpb.LedgerBoundaries,
	) *raftcmdpb.LedgerBoundaries {
		id := attributes.U128FromBytes(attrID.GetId())

		// fsm.logger.WithFields(map[string]any{
		//	"id": id.Hex(),
		// }).Debugf("Preload boundary")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*raftcmdpb.LedgerBoundaries]{
			Tag:       attrID.GetTag(),
			Data:      value,
			BaseIndex: attrID.GetBaseIndex(),
		})

		return value
	}

	// Helper function to put a preloaded sink config into a cache generation
	putInCacheSinkConfig := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.SinkConfig]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.SinkConfig,
	) *commonpb.SinkConfig {
		id := attributes.U128FromBytes(attrID.GetId())

		// fsm.logger.WithFields(map[string]any{
		//	"id":   id.Hex(),
		//	"name": value.GetName(),
		// }).Debugf("Preload sink config")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.SinkConfig]{
			Tag:       attrID.GetTag(),
			Data:      value,
			BaseIndex: attrID.GetBaseIndex(),
		})

		return value
	}

	// Helper function to put a preloaded string value into a cache generation
	putInCacheString := func(
		kv kv.KV[attributes.U128, attributes.Entry[string]],
		attrID *raftcmdpb.AttributeID,
		value string,
	) string {
		id := attributes.U128FromBytes(attrID.GetId())

		// fsm.logger.WithFields(map[string]any{
		//	"id":    id.Hex(),
		//	"value": value,
		// }).Debugf("Preload string")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[string]{
			Tag:       attrID.GetTag(),
			Data:      value,
			BaseIndex: attrID.GetBaseIndex(),
		})

		return value
	}

	putInCacheBool := func(
		kv kv.KV[attributes.U128, attributes.Entry[bool]],
		attrID *raftcmdpb.AttributeID,
		value bool,
	) bool {
		id := attributes.U128FromBytes(attrID.GetId())

		// fsm.logger.WithFields(map[string]any{
		//	"id":    id.Hex(),
		//	"value": value,
		// }).Debugf("Preload bool")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[bool]{
			Tag:       attrID.GetTag(),
			Data:      value,
			BaseIndex: attrID.GetBaseIndex(),
		})

		return value
	}

	// Helper function to put a preloaded account metadata value into a cache generation
	putInCacheMetadataValue := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.MetadataValue]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.MetadataValue,
	) *commonpb.MetadataValue {
		id := attributes.U128FromBytes(attrID.GetId())

		// fsm.logger.WithFields(map[string]any{
		//	"id": id.Hex(),
		// }).Debugf("Preload account metadata")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.MetadataValue]{
			Tag:       attrID.GetTag(),
			Data:      value,
			BaseIndex: attrID.GetBaseIndex(),
		})

		return value
	}

	// Helper function to put a preloaded transaction state into a cache generation
	putInCacheTransactionState := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.TransactionState]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.TransactionState,
	) *commonpb.TransactionState {
		id := attributes.U128FromBytes(attrID.GetId())

		// fsm.logger.WithFields(map[string]any{
		//	"id": id.Hex(),
		// }).Debugf("Preload transaction state")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.TransactionState]{
			Tag:  attrID.GetTag(),
			Data: value,
		})

		return value
	}

	for _, preload := range preloadSet.GetPreloads() {
		switch preloadType := preload.GetType().(type) {
		case *raftcmdpb.Preload_Volume:
			pair := &raftcmdpb.VolumePair{
				Input:  preloadType.Volume.GetInput(),
				Output: preloadType.Volume.GetOutput(),
			}
			// Always check Gen1 first: after rotation, Gen1 holds the previous Gen0's
			// data which may be more recent than the preload (read from committed Pebble,
			// which lags behind uncommitted batch writes). If Gen1 already has the key,
			// putInCacheVolumePair returns the existing (fresher) value; that value is then
			// propagated to Gen0, preventing a stale preload from shadowing it.
			aggregated := putInCacheVolumePair(fsm.Registry.Cache.Volumes.Gen1(), preloadType.Volume.GetId(), pair)
			putInCacheVolumePair(fsm.Registry.Cache.Volumes.Gen0(), preloadType.Volume.GetId(), aggregated)

		case *raftcmdpb.Preload_IdempotencyKey:
			idempotencyValue := &commonpb.IdempotencyKeyValue{
				LogSequence: preloadType.IdempotencyKey.GetLogSequence(),
				Hash:        preloadType.IdempotencyKey.GetHash(),
			}
			value := putInCacheIdempotencyValue(fsm.Registry.Cache.IdempotencyKeys.Gen1(), preloadType.IdempotencyKey.GetId(), idempotencyValue)
			putInCacheIdempotencyValue(fsm.Registry.Cache.IdempotencyKeys.Gen0(), preloadType.IdempotencyKey.GetId(), value)

		case *raftcmdpb.Preload_Ledger:
			value := putInCacheLedger(fsm.Registry.Cache.Ledgers.Gen1(), preloadType.Ledger.GetId(), preloadType.Ledger.GetInfo())
			putInCacheLedger(fsm.Registry.Cache.Ledgers.Gen0(), preloadType.Ledger.GetId(), value)

		case *raftcmdpb.Preload_Boundary:
			value := putInCacheBoundary(fsm.Registry.Cache.Boundaries.Gen1(), preloadType.Boundary.GetId(), preloadType.Boundary.GetBoundaries())
			putInCacheBoundary(fsm.Registry.Cache.Boundaries.Gen0(), preloadType.Boundary.GetId(), value)

		case *raftcmdpb.Preload_TransactionReference:
			referenceValue := &commonpb.TransactionReferenceValue{
				TransactionId: preloadType.TransactionReference.GetTransactionId(),
			}
			value := putInCacheReferenceValue(fsm.Registry.Cache.References.Gen1(), preloadType.TransactionReference.GetId(), referenceValue)
			putInCacheReferenceValue(fsm.Registry.Cache.References.Gen0(), preloadType.TransactionReference.GetId(), value)

		case *raftcmdpb.Preload_SinkConfig:
			value := putInCacheSinkConfig(fsm.Registry.Cache.SinkConfigs.Gen1(), preloadType.SinkConfig.GetId(), preloadType.SinkConfig.GetConfig())
			putInCacheSinkConfig(fsm.Registry.Cache.SinkConfigs.Gen0(), preloadType.SinkConfig.GetId(), value)

		case *raftcmdpb.Preload_AccountMetadata:
			value := putInCacheMetadataValue(fsm.Registry.Cache.AccountMetadata.Gen1(), preloadType.AccountMetadata.GetId(), preloadType.AccountMetadata.GetValue())
			putInCacheMetadataValue(fsm.Registry.Cache.AccountMetadata.Gen0(), preloadType.AccountMetadata.GetId(), value)

		case *raftcmdpb.Preload_NumscriptVersion:
			value := putInCacheString(fsm.Registry.Cache.NumscriptVersions.Gen1(), preloadType.NumscriptVersion.GetId(), preloadType.NumscriptVersion.GetVersion())
			putInCacheString(fsm.Registry.Cache.NumscriptVersions.Gen0(), preloadType.NumscriptVersion.GetId(), value)

		case *raftcmdpb.Preload_NumscriptEntry:
			value := putInCacheBool(fsm.Registry.Cache.NumscriptEntries.Gen1(), preloadType.NumscriptEntry.GetId(), preloadType.NumscriptEntry.GetExists())
			putInCacheBool(fsm.Registry.Cache.NumscriptEntries.Gen0(), preloadType.NumscriptEntry.GetId(), value)

		case *raftcmdpb.Preload_TransactionState:
			value := putInCacheTransactionState(fsm.Registry.Cache.Transactions.Gen1(), preloadType.TransactionState.GetId(), preloadType.TransactionState.GetState())
			putInCacheTransactionState(fsm.Registry.Cache.Transactions.Gen0(), preloadType.TransactionState.GetId(), value)

		case *raftcmdpb.Preload_NumscriptParsed:
			value := putInCacheString(fsm.Registry.Cache.NumscriptParsed.Gen1(), preloadType.NumscriptParsed.GetId(), preloadType.NumscriptParsed.GetPlain())
			putInCacheString(fsm.Registry.Cache.NumscriptParsed.Gen0(), preloadType.NumscriptParsed.GetId(), value)
		}
	}

	// Apply touches: promote keys from Gen1 to Gen0 without a store read.
	for _, touch := range preloadSet.GetTouches() {
		id := attributes.U128FromBytes(touch.GetId())

		switch touch.GetType() {
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_VOLUMES:
			fsm.Registry.Cache.Volumes.Touch(id)
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_IDEMPOTENCY_KEYS:
			fsm.Registry.Cache.IdempotencyKeys.Touch(id)
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_REFERENCES:
			fsm.Registry.Cache.References.Touch(id)
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_LEDGERS:
			fsm.Registry.Cache.Ledgers.Touch(id)
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_BOUNDARIES:
			fsm.Registry.Cache.Boundaries.Touch(id)
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_SINK_CONFIGS:
			fsm.Registry.Cache.SinkConfigs.Touch(id)
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_ACCOUNT_METADATA:
			fsm.Registry.Cache.AccountMetadata.Touch(id)
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_NUMSCRIPT_VERSIONS:
			fsm.Registry.Cache.NumscriptVersions.Touch(id)
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_NUMSCRIPT_ENTRIES:
			fsm.Registry.Cache.NumscriptEntries.Touch(id)
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_NUMSCRIPT_PARSED:
			fsm.Registry.Cache.NumscriptParsed.Touch(id)
		case raftcmdpb.CacheTouchType_CACHE_TOUCH_TRANSACTIONS:
			fsm.Registry.Cache.Transactions.Touch(id)
		}
	}

	return nil
}

// hlcTimestamp advances the Hybrid Logical Clock and returns the effective timestamp.
// It guarantees monotonicity: each returned timestamp is strictly greater than the previous one.
// If the proposal date is ahead of the last applied timestamp, it is used directly.
// Otherwise, the last applied timestamp is incremented by 1 microsecond.
func (fsm *Machine) hlcTimestamp(proposalDate *commonpb.Timestamp) *commonpb.Timestamp {
	if proposalDate.GetData() > fsm.lastAppliedTimestamp {
		fsm.lastAppliedTimestamp = proposalDate.GetData()
	} else {
		fsm.lastAppliedTimestamp++
	}

	return &commonpb.Timestamp{Data: fsm.lastAppliedTimestamp}
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

// applyProposal processes all orders in a proposal atomically.
// Uses RequestProcessor which handles rollback internally via Buffered.
func (fsm *Machine) applyProposal(ctx context.Context, raftIndex uint64, batch *dal.Batch, proposal *raftcmdpb.Proposal) (*ApplyResult, error) {
	// Handle per-sink cursor and status updates (Raft-replicated, no orders needed)
	for _, update := range proposal.GetEventsSinkUpdates() {
		if update.GetCursor() > 0 {
			err := SetSinkCursor(batch, update.GetSinkName(), update.GetCursor())
			if err != nil {
				return nil, fmt.Errorf("setting sink cursor: %w", err)
			}
		}

		if update.GetClearError() {
			err := ClearSinkStatus(batch, update.GetSinkName())
			if err != nil {
				return nil, fmt.Errorf("clearing sink status: %w", err)
			}
		} else if update.GetError() != nil {
			err := SetSinkStatus(batch, &commonpb.SinkStatus{
				SinkName: update.GetSinkName(),
				Cursor:   update.GetCursor(),
				Error:    update.GetError(),
			})
			if err != nil {
				return nil, fmt.Errorf("setting sink status: %w", err)
			}
		}
	}

	// Handle per-ledger mirror cursor and status updates (Raft-replicated)
	for _, update := range proposal.GetMirrorSyncUpdates() {
		if update.GetCursor() > 0 {
			err := SetMirrorCursor(batch, update.GetLedgerName(), update.GetCursor())
			if err != nil {
				return nil, fmt.Errorf("setting mirror cursor: %w", err)
			}
		}

		if update.GetSourceLogCount() > 0 {
			err := SetMirrorSourceHead(batch, update.GetLedgerName(), update.GetSourceLogCount())
			if err != nil {
				return nil, fmt.Errorf("setting mirror source head: %w", err)
			}
		}

		if update.GetClearError() {
			err := ClearMirrorStatus(batch, update.GetLedgerName())
			if err != nil {
				return nil, fmt.Errorf("clearing mirror status: %w", err)
			}
		} else if update.GetError() != nil {
			err := SetMirrorStatus(batch, update.GetLedgerName(), update.GetError())
			if err != nil {
				return nil, fmt.Errorf("setting mirror status: %w", err)
			}
		}
	}

	// If this proposal only carries sink updates, skip order processing
	if len(proposal.GetOrders()) == 0 {
		return &ApplyResult{ProposalID: proposal.GetId()}, nil
	}

	// FSM-level maintenance mode check: reject proposals containing non-maintenance
	// orders that were admitted before maintenance mode was enabled but batched into
	// a Raft entry applied after the maintenance mode flag was set.
	if fsm.sharedState.MaintenanceMode() && !authorizedInMaintenanceMode(proposal.GetOrders()) {
		return &ApplyResult{
			ProposalID: proposal.GetId(),
			Error:      &domain.BusinessError{Err: domain.ErrMaintenanceMode},
		}, nil
	}

	if err := fsm.Preload(proposal.GetPreload()); err != nil {
		return nil, err
	}

	// Compute the effective date using the HLC to guarantee monotonicity
	effectiveDate := fsm.hlcTimestamp(proposal.GetDate())

	// Auto-bootstrap first period deterministically at first proposal
	// TODO: Move at initialization maybe?
	if fsm.Periods.CurrentOpenPeriod() == nil {
		p := &commonpb.Period{
			Id:            1,
			Start:         effectiveDate,
			Status:        commonpb.PeriodStatus_PERIOD_OPEN,
			StartSequence: 1,
		}
		fsm.Periods.SetCurrentOpenPeriod(p)
		fsm.Periods.SetNextPeriodID(2)

		// Persist the bootstrapped period so it survives restarts
		err := StorePeriod(batch, p)
		if err != nil {
			return nil, fmt.Errorf("storing bootstrapped period: %w", err)
		}

		err = StoreNextPeriodID(batch, fsm.Periods.NextPeriodID())
		if err != nil {
			return nil, fmt.Errorf("storing next period ID: %w", err)
		}
	}

	// Resolve numscript text from dual-gen cache for hash-only scripts
	// TODO: let the processor do that, needs to add the required accessor
	for _, order := range proposal.GetOrders() {
		if apply, ok := order.GetType().(*raftcmdpb.Order_Apply); ok {
			if ct := apply.Apply.GetCreateTransaction(); ct != nil {
				if script := ct.GetScript(); script != nil &&
					len(script.GetContentHash()) > 0 && script.GetPlain() == "" {
					id, _ := attributes.MakeKey(attributes.DefaultSeeds, script.GetContentHash())
					entry, ok := fsm.Registry.Cache.NumscriptParsed.Get(id)
					if !ok {
						return nil, fmt.Errorf("numscript text not in cache for hash %x", script.GetContentHash())
					}

					script.Plain = entry.Data
				}
			}
		}
	}

	// Create buffer for this proposal
	buffer := NewBuffer(effectiveDate, fsm)

	// Process the proposal
	logs, err := fsm.processor.ProcessOrders(proposal.GetOrders(), buffer)
	if err != nil {
		// FAILURE: write audit entry and return business error
		if fsm.sharedState.AuditEnabled() {
			auditEntry := &auditpb.AuditEntry{
				Sequence:   fsm.nextAuditSequenceID,
				Timestamp:  effectiveDate,
				ProposalId: proposal.GetId(),
				Orders:     proposal.GetOrders(),
				Outcome: &auditpb.AuditEntry_Failure{
					Failure: buildAuditFailure(err),
				},
			}
			fsm.nextAuditSequenceID++

			appendErr := AppendAuditEntries(batch, auditEntry)
			if appendErr != nil {
				return nil, fmt.Errorf("appending audit entry for failure: %w", appendErr)
			}
		}

		return &ApplyResult{
			ProposalID: proposal.GetId(),
			Error:      &domain.BusinessError{Err: err},
		}, nil
	}

	// Extract created logs for the write buffer (reference sequences are idempotent
	// responses that don't produce new logs)
	var createdLogs []*commonpb.Log

	for _, logOrRef := range logs {
		if created := logOrRef.GetCreatedLog(); created != nil {
			createdLogs = append(createdLogs, created)
		}
	}

	// Add only created logs to buffer and merge
	// TODO: buffer does not need to have PendingLogs property
	buffer.PendingLogs = append(buffer.PendingLogs, createdLogs...)
	configChanged := buffer.HasPendingSinkChanges()
	mirrorConfigChanged := hasMirrorConfigChange(proposal)
	hasArchiveRequests := len(buffer.pendingArchives) > 0
	hasPurges := buffer.HasPurges()
	// Capture audit state before Merge, which may toggle sharedState via SetAuditConfig.
	// We record the audit entry if audit was enabled before OR after, so that
	// SetAuditConfig(true) and SetAuditConfig(false) both record themselves.
	auditBefore := fsm.sharedState.AuditEnabled()

	if err := buffer.Merge(raftIndex, batch); err != nil {
		return nil, err
	}

	// Cross-check: volume deltas must match postings in the committed logs.
	if fsm.volumeAssertions {
		if err := verifyVolumeDeltasMatchPostings(buffer.VolumeUpdates(), createdLogs); err != nil {
			return nil, fmt.Errorf("volume delta/posting cross-check failed at raft index %d: %w", raftIndex, err)
		}

		// Global check: aggregated volumes must be balanced (input == output per asset).
		ledgerNames := collectLedgerNames(proposal.GetOrders())
		if len(ledgerNames) > 0 {
			fsm.logger.Debugf("Verifying aggregated volume balance for %d ledgers at raft index %d", len(ledgerNames), raftIndex)
			if err := verifyAggregatedVolumesBalanced(
				fsm.dataStore, fsm.Registry.Attrs.Volume, ledgerNames, raftIndex,
			); err != nil {
				fsm.logger.Errorf("AGGREGATED VOLUME BALANCE CHECK FAILED: %v", err)

				return nil, fmt.Errorf("aggregated volume balance check failed: %w", err)
			}
		}
	}

	auditAfter := fsm.sharedState.AuditEnabled()

	// SUCCESS: write audit entry
	if auditBefore || auditAfter {
		auditEntry := &auditpb.AuditEntry{
			Sequence:   fsm.nextAuditSequenceID,
			Timestamp:  effectiveDate,
			ProposalId: proposal.GetId(),
			Orders:     proposal.GetOrders(),
			Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{
					LogSequences: extractLogSequencesFromLogsOrRefs(logs),
				},
			},
		}
		fsm.nextAuditSequenceID++

		err := AppendAuditEntries(batch, auditEntry)
		if err != nil {
			return nil, fmt.Errorf("appending audit entry for success: %w", err)
		}
	}

	fsm.logsAppendedCounter.Add(ctx, int64(len(createdLogs)))

	return &ApplyResult{
		ProposalID:              proposal.GetId(),
		Logs:                    logs,
		ConfigChanged:           configChanged,
		MirrorConfigChanged:     mirrorConfigChanged,
		HasArchiveRequests:      hasArchiveRequests,
		HasPurges:               hasPurges,
		MetadataConvertRequests: buffer.MetadataConvertRequests(),
		volumeUpdates:           buffer.VolumeUpdates(),
		createdLogs:             createdLogs,
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

// CreateSnapshot creates a snapshot of the Machine state.
// Cache data is persisted to Pebble under the 0xFF prefix before creating the
// checkpoint so that the checkpoint includes it. The MemorySnapshot itself is
// lightweight (no cache data).
func (fsm *Machine) CreateSnapshot(_ context.Context) ([]byte, error) {
	totalStart := time.Now()

	// Persist cache into Pebble so it's included in the checkpoint
	persistStart := time.Now()

	if err := fsm.persistCacheToStore(); err != nil {
		return nil, fmt.Errorf("persisting cache to store: %w", err)
	}

	fsm.logger.WithFields(map[string]any{
		"duration": time.Since(persistStart).String(),
	}).Infof("Persisted cache to Pebble")

	checkpointID, err := fsm.dataStore.CreateSnapshot()
	if err != nil {
		return nil, fmt.Errorf("creating snapshot: %w", err)
	}

	// Collect CLOSED/ARCHIVED periods (OPEN and CLOSING are stored separately)
	closedPeriods := make([]*commonpb.Period, 0)

	for _, p := range fsm.Periods.AllPeriods() {
		if p.GetStatus() != commonpb.PeriodStatus_PERIOD_OPEN && p.GetStatus() != commonpb.PeriodStatus_PERIOD_CLOSING {
			closedPeriods = append(closedPeriods, p)
		}
	}

	// Serialize reversion bitsets
	reversions := make([]*raftcmdpb.ReversionBitsetEntry, 0, len(fsm.Registry.Reversions))
	for ledger, bs := range fsm.Registry.Reversions {
		reversions = append(reversions, &raftcmdpb.ReversionBitsetEntry{
			Ledger: ledger,
			Words:  bs.MarshalWords(),
		})
	}

	// Serialize pending ledger cleanups
	pendingCleanups := make([]*raftcmdpb.PendingLedgerCleanup, 0, len(fsm.pendingLedgerCleanups))
	for ledger, seq := range fsm.pendingLedgerCleanups {
		pendingCleanups = append(pendingCleanups, &raftcmdpb.PendingLedgerCleanup{
			Ledger:         ledger,
			DeleteSequence: seq,
		})
	}

	serializeStart := time.Now()
	snapshot := &raftcmdpb.MemorySnapshot{
		NextSequenceId:        fsm.nextSequenceID,
		LastLogHash:           fsm.lastLogHash,
		CheckpointId:          checkpointID,
		CurrentGeneration:     fsm.Registry.Cache.CurrentGeneration(),
		LastAppliedTimestamp:  fsm.lastAppliedTimestamp,
		NextAuditSequenceId:   fsm.nextAuditSequenceID,
		OpenPeriod:            fsm.Periods.CurrentOpenPeriod(),
		ClosingPeriods:        fsm.Periods.ClosingPeriods(),
		NextPeriodId:          fsm.Periods.NextPeriodID(),
		ClosedPeriods:         closedPeriods,
		Reversions:            reversions,
		PendingLedgerCleanups: pendingCleanups,
	}

	size := snapshot.SizeVT()
	if cap(fsm.snapshotBuf) < size {
		fsm.snapshotBuf = make([]byte, size)
	} else {
		fsm.snapshotBuf = fsm.snapshotBuf[:size]
	}

	n, err := snapshot.MarshalToVT(fsm.snapshotBuf)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot: %w", err)
	}

	lifecycle.SendEvent("spool replay completed", map[string]any{
		"checkpointId": checkpointID,
		"snapshotSize": n,
	})
	fsm.logger.WithFields(map[string]any{
		"totalDuration":     time.Since(totalStart).String(),
		"serializeDuration": time.Since(serializeStart).String(),
		"snapshotSize":      n,
		"checkpointId":      checkpointID,
	}).Infof("Created MemorySnapshot")

	return fsm.snapshotBuf[:n], nil
}

// persistCacheToStore writes all cache data into Pebble under the 0xFF prefix.
// This is called before creating a checkpoint so the checkpoint includes the cache.
func (fsm *Machine) persistCacheToStore() error {
	batch := fsm.dataStore.NewBatch()
	defer func() { _ = batch.Cancel() }()

	// Clear previous cache snapshot data: delete range [0xFF, 0xFF 0xFF 0x01)
	// This covers all per-gen data and gen metadata but stops before the cache meta key.
	// Then we re-write everything fresh.
	if err := batch.DeleteRangeNoSync(
		[]byte{dal.KeyPrefixCacheSnapshot},
		[]byte{dal.KeyPrefixCacheSnapshot, dal.CacheMetaKey, 0x01},
	); err != nil {
		return fmt.Errorf("clearing cache snapshot range: %w", err)
	}

	// Persist both generations
	for genIndex := range 2 {
		genByte := byte(genIndex)

		if err := fsm.persistCacheGeneration(batch, genByte); err != nil {
			return fmt.Errorf("persisting cache gen%d: %w", genIndex, err)
		}
	}

	// Write cache-level metadata
	if err := batch.SetProto(
		[]byte{dal.KeyPrefixCacheSnapshot, dal.CacheMetaKey},
		&raftcmdpb.CacheSnapshotMeta{
			CurrentGeneration: fsm.Registry.Cache.CurrentGeneration(),
		},
	); err != nil {
		return fmt.Errorf("writing cache snapshot meta: %w", err)
	}

	return batch.Commit()
}

// persistCacheGeneration writes a single cache generation to Pebble.
func (fsm *Machine) persistCacheGeneration(batch *dal.Batch, genByte byte) error {
	c := fsm.Registry.Cache
	genIndex := int(genByte)

	var (
		baseIndex            uint64
		volumeStore          kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]]
		metadataStore        kv.KV[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerStore          kv.KV[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]]
		boundaryStore        kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]]
		referenceStore       kv.KV[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]]
		transactionStore     kv.KV[attributes.U128, attributes.Entry[*commonpb.TransactionState]]
		numscriptParsedStore kv.KV[attributes.U128, attributes.Entry[string]]
		idempotencyStore     kv.KV[attributes.U128, attributes.Entry[*commonpb.IdempotencyKeyValue]]
	)

	if genIndex == 0 {
		baseIndex = c.BaseIndex.Gen0
		volumeStore = c.Volumes.Gen0()
		metadataStore = c.AccountMetadata.Gen0()
		ledgerStore = c.Ledgers.Gen0()
		boundaryStore = c.Boundaries.Gen0()
		referenceStore = c.References.Gen0()
		transactionStore = c.Transactions.Gen0()
		numscriptParsedStore = c.NumscriptParsed.Gen0()
		idempotencyStore = c.IdempotencyKeys.Gen0()
	} else {
		baseIndex = c.BaseIndex.Gen1
		volumeStore = c.Volumes.Gen1()
		metadataStore = c.AccountMetadata.Gen1()
		ledgerStore = c.Ledgers.Gen1()
		boundaryStore = c.Boundaries.Gen1()
		referenceStore = c.References.Gen1()
		transactionStore = c.Transactions.Gen1()
		numscriptParsedStore = c.NumscriptParsed.Gen1()
		idempotencyStore = c.IdempotencyKeys.Gen1()
	}

	// Write generation metadata
	if err := batch.SetProto(
		[]byte{dal.KeyPrefixCacheSnapshot, genByte, dal.CacheGenMeta},
		&raftcmdpb.CacheGenerationMeta{BaseIndex: baseIndex},
	); err != nil {
		return fmt.Errorf("writing gen meta: %w", err)
	}

	// Helper to build a cache key: [0xFF][gen][type][16-byte U128]
	makeKey := func(cacheType byte, u128 attributes.U128) []byte {
		key := make([]byte, 3+16)
		key[0] = dal.KeyPrefixCacheSnapshot
		key[1] = genByte
		key[2] = cacheType
		copy(key[3:], u128[:])

		return key
	}

	// Volumes
	for u128, entry := range volumeStore.Iter() {
		e := &raftcmdpb.VolumeAttributeSnapshotEntry{
			Id: &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
		}
		if entry.Data != nil {
			e.Input = entry.Data.GetInput()
			e.Output = entry.Data.GetOutput()
		}

		if err := batch.SetProto(makeKey(dal.CacheTypeVolumes, u128), e); err != nil {
			return err
		}
	}

	// Metadata
	for u128, entry := range metadataStore.Iter() {
		e := &raftcmdpb.MetadataAttributeEntry{
			Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Value: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.CacheTypeMetadata, u128), e); err != nil {
			return err
		}
	}

	// Ledgers
	for u128, entry := range ledgerStore.Iter() {
		e := &raftcmdpb.LedgerAttributeEntry{
			Id:   &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Info: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.CacheTypeLedgers, u128), e); err != nil {
			return err
		}
	}

	// Boundaries
	for u128, entry := range boundaryStore.Iter() {
		e := &raftcmdpb.BoundaryAttributeEntry{
			Id:         &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Boundaries: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.CacheTypeBoundaries, u128), e); err != nil {
			return err
		}
	}

	// References
	for u128, entry := range referenceStore.Iter() {
		e := &raftcmdpb.TransactionReferenceAttributeEntry{
			Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Value: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.CacheTypeReferences, u128), e); err != nil {
			return err
		}
	}

	// Transactions
	for u128, entry := range transactionStore.Iter() {
		e := &raftcmdpb.TransactionStateAttributeEntry{
			Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			State: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.CacheTypeTransactions, u128), e); err != nil {
			return err
		}
	}

	// NumscriptParsed
	for u128, entry := range numscriptParsedStore.Iter() {
		e := &raftcmdpb.NumscriptParsedAttributeEntry{
			Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Plain: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.CacheTypeNumscript, u128), e); err != nil {
			return err
		}
	}

	// IdempotencyKeys
	for u128, entry := range idempotencyStore.Iter() {
		e := &raftcmdpb.IdempotencyKeyAttributeEntry{
			Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Value: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.CacheTypeIdempotency, u128), e); err != nil {
			return err
		}
	}

	return nil
}

// RestoreCacheFromStore rebuilds the in-memory cache from Pebble (0xFF prefix).
// Called on restart (when store is up to date) and after follower sync.
func (fsm *Machine) RestoreCacheFromStore() error {
	restoreStart := time.Now()

	// Read cache-level metadata
	metaVal, closer, err := fsm.dataStore.Get([]byte{dal.KeyPrefixCacheSnapshot, dal.CacheMetaKey})
	if err != nil {
		// No cache data in Pebble — leave cache empty (fresh node)
		fsm.logger.Infof("No cache snapshot found in Pebble, starting with empty cache")

		return nil
	}

	meta := &raftcmdpb.CacheSnapshotMeta{}
	if err := meta.UnmarshalVT(metaVal); err != nil {
		_ = closer.Close()

		return fmt.Errorf("unmarshaling cache snapshot meta: %w", err)
	}

	_ = closer.Close()

	fsm.Registry.Cache.Reset()

	// Restore both generations
	for genIndex := range 2 {
		genByte := byte(genIndex)

		if err := fsm.restoreCacheGeneration(genByte); err != nil {
			return fmt.Errorf("restoring cache gen%d: %w", genIndex, err)
		}
	}

	fsm.Registry.Cache.SetCurrentGeneration(meta.GetCurrentGeneration())

	fsm.logger.WithFields(map[string]any{
		"duration":          time.Since(restoreStart).String(),
		"currentGeneration": meta.GetCurrentGeneration(),
	}).Infof("Restored cache from Pebble")

	return nil
}

// restoreCacheGeneration restores a single cache generation from Pebble.
func (fsm *Machine) restoreCacheGeneration(genByte byte) error {
	genIndex := int(genByte)

	// Read generation metadata
	genMetaKey := []byte{dal.KeyPrefixCacheSnapshot, genByte, dal.CacheGenMeta}

	genMetaVal, closer, err := fsm.dataStore.Get(genMetaKey)
	if err != nil {
		return nil // No data for this generation
	}

	genMeta := &raftcmdpb.CacheGenerationMeta{}
	if err := genMeta.UnmarshalVT(genMetaVal); err != nil {
		_ = closer.Close()

		return fmt.Errorf("unmarshaling gen meta: %w", err)
	}

	_ = closer.Close()

	if genIndex == 0 {
		fsm.Registry.Cache.BaseIndex.Gen0 = genMeta.GetBaseIndex()
	} else {
		fsm.Registry.Cache.BaseIndex.Gen1 = genMeta.GetBaseIndex()
	}

	var (
		volumeStore          kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]]
		metadataStore        kv.KV[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerStore          kv.KV[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]]
		boundaryStore        kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]]
		referenceStore       kv.KV[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]]
		transactionStore     kv.KV[attributes.U128, attributes.Entry[*commonpb.TransactionState]]
		numscriptParsedStore kv.KV[attributes.U128, attributes.Entry[string]]
		idempotencyStore     kv.KV[attributes.U128, attributes.Entry[*commonpb.IdempotencyKeyValue]]
	)

	if genIndex == 0 {
		volumeStore = fsm.Registry.Cache.Volumes.Gen0()
		metadataStore = fsm.Registry.Cache.AccountMetadata.Gen0()
		ledgerStore = fsm.Registry.Cache.Ledgers.Gen0()
		boundaryStore = fsm.Registry.Cache.Boundaries.Gen0()
		referenceStore = fsm.Registry.Cache.References.Gen0()
		transactionStore = fsm.Registry.Cache.Transactions.Gen0()
		numscriptParsedStore = fsm.Registry.Cache.NumscriptParsed.Gen0()
		idempotencyStore = fsm.Registry.Cache.IdempotencyKeys.Gen0()
	} else {
		volumeStore = fsm.Registry.Cache.Volumes.Gen1()
		metadataStore = fsm.Registry.Cache.AccountMetadata.Gen1()
		ledgerStore = fsm.Registry.Cache.Ledgers.Gen1()
		boundaryStore = fsm.Registry.Cache.Boundaries.Gen1()
		referenceStore = fsm.Registry.Cache.References.Gen1()
		transactionStore = fsm.Registry.Cache.Transactions.Gen1()
		numscriptParsedStore = fsm.Registry.Cache.NumscriptParsed.Gen1()
		idempotencyStore = fsm.Registry.Cache.IdempotencyKeys.Gen1()
	}

	// Restore each cache type by iterating over its prefix
	type restoreSpec struct {
		cacheType byte
		restore   func(u128 attributes.U128, value []byte) error
	}

	specs := []restoreSpec{
		{dal.CacheTypeVolumes, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.VolumeAttributeSnapshotEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			pair := &raftcmdpb.VolumePair{Input: e.GetInput(), Output: e.GetOutput()}
			volumeStore.Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{
				Tag: e.GetId().GetTag(), Data: pair,
			})

			return nil
		}},
		{dal.CacheTypeMetadata, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.MetadataAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			metadataStore.Put(u128, attributes.Entry[*commonpb.MetadataValue]{
				Tag: e.GetId().GetTag(), Data: e.GetValue(),
			})

			return nil
		}},
		{dal.CacheTypeLedgers, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.LedgerAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			ledgerStore.Put(u128, attributes.Entry[*commonpb.LedgerInfo]{
				Tag: e.GetId().GetTag(), Data: e.GetInfo(),
			})

			return nil
		}},
		{dal.CacheTypeBoundaries, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.BoundaryAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			boundaryStore.Put(u128, attributes.Entry[*raftcmdpb.LedgerBoundaries]{
				Tag: e.GetId().GetTag(), Data: e.GetBoundaries(),
			})

			return nil
		}},
		{dal.CacheTypeReferences, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.TransactionReferenceAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			referenceStore.Put(u128, attributes.Entry[*commonpb.TransactionReferenceValue]{
				Tag: e.GetId().GetTag(), Data: e.GetValue(),
			})

			return nil
		}},
		{dal.CacheTypeTransactions, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.TransactionStateAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			transactionStore.Put(u128, attributes.Entry[*commonpb.TransactionState]{
				Tag: e.GetId().GetTag(), Data: e.GetState(),
			})

			return nil
		}},
		{dal.CacheTypeNumscript, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.NumscriptParsedAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			numscriptParsedStore.Put(u128, attributes.Entry[string]{
				Tag: e.GetId().GetTag(), Data: e.GetPlain(),
			})

			return nil
		}},
		{dal.CacheTypeIdempotency, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.IdempotencyKeyAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			idempotencyStore.Put(u128, attributes.Entry[*commonpb.IdempotencyKeyValue]{
				Tag: e.GetId().GetTag(), Data: e.GetValue(),
			})

			return nil
		}},
	}

	for _, spec := range specs {
		lower := []byte{dal.KeyPrefixCacheSnapshot, genByte, spec.cacheType}
		upper := []byte{dal.KeyPrefixCacheSnapshot, genByte, spec.cacheType + 1}

		iter, err := fsm.dataStore.NewIter(&pebble.IterOptions{
			LowerBound: lower,
			UpperBound: upper,
		})
		if err != nil {
			return fmt.Errorf("creating cache iter for type 0x%02x: %w", spec.cacheType, err)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			// Key format: [0xFF][gen][type][16-byte U128]
			if len(key) < 3+16 {
				continue
			}

			u128 := attributes.U128FromBytes(key[3:19])

			value, err := iter.ValueAndErr()
			if err != nil {
				_ = iter.Close()

				return fmt.Errorf("reading cache value: %w", err)
			}

			if err := spec.restore(u128, value); err != nil {
				_ = iter.Close()

				return fmt.Errorf("restoring cache entry: %w", err)
			}
		}

		if err := iter.Error(); err != nil {
			_ = iter.Close()

			return fmt.Errorf("cache iter error: %w", err)
		}

		_ = iter.Close()
	}

	return nil
}

func (fsm *Machine) InstallSnapshot(ctx context.Context, snapshot raftpb.Snapshot) error {
	totalStart := time.Now()
	fsm.snapshotIndex = snapshot.Metadata.Index

	deserializeStart := time.Now()

	memSnapshot := &raftcmdpb.MemorySnapshot{}

	err := memSnapshot.UnmarshalVT(snapshot.Data)
	if err != nil {
		return err
	}

	fsm.logger.WithFields(map[string]any{
		"duration":     time.Since(deserializeStart).String(),
		"dataSize":     len(snapshot.Data),
		"checkpointId": memSnapshot.GetCheckpointId(),
	}).Infof("Deserialized MemorySnapshot")

	// Restore memory state from snapshot
	fsm.nextSequenceID = memSnapshot.GetNextSequenceId()
	fsm.nextAuditSequenceID = memSnapshot.GetNextAuditSequenceId()
	fsm.lastLogHash = memSnapshot.GetLastLogHash()
	fsm.lastCheckpointID = memSnapshot.GetCheckpointId()
	fsm.lastAppliedTimestamp = memSnapshot.GetLastAppliedTimestamp()
	// Rebuild allPeriods from all three sources in the snapshot
	allPeriods := make(map[uint64]*commonpb.Period)
	if memSnapshot.GetOpenPeriod() != nil {
		allPeriods[memSnapshot.GetOpenPeriod().GetId()] = memSnapshot.GetOpenPeriod()
	}

	for _, cp := range memSnapshot.GetClosingPeriods() {
		allPeriods[cp.GetId()] = cp
	}

	for _, p := range memSnapshot.GetClosedPeriods() {
		allPeriods[p.GetId()] = p
	}

	fsm.Periods.Reset(allPeriods, memSnapshot.GetOpenPeriod(), memSnapshot.GetClosingPeriods(), memSnapshot.GetNextPeriodId())

	// Reset the cache — it will be restored from Pebble later:
	// - On restart: after InstallSnapshot, via RestoreCacheFromStore
	// - On follower sync: after restoreCheckpoint, via RestoreCacheFromStore
	fsm.Registry.Cache.Reset()
	fsm.Registry.Cache.SetCurrentGeneration(memSnapshot.GetCurrentGeneration())

	// Restore reversion bitsets from snapshot
	fsm.Registry.ResetReversions()

	for _, entry := range memSnapshot.GetReversions() {
		fsm.Registry.Reversions[entry.GetLedger()] = domain.ReversionBitsetFromWords(entry.GetWords())
	}

	// Restore pending ledger cleanups from snapshot
	fsm.pendingLedgerCleanups = make(map[string]uint64, len(memSnapshot.GetPendingLedgerCleanups()))

	for _, entry := range memSnapshot.GetPendingLedgerCleanups() {
		fsm.pendingLedgerCleanups[entry.GetLedger()] = entry.GetDeleteSequence()
	}

	fsm.logger.WithFields(map[string]any{
		"totalDuration": time.Since(totalStart).String(),
		"snapshotIndex": snapshot.Metadata.Index,
	}).Infof("InstallSnapshot complete")

	return nil
}

// reloadStateFromStore reloads signing keys and shared runtime flags from Pebble.
// Called after SynchronizeWithLeader restores the Pebble checkpoint from the leader.
func (fsm *Machine) reloadStateFromStore() error {
	if fsm.keyStore != nil {
		fsm.keyStore.Reset()

		signingKeys, err := query.ReadSigningKeys(fsm.dataStore)
		if err != nil {
			return fmt.Errorf("loading signing keys: %w", err)
		}

		for keyID, entry := range signingKeys {
			fsm.keyStore.AddPublicKey(keyID, entry.PublicKey, entry.ParentKeyID)
		}
	}

	fsm.sharedState.Reset()

	requireSig, err := query.ReadSigningConfig(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("loading signing config: %w", err)
	}

	fsm.sharedState.SetRequireSignatures(requireSig)

	maintenanceMode, err := query.ReadMaintenanceMode(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("loading maintenance mode: %w", err)
	}

	fsm.sharedState.SetMaintenanceMode(maintenanceMode)

	auditEnabled, err := query.ReadAuditConfig(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("loading audit config: %w", err)
	}

	fsm.sharedState.SetAuditEnabled(auditEnabled)

	return nil
}

func (fsm *Machine) SynchronizeWithLeader(ctx context.Context, snapshotFetcher SnapshotFetcher, progress *SyncProgress) (uint64, error) {
	// Restore checkpoint from the leader if needed
	// The checkpoint ID is stored in the Machine state from the snapshot
	if fsm.lastCheckpointID > 0 {
		currentCheckpointID := fsm.dataStore.GetCurrentCheckpointID()
		if currentCheckpointID < fsm.lastCheckpointID {
			err := fsm.restoreCheckpoint(ctx, snapshotFetcher, progress)
			if err != nil {
				return 0, fmt.Errorf("restoring checkpoint from leader: %w", err)
			}
		}
	}

	// Restore cache from Pebble (the checkpoint contains the leader's cache data)
	if err := fsm.RestoreCacheFromStore(); err != nil {
		return 0, fmt.Errorf("restoring cache after sync: %w", err)
	}

	// Reload signing keys from Pebble (the checkpoint contains the leader's keys)
	err := fsm.reloadStateFromStore()
	if err != nil {
		return 0, fmt.Errorf("reloading state after sync: %w", err)
	}

	// Sink configs are not reloaded at sync time — they live in the cache
	// and will be preloaded on demand by the admission layer.

	fsm.lastAppliedIndex = fsm.snapshotIndex

	return fsm.snapshotIndex, nil
}

// restoreCheckpoint restores a checkpoint from the leader.
func (fsm *Machine) restoreCheckpoint(ctx context.Context, snapshotFetcher SnapshotFetcher, progress *SyncProgress) error {
	fsm.logger.WithFields(map[string]any{
		"currentCheckpointId": fsm.dataStore.GetCurrentCheckpointID(),
		"targetCheckpointId":  fsm.lastCheckpointID,
	}).Infof("Fetching checkpoint from leader")

	if progress != nil {
		progress.SetCheckpointID(fsm.lastCheckpointID)
	}

	// Prepare the checkpoint directory
	checkpointDir, err := fsm.dataStore.PrepareCheckpointRestore(fsm.lastCheckpointID)
	if err != nil {
		return fmt.Errorf("preparing checkpoint restore: %w", err)
	}

	// Fetch the checkpoint from the leader
	size, hash, err := snapshotFetcher.FetchSnapshot(ctx, fsm.lastCheckpointID, checkpointDir, progress)
	if err != nil {
		return fmt.Errorf("fetching snapshot from leader: %w", err)
	}

	fsm.logger.WithFields(map[string]any{
		"checkpointId": fsm.lastCheckpointID,
		"size":         size,
		"hash":         hash,
	}).Infof("Checkpoint fetched from leader")

	// Restore the checkpoint
	if err := fsm.dataStore.RestoreCheckpoint(fsm.lastCheckpointID); err != nil {
		return fmt.Errorf("restoring checkpoint: %w", err)
	}

	fsm.logger.WithFields(map[string]any{
		"checkpointId": fsm.lastCheckpointID,
	}).Infof("Checkpoint restored successfully")

	fsm.lastAppliedIndex = fsm.snapshotIndex

	return nil
}

func (fsm *Machine) IsStoreUpToDate(ctx context.Context) (bool, error) {
	return fsm.lastAppliedIndex >= fsm.snapshotIndex, nil
}

// SealRequestCh returns the channel used to communicate seal requests between
// the Machine (writer, on ClosePeriod) and the Sealer (reader).
// Both sides need send access (Machine for normal flow, Sealer/Node for recovery).
func (fsm *Machine) SealRequestCh() chan SealRequest {
	return fsm.sealRequestCh
}

// ArchiveRequestCh returns the channel used to dispatch archive requests to the Archiver.
func (fsm *Machine) ArchiveRequestCh() chan ArchiveRequest {
	return fsm.archiveRequestCh
}

// MetadataConvertRequestCh returns the channel used to dispatch metadata
// conversion requests to the MetadataConverter.
func (fsm *Machine) MetadataConvertRequestCh() chan MetadataConvertRequest {
	return fsm.metadataConvertRequestCh
}

// ColdCompactionCh returns the channel that signals the SmartCompactor when
// cold zone compaction is needed (after period purges).
func (fsm *Machine) ColdCompactionCh() <-chan struct{} {
	return fsm.coldCompactionCh
}

// dispatchArchiveRequests sends archive requests for all ARCHIVING periods
// to the archiver channel. Called internally after batch commit on all nodes.
// The Archiver itself skips execution when the node is not the leader.
func (fsm *Machine) dispatchArchiveRequests() {
	for _, p := range fsm.Periods.AllPeriods() {
		if p.GetStatus() == commonpb.PeriodStatus_PERIOD_ARCHIVING {
			select {
			case fsm.archiveRequestCh <- ArchiveRequest{
				PeriodID:      p.GetId(),
				StartSequence: p.GetStartSequence(),
				CloseSequence: p.GetCloseSequence(),
			}:
			default:
			}
		}
	}
}

// dispatchMetadataConversionRequests iterates all ledgers and dispatches
// conversion requests for metadata fields still in CONVERTING status.
// Called on leadership acquisition to recover incomplete conversions.
func (fsm *Machine) dispatchMetadataConversionRequests() {
	handle := fsm.dataStore.NewReadHandle()

	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadLedgers(context.Background(), handle)
	if err != nil {
		fsm.logger.Errorf("Failed to read ledgers for metadata conversion recovery: %v", err)

		return
	}

	defer func() { _ = cursor.Close() }()

	for {
		info, err := cursor.Next()
		if err != nil {
			break
		}

		if info.GetMetadataSchema() == nil || info.GetDeletedAt() != nil {
			continue
		}

		fsm.dispatchConvertingFields(info, commonpb.TargetType_TARGET_TYPE_ACCOUNT, info.GetMetadataSchema().GetAccountFields())
		fsm.dispatchConvertingFields(info, commonpb.TargetType_TARGET_TYPE_TRANSACTION, info.GetMetadataSchema().GetTransactionFields())
	}
}

func (fsm *Machine) dispatchConvertingFields(info *commonpb.LedgerInfo, targetType commonpb.TargetType, fields map[string]*commonpb.MetadataFieldSchema) {
	for key, field := range fields {
		if field.GetStatus() == commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING {
			select {
			case fsm.metadataConvertRequestCh <- MetadataConvertRequest{
				LedgerName: info.GetName(),
				TargetType: targetType,
				Key:        key,
				Type:       field.GetType(),
			}:
			default:
			}
		}
	}
}

// OnLeadershipAcquired is called when this node becomes the Raft leader.
// It performs recovery actions that only the leader should handle.
func (fsm *Machine) OnLeadershipAcquired() {
	// Recover periods stuck in ARCHIVING state: if the previous leader crashed
	// mid-archive, the new leader retries automatically.
	fsm.dispatchArchiveRequests()

	// Recover metadata fields stuck in CONVERTING status: if the previous leader
	// crashed mid-conversion, the new leader retries automatically.
	fsm.dispatchMetadataConversionRequests()
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

// PeriodSchedule returns the current period schedule cron expression.
// Empty string means the schedule is disabled.
func (fsm *Machine) PeriodSchedule() string {
	return fsm.Periods.Schedule()
}

// ScheduleChanged returns the Signal that fires when the period schedule changes.
func (fsm *Machine) ScheduleChanged() signal.Signal {
	return fsm.Periods.ScheduleChanged()
}

// checkClosePeriod checks if the apply result contains a ClosePeriod log
// and returns a SealRequest if the sealer should be triggered.
// Only created logs are checked since reference sequences are idempotent
// responses that already triggered sealing when first applied.
//
// Uses the FSM state's closing period (not the log payload snapshot) because
// processor.go updates closingPeriod.LastLogHash to include the ClosePeriod
// log's own hash after creating the log. The Pebble-stored period also has
// this updated hash, so the sealer must use the same value for the sealing
// hash to be verifiable by the checker.
func (fsm *Machine) checkClosePeriod(result *ApplyResult) *SealRequest {
	if result == nil {
		return nil
	}

	for _, logOrRef := range result.Logs {
		if created := logOrRef.GetCreatedLog(); created != nil {
			if created.GetPayload().GetClosePeriod() != nil {
				// Use the FSM state's latest closing period which has LastLogHash
				// updated to include the ClosePeriod log's hash.
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
		LastLogHash:   period.GetLastLogHash(),
	}
}

type ApplyResult struct {
	ProposalID              uint64
	Logs                    []*raftcmdpb.CreatedLogOrReference
	Error                   error
	CheckpointPath          string // Set by Node after checkpoint creation (ClosePeriod proposals)
	ConfigChanged           bool   // True when sink configuration changed
	MirrorConfigChanged     bool   // True when mirror ledger configuration changed
	HasArchiveRequests      bool   // True when there are pending archive requests
	HasPurges               bool   // True when cold zone data was purged (triggers cold compaction)
	MetadataConvertRequests []MetadataConvertRequest

	// volumeUpdates and createdLogs are captured for post-commit verification.
	// Not exported because they are only used internally by ApplyEntries.
	volumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]
	createdLogs   []*commonpb.Log
}

// ApplyEntriesResult is the structured return value of ApplyEntries.
type ApplyEntriesResult struct {
	// Results contains one ApplyResult per processed entry that carried a proposal.
	Results []ApplyResult

	// RemainingEntries holds unprocessed entries when a ClosePeriod stopped processing early.
	RemainingEntries []raftpb.Entry

	// CheckpointRequired is true when the caller must create a Pebble checkpoint
	// before resuming entry processing (e.g. after a ClosePeriod).
	CheckpointRequired bool

	// CheckpointPeriodID is the period ID that triggered the checkpoint.
	// Used by the Applier to name the checkpoint uniquely per period.
	CheckpointPeriodID uint64

	// OnCheckpointDone is called by Node once the Pebble checkpoint has been created.
	// It forges a SealRequest and sends it to the sealer.  Nil when CheckpointRequired is false.
	OnCheckpointDone func(checkpointPath string)
}
