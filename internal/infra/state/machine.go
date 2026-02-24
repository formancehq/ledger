package state

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"crypto/ed25519"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/keystore"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/kv"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
)

// Notifier is notified by the FSM when logs are committed or config changes.
// Used by the events Manager and mirror Manager.
type Notifier interface {
	NotifyLogsCommitted()
	NotifyConfigChanged()
}

// NoopNotifier is a no-op implementation of Notifier for use in tests.
type NoopNotifier struct{}

func (NoopNotifier) NotifyLogsCommitted() {}
func (NoopNotifier) NotifyConfigChanged() {}

type Machine struct {
	logger    logging.Logger
	dataStore *dal.Store

	mu sync.Mutex

	// Composed subsystems
	Registry *StateRegistry // 10 KeyStores + Cache + Attrs (includes NumscriptVersions/NumscriptEntries)
	Periods  *PeriodTracker // Period lifecycle

	// FSM mechanics
	nextSequenceID      uint64
	nextAuditSequenceID uint64
	lastLogHash         []byte
	lastCheckpointID    uint64

	lastAppliedIndex            uint64
	lastAppliedTimestamp        uint64
	snapshotIndex               uint64
	generationRotationThreshold uint64

	// KeyStore holds registered signing keys (updated after proposal apply)
	keyStore *keystore.KeyStore

	// sharedState holds maintenance mode and require-signatures flags
	sharedState *SharedState

	// RequestProcessor handles business logic
	processor *processing.RequestProcessor

	// dirtyVolumeKeys tracks canonical key bytes written during each generation.
	// The uint32 value counts how many diffs were written for this key in that generation.
	// [0]=current gen, [1]=previous gen, [2]=gen before (consumed at compaction).
	dirtyVolumeKeys [3]map[string]uint32

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
	rotationKeysCompacted     metric.Int64Counter
	batchCommitHistogram      metric.Int64Histogram
	lastPersistedIndex        atomic.Uint64

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

func NewMachine(logger logging.Logger, dataStore *dal.Store, meter metric.Meter, cache *cache.Cache, attrs *attributes.Attributes, generationRotationThreshold uint64, ks *keystore.KeyStore, sharedState *SharedState, eventNotifier Notifier, mirrorNotifier Notifier, indexNotifier Notifier, numscriptCacheSize int) (*Machine, error) {
	lastAppliedIndex, err := query.ReadLastAppliedIndex(dataStore)
	if err != nil {
		return nil, err
	}

	lastAppliedTimestamp, err := query.ReadLastAppliedTimestamp(dataStore)
	if err != nil {
		return nil, err
	}

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

	rotationKeysCompacted, err := meter.Int64Counter(
		"raft.fsm.rotation.keys_compacted",
		metric.WithDescription("Number of volume keys compacted during generation rotation. Use rate() for keys/s."),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating rotation_keys_compacted counter: %w", err)
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

	periodsFromStore, err := query.ReadAllPeriods(dataStore)
	if err != nil {
		return nil, fmt.Errorf("loading periods from store: %w", err)
	}

	allPeriods := make(map[uint64]*commonpb.Period, len(periodsFromStore))
	var currentOpenPeriod, closingPeriod *commonpb.Period
	for _, p := range periodsFromStore {
		allPeriods[p.Id] = p
		switch p.Status {
		case commonpb.PeriodStatus_PERIOD_OPEN:
			currentOpenPeriod = p
		case commonpb.PeriodStatus_PERIOD_CLOSING:
			closingPeriod = p
		}
	}

	nextPeriodID, err := query.ReadNextPeriodID(dataStore)
	if err != nil {
		return nil, fmt.Errorf("loading next period ID from store: %w", err)
	}

	periodSchedule, err := query.ReadPeriodSchedule(dataStore)
	if err != nil {
		return nil, fmt.Errorf("loading period schedule from store: %w", err)
	}

	processor, err := processing.NewRequestProcessor(meter, numscriptCacheSize)
	if err != nil {
		return nil, fmt.Errorf("creating request processor: %w", err)
	}

	// Load signing keys from Pebble on startup
	if ks != nil {
		signingKeys, err := query.ReadSigningKeys(dataStore)
		if err != nil {
			return nil, fmt.Errorf("loading signing keys from store: %w", err)
		}
		for keyID, entry := range signingKeys {
			ks.AddPublicKey(keyID, entry.PublicKey, entry.ParentKeyID)
		}
	}

	// Load shared runtime flags from Pebble on startup
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

	fsm := &Machine{
		logger:                      logger,
		dataStore:                   dataStore,
		lastAppliedIndex:            lastAppliedIndex,
		lastAppliedTimestamp:        lastAppliedTimestamp,
		generationRotationThreshold: generationRotationThreshold,
		logsAppendedCounter:         logsAppendedCounter,
		rotationDurationHistogram:   rotationDurationHistogram,
		rotationKeysCompacted:       rotationKeysCompacted,
		batchCommitHistogram:        batchCommitHistogram,
		processor:                   processor,
		eventNotifier:               eventNotifier,
		mirrorNotifier:              mirrorNotifier,
		indexNotifier:               indexNotifier,
		keyStore:                    ks,
		sharedState:                 sharedState,
		Registry:                    NewStateRegistry(cache, attrs),
		Periods:                     NewPeriodTracker(allPeriods, currentOpenPeriod, closingPeriod, nextPeriodID, periodSchedule),
		nextSequenceID:              1,
		nextAuditSequenceID:         1,
		dirtyVolumeKeys: [3]map[string]uint32{
			make(map[string]uint32),
			make(map[string]uint32),
			make(map[string]uint32),
		},
		sealRequestCh:            make(chan SealRequest, 1),
		archiveRequestCh:         make(chan ArchiveRequest, 1),
		metadataConvertRequestCh: make(chan MetadataConvertRequest, 16),
		coldCompactionCh:         make(chan struct{}, 1),
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
		fsm.lastLogHash = lastLog.Hash
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

func (fsm *Machine) LastPersistedIndex() (uint64, error) {
	return fsm.lastPersistedIndex.Load(), nil
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
			return nil, &ErrInvalidEntryIndex{
				ReceivedIndex: entry.Index,
				ExpectedIndex: fsm.lastAppliedIndex + 1,
			}
		}

		if rotated, oldGen1BaseIndex := fsm.Registry.Cache.CheckRotationNeeded(entry.Index); rotated {
			rotationStart := time.Now()

			// Rotate dirty key tracking: consume slot[2], shift down, allocate new slot[0]
			keysToCompact := fsm.dirtyVolumeKeys[2]
			fsm.dirtyVolumeKeys[2] = fsm.dirtyVolumeKeys[1]
			fsm.dirtyVolumeKeys[1] = fsm.dirtyVolumeKeys[0]
			fsm.dirtyVolumeKeys[0] = make(map[string]uint32)

			// Compaction using tracked keys in the same batch (no Pebble scan)
			compactedKeys, err := fsm.compactVolumeDiffs(batch, oldGen1BaseIndex, keysToCompact)
			if err != nil {
				return nil, fmt.Errorf("compacting volume diffs: %w", err)
			}

			fsm.rotationDurationHistogram.Record(context.Background(), time.Since(rotationStart).Microseconds())
			fsm.rotationKeysCompacted.Add(context.Background(), compactedKeys)
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
		if len(cmd.Orders) == 0 && len(cmd.MirrorSyncUpdates) == 0 && len(cmd.EventsSinkUpdates) == 0 {
			ret.Results = append(ret.Results, ApplyResult{ProposalID: cmd.Id})
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
			return fsm.commitAndRequestCheckpoint(batch, ret, entries[i+1:], needsArchiveDispatch, func(checkpointPath string) {
				sealReqBase.CheckpointPath = checkpointPath
				select {
				case fsm.sealRequestCh <- *sealReqBase:
				default:
				}
			})
		}

	}

	if err := SetAppliedIndex(batch, fsm.lastAppliedIndex); err != nil {
		return nil, fmt.Errorf("setting applied index: %w", err)
	}
	if err := SetLastAppliedTimestamp(batch, fsm.lastAppliedTimestamp); err != nil {
		return nil, fmt.Errorf("setting last applied timestamp: %w", err)
	}
	commitStart := time.Now()
	if err := batch.Commit(); err != nil {
		return nil, fmt.Errorf("committing batch: %w", err)
	}
	fsm.batchCommitHistogram.Record(context.Background(), time.Since(commitStart).Microseconds())

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
	fsm.eventNotifier.NotifyLogsCommitted()
	if eventsConfigChanged {
		fsm.eventNotifier.NotifyConfigChanged()
	}

	// Notify mirror Manager that new logs are available.
	fsm.mirrorNotifier.NotifyLogsCommitted()

	// Notify index builder that new logs are available.
	fsm.indexNotifier.NotifyLogsCommitted()
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
	onCheckpointDone func(checkpointPath string),
) (*ApplyEntriesResult, error) {
	if err := SetAppliedIndex(batch, fsm.lastAppliedIndex); err != nil {
		return nil, fmt.Errorf("setting applied index: %w", err)
	}
	if err := SetLastAppliedTimestamp(batch, fsm.lastAppliedTimestamp); err != nil {
		return nil, fmt.Errorf("setting last applied timestamp: %w", err)
	}
	if err := batch.Commit(); err != nil {
		return nil, fmt.Errorf("committing batch for checkpoint: %w", err)
	}
	fsm.lastPersistedIndex.Store(fsm.lastAppliedIndex)
	fsm.appliedCond.Broadcast()
	if needsArchiveDispatch {
		fsm.dispatchArchiveRequests()
	}

	ret.CheckpointRequired = true
	if len(remaining) > 0 {
		ret.RemainingEntries = make([]raftpb.Entry, len(remaining))
		copy(ret.RemainingEntries, remaining)
	}
	ret.OnCheckpointDone = onCheckpointDone

	return ret, nil
}

// volumeCompactionMinDiffs is the minimum number of diff entries a volume key
// must have accumulated in a generation before compaction (DeleteRange) is
// worthwhile.  For keys with fewer diffs the Pebble range-delete tombstone
// costs more than letting the native LSM compaction clean them up.
const volumeCompactionMinDiffs uint32 = 4

// compactVolumeDiffs prunes old volume diff entries during generation rotation.
//
// Volume diffs are cumulative: each diff stores the total delta since the original base.
// Only the latest diff is needed by ComputeValue, so older diffs can be safely removed.
//
// We delete all entries strictly before compactionIndex. This removes superseded diffs
// while preserving the latest cumulative diff and any base that might exist.
// We do NOT create a new base because existing diffs are cumulative from the original base,
// and introducing a new base would make subsequent diffs inconsistent.
//
// Safety: DeleteOldest removes both bases and diffs before compactionIndex.
// A key that was only active in the compacted generation (no entries in newer
// generations) would lose all its Pebble data. To prevent this, we skip
// compaction for keys that have no entries in a more recent generation —
// their stale entries are left for Pebble's native LSM compaction or will be
// superseded when the account is next preloaded.
//
// dirtyKeys maps canonical keys to the number of diffs written during the
// generation being compacted. Keys with fewer than volumeCompactionMinDiffs
// writes are skipped — the overhead of a Pebble range-delete tombstone is
// not worth it for a handful of entries.
func (fsm *Machine) compactVolumeDiffs(batch *dal.Batch, compactionIndex uint64, dirtyKeys map[string]uint32) (int64, error) {
	var compacted int64
	for keyStr, count := range dirtyKeys {
		if count < volumeCompactionMinDiffs {
			continue
		}
		// Only compact if newer entries exist in a more recent generation.
		// After the dirty key shift: slot[1] = Gen N, slot[2] = Gen N-1.
		_, inPrevGen := fsm.dirtyVolumeKeys[2][keyStr]
		_, inCurGen := fsm.dirtyVolumeKeys[1][keyStr]
		if !inPrevGen && !inCurGen {
			continue
		}
		canonicalKey := []byte(keyStr)
		if err := fsm.Registry.Attrs.Volume.DeleteOldest(batch, compactionIndex, canonicalKey); err != nil {
			return 0, fmt.Errorf("compacting volume: %w", err)
		}
		compacted++
	}
	return compacted, nil
}

// Preload applies preloaded data to the Machine's volatile state.
func (fsm *Machine) Preload(preloadSet *raftcmdpb.PreloadSet) error {

	if preloadSet == nil || len(preloadSet.Preloads) == 0 {
		return nil
	}

	// The preloads must be for the gen0 or the gen1
	// This is the role of the admission to ensure this invariant
	switch preloadSet.LastPersistedIndex {
	case fsm.Registry.Cache.BaseIndex.Gen0:
		fsm.logger.Debug("Selecting cache generation 0")
	case fsm.Registry.Cache.BaseIndex.Gen1:
		fsm.logger.Debug("Selecting cache generation 1")
	default:
		return &ErrGenerationMismatch{
			LastPersistedIndex: preloadSet.LastPersistedIndex,
			Gen0BaseIndex:      fsm.Registry.Cache.BaseIndex.Gen0,
			Gen1BaseIndex:      fsm.Registry.Cache.BaseIndex.Gen1,
		}
	}

	// Helper function to put a preloaded volume pair into a cache generation
	var scratchA, scratchB uint256.Int // reused across all preload volume merges
	putInCacheVolumePair := func(
		kv kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]],
		attrID *raftcmdpb.AttributeID,
		pair *raftcmdpb.VolumePair,
	) *raftcmdpb.VolumePair {
		id := attributes.U128FromBytes(attrID.Id)

		fsm.logger.WithFields(map[string]any{
			"id": id.Hex(),
		}).Debugf("Preload volume")

		value, ok := kv.Get(id)
		if ok {
			// If InputKnown is not yet set, merge preloaded input with any existing diff
			if value.Data.InputKnown == nil && pair.InputKnown != nil {
				if value.Data.InputDiff != nil {
					pair.InputKnown.IntoUint256(&scratchA)
					value.Data.InputDiff.IntoUint256(&scratchB)
					scratchA.Add(&scratchA, &scratchB)
					value.Data.InputKnown = commonpb.NewUint256(&scratchA)
				} else {
					value.Data.InputKnown = pair.InputKnown
				}
			}
			// If OutputKnown is not yet set, merge preloaded output with any existing diff
			if value.Data.OutputKnown == nil && pair.OutputKnown != nil {
				if value.Data.OutputDiff != nil {
					pair.OutputKnown.IntoUint256(&scratchA)
					value.Data.OutputDiff.IntoUint256(&scratchB)
					scratchA.Add(&scratchA, &scratchB)
					value.Data.OutputKnown = commonpb.NewUint256(&scratchA)
				} else {
					value.Data.OutputKnown = pair.OutputKnown
				}
			}
			return value.Data
		}

		kv.Put(id, attributes.Entry[*raftcmdpb.VolumePair]{
			Tag:  attrID.Tag,
			Data: pair,
		})

		return pair
	}

	// Helper function to put a preloaded idempotency value into a cache generation
	putInCacheIdempotencyValue := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.IdempotencyKeyValue]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.IdempotencyKeyValue,
	) *commonpb.IdempotencyKeyValue {
		id := attributes.U128FromBytes(attrID.Id)

		fsm.logger.WithFields(map[string]any{
			"id":           id.Hex(),
			"log_sequence": value.LogSequence,
			"hash":         value.Hash,
		}).Debugf("Preload idempotency value")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.IdempotencyKeyValue]{
			Tag:  attrID.Tag,
			Data: value,
		})

		return value
	}

	// Helper function to put a preloaded transaction reference value into a cache generation
	putInCacheReferenceValue := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.TransactionReferenceValue,
	) *commonpb.TransactionReferenceValue {
		id := attributes.U128FromBytes(attrID.Id)

		fsm.logger.WithFields(map[string]any{
			"id":             id.Hex(),
			"transaction_id": value.TransactionId,
		}).Debugf("Preload transaction reference value")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.TransactionReferenceValue]{
			Tag:  attrID.Tag,
			Data: value,
		})

		return value
	}

	// Helper function to put a preloaded ledger info into a cache generation
	putInCacheLedger := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.LedgerInfo,
	) *commonpb.LedgerInfo {
		id := attributes.U128FromBytes(attrID.Id)

		fsm.logger.WithFields(map[string]any{
			"id":   id.Hex(),
			"name": value.Name,
		}).Debugf("Preload ledger")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.LedgerInfo]{
			Tag:  attrID.Tag,
			Data: value,
		})

		return value
	}

	// Helper function to put a preloaded boundary into a cache generation
	putInCacheBoundary := func(
		kv kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]],
		attrID *raftcmdpb.AttributeID,
		value *raftcmdpb.LedgerBoundaries,
	) *raftcmdpb.LedgerBoundaries {
		id := attributes.U128FromBytes(attrID.Id)

		fsm.logger.WithFields(map[string]any{
			"id": id.Hex(),
		}).Debugf("Preload boundary")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*raftcmdpb.LedgerBoundaries]{
			Tag:  attrID.Tag,
			Data: value,
		})

		return value
	}

	// Helper function to put a preloaded sink config into a cache generation
	putInCacheSinkConfig := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.SinkConfig]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.SinkConfig,
	) *commonpb.SinkConfig {
		id := attributes.U128FromBytes(attrID.Id)

		fsm.logger.WithFields(map[string]any{
			"id":   id.Hex(),
			"name": value.Name,
		}).Debugf("Preload sink config")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.SinkConfig]{
			Tag:  attrID.Tag,
			Data: value,
		})

		return value
	}

	// Helper function to put a preloaded string value into a cache generation
	putInCacheString := func(
		kv kv.KV[attributes.U128, attributes.Entry[string]],
		attrID *raftcmdpb.AttributeID,
		value string,
	) string {
		id := attributes.U128FromBytes(attrID.Id)

		fsm.logger.WithFields(map[string]any{
			"id":    id.Hex(),
			"value": value,
		}).Debugf("Preload string")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[string]{
			Tag:  attrID.Tag,
			Data: value,
		})

		return value
	}

	// Helper function to put a preloaded account metadata value into a cache generation
	putInCacheMetadataValue := func(
		kv kv.KV[attributes.U128, attributes.Entry[*commonpb.MetadataValue]],
		attrID *raftcmdpb.AttributeID,
		value *commonpb.MetadataValue,
	) *commonpb.MetadataValue {
		id := attributes.U128FromBytes(attrID.Id)

		fsm.logger.WithFields(map[string]any{
			"id": id.Hex(),
		}).Debugf("Preload account metadata")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[*commonpb.MetadataValue]{
			Tag:  attrID.Tag,
			Data: value,
		})

		return value
	}

	for _, preload := range preloadSet.GetPreloads() {
		switch preloadType := preload.Type.(type) {
		case *raftcmdpb.Preload_Volume:
			pair := &raftcmdpb.VolumePair{
				InputKnown:  preloadType.Volume.Input,
				OutputKnown: preloadType.Volume.Output,
			}
			if preloadSet.LastPersistedIndex == fsm.Registry.Cache.BaseIndex.Gen1 {
				aggregated := putInCacheVolumePair(fsm.Registry.Cache.Volumes.Gen1(), preloadType.Volume.Id, pair)
				putInCacheVolumePair(fsm.Registry.Cache.Volumes.Gen0(), preloadType.Volume.Id, aggregated)
			} else {
				putInCacheVolumePair(fsm.Registry.Cache.Volumes.Gen0(), preloadType.Volume.Id, pair)
			}

		case *raftcmdpb.Preload_IdempotencyKey:
			idempotencyValue := &commonpb.IdempotencyKeyValue{
				LogSequence: preloadType.IdempotencyKey.LogSequence,
				Hash:        preloadType.IdempotencyKey.Hash,
			}
			if preloadSet.LastPersistedIndex == fsm.Registry.Cache.BaseIndex.Gen1 {
				value := putInCacheIdempotencyValue(fsm.Registry.Cache.IdempotencyKeys.Gen1(), preloadType.IdempotencyKey.Id, idempotencyValue)
				putInCacheIdempotencyValue(fsm.Registry.Cache.IdempotencyKeys.Gen0(), preloadType.IdempotencyKey.Id, value)
			} else {
				putInCacheIdempotencyValue(fsm.Registry.Cache.IdempotencyKeys.Gen0(), preloadType.IdempotencyKey.Id, idempotencyValue)
			}

		case *raftcmdpb.Preload_Ledger:
			if preloadSet.LastPersistedIndex == fsm.Registry.Cache.BaseIndex.Gen1 {
				value := putInCacheLedger(fsm.Registry.Cache.Ledgers.Gen1(), preloadType.Ledger.Id, preloadType.Ledger.Info)
				putInCacheLedger(fsm.Registry.Cache.Ledgers.Gen0(), preloadType.Ledger.Id, value)
			} else {
				putInCacheLedger(fsm.Registry.Cache.Ledgers.Gen0(), preloadType.Ledger.Id, preloadType.Ledger.Info)
			}

		case *raftcmdpb.Preload_Boundary:
			if preloadSet.LastPersistedIndex == fsm.Registry.Cache.BaseIndex.Gen1 {
				value := putInCacheBoundary(fsm.Registry.Cache.Boundaries.Gen1(), preloadType.Boundary.Id, preloadType.Boundary.Boundaries)
				putInCacheBoundary(fsm.Registry.Cache.Boundaries.Gen0(), preloadType.Boundary.Id, value)
			} else {
				putInCacheBoundary(fsm.Registry.Cache.Boundaries.Gen0(), preloadType.Boundary.Id, preloadType.Boundary.Boundaries)
			}

		case *raftcmdpb.Preload_TransactionReference:
			referenceValue := &commonpb.TransactionReferenceValue{
				TransactionId: preloadType.TransactionReference.TransactionId,
			}
			if preloadSet.LastPersistedIndex == fsm.Registry.Cache.BaseIndex.Gen1 {
				value := putInCacheReferenceValue(fsm.Registry.Cache.References.Gen1(), preloadType.TransactionReference.Id, referenceValue)
				putInCacheReferenceValue(fsm.Registry.Cache.References.Gen0(), preloadType.TransactionReference.Id, value)
			} else {
				putInCacheReferenceValue(fsm.Registry.Cache.References.Gen0(), preloadType.TransactionReference.Id, referenceValue)
			}

		case *raftcmdpb.Preload_SinkConfig:
			if preloadSet.LastPersistedIndex == fsm.Registry.Cache.BaseIndex.Gen1 {
				value := putInCacheSinkConfig(fsm.Registry.Cache.SinkConfigs.Gen1(), preloadType.SinkConfig.Id, preloadType.SinkConfig.Config)
				putInCacheSinkConfig(fsm.Registry.Cache.SinkConfigs.Gen0(), preloadType.SinkConfig.Id, value)
			} else {
				putInCacheSinkConfig(fsm.Registry.Cache.SinkConfigs.Gen0(), preloadType.SinkConfig.Id, preloadType.SinkConfig.Config)
			}

		case *raftcmdpb.Preload_AccountMetadata:
			if preloadSet.LastPersistedIndex == fsm.Registry.Cache.BaseIndex.Gen1 {
				value := putInCacheMetadataValue(fsm.Registry.Cache.AccountMetadata.Gen1(), preloadType.AccountMetadata.Id, preloadType.AccountMetadata.Value)
				putInCacheMetadataValue(fsm.Registry.Cache.AccountMetadata.Gen0(), preloadType.AccountMetadata.Id, value)
			} else {
				putInCacheMetadataValue(fsm.Registry.Cache.AccountMetadata.Gen0(), preloadType.AccountMetadata.Id, preloadType.AccountMetadata.Value)
			}

		case *raftcmdpb.Preload_NumscriptVersion:
			if preloadSet.LastPersistedIndex == fsm.Registry.Cache.BaseIndex.Gen1 {
				value := putInCacheString(fsm.Registry.Cache.NumscriptVersions.Gen1(), preloadType.NumscriptVersion.Id, preloadType.NumscriptVersion.Version)
				putInCacheString(fsm.Registry.Cache.NumscriptVersions.Gen0(), preloadType.NumscriptVersion.Id, value)
			} else {
				putInCacheString(fsm.Registry.Cache.NumscriptVersions.Gen0(), preloadType.NumscriptVersion.Id, preloadType.NumscriptVersion.Version)
			}

		case *raftcmdpb.Preload_NumscriptEntry:
			if preloadSet.LastPersistedIndex == fsm.Registry.Cache.BaseIndex.Gen1 {
				value := putInCacheBool(fsm.Registry.Cache.NumscriptEntries.Gen1(), preloadType.NumscriptEntry.Id, preloadType.NumscriptEntry.Exists)
				putInCacheBool(fsm.Registry.Cache.NumscriptEntries.Gen0(), preloadType.NumscriptEntry.Id, value)
			} else {
				putInCacheBool(fsm.Registry.Cache.NumscriptEntries.Gen0(), preloadType.NumscriptEntry.Id, preloadType.NumscriptEntry.Exists)
			}
		}
	}

	return nil
}

// hlcTimestamp advances the Hybrid Logical Clock and returns the effective timestamp.
// It guarantees monotonicity: each returned timestamp is strictly greater than the previous one.
// If the proposal date is ahead of the last applied timestamp, it is used directly.
// Otherwise, the last applied timestamp is incremented by 1 microsecond.
func (fsm *Machine) hlcTimestamp(proposalDate *commonpb.Timestamp) *commonpb.Timestamp {
	if proposalDate.Data > fsm.lastAppliedTimestamp {
		fsm.lastAppliedTimestamp = proposalDate.Data
	} else {
		fsm.lastAppliedTimestamp++
	}
	return &commonpb.Timestamp{Data: fsm.lastAppliedTimestamp}
}

// allOrdersAreMaintenanceMode returns true if every order in the batch is a SetMaintenanceMode order.
func allOrdersAreMaintenanceMode(orders []*raftcmdpb.Order) bool {
	for _, order := range orders {
		if _, ok := order.Type.(*raftcmdpb.Order_SetMaintenanceMode); !ok {
			return false
		}
	}
	return true
}

// applyProposal processes all orders in a proposal atomically.
// Uses RequestProcessor which handles rollback internally via Buffered.
func (fsm *Machine) applyProposal(ctx context.Context, raftIndex uint64, batch *dal.Batch, proposal *raftcmdpb.Proposal) (*ApplyResult, error) {
	// Handle per-sink cursor and status updates (Raft-replicated, no orders needed)
	for _, update := range proposal.EventsSinkUpdates {
		if update.Cursor > 0 {
			if err := SetSinkCursor(batch, update.SinkName, update.Cursor); err != nil {
				return nil, fmt.Errorf("setting sink cursor: %w", err)
			}
		}
		if update.ClearError {
			if err := ClearSinkStatus(batch, update.SinkName); err != nil {
				return nil, fmt.Errorf("clearing sink status: %w", err)
			}
		} else if update.Error != nil {
			if err := SetSinkStatus(batch, &commonpb.SinkStatus{
				SinkName: update.SinkName,
				Cursor:   update.Cursor,
				Error:    update.Error,
			}); err != nil {
				return nil, fmt.Errorf("setting sink status: %w", err)
			}
		}
	}

	// Handle per-ledger mirror cursor and status updates (Raft-replicated)
	for _, update := range proposal.MirrorSyncUpdates {
		if update.Cursor > 0 {
			if err := SetMirrorCursor(batch, update.LedgerName, update.Cursor); err != nil {
				return nil, fmt.Errorf("setting mirror cursor: %w", err)
			}
		}
		if update.SourceLogCount > 0 {
			if err := SetMirrorSourceHead(batch, update.LedgerName, update.SourceLogCount); err != nil {
				return nil, fmt.Errorf("setting mirror source head: %w", err)
			}
		}
		if update.ClearError {
			if err := ClearMirrorStatus(batch, update.LedgerName); err != nil {
				return nil, fmt.Errorf("clearing mirror status: %w", err)
			}
		} else if update.Error != nil {
			if err := SetMirrorStatus(batch, update.LedgerName, update.Error); err != nil {
				return nil, fmt.Errorf("setting mirror status: %w", err)
			}
		}
	}

	// If this proposal only carries sink updates, skip order processing
	if len(proposal.Orders) == 0 {
		return &ApplyResult{ProposalID: proposal.Id}, nil
	}

	// FSM-level maintenance mode check: reject proposals containing non-maintenance
	// orders that were admitted before maintenance mode was enabled but batched into
	// a Raft entry applied after the maintenance mode flag was set.
	if fsm.sharedState.MaintenanceMode() && !allOrdersAreMaintenanceMode(proposal.Orders) {
		return &ApplyResult{
			ProposalID: proposal.Id,
			Error:      &domain.BusinessError{Err: domain.ErrMaintenanceMode},
		}, nil
	}

	if err := fsm.Preload(proposal.Preload); err != nil {
		return nil, err
	}

	// Compute the effective date using the HLC to guarantee monotonicity
	effectiveDate := fsm.hlcTimestamp(proposal.Date)

	// Auto-bootstrap first period deterministically at first proposal
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
		if err := StorePeriod(batch, p); err != nil {
			return nil, fmt.Errorf("storing bootstrapped period: %w", err)
		}
		if err := StoreNextPeriodID(batch, fsm.Periods.NextPeriodID()); err != nil {
			return nil, fmt.Errorf("storing next period ID: %w", err)
		}
	}

	// Create buffer for this proposal
	buffer := NewBuffer(effectiveDate, fsm)

	// Process the proposal
	logs, err := fsm.processor.ProcessOrders(proposal.Orders, buffer)
	if err != nil {
		// FAILURE: write audit entry and return business error
		if fsm.sharedState.AuditEnabled() {
			auditEntry := &auditpb.AuditEntry{
				Sequence:   fsm.nextAuditSequenceID,
				Timestamp:  effectiveDate,
				ProposalId: proposal.Id,
				Orders:     proposal.Orders,
				Outcome: &auditpb.AuditEntry_Failure{
					Failure: buildAuditFailure(err),
				},
			}
			fsm.nextAuditSequenceID++
			if appendErr := AppendAuditEntries(batch, auditEntry); appendErr != nil {
				return nil, fmt.Errorf("appending audit entry for failure: %w", appendErr)
			}
		}

		return &ApplyResult{
			ProposalID: proposal.Id,
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
	auditAfter := fsm.sharedState.AuditEnabled()

	// SUCCESS: write audit entry
	if auditBefore || auditAfter {
		auditEntry := &auditpb.AuditEntry{
			Sequence:   fsm.nextAuditSequenceID,
			Timestamp:  effectiveDate,
			ProposalId: proposal.Id,
			Orders:     proposal.Orders,
			Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{
					LogSequences: extractLogSequencesFromLogsOrRefs(logs),
				},
			},
		}
		fsm.nextAuditSequenceID++
		if err := AppendAuditEntries(batch, auditEntry); err != nil {
			return nil, fmt.Errorf("appending audit entry for success: %w", err)
		}
	}

	fsm.logsAppendedCounter.Add(ctx, int64(len(createdLogs)))

	return &ApplyResult{
		ProposalID:              proposal.Id,
		Logs:                    logs,
		ConfigChanged:           configChanged,
		MirrorConfigChanged:     mirrorConfigChanged,
		HasArchiveRequests:      hasArchiveRequests,
		HasPurges:               hasPurges,
		MetadataConvertRequests: buffer.MetadataConvertRequests(),
	}, nil
}

// hasMirrorConfigChange returns true if any order in the proposal creates or promotes a mirror ledger.
func hasMirrorConfigChange(proposal *raftcmdpb.Proposal) bool {
	for _, order := range proposal.Orders {
		switch o := order.Type.(type) {
		case *raftcmdpb.Order_CreateLedger:
			if o.CreateLedger.Mode == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
				return true
			}
		case *raftcmdpb.Order_PromoteLedger:
			return true
		}
	}
	return false
}

// CreateSnapshot creates a snapshot of the Machine state
func (fsm *Machine) CreateSnapshot(_ context.Context) ([]byte, error) {
	totalStart := time.Now()

	checkpointID, err := fsm.dataStore.CreateSnapshot()
	if err != nil {
		return nil, fmt.Errorf("creating snapshot: %w", err)
	}

	// Collect CLOSED/ARCHIVED periods (OPEN and CLOSING are stored separately)
	closedPeriods := make([]*commonpb.Period, 0)
	for _, p := range fsm.Periods.AllPeriods() {
		if p.Status != commonpb.PeriodStatus_PERIOD_OPEN && p.Status != commonpb.PeriodStatus_PERIOD_CLOSING {
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

	serializeStart := time.Now()
	snapshot := &raftcmdpb.MemorySnapshot{
		NextSequenceId:       fsm.nextSequenceID,
		LastLogHash:          fsm.lastLogHash,
		Gen0:                 serializeCacheGeneration(fsm.Registry.Cache, 0),
		Gen1:                 serializeCacheGeneration(fsm.Registry.Cache, 1),
		CheckpointId:         checkpointID,
		CurrentGeneration:    fsm.Registry.Cache.CurrentGeneration(),
		LastAppliedTimestamp: fsm.lastAppliedTimestamp,
		NextAuditSequenceId:  fsm.nextAuditSequenceID,
		OpenPeriod:           fsm.Periods.CurrentOpenPeriod(),
		ClosingPeriod:        fsm.Periods.ClosingPeriod(),
		NextPeriodId:         fsm.Periods.NextPeriodID(),
		ClosedPeriods:        closedPeriods,
		Reversions:           reversions,
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

	fsm.logger.WithFields(map[string]any{
		"totalDuration":      time.Since(totalStart).String(),
		"serializeDuration":  time.Since(serializeStart).String(),
		"snapshotSize":       n,
		"checkpointId":       checkpointID,
		"gen0Volumes":        countGenVolumes(snapshot.Gen0),
		"gen1Volumes":        countGenVolumes(snapshot.Gen1),
	}).Infof("Created MemorySnapshot")

	return fsm.snapshotBuf[:n], nil
}

// serializeCacheGeneration serializes either Gen0 (genIndex=0) or Gen1 (genIndex=1) from the cache
func serializeCacheGeneration(cache *cache.Cache, genIndex int) *raftcmdpb.GenerationSnapshot {
	if cache == nil {
		return nil
	}

	var (
		baseIndex           uint64
		volumeStore         kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]]
		metadataStore  kv.KV[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerStore         kv.KV[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]]
		boundaryStore       kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]]
		referenceStore      kv.KV[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]]
	)

	if genIndex == 0 {
		baseIndex = cache.BaseIndex.Gen0
		volumeStore = cache.Volumes.Gen0()
		metadataStore = cache.AccountMetadata.Gen0()

		ledgerStore = cache.Ledgers.Gen0()
		boundaryStore = cache.Boundaries.Gen0()
		referenceStore = cache.References.Gen0()
	} else {
		baseIndex = cache.BaseIndex.Gen1
		volumeStore = cache.Volumes.Gen1()
		metadataStore = cache.AccountMetadata.Gen1()

		ledgerStore = cache.Ledgers.Gen1()
		boundaryStore = cache.Boundaries.Gen1()
		referenceStore = cache.References.Gen1()
	}

	snapshot := &raftcmdpb.GenerationSnapshot{
		BaseIndex:      baseIndex,
		Volumes:        make([]*raftcmdpb.VolumeAttributeSnapshotEntry, 0, volumeStore.Size()),
		Metadata:       make([]*raftcmdpb.MetadataAttributeEntry, 0, metadataStore.Size()),
		Ledgers:        make([]*raftcmdpb.LedgerAttributeEntry, 0, ledgerStore.Size()),
		Boundaries:     make([]*raftcmdpb.BoundaryAttributeEntry, 0, boundaryStore.Size()),
		References:     make([]*raftcmdpb.TransactionReferenceAttributeEntry, 0, referenceStore.Size()),
	}

	// Serialize Volumes KeyStore
	for u128, entry := range volumeStore.Iter() {
		ksEntry := &raftcmdpb.VolumeAttributeSnapshotEntry{
			Id: &raftcmdpb.AttributeID{
				Id:  u128[:],
				Tag: entry.Tag,
			},
		}
		if entry.Data != nil {
			ksEntry.InputKnown = entry.Data.InputKnown
			ksEntry.InputDiff = entry.Data.InputDiff
			ksEntry.OutputKnown = entry.Data.OutputKnown
			ksEntry.OutputDiff = entry.Data.OutputDiff
		}
		snapshot.Volumes = append(snapshot.Volumes, ksEntry)
	}

	// Serialize Metadata KeyStore
	for u128, entry := range metadataStore.Iter() {
		ksEntry := &raftcmdpb.MetadataAttributeEntry{
			Id: &raftcmdpb.AttributeID{
				Id:  u128[:],
				Tag: entry.Tag,
			},
			Value: entry.Data,
		}
		snapshot.Metadata = append(snapshot.Metadata, ksEntry)
	}

	// Serialize Ledgers KeyStore
	for u128, entry := range ledgerStore.Iter() {
		ksEntry := &raftcmdpb.LedgerAttributeEntry{
			Id: &raftcmdpb.AttributeID{
				Id:  u128[:],
				Tag: entry.Tag,
			},
			Info: entry.Data,
		}
		snapshot.Ledgers = append(snapshot.Ledgers, ksEntry)
	}

	// Serialize Boundaries KeyStore
	for u128, entry := range boundaryStore.Iter() {
		ksEntry := &raftcmdpb.BoundaryAttributeEntry{
			Id: &raftcmdpb.AttributeID{
				Id:  u128[:],
				Tag: entry.Tag,
			},
			Boundaries: entry.Data,
		}
		snapshot.Boundaries = append(snapshot.Boundaries, ksEntry)
	}

	// Serialize References KeyStore
	for u128, entry := range referenceStore.Iter() {
		ksEntry := &raftcmdpb.TransactionReferenceAttributeEntry{
			Id: &raftcmdpb.AttributeID{
				Id:  u128[:],
				Tag: entry.Tag,
			},
			Value: entry.Data,
		}
		snapshot.References = append(snapshot.References, ksEntry)
	}

	return snapshot
}

func (fsm *Machine) InstallSnapshot(ctx context.Context, snapshot raftpb.Snapshot) error {
	totalStart := time.Now()
	fsm.snapshotIndex = snapshot.Metadata.Index

	deserializeStart := time.Now()
	memSnapshot := &raftcmdpb.MemorySnapshot{}
	if err := memSnapshot.UnmarshalVT(snapshot.Data); err != nil {
		return err
	}
	fsm.logger.WithFields(map[string]any{
		"duration":    time.Since(deserializeStart).String(),
		"dataSize":    len(snapshot.Data),
		"checkpointId": memSnapshot.CheckpointId,
	}).Infof("Deserialized MemorySnapshot")

	// Restore memory state from snapshot
	fsm.nextSequenceID = memSnapshot.NextSequenceId
	fsm.nextAuditSequenceID = memSnapshot.NextAuditSequenceId
	fsm.lastLogHash = memSnapshot.LastLogHash
	fsm.lastCheckpointID = memSnapshot.CheckpointId
	fsm.lastAppliedTimestamp = memSnapshot.LastAppliedTimestamp
	// Rebuild allPeriods from all three sources in the snapshot
	allPeriods := make(map[uint64]*commonpb.Period)
	if memSnapshot.OpenPeriod != nil {
		allPeriods[memSnapshot.OpenPeriod.Id] = memSnapshot.OpenPeriod
	}
	if memSnapshot.ClosingPeriod != nil {
		allPeriods[memSnapshot.ClosingPeriod.Id] = memSnapshot.ClosingPeriod
	}
	for _, p := range memSnapshot.ClosedPeriods {
		allPeriods[p.Id] = p
	}
	fsm.Periods.Reset(allPeriods, memSnapshot.OpenPeriod, memSnapshot.ClosingPeriod, memSnapshot.NextPeriodId)

	// Reset the cache and deserialize both generations into it
	// Ledger info and boundaries are restored via deserializeCacheGeneration (from cache generations)
	cacheStart := time.Now()
	fsm.Registry.Cache.Reset()
	deserializeCacheGeneration(fsm.Registry.Cache, memSnapshot.Gen0, 0)
	deserializeCacheGeneration(fsm.Registry.Cache, memSnapshot.Gen1, 1)
	fsm.logger.WithFields(map[string]any{
		"duration":       time.Since(cacheStart).String(),
		"gen0Volumes":    countGenVolumes(memSnapshot.Gen0),
		"gen1Volumes":    countGenVolumes(memSnapshot.Gen1),
		"gen0Metadata":   countGenMetadata(memSnapshot.Gen0),
		"gen1Metadata":   countGenMetadata(memSnapshot.Gen1),
		"gen0Ledgers":    countGenLedgers(memSnapshot.Gen0),
		"gen0Boundaries": countGenBoundaries(memSnapshot.Gen0),
		"gen0References": countGenReferences(memSnapshot.Gen0),
	}).Infof("Restored cache from snapshot")

	// Restore reversion bitsets from snapshot
	fsm.Registry.ResetReversions()
	for _, entry := range memSnapshot.Reversions {
		fsm.Registry.Reversions[entry.Ledger] = domain.ReversionBitsetFromWords(entry.Words)
	}

	// Update currentGeneration to match the snapshot
	fsm.Registry.Cache.SetCurrentGeneration(memSnapshot.CurrentGeneration)

	// Signing keys are not in the memory snapshot — they live in Pebble.
	// They will be reloaded from Pebble after SynchronizeWithLeader restores the checkpoint.

	// Reset dirty volume key tracking. The first 2 rotations after restore will do
	// less cleanup, but the system self-corrects as new keys are tracked.
	fsm.dirtyVolumeKeys = [3]map[string]uint32{
		make(map[string]uint32),
		make(map[string]uint32),
		make(map[string]uint32),
	}

	fsm.logger.WithFields(map[string]any{
		"totalDuration": time.Since(totalStart).String(),
		"snapshotIndex": snapshot.Metadata.Index,
	}).Infof("InstallSnapshot complete")

	return nil
}

// countGenVolumes returns the number of volume entries in a generation snapshot, or 0 if nil.
func countGenVolumes(gen *raftcmdpb.GenerationSnapshot) int {
	if gen == nil {
		return 0
	}
	return len(gen.Volumes)
}

// countGenMetadata returns the number of metadata entries in a generation snapshot, or 0 if nil.
func countGenMetadata(gen *raftcmdpb.GenerationSnapshot) int {
	if gen == nil {
		return 0
	}
	return len(gen.Metadata) + len(gen.LedgerMetadata)
}

// countGenLedgers returns the number of ledger entries in a generation snapshot, or 0 if nil.
func countGenLedgers(gen *raftcmdpb.GenerationSnapshot) int {
	if gen == nil {
		return 0
	}
	return len(gen.Ledgers)
}

// countGenBoundaries returns the number of boundary entries in a generation snapshot, or 0 if nil.
func countGenBoundaries(gen *raftcmdpb.GenerationSnapshot) int {
	if gen == nil {
		return 0
	}
	return len(gen.Boundaries)
}

// countGenReferences returns the number of reference entries in a generation snapshot, or 0 if nil.
func countGenReferences(gen *raftcmdpb.GenerationSnapshot) int {
	if gen == nil {
		return 0
	}
	return len(gen.References)
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
			fsm.keyStore.AddPublicKey(keyID, ed25519.PublicKey(entry.PublicKey), entry.ParentKeyID)
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

// deserializeCacheGeneration deserializes a GenerationSnapshot into either Gen0 (genIndex=0) or Gen1 (genIndex=1)
func deserializeCacheGeneration(cache *cache.Cache, snapshot *raftcmdpb.GenerationSnapshot, genIndex int) {
	if snapshot == nil || cache == nil {
		return
	}

	var (
		volumeStore         kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]]
		metadataStore  kv.KV[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerStore         kv.KV[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]]
		boundaryStore       kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]]
		referenceStore      kv.KV[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]]
	)

	if genIndex == 0 {
		cache.BaseIndex.Gen0 = snapshot.BaseIndex
		volumeStore = cache.Volumes.Gen0()
		metadataStore = cache.AccountMetadata.Gen0()

		ledgerStore = cache.Ledgers.Gen0()
		boundaryStore = cache.Boundaries.Gen0()
		referenceStore = cache.References.Gen0()
	} else {
		cache.BaseIndex.Gen1 = snapshot.BaseIndex
		volumeStore = cache.Volumes.Gen1()
		metadataStore = cache.AccountMetadata.Gen1()

		ledgerStore = cache.Ledgers.Gen1()
		boundaryStore = cache.Boundaries.Gen1()
		referenceStore = cache.References.Gen1()
	}

	// Deserialize Volumes KeyStore
	for _, ksEntry := range snapshot.Volumes {
		u128 := attributes.U128FromBytes(ksEntry.Id.Id)
		pair := &raftcmdpb.VolumePair{
			InputKnown:  ksEntry.InputKnown,
			InputDiff:   ksEntry.InputDiff,
			OutputKnown: ksEntry.OutputKnown,
			OutputDiff:  ksEntry.OutputDiff,
		}
		volumeStore.Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{
			Tag:  ksEntry.Id.Tag,
			Data: pair,
		})
	}

	// Deserialize Metadata KeyStore
	for _, ksEntry := range snapshot.Metadata {
		u128 := attributes.U128FromBytes(ksEntry.Id.Id)
		metadataStore.Put(u128, attributes.Entry[*commonpb.MetadataValue]{
			Tag:  ksEntry.Id.Tag,
			Data: ksEntry.Value,
		})
	}

	// Deserialize Ledgers KeyStore
	for _, ksEntry := range snapshot.Ledgers {
		u128 := attributes.U128FromBytes(ksEntry.Id.Id)
		ledgerStore.Put(u128, attributes.Entry[*commonpb.LedgerInfo]{
			Tag:  ksEntry.Id.Tag,
			Data: ksEntry.Info,
		})
	}

	// Deserialize Boundaries KeyStore
	for _, ksEntry := range snapshot.Boundaries {
		u128 := attributes.U128FromBytes(ksEntry.Id.Id)
		boundaryStore.Put(u128, attributes.Entry[*raftcmdpb.LedgerBoundaries]{
			Tag:  ksEntry.Id.Tag,
			Data: ksEntry.Boundaries,
		})
	}

	// Deserialize References KeyStore
	for _, ksEntry := range snapshot.References {
		u128 := attributes.U128FromBytes(ksEntry.Id.Id)
		referenceStore.Put(u128, attributes.Entry[*commonpb.TransactionReferenceValue]{
			Tag:  ksEntry.Id.Tag,
			Data: ksEntry.Value,
		})
	}
}

func (fsm *Machine) SynchronizeWithLeader(ctx context.Context, snapshotFetcher SnapshotFetcher, progress *SyncProgress) (uint64, error) {
	// Restore checkpoint from the leader if needed
	// The checkpoint ID is stored in the Machine state from the snapshot
	if fsm.lastCheckpointID > 0 {
		currentCheckpointID := fsm.dataStore.GetCurrentCheckpointID()
		if currentCheckpointID < fsm.lastCheckpointID {
			if err := fsm.restoreCheckpoint(ctx, snapshotFetcher, progress); err != nil {
				return 0, fmt.Errorf("restoring checkpoint from leader: %w", err)
			}
		}
	}

	// Reload signing keys from Pebble (the checkpoint contains the leader's keys)
	if err := fsm.reloadStateFromStore(); err != nil {
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
		if p.Status == commonpb.PeriodStatus_PERIOD_ARCHIVING {
			select {
			case fsm.archiveRequestCh <- ArchiveRequest{
				PeriodID:      p.Id,
				StartSequence: p.StartSequence,
				CloseSequence: p.CloseSequence,
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

	cursor, err := query.ReadLedgers(handle)
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
		if info.MetadataSchema == nil || info.DeletedAt != nil {
			continue
		}
		fsm.dispatchConvertingFields(info, commonpb.TargetType_TARGET_TYPE_ACCOUNT, info.MetadataSchema.AccountFields)
		fsm.dispatchConvertingFields(info, commonpb.TargetType_TARGET_TYPE_TRANSACTION, info.MetadataSchema.TransactionFields)
	}
}

func (fsm *Machine) dispatchConvertingFields(info *commonpb.LedgerInfo, targetType commonpb.TargetType, fields map[string]*commonpb.MetadataFieldSchema) {
	for key, field := range fields {
		if field.Status == commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING {
			select {
			case fsm.metadataConvertRequestCh <- MetadataConvertRequest{
				LedgerName: info.Name,
				TargetType: targetType,
				Key:        key,
				Type:       field.Type,
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

// ClosingPeriod returns the period currently in CLOSING state, or nil.
// Used for crash recovery on startup.
func (fsm *Machine) ClosingPeriod() *commonpb.Period {
	return fsm.Periods.ClosingPeriod()
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
func (fsm *Machine) checkClosePeriod(result *ApplyResult) *SealRequest {
	if result == nil {
		return nil
	}
	for _, logOrRef := range result.Logs {
		if created := logOrRef.GetCreatedLog(); created != nil {
			if closePeriodLog := created.Payload.GetClosePeriod(); closePeriodLog != nil {
				return SealRequestFromPeriod(closePeriodLog.ClosedPeriod)
			}
		}
	}
	return nil
}

func SealRequestFromPeriod(period *commonpb.Period) *SealRequest {
	return &SealRequest{
		PeriodID:      period.Id,
		CloseSequence: period.CloseSequence,
		LastLogHash:   period.LastLogHash,
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

	// OnCheckpointDone is called by Node once the Pebble checkpoint has been created.
	// It forges a SealRequest and sends it to the sealer.  Nil when CheckpointRequired is false.
	OnCheckpointDone func(checkpointPath string)
}
