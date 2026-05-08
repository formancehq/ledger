package state

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/bloom"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/keystore"
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

type Machine struct {
	logger    logging.Logger
	dataStore *dal.Store

	mu sync.Mutex

	// Composed subsystems
	Registry *StateRegistry // KeyStores + Cache + Attrs
	Periods  *PeriodTracker // Period lifecycle

	// pendingLedgerCleanups tracks deleted ledgers whose Pebble data has not
	// yet been purged. Map key is the ledger name; value is the sequence number
	// of the DeleteLedger log. Data is cleaned up when the purge range covers
	// the delete sequence.
	pendingLedgerCleanups map[string]uint64

	// FSM mechanics
	nextSequenceID                 uint64
	nextAuditSequenceID            uint64
	nextQueryCheckpointID          uint64
	queryCheckpointSchedule        string
	queryCheckpointScheduleChanged signal.Signal
	lastLogHash                    []byte

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

	// accountMigrateRequestCh receives migration requests when a StartAccountMigration
	// log is applied. The AccountMigrator reads from this channel to perform
	// background account address migration.
	accountMigrateRequestCh chan AccountMigrateRequest

	// coldCompactionCh signals the SmartCompactor that a period purge has been applied,
	// meaning the cold zone [0x01, 0xF1) contains fresh tombstones that benefit from compaction.
	coldCompactionCh chan struct{}

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
	lastPersistedIndex        atomic.Uint64

	// sentinelMode enables runtime volume consistency checks
	// (monotonicity, delta/posting cross-check, post-commit cache/Pebble verification).
	sentinelMode bool

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
}

func NewMachine(logger logging.Logger, dataStore *dal.Store, meter metric.Meter, cache *cache.Cache, attrs *attributes.Attributes, ks *keystore.KeyStore, sharedState *SharedState, eventNotifier Notifier, mirrorNotifier Notifier, indexNotifier Notifier, bloomFilters *bloom.FilterSet, numscriptCacheSize int, sentinelMode bool, idempotencyTTLMicros uint64) (*Machine, error) {
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
		dataStore:                      dataStore,
		BloomFilters:                   bloomFilters,
		sentinelMode:                   sentinelMode,
		logsAppendedCounter:            logsAppendedCounter,
		rotationDurationHistogram:      rotationDurationHistogram,
		batchCommitHistogram:           batchCommitHistogram,
		processor:                      processor,
		eventNotifier:                  eventNotifier,
		mirrorNotifier:                 mirrorNotifier,
		indexNotifier:                  indexNotifier,
		keyStore:                       ks,
		sharedState:                    sharedState,
		Registry:                       newStateRegistryWithIdempotency(cache, attrs, idempotencyTTLMicros),
		Periods:                        NewPeriodTracker(nil, nil, nil, 0, ""),
		nextSequenceID:                 1,
		nextAuditSequenceID:            1,
		queryCheckpointScheduleChanged: signal.New(),
		sealRequestCh:                  make(chan SealRequest, 10),
		archiveRequestCh:               make(chan ArchiveRequest, 1),
		metadataConvertRequestCh:       make(chan MetadataConvertRequest, 16),
		accountMigrateRequestCh:        make(chan AccountMigrateRequest, 16),
		coldCompactionCh:               make(chan struct{}, 1),
	}
	fsm.appliedCond = sync.NewCond(&fsm.appliedMu)
	fsm.cacheSnapshotter = NewCacheSnapshotter(logger, dataStore, fsm.Registry, bloomFilters)

	if err := fsm.RecoverState(); err != nil {
		return nil, fmt.Errorf("recovering state: %w", err)
	}

	return fsm, nil
}

// RecoverState loads all FSM in-memory state from the Pebble data store.
// Called on restart (via RecoverAndReplay) and after follower sync
// (via reloadStateFromStore).
func (fsm *Machine) RecoverState() error {
	// Recover lastAppliedIndex from Pebble
	lastAppliedIndex, err := query.ReadLastAppliedIndex(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering last applied index: %w", err)
	}

	fsm.lastAppliedIndex = lastAppliedIndex
	fsm.lastPersistedIndex.Store(lastAppliedIndex)

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

	// Recover nextQueryCheckpointID from persisted counter
	nextQCPID, err := query.ReadNextQueryCheckpointID(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering next query checkpoint ID: %w", err)
	}

	fsm.nextQueryCheckpointID = nextQCPID

	// Recover query checkpoint schedule
	qcpSchedule, err := query.ReadQueryCheckpointSchedule(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering query checkpoint schedule: %w", err)
	}

	fsm.queryCheckpointSchedule = qcpSchedule

	// Recover lastAppliedTimestamp from Pebble
	lastAppliedTimestamp, err := query.ReadLastAppliedTimestamp(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering last applied timestamp: %w", err)
	}

	fsm.lastAppliedTimestamp = lastAppliedTimestamp

	// Recover periods from Pebble
	periodsCursor, err := query.ReadPeriods(context.Background(), fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering periods: %w", err)
	}

	periodsFromStore, err := dal.Collect(periodsCursor)
	if err != nil {
		return fmt.Errorf("collecting periods: %w", err)
	}

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

	nextPeriodID, err := query.ReadNextPeriodID(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering next period ID: %w", err)
	}

	fsm.Periods.Reset(allPeriods, currentOpenPeriod, closingPeriods, nextPeriodID)

	// Recover period schedule from Pebble
	periodSchedule, err := query.ReadPeriodSchedule(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering period schedule: %w", err)
	}

	fsm.Periods.SetSchedule(periodSchedule)

	// Recover reversions from Pebble
	reversions, err := query.ReadReversions(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering reversions: %w", err)
	}

	fsm.Registry.Reversions = reversions

	// Recover pending ledger cleanups from Pebble
	pendingCleanups, err := query.ReadPendingLedgerCleanups(fsm.dataStore)
	if err != nil {
		return fmt.Errorf("recovering pending ledger cleanups: %w", err)
	}

	fsm.pendingLedgerCleanups = pendingCleanups

	// Recover signing keys from Pebble
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

	// Recover shared runtime flags from Pebble
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

	fsm.logger.WithFields(map[string]any{
		"nextSequenceID":        fsm.nextSequenceID,
		"nextAuditSequenceID":   fsm.nextAuditSequenceID,
		"nextQueryCheckpointID": fsm.nextQueryCheckpointID,
		"hasLogHash":            len(fsm.lastLogHash) > 0,
		"periodCount":           len(allPeriods),
		"reversionLedgers":      len(reversions),
		"pendingCleanups":       len(pendingCleanups),
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

	persistedIdx := fsm.lastPersistedIndex.Load()
	if persistedIdx != fsm.lastAppliedIndex {
		fsm.logger.WithFields(map[string]any{
			"lastPersistedIndex": persistedIdx,
			"lastAppliedIndex":   fsm.lastAppliedIndex,
			"snapshotIndex":      fsm.snapshotIndex,
			"entryCount":         len(entries),
			"firstEntryIndex":    entries[0].Index,
			"gen0":               fsm.Registry.Cache.BaseIndex.Gen0,
			"gen1":               fsm.Registry.Cache.BaseIndex.Gen1,
			"currentGeneration":  fsm.Registry.Cache.CurrentGeneration(),
		}).Errorf("ApplyEntries: lastPersistedIndex != lastAppliedIndex")
	}

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
	var pendingAccountMigrateRequests []AccountMigrateRequest

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
			if fsm.logger.Enabled(logging.DebugLevel) {
				fsm.logger.WithFields(map[string]any{
					"entryIndex":        entry.Index,
					"currentGeneration": fsm.Registry.Cache.CurrentGeneration(),
					"gen0":              fsm.Registry.Cache.BaseIndex.Gen0,
					"gen1":              fsm.Registry.Cache.BaseIndex.Gen1,
				}).Debugf("Cache generation rotated")
			}
			rotationStart := time.Now()

			// Persist rotation to the 0xFF cache zone: purge old gen1, update metadata.
			if err := writeCacheRotation(
				batch,
				fsm.Registry.Cache.CurrentGeneration(),
				fsm.Registry.Cache.BaseIndex.Gen0,
				fsm.Registry.Cache.BaseIndex.Gen1,
			); err != nil {
				return nil, fmt.Errorf("writing cache rotation: %w", err)
			}

			fsm.rotationDurationHistogram.Record(context.Background(), time.Since(rotationStart).Microseconds())
		}

		fsm.lastAppliedIndex++

		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}

		cmd.ResetVT()

		if err := cmd.UnmarshalVT(entry.Data); err != nil {
			return nil, err
		}

		// Skip applyProposal for system-only proposals with no orders AND
		// no sink/mirror updates. Proposals carrying MirrorSyncUpdates or
		// EventsSinkUpdates must still go through applyProposal so that
		// cursor and status writes reach the Pebble batch.
		if len(cmd.GetOrders()) == 0 && len(cmd.GetMirrorSyncUpdates()) == 0 && len(cmd.GetEventsSinkUpdates()) == 0 && cmd.GetIdempotencyEviction() == nil {
			ret.Results = append(ret.Results, ApplyResult{ProposalID: cmd.GetId(), AppliedIndex: entry.Index})

			continue
		}

		result, err := fsm.applyProposal(ctx, entry.Index, batch, cmd)
		if err != nil {
			return nil, err
		}

		result.AppliedIndex = entry.Index

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
		pendingAccountMigrateRequests = append(pendingAccountMigrateRequests, result.AccountMigrateRequests...)

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

		// If CreateQueryCheckpoint was detected, stop processing and enter gating.
		// The Applier will spool remaining entries and create the main store checkpoint.
		// The read index checkpoint is created later by the index builder.
		if cpID := result.QueryCheckpointCreated; cpID > 0 {
			ret.QueryCheckpointID = cpID

			return fsm.commitAndRequestCheckpoint(batch, ret, entries[i+1:], needsArchiveDispatch, 0, nil)
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
	// key, only the last entry's value survives in Pebble (Set overwrites in
	// place). We must deduplicate by canonical key, keeping only the latest
	// update, before comparing with Pebble.
	if fsm.sentinelMode {
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

	previousPersisted := fsm.lastPersistedIndex.Load()
	fsm.lastPersistedIndex.Store(fsm.lastAppliedIndex)
	fsm.appliedCond.Broadcast()

	if fsm.lastAppliedIndex != previousPersisted+uint64(len(entries)) {
		if fsm.logger.Enabled(logging.DebugLevel) {
			fsm.logger.WithFields(map[string]any{
				"previousPersisted": previousPersisted,
				"newPersisted":      fsm.lastAppliedIndex,
				"entryCount":        len(entries),
				"gen0":              fsm.Registry.Cache.BaseIndex.Gen0,
				"gen1":              fsm.Registry.Cache.BaseIndex.Gen1,
				"currentGeneration": fsm.Registry.Cache.CurrentGeneration(),
			}).Debugf("lastPersistedIndex updated (non-trivial jump)")
		}
	}

	if needsArchiveDispatch {
		fsm.dispatchArchiveRequests()
	}

	// Clean up physical files for deleted query checkpoints.
	for _, r := range ret.Results {
		if cpID := r.QueryCheckpointDeleted; cpID > 0 {
			fsm.deleteQueryCheckpointFiles(cpID)
		}
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

	for _, req := range pendingAccountMigrateRequests {
		select {
		case fsm.accountMigrateRequestCh <- req:
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

	// Notify all log consumers that new logs are available.
	// Without this, the index builder's fast-path check (LastSequence > cursor)
	// would never trigger for logs committed in this batch, causing
	// WaitForSequence to block indefinitely.
	lastSeq := fsm.nextSequenceID - 1
	fsm.eventNotifier.NotifyLogsCommitted(lastSeq)
	fsm.mirrorNotifier.NotifyLogsCommitted(lastSeq)
	fsm.indexNotifier.NotifyLogsCommitted(lastSeq)

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

// deleteQueryCheckpointFiles removes the physical files for a deleted checkpoint.
// Called after the batch containing the DeleteQueryCheckpoint metadata removal is committed.
func (fsm *Machine) deleteQueryCheckpointFiles(checkpointID uint64) {
	if err := fsm.dataStore.DeleteQueryCheckpointFiles(checkpointID); err != nil {
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
func (fsm *Machine) Preload(preloadSet *raftcmdpb.PreloadSet, batch *dal.Batch, genByte byte) error {
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
			"generationThreshold": fsm.Registry.Cache.GenerationThreshold,
			"lastAppliedIndex":    fsm.lastAppliedIndex,
			"preloadCount":        len(preloadSet.GetPreloads()),
			"touchCount":          len(preloadSet.GetTouches()),
		}
		fsm.logger.WithFields(details).Errorf("Preload boundary mismatch: LastPersistedIndex does not match Gen0 or Gen1")
		assert.Unreachable("preload boundary mismatch should be prevented by predicted_index check", details)

		return fmt.Errorf("preloading preloaded index is invalid: lastPersistedIndex=%d gen0=%d gen1=%d currentGen=%d lastApplied=%d",
			preloadSet.GetLastPersistedIndex(),
			fsm.Registry.Cache.BaseIndex.Gen0,
			fsm.Registry.Cache.BaseIndex.Gen1,
			fsm.Registry.Cache.CurrentGeneration(),
			fsm.lastAppliedIndex,
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

	// Handle idempotency eviction (deterministic cleanup via Raft)
	if eviction := proposal.GetIdempotencyEviction(); eviction != nil {
		evicted, err := fsm.Registry.Idempotency.EvictBefore(batch, fsm.dataStore, eviction.GetCutoffMicros())
		if err != nil {
			return nil, fmt.Errorf("evicting idempotency keys: %w", err)
		}

		if evicted > 0 {
			fsm.logger.Infof("Evicted %d expired idempotency keys (cutoff=%d)", evicted, eviction.GetCutoffMicros())
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

	// Reject proposals whose predicted index doesn't match the actual Raft index.
	// This detects stale proposals admitted with an inflated IndexTracker (e.g.
	// after leadership transition). The preloadSet is invalid — reject cleanly
	// so the caller retries with fresh preloads.
	if predicted := proposal.GetPredictedIndex(); predicted != 0 && predicted != raftIndex {
		if fsm.logger.Enabled(logging.DebugLevel) {
			fsm.logger.WithFields(map[string]any{
				"predictedIndex": predicted,
				"actualIndex":    raftIndex,
				"proposalID":     proposal.GetId(),
			}).Debugf("Rejecting proposal: predicted index mismatch (stale tracker)")
		}

		lifecycle.SendEvent("stale_proposal_rejected", map[string]any{
			"predictedIndex": predicted,
			"actualIndex":    raftIndex,
		})

		return &ApplyResult{
			ProposalID: proposal.GetId(),
			Error:      &domain.BusinessError{Err: domain.ErrStaleProposal},
		}, nil
	}

	genByte := byte(fsm.Registry.Cache.CurrentGeneration() % 2)

	if err := fsm.Preload(proposal.GetPreload(), batch, genByte); err != nil {
		return nil, fmt.Errorf("raftIndex=%d: %w", raftIndex, err)
	}

	// Compute the effective date using the HLC to guarantee monotonicity
	effectiveDate := fsm.hlcTimestamp(proposal.GetDate())

	if err := fsm.ensurePeriodBootstrapped(effectiveDate, batch); err != nil {
		return nil, err
	}

	// Create buffer for this proposal
	buffer := NewBuffer(effectiveDate, fsm)

	// Process the proposal
	logs, err := fsm.processor.ProcessOrders(proposal.GetOrders(), buffer)
	if err != nil {
		// FAILURE: write audit entry and return business error
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

		return &ApplyResult{
			ProposalID: proposal.GetId(),
			Error:      &domain.BusinessError{Err: err},
		}, nil
	}

	// Validate transient volumes have zero balance. This is a business error
	// (rejected proposal), not a fatal FSM error, so it must be checked before Commit.
	if err := buffer.ValidateTransientVolumes(); err != nil {
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
			return nil, fmt.Errorf("appending audit entry for transient validation failure: %w", appendErr)
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
		if err := verifyVolumeDeltasMatchPostings(buffer.AllVolumeUpdates(), createdLogs); err != nil {
			return nil, fmt.Errorf("volume delta/posting cross-check failed at raft index %d: %w", raftIndex, err)
		}

		// Global check: aggregated volumes must be balanced (input == output per asset).
		ledgerNames := collectLedgerNames(proposal.GetOrders())
		if len(ledgerNames) > 0 {
			if fsm.logger.Enabled(logging.DebugLevel) {
				fsm.logger.Debugf("Verifying aggregated volume balance for %d ledgers at raft index %d", len(ledgerNames), raftIndex)
			}

			if err := verifyAggregatedVolumesBalanced(
				fsm.dataStore, fsm.Registry.Attrs.Volume, ledgerNames, raftIndex,
			); err != nil {
				fsm.logger.Errorf("AGGREGATED VOLUME BALANCE CHECK FAILED: %v", err)

				return nil, fmt.Errorf("aggregated volume balance check failed: %w", err)
			}
		}
	}

	// SUCCESS: write audit entry with batch-level side effects.
	minLogSeq, maxLogSeq := extractLogSequenceRange(logs)
	auditSuccess := &auditpb.AuditSuccess{
		MinLogSequence: minLogSeq,
		MaxLogSequence: maxLogSeq,
	}

	if ta := buffer.TransientAccounts(); len(ta) > 0 {
		auditSuccess.TransientAccounts = make(map[string]*auditpb.AccountList, len(ta))
		for ledger, accounts := range ta {
			auditSuccess.TransientAccounts[ledger] = &auditpb.AccountList{Accounts: accounts}
		}
	}

	if pa := buffer.PurgedAccounts(); len(pa) > 0 {
		auditSuccess.PurgedAccounts = make(map[string]*auditpb.AccountList, len(pa))
		for ledger, accounts := range pa {
			auditSuccess.PurgedAccounts[ledger] = &auditpb.AccountList{Accounts: accounts}
		}
	}

	auditEntry := &auditpb.AuditEntry{
		Sequence:   fsm.nextAuditSequenceID,
		Timestamp:  effectiveDate,
		ProposalId: proposal.GetId(),
		Orders:     proposal.GetOrders(),
		Outcome: &auditpb.AuditEntry_Success{
			Success: auditSuccess,
		},
	}
	fsm.nextAuditSequenceID++

	if err := AppendAuditEntries(batch, auditEntry); err != nil {
		return nil, fmt.Errorf("appending audit entry for success: %w", err)
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
		AccountMigrateRequests:  buffer.AccountMigrateRequests(),
		volumeUpdates:           buffer.KeptVolumeUpdates(),
		purgedVolumeKeys:        buffer.PurgedVolumeKeys(),
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

// RestoreCacheFromStore delegates to the CacheSnapshotter.
// Kept as a Machine method for external callers (e.g. applier).
func (fsm *Machine) RestoreCacheFromStore() error {
	return fsm.cacheSnapshotter.RestoreFromStore()
}

// Close stops background work owned by the Machine (e.g. bloom populate).
func (fsm *Machine) Close() {
	fsm.cacheSnapshotter.Stop()
}

func (fsm *Machine) InstallSnapshot(_ context.Context, snapshot raftpb.Snapshot) error {
	fsm.snapshotIndex = snapshot.Metadata.Index

	// Reset the cache — it will be restored from Pebble later:
	// - On restart: after InstallSnapshot, via RestoreCacheFromStore
	// - On follower sync: after restoreCheckpoint, via RestoreCacheFromStore
	fsm.Registry.Cache.Reset()
	fsm.Registry.Idempotency.Reset()

	fsm.logger.WithFields(map[string]any{
		"snapshotIndex": snapshot.Metadata.Index,
	}).Infof("InstallSnapshot complete")

	return nil
}

func (fsm *Machine) SynchronizeWithLeader(ctx context.Context, snapshotFetcher SnapshotFetcher, progress *SyncProgress) (uint64, error) {
	if err := fsm.restoreCheckpoint(ctx, snapshotFetcher, progress); err != nil {
		return 0, fmt.Errorf("restoring checkpoint from leader: %w", err)
	}

	// Restore cache from Pebble (the checkpoint contains the leader's cache data)
	if err := fsm.cacheSnapshotter.RestoreFromStore(); err != nil {
		return 0, fmt.Errorf("restoring cache after sync: %w", err)
	}

	// Reload all FSM state from Pebble (the checkpoint contains the leader's state).
	// This also recovers lastAppliedIndex from the restored Pebble — the fresh
	// checkpoint is at an index >= snapshotIndex, so spool replay correctly
	// skips entries already in the checkpoint.
	// Hold mu because concurrent readers (e.g. QueryCheckpointScheduler) access
	// fields like queryCheckpointSchedule under the same lock.
	fsm.mu.Lock()
	err := fsm.RecoverState()
	fsm.mu.Unlock()

	if err != nil {
		return 0, fmt.Errorf("recovering state after sync: %w", err)
	}

	return fsm.lastAppliedIndex, nil
}

// restoreCheckpoint fetches a fresh checkpoint from the leader and restores it.
// The fetch writes to a staging directory ("incoming") that cannot collide with
// the numbered checkpoints created by the background goroutine. After the fetch,
// ActivateIncomingRestore atomically reserves a checkpoint ID and renames.
func (fsm *Machine) restoreCheckpoint(ctx context.Context, snapshotFetcher SnapshotFetcher, progress *SyncProgress) error {
	fsm.logger.Infof("Fetching fresh checkpoint from leader")

	// Prepare a staging directory outside the numbered checkpoint space.
	incomingDir, err := fsm.dataStore.PrepareIncomingRestore()
	if err != nil {
		return fmt.Errorf("preparing incoming restore: %w", err)
	}

	// Fetch a fresh checkpoint from the leader into the staging directory.
	size, hash, err := snapshotFetcher.FetchSnapshot(ctx, incomingDir, progress)
	if err != nil {
		return fmt.Errorf("fetching snapshot from leader: %w", err)
	}

	fsm.logger.WithFields(map[string]any{
		"size": size,
		"hash": hash,
	}).Infof("Checkpoint fetched from leader")

	// Move the incoming checkpoint to a numbered slot (holds snapshotMu briefly).
	checkpointID, err := fsm.dataStore.ActivateIncomingRestore()
	if err != nil {
		return fmt.Errorf("activating incoming checkpoint: %w", err)
	}

	// Restore the checkpoint (closes DB, hard-links to live/, reopens).
	if err := fsm.dataStore.RestoreCheckpoint(checkpointID); err != nil {
		return fmt.Errorf("restoring checkpoint: %w", err)
	}

	fsm.logger.WithFields(map[string]any{
		"checkpointId": checkpointID,
	}).Infof("Checkpoint restored successfully")

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

// AccountMigrateRequestCh returns the channel used to dispatch account
// migration requests to the AccountMigrator.
func (fsm *Machine) AccountMigrateRequestCh() chan AccountMigrateRequest {
	return fsm.accountMigrateRequestCh
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
	handle, err := fsm.dataStore.NewReadHandle()
	if err != nil {
		fsm.logger.Errorf("Failed to create read handle for metadata conversion recovery: %v", err)

		return
	}

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

// dispatchAccountMigrationRequests iterates all ledgers and dispatches
// migration requests for account types still in MIGRATING status.
// Called on leadership acquisition to recover incomplete migrations.
func (fsm *Machine) dispatchAccountMigrationRequests() {
	handle, err := fsm.dataStore.NewReadHandle()
	if err != nil {
		fsm.logger.Errorf("Failed to create read handle for account migration recovery: %v", err)

		return
	}
	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadLedgers(context.Background(), handle)
	if err != nil {
		fsm.logger.Errorf("Failed to read ledgers for account migration recovery: %v", err)

		return
	}
	defer func() { _ = cursor.Close() }()

	for {
		info, err := cursor.Next()
		if err != nil {
			break
		}

		if info.GetDeletedAt() != nil {
			continue
		}

		for _, at := range info.GetAccountTypes() {
			if at.GetStatus() == commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING && at.GetMigration() != nil {
				select {
				case fsm.accountMigrateRequestCh <- AccountMigrateRequest{
					LedgerName:      info.GetName(),
					AccountTypeName: at.GetName(),
					OldPattern:      at.GetPattern(),
					TargetPattern:   at.GetMigration().GetTargetPattern(),
				}:
				default:
				}
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

	// Recover account types stuck in MIGRATING status: if the previous leader
	// crashed mid-migration, the new leader retries automatically.
	fsm.dispatchAccountMigrationRequests()
}

// ensurePeriodBootstrapped creates the first period deterministically at the
// first proposal. The period start timestamp is derived from the proposal's
// effective date so that all nodes produce the same deterministic state.
func (fsm *Machine) ensurePeriodBootstrapped(effectiveDate *commonpb.Timestamp, batch *dal.Batch) error {
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

	return fsm.queryCheckpointSchedule
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
	fsm.queryCheckpointSchedule = s
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
	AppliedIndex            uint64 // Raft index at which this entry was applied
	Logs                    []*raftcmdpb.CreatedLogOrReference
	Error                   error
	CheckpointPath          string // Set by Node after checkpoint creation (ClosePeriod proposals)
	ConfigChanged           bool   // True when sink configuration changed
	MirrorConfigChanged     bool   // True when mirror ledger configuration changed
	HasArchiveRequests      bool   // True when there are pending archive requests
	HasPurges               bool   // True when cold zone data was purged (triggers cold compaction)
	MetadataConvertRequests []MetadataConvertRequest
	AccountMigrateRequests  []AccountMigrateRequest

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
}

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

	// QueryCheckpointID is set when the checkpoint was triggered by a
	// CreateQueryCheckpointOrder (not a ClosePeriod). The Applier uses this
	// to create the main store checkpoint. The read index checkpoint is
	// created separately by the index builder when it processes the log.
	QueryCheckpointID uint64
}
