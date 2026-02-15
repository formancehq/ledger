package state

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/service/kv"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
)

type Machine struct {
	logger    logging.Logger
	dataStore *data.Store

	mu sync.Mutex

	// Attributes for writing to PebbleDB (each Machine has its own instance)
	Attrs *attributes.Attributes

	Cache           *cache.Cache
	Volumes         *attributes.KeyStore[data.VolumeKey, *raftcmdpb.VolumePair]
	AccountMetadata *attributes.KeyStore[data.MetadataKey, *commonpb.MetadataValue]
	LedgerMetadata  *attributes.KeyStore[data.LedgerMetadataKey, *commonpb.MetadataValue]
	Reversions      *attributes.KeyStore[data.TransactionKey, bool]
	IdempotencyKeys *attributes.KeyStore[data.IdempotencyKey, *commonpb.IdempotencyKeyValue]
	References      *attributes.KeyStore[data.TransactionReferenceKey, *commonpb.TransactionReferenceValue]
	Ledgers         *attributes.KeyStore[data.LedgerKey, *commonpb.LedgerInfo]
	Boundaries      *attributes.KeyStore[data.LedgerKey, *raftcmdpb.LedgerBoundaries]

	nextLedgerID        uint32
	nextSequenceID      uint64
	nextAuditSequenceID uint64
	lastLogHash         []byte
	lastCheckpointID    uint64

	lastAppliedIndex            uint64
	lastAppliedTimestamp        uint64
	snapshotIndex               uint64
	generationRotationThreshold uint64
	auditEnabled                bool

	// RequestProcessor handles business logic
	processor *processing.RequestProcessor

	// dirtyVolumeKeys tracks canonical key bytes written during each generation.
	// [0]=current gen, [1]=previous gen, [2]=gen before (consumed at compaction).
	dirtyVolumeKeys [3]map[string]struct{}

	// dirtyBoundaryKeys tracks boundary canonical keys that have been updated
	// since the last generation rotation. Boundaries are flushed to Pebble only
	// at rotation time instead of on every log entry.
	dirtyBoundaryKeys map[string]*raftcmdpb.LedgerBoundaries

	// Metrics
	logsAppendedCounter metric.Int64Counter
	lastPersistedIndex  atomic.Uint64
}

func NewMachine(logger logging.Logger, dataStore *data.Store, meter metric.Meter, cache *cache.Cache, attrs *attributes.Attributes, generationRotationThreshold uint64, auditEnabled bool) (*Machine, error) {
	lastAppliedIndex, err := dataStore.GetLastAppliedIndex()
	if err != nil {
		return nil, err
	}

	lastAppliedTimestamp, err := dataStore.GetLastAppliedTimestamp()
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

	processor, err := processing.NewRequestProcessor(meter)
	if err != nil {
		return nil, fmt.Errorf("creating request processor: %w", err)
	}

	fsm := &Machine{
		logger:                      logger,
		dataStore:                   dataStore,
		lastAppliedIndex:            lastAppliedIndex,
		lastAppliedTimestamp:        lastAppliedTimestamp,
		generationRotationThreshold: generationRotationThreshold,
		logsAppendedCounter:         logsAppendedCounter,
		processor:                   processor,
		auditEnabled:                auditEnabled,
		Attrs:                       attrs,
		Cache:                       cache,
		Volumes: attributes.NewKeyStore[data.VolumeKey, *raftcmdpb.VolumePair](
			attributes.DefaultKeys,
			cache.Volumes,
		),
		AccountMetadata: attributes.NewKeyStore[data.MetadataKey, *commonpb.MetadataValue](
			attributes.DefaultKeys,
			cache.AccountMetadata,
		),
		LedgerMetadata: attributes.NewKeyStore[data.LedgerMetadataKey, *commonpb.MetadataValue](
			attributes.DefaultKeys,
			cache.LedgerMetadata,
		),
		Reversions: attributes.NewKeyStore[data.TransactionKey, bool](
			attributes.DefaultKeys,
			cache.Reversions,
		),
		IdempotencyKeys: attributes.NewKeyStore[data.IdempotencyKey, *commonpb.IdempotencyKeyValue](
			attributes.DefaultKeys,
			cache.IdempotencyKeys,
		),
		References: attributes.NewKeyStore[data.TransactionReferenceKey, *commonpb.TransactionReferenceValue](
			attributes.DefaultKeys,
			cache.References,
		),
		Ledgers: attributes.NewKeyStore[data.LedgerKey, *commonpb.LedgerInfo](
			attributes.DefaultKeys,
			cache.Ledgers,
		),
		Boundaries: attributes.NewKeyStore[data.LedgerKey, *raftcmdpb.LedgerBoundaries](
			attributes.DefaultKeys,
			cache.Boundaries,
		),
		nextLedgerID:        1,
		nextSequenceID:      1,
		nextAuditSequenceID: 0,
		dirtyVolumeKeys: [3]map[string]struct{}{
			make(map[string]struct{}),
			make(map[string]struct{}),
			make(map[string]struct{}),
		},
		dirtyBoundaryKeys: make(map[string]*raftcmdpb.LedgerBoundaries),
	}

	return fsm, nil
}

func (fsm *Machine) LastPersistedIndex() (uint64, error) {
	return fsm.lastPersistedIndex.Load(), nil
}

func (fsm *Machine) ApplyEntries(ctx context.Context, entries ...raftpb.Entry) ([]ApplyResult, error) {
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
	ret := make([]ApplyResult, 0, len(entries))

	for _, entry := range entries {
		if entry.Index <= fsm.lastAppliedIndex {
			ret = append(ret, ApplyResult{})
			continue
		}
		if entry.Index > fsm.lastAppliedIndex+1 {
			return nil, &ErrInvalidEntryIndex{
				ReceivedIndex: entry.Index,
				ExpectedIndex: fsm.lastAppliedIndex + 1,
			}
		}

		if rotated, oldGen1BaseIndex := fsm.Cache.CheckRotationNeeded(fsm.lastAppliedIndex); rotated {
			// Rotate dirty key tracking: consume slot[2], shift down, allocate new slot[0]
			keysToCompact := fsm.dirtyVolumeKeys[2]
			fsm.dirtyVolumeKeys[2] = fsm.dirtyVolumeKeys[1]
			fsm.dirtyVolumeKeys[1] = fsm.dirtyVolumeKeys[0]
			fsm.dirtyVolumeKeys[0] = make(map[string]struct{})

			// Compaction using tracked keys in the same batch (no Pebble scan)
			if err := fsm.compactVolumeDiffs(batch, oldGen1BaseIndex, keysToCompact); err != nil {
				return nil, fmt.Errorf("compacting volume diffs: %w", err)
			}

			// Flush dirty boundaries to Pebble at the rotation boundary index
			if err := fsm.flushBoundaries(batch, fsm.Cache.BaseIndex.Gen0); err != nil {
				return nil, fmt.Errorf("flushing boundaries at rotation: %w", err)
			}
		}
		fsm.lastAppliedIndex++

		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}

		cmd.Reset()
		if err := proto.Unmarshal(entry.Data, cmd); err != nil {
			return nil, err
		}

		result, err := fsm.applyProposal(ctx, entry.Index, batch, cmd)
		if err != nil {
			return nil, err
		}
		ret = append(ret, *result)
	}

	if err := batch.SetAppliedIndex(fsm.lastAppliedIndex); err != nil {
		return nil, fmt.Errorf("setting applied index: %w", err)
	}
	if err := batch.SetLastAppliedTimestamp(fsm.lastAppliedTimestamp); err != nil {
		return nil, fmt.Errorf("setting last applied timestamp: %w", err)
	}
	if err := batch.Commit(); err != nil {
		return nil, fmt.Errorf("committing batch: %w", err)
	}

	fsm.lastPersistedIndex.Store(fsm.lastAppliedIndex)

	return ret, nil
}

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
// dirtyKeys contain the canonical keys written during the generation being
// compacted. This eliminates the need for a full Pebble prefix scan (List()).
func (fsm *Machine) compactVolumeDiffs(batch *data.Batch, compactionIndex uint64, dirtyKeys map[string]struct{}) error {
	for keyStr := range dirtyKeys {
		canonicalKey := []byte(keyStr)
		if err := fsm.Attrs.Volume.DeleteOldest(batch, compactionIndex, canonicalKey); err != nil {
			return fmt.Errorf("compacting volume: %w", err)
		}
	}
	return nil
}

// flushBoundaries writes all dirty boundary keys to Pebble and clears the tracking map.
// Called at generation rotation to batch boundary writes instead of writing on every log entry.
func (fsm *Machine) flushBoundaries(batch *data.Batch, index uint64) error {
	for keyStr, value := range fsm.dirtyBoundaryKeys {
		canonicalKey := []byte(keyStr)
		if err := fsm.Attrs.Boundary.SetBase(batch, index, canonicalKey, value); err != nil {
			return fmt.Errorf("setting boundary base: %w", err)
		}
		if err := fsm.Attrs.Boundary.DeleteOldest(batch, index, canonicalKey); err != nil {
			return fmt.Errorf("compacting old boundary: %w", err)
		}
	}
	clear(fsm.dirtyBoundaryKeys)
	return nil
}

// Preload applies preloaded data to the Machine's volatile state.
func (fsm *Machine) Preload(preloadSet *raftcmdpb.PreloadSet) error {

	if preloadSet == nil || len(preloadSet.Preloads) == 0 {
		return nil
	}

	// The preloads must be for the gen0 or the gen1
	// This is the role of the admission to ensure this invariant
	switch preloadSet.LastPersistedIndex {
	case fsm.Cache.BaseIndex.Gen0:
		fsm.logger.Debug("Selecting cache generation 0")
	case fsm.Cache.BaseIndex.Gen1:
		fsm.logger.Debug("Selecting cache generation 1")
	default:
		return &ErrGenerationMismatch{
			LastPersistedIndex: preloadSet.LastPersistedIndex,
			Gen0BaseIndex:      fsm.Cache.BaseIndex.Gen0,
			Gen1BaseIndex:      fsm.Cache.BaseIndex.Gen1,
		}
	}

	// Helper function to put a preloaded volume pair into a cache generation
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
				value.Data.InputKnown = pair.InputKnown
				if value.Data.InputDiff != nil {
					value.Data.InputKnown = commonpb.NewBigInt(
						new(big.Int).Add(value.Data.InputKnown.Value(), value.Data.InputDiff.Value()),
					)
				}
			}
			// If OutputKnown is not yet set, merge preloaded output with any existing diff
			if value.Data.OutputKnown == nil && pair.OutputKnown != nil {
				value.Data.OutputKnown = pair.OutputKnown
				if value.Data.OutputDiff != nil {
					value.Data.OutputKnown = commonpb.NewBigInt(
						new(big.Int).Add(value.Data.OutputKnown.Value(), value.Data.OutputDiff.Value()),
					)
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

	// Helper function to put a preloaded boolean into a cache generation
	putInCacheBool := func(
		kv kv.KV[attributes.U128, attributes.Entry[bool]],
		attrID *raftcmdpb.AttributeID,
		value bool,
	) bool {
		id := attributes.U128FromBytes(attrID.Id)

		fsm.logger.WithFields(map[string]any{
			"id":    id.Hex(),
			"value": value,
		}).Debugf("Preload bool")

		existing, ok := kv.Get(id)
		if ok {
			return existing.Data
		}

		kv.Put(id, attributes.Entry[bool]{
			Tag:  attrID.Tag,
			Data: value,
		})

		return value
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

	// todo: handle metadata preload
	for _, preload := range preloadSet.GetPreloads() {
		switch preloadType := preload.Type.(type) {
		case *raftcmdpb.Preload_Volume:
			pair := &raftcmdpb.VolumePair{
				InputKnown:  preloadType.Volume.Input,
				OutputKnown: preloadType.Volume.Output,
			}
			if preloadSet.LastPersistedIndex == fsm.Cache.BaseIndex.Gen1 {
				aggregated := putInCacheVolumePair(fsm.Cache.Volumes.Gen1, preloadType.Volume.Id, pair)
				putInCacheVolumePair(fsm.Cache.Volumes.Gen0, preloadType.Volume.Id, aggregated)
			} else {
				putInCacheVolumePair(fsm.Cache.Volumes.Gen0, preloadType.Volume.Id, pair)
			}

		case *raftcmdpb.Preload_Reverted:
			if preloadSet.LastPersistedIndex == fsm.Cache.BaseIndex.Gen1 {
				value := putInCacheBool(fsm.Cache.Reversions.Gen1, preloadType.Reverted.Id, preloadType.Reverted.Reverted)
				putInCacheBool(fsm.Cache.Reversions.Gen0, preloadType.Reverted.Id, value)
			} else {
				putInCacheBool(fsm.Cache.Reversions.Gen0, preloadType.Reverted.Id, preloadType.Reverted.Reverted)
			}

		case *raftcmdpb.Preload_IdempotencyKey:
			idempotencyValue := &commonpb.IdempotencyKeyValue{
				LogSequence: preloadType.IdempotencyKey.LogSequence,
				Hash:        preloadType.IdempotencyKey.Hash,
			}
			if preloadSet.LastPersistedIndex == fsm.Cache.BaseIndex.Gen1 {
				value := putInCacheIdempotencyValue(fsm.Cache.IdempotencyKeys.Gen1, preloadType.IdempotencyKey.Id, idempotencyValue)
				putInCacheIdempotencyValue(fsm.Cache.IdempotencyKeys.Gen0, preloadType.IdempotencyKey.Id, value)
			} else {
				putInCacheIdempotencyValue(fsm.Cache.IdempotencyKeys.Gen0, preloadType.IdempotencyKey.Id, idempotencyValue)
			}

		case *raftcmdpb.Preload_Ledger:
			if preloadSet.LastPersistedIndex == fsm.Cache.BaseIndex.Gen1 {
				value := putInCacheLedger(fsm.Cache.Ledgers.Gen1, preloadType.Ledger.Id, preloadType.Ledger.Info)
				putInCacheLedger(fsm.Cache.Ledgers.Gen0, preloadType.Ledger.Id, value)
			} else {
				putInCacheLedger(fsm.Cache.Ledgers.Gen0, preloadType.Ledger.Id, preloadType.Ledger.Info)
			}

		case *raftcmdpb.Preload_Boundary:
			if preloadSet.LastPersistedIndex == fsm.Cache.BaseIndex.Gen1 {
				value := putInCacheBoundary(fsm.Cache.Boundaries.Gen1, preloadType.Boundary.Id, preloadType.Boundary.Boundaries)
				putInCacheBoundary(fsm.Cache.Boundaries.Gen0, preloadType.Boundary.Id, value)
			} else {
				putInCacheBoundary(fsm.Cache.Boundaries.Gen0, preloadType.Boundary.Id, preloadType.Boundary.Boundaries)
			}

		case *raftcmdpb.Preload_TransactionReference:
			referenceValue := &commonpb.TransactionReferenceValue{
				TransactionId: preloadType.TransactionReference.TransactionId,
			}
			if preloadSet.LastPersistedIndex == fsm.Cache.BaseIndex.Gen1 {
				value := putInCacheReferenceValue(fsm.Cache.References.Gen1, preloadType.TransactionReference.Id, referenceValue)
				putInCacheReferenceValue(fsm.Cache.References.Gen0, preloadType.TransactionReference.Id, value)
			} else {
				putInCacheReferenceValue(fsm.Cache.References.Gen0, preloadType.TransactionReference.Id, referenceValue)
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

// applyProposal processes all orders in a proposal atomically.
// Uses RequestProcessor which handles rollback internally via Buffered.
func (fsm *Machine) applyProposal(ctx context.Context, raftIndex uint64, batch *data.Batch, proposal *raftcmdpb.Proposal) (*ApplyResult, error) {
	if err := fsm.Preload(proposal.Preload); err != nil {
		return nil, err
	}

	// Compute the effective date using the HLC to guarantee monotonicity
	effectiveDate := fsm.hlcTimestamp(proposal.Date)

	// Create buffer for this proposal
	buffer := NewBuffer(effectiveDate, fsm)

	// Process the proposal
	response, err := fsm.processor.ProcessProposal(proposal, buffer)
	if err != nil {
		// FAILURE: write audit entry and return business error
		if fsm.auditEnabled {
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
			if appendErr := batch.AppendAuditEntries(auditEntry); appendErr != nil {
				return nil, fmt.Errorf("appending audit entry for failure: %w", appendErr)
			}
		}

		return &ApplyResult{
			ProposalID: proposal.Id,
			Error:      &processing.BusinessError{Err: err},
		}, nil
	}

	// Extract created logs and resolve reference sequences
	var (
		createdLogs  []*commonpb.Log
		responseLogs []*commonpb.Log
	)
	for _, logOrRef := range response.Logs {
		if created := logOrRef.GetCreatedLog(); created != nil {
			createdLogs = append(createdLogs, created)
			responseLogs = append(responseLogs, created)
		} else if refSeq := logOrRef.GetReferenceSequence(); refSeq > 0 {
			// Idempotent response - fetch the existing log by sequence
			// todo: remove that here!
			// This should be fetched in admission callback
			// Limit data store interface to only writes to prevent any error regarding this point
			log, err := fsm.dataStore.GetLogBySequence(refSeq)
			if err != nil {
				return nil, fmt.Errorf("fetching referenced log %d for idempotent response: %w", refSeq, err)
			}
			responseLogs = append(responseLogs, log)
		}
	}

	// Add only created logs to buffer and merge
	buffer.PendingLogs = append(buffer.PendingLogs, createdLogs...)
	if err := buffer.Merge(raftIndex, batch); err != nil {
		return nil, err
	}

	// SUCCESS: write audit entry
	if fsm.auditEnabled {
		auditEntry := &auditpb.AuditEntry{
			Sequence:   fsm.nextAuditSequenceID,
			Timestamp:  effectiveDate,
			ProposalId: proposal.Id,
			Orders:     proposal.Orders,
			Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{
					LogSequences: extractLogSequences(responseLogs),
				},
			},
		}
		fsm.nextAuditSequenceID++
		if err := batch.AppendAuditEntries(auditEntry); err != nil {
			return nil, fmt.Errorf("appending audit entry for success: %w", err)
		}
	}

	fsm.logsAppendedCounter.Add(ctx, int64(len(createdLogs)))

	return &ApplyResult{
		ProposalID: proposal.Id,
		Logs:       responseLogs,
	}, nil
}

// CreateSnapshot creates a snapshot of the Machine state
func (fsm *Machine) CreateSnapshot(_ context.Context) ([]byte, error) {
	checkpointID, err := fsm.dataStore.CreateSnapshot()
	if err != nil {
		return nil, fmt.Errorf("creating snapshot: %w", err)
	}

	snapshot := &raftcmdpb.MemorySnapshot{
		NextLedgerId:         fsm.nextLedgerID,
		NextSequenceId:       fsm.nextSequenceID,
		LastLogHash:          fsm.lastLogHash,
		Gen0:                 serializeCacheGeneration(fsm.Cache, 0),
		Gen1:                 serializeCacheGeneration(fsm.Cache, 1),
		CheckpointId:         checkpointID,
		CurrentGeneration:    fsm.Cache.CurrentGeneration,
		LastAppliedTimestamp: fsm.lastAppliedTimestamp,
		NextAuditSequenceId: fsm.nextAuditSequenceID,
	}

	return proto.Marshal(snapshot)
}

// serializeCacheGeneration serializes either Gen0 (genIndex=0) or Gen1 (genIndex=1) from the cache
func serializeCacheGeneration(cache *cache.Cache, genIndex int) *raftcmdpb.GenerationSnapshot {
	if cache == nil {
		return nil
	}

	var (
		baseIndex           uint64
		volumeStore         kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]]
		metadataStore       kv.KV[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerMetadataStore kv.KV[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerStore         kv.KV[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]]
		boundaryStore       kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]]
		referenceStore      kv.KV[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]]
	)

	if genIndex == 0 {
		baseIndex = cache.BaseIndex.Gen0
		volumeStore = cache.Volumes.Gen0
		metadataStore = cache.AccountMetadata.Gen0
		ledgerMetadataStore = cache.LedgerMetadata.Gen0
		ledgerStore = cache.Ledgers.Gen0
		boundaryStore = cache.Boundaries.Gen0
		referenceStore = cache.References.Gen0
	} else {
		baseIndex = cache.BaseIndex.Gen1
		volumeStore = cache.Volumes.Gen1
		metadataStore = cache.AccountMetadata.Gen1
		ledgerMetadataStore = cache.LedgerMetadata.Gen1
		ledgerStore = cache.Ledgers.Gen1
		boundaryStore = cache.Boundaries.Gen1
		referenceStore = cache.References.Gen1
	}

	snapshot := &raftcmdpb.GenerationSnapshot{
		BaseIndex:      baseIndex,
		Volumes:        make([]*raftcmdpb.VolumeAttributeSnapshotEntry, 0, volumeStore.Size()),
		Metadata:       make([]*raftcmdpb.MetadataAttributeEntry, 0, metadataStore.Size()),
		LedgerMetadata: make([]*raftcmdpb.MetadataAttributeEntry, 0, ledgerMetadataStore.Size()),
		Ledgers:        make([]*raftcmdpb.LedgerAttributeEntry, 0, ledgerStore.Size()),
		Boundaries:     make([]*raftcmdpb.BoundaryAttributeEntry, 0, boundaryStore.Size()),
		References:     make([]*raftcmdpb.TransactionReferenceAttributeEntry, 0, referenceStore.Size()),
	}

	// Serialize Volumes KeyStore
	if volumeMap, ok := volumeStore.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]]); ok {
		for u128, entry := range volumeMap.Iter() {
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
	}

	// Serialize Metadata KeyStore
	if metadataMap, ok := metadataStore.(kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]); ok {
		for u128, entry := range metadataMap.Iter() {
			ksEntry := &raftcmdpb.MetadataAttributeEntry{
				Id: &raftcmdpb.AttributeID{
					Id:  u128[:],
					Tag: entry.Tag,
				},
				Value: entry.Data,
			}
			snapshot.Metadata = append(snapshot.Metadata, ksEntry)
		}
	}

	// Serialize LedgerMetadata KeyStore
	if ledgerMetadataMap, ok := ledgerMetadataStore.(kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]); ok {
		for u128, entry := range ledgerMetadataMap.Iter() {
			ksEntry := &raftcmdpb.MetadataAttributeEntry{
				Id: &raftcmdpb.AttributeID{
					Id:  u128[:],
					Tag: entry.Tag,
				},
				Value: entry.Data,
			}
			snapshot.LedgerMetadata = append(snapshot.LedgerMetadata, ksEntry)
		}
	}

	// Serialize Ledgers KeyStore
	if ledgerMap, ok := ledgerStore.(kv.Map[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]]); ok {
		for u128, entry := range ledgerMap.Iter() {
			ksEntry := &raftcmdpb.LedgerAttributeEntry{
				Id: &raftcmdpb.AttributeID{
					Id:  u128[:],
					Tag: entry.Tag,
				},
				Info: entry.Data,
			}
			snapshot.Ledgers = append(snapshot.Ledgers, ksEntry)
		}
	}

	// Serialize Boundaries KeyStore
	if boundaryMap, ok := boundaryStore.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]]); ok {
		for u128, entry := range boundaryMap.Iter() {
			ksEntry := &raftcmdpb.BoundaryAttributeEntry{
				Id: &raftcmdpb.AttributeID{
					Id:  u128[:],
					Tag: entry.Tag,
				},
				Boundaries: entry.Data,
			}
			snapshot.Boundaries = append(snapshot.Boundaries, ksEntry)
		}
	}

	// Serialize References KeyStore
	if referenceMap, ok := referenceStore.(kv.Map[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]]); ok {
		for u128, entry := range referenceMap.Iter() {
			ksEntry := &raftcmdpb.TransactionReferenceAttributeEntry{
				Id: &raftcmdpb.AttributeID{
					Id:  u128[:],
					Tag: entry.Tag,
				},
				Value: entry.Data,
			}
			snapshot.References = append(snapshot.References, ksEntry)
		}
	}

	return snapshot
}

func (fsm *Machine) InstallSnapshot(ctx context.Context, snapshot raftpb.Snapshot) error {
	fsm.snapshotIndex = snapshot.Metadata.Index

	memSnapshot := &raftcmdpb.MemorySnapshot{}
	if err := proto.Unmarshal(snapshot.Data, memSnapshot); err != nil {
		return err
	}

	// Restore memory state from snapshot
	fsm.nextLedgerID = memSnapshot.NextLedgerId
	fsm.nextSequenceID = memSnapshot.NextSequenceId
	fsm.nextAuditSequenceID = memSnapshot.NextAuditSequenceId
	fsm.lastLogHash = memSnapshot.LastLogHash
	fsm.lastCheckpointID = memSnapshot.CheckpointId
	fsm.lastAppliedTimestamp = memSnapshot.LastAppliedTimestamp

	// Reset the cache and deserialize both generations into it
	// Ledger info and boundaries are restored via deserializeCacheGeneration (from cache generations)
	fsm.Cache.Reset()
	deserializeCacheGeneration(fsm.Cache, memSnapshot.Gen0, 0)
	deserializeCacheGeneration(fsm.Cache, memSnapshot.Gen1, 1)

	// Update currentGeneration to match the snapshot
	fsm.Cache.CurrentGeneration = memSnapshot.CurrentGeneration

	// Reset dirty key tracking. The first 2 rotations after restore will do
	// less cleanup, but the system self-corrects as new keys are tracked.
	fsm.dirtyVolumeKeys = [3]map[string]struct{}{
		make(map[string]struct{}),
		make(map[string]struct{}),
		make(map[string]struct{}),
	}
	fsm.dirtyBoundaryKeys = make(map[string]*raftcmdpb.LedgerBoundaries)

	return nil
}

// deserializeCacheGeneration deserializes a GenerationSnapshot into either Gen0 (genIndex=0) or Gen1 (genIndex=1)
func deserializeCacheGeneration(cache *cache.Cache, snapshot *raftcmdpb.GenerationSnapshot, genIndex int) {
	if snapshot == nil || cache == nil {
		return
	}

	var (
		volumeStore         kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]]
		metadataStore       kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerMetadataStore kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerStore         kv.Map[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]]
		boundaryStore       kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]]
		referenceStore      kv.Map[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]]
	)

	if genIndex == 0 {
		cache.BaseIndex.Gen0 = snapshot.BaseIndex
		volumeStore = cache.Volumes.Gen0.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]])
		metadataStore = cache.AccountMetadata.Gen0.(kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]])
		ledgerMetadataStore = cache.LedgerMetadata.Gen0.(kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]])
		ledgerStore = cache.Ledgers.Gen0.(kv.Map[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]])
		boundaryStore = cache.Boundaries.Gen0.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]])
		referenceStore = cache.References.Gen0.(kv.Map[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]])
	} else {
		cache.BaseIndex.Gen1 = snapshot.BaseIndex
		volumeStore = cache.Volumes.Gen1.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumePair]])
		metadataStore = cache.AccountMetadata.Gen1.(kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]])
		ledgerMetadataStore = cache.LedgerMetadata.Gen1.(kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]])
		ledgerStore = cache.Ledgers.Gen1.(kv.Map[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]])
		boundaryStore = cache.Boundaries.Gen1.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]])
		referenceStore = cache.References.Gen1.(kv.Map[attributes.U128, attributes.Entry[*commonpb.TransactionReferenceValue]])
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

	// Deserialize LedgerMetadata KeyStore
	for _, ksEntry := range snapshot.LedgerMetadata {
		u128 := attributes.U128FromBytes(ksEntry.Id.Id)
		ledgerMetadataStore.Put(u128, attributes.Entry[*commonpb.MetadataValue]{
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

func (fsm *Machine) SynchronizeWithLeader(ctx context.Context, snapshotFetcher SnapshotFetcher) (uint64, error) {
	// Restore checkpoint from the leader if needed
	// The checkpoint ID is stored in the Machine state from the snapshot
	if fsm.lastCheckpointID > 0 {
		currentCheckpointID := fsm.dataStore.GetCurrentCheckpointID()
		if currentCheckpointID < fsm.lastCheckpointID {
			if err := fsm.restoreCheckpoint(ctx, snapshotFetcher); err != nil {
				return 0, fmt.Errorf("restoring checkpoint from leader: %w", err)
			}
		}
	}
	fsm.lastAppliedIndex = fsm.snapshotIndex

	return fsm.snapshotIndex, nil
}

// restoreCheckpoint restores a checkpoint from the leader.
func (fsm *Machine) restoreCheckpoint(ctx context.Context, snapshotFetcher SnapshotFetcher) error {
	fsm.logger.WithFields(map[string]any{
		"currentCheckpointId": fsm.dataStore.GetCurrentCheckpointID(),
		"targetCheckpointId":  fsm.lastCheckpointID,
	}).Infof("Fetching checkpoint from leader")

	// Prepare the checkpoint directory
	checkpointDir, err := fsm.dataStore.PrepareCheckpointRestore(fsm.lastCheckpointID)
	if err != nil {
		return fmt.Errorf("preparing checkpoint restore: %w", err)
	}

	// Fetch the checkpoint from the leader
	size, hash, err := snapshotFetcher.FetchSnapshot(ctx, fsm.lastCheckpointID, checkpointDir)
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

type ApplyResult struct {
	ProposalID uint64
	Logs       []*commonpb.Log
	Error      error
}
