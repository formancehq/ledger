package state

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/formancehq/go-libs/v3/logging"
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
	Input           *attributes.KeyStore[data.VolumeKey, *raftcmdpb.VolumeHolder]
	Output          *attributes.KeyStore[data.VolumeKey, *raftcmdpb.VolumeHolder]
	AccountMetadata *attributes.KeyStore[data.MetadataKey, *commonpb.MetadataValue]
	LedgerMetadata  *attributes.KeyStore[data.LedgerMetadataKey, *commonpb.MetadataValue]
	Reversions      *attributes.KeyStore[data.TransactionKey, bool]
	IdempotencyKeys *attributes.KeyStore[data.IdempotencyKey, *commonpb.IdempotencyKeyValue]
	Ledgers         *attributes.KeyStore[data.LedgerKey, *commonpb.LedgerInfo]
	Boundaries      *attributes.KeyStore[data.LedgerKey, *raftcmdpb.LedgerBoundaries]

	nextLedgerID     uint32
	nextSequenceID   uint64
	lastCheckpointID uint64

	lastAppliedIndex            uint64
	snapshotIndex               uint64
	generationRotationThreshold uint64

	// RequestProcessor handles business logic
	processor *processing.RequestProcessor

	// Metrics
	logsAppendedCounter metric.Int64Counter
	lastPersistedIndex  atomic.Uint64
}

func NewMachine(logger logging.Logger, dataStore *data.Store, meter metric.Meter, cache *cache.Cache, attrs *attributes.Attributes, generationRotationThreshold uint64) (*Machine, error) {
	lastAppliedIndex, err := dataStore.GetLastAppliedIndex()
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
		generationRotationThreshold: generationRotationThreshold,
		logsAppendedCounter:         logsAppendedCounter,
		processor:                   processor,
		Attrs:                       attrs,
		Cache:                       cache,
		Input: attributes.NewKeyStore[data.VolumeKey, *raftcmdpb.VolumeHolder](
			attributes.DefaultKeys,
			cache.Input,
		),
		Output: attributes.NewKeyStore[data.VolumeKey, *raftcmdpb.VolumeHolder](
			attributes.DefaultKeys,
			cache.Output,
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
		Ledgers: attributes.NewKeyStore[data.LedgerKey, *commonpb.LedgerInfo](
			attributes.DefaultKeys,
			cache.Ledgers,
		),
		Boundaries: attributes.NewKeyStore[data.LedgerKey, *raftcmdpb.LedgerBoundaries](
			attributes.DefaultKeys,
			cache.Boundaries,
		),
		nextLedgerID:   1,
		nextSequenceID: 1,
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
		return nil, fmt.Errorf("last snapshot index is %d, expecting lower than %d, node out of sync", fsm.snapshotIndex, fsm.lastAppliedIndex)
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
			return nil, fmt.Errorf("invalid index, got %d, expected %d", entry.Index, fsm.lastAppliedIndex+1)
		}

		fsm.Cache.CheckRotationNeeded(fsm.lastAppliedIndex)
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
	if err := batch.Commit(); err != nil {
		return nil, fmt.Errorf("committing batch: %w", err)
	}

	fsm.lastPersistedIndex.Store(fsm.lastAppliedIndex)

	return ret, nil
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
		return errors.New("invalid preload, generation mismatch")
	}

	// Helper function to put a preloaded amount into a cache generation
	putInCacheAmount := func(
		kv kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]],
		attrID *raftcmdpb.AttributeID,
		amount *commonpb.BigInt,
	) *commonpb.BigInt {
		id := attributes.U128FromBytes(attrID.Id)

		fsm.logger.WithFields(map[string]any{
			"id": id.Hex(),
		}).Debugf("Preload volume")

		value, ok := kv.Get(id)
		if ok {
			if value.Data.Known == nil {
				value.Data.Known = amount
				if value.Data.DiffSinceBaseIndex != nil {
					value.Data.Known = commonpb.NewBigInt(
						new(big.Int).Add(value.Data.Known.Value(), value.Data.DiffSinceBaseIndex.Value()),
					)
				}
			}
			return value.Data.Known
		}

		kv.Put(id, attributes.Entry[*raftcmdpb.VolumeHolder]{
			Tag:  attrID.Tag,
			Data: &raftcmdpb.VolumeHolder{Known: amount},
		})

		return amount
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
		case *raftcmdpb.Preload_Input:
			if preloadSet.LastPersistedIndex == fsm.Cache.BaseIndex.Gen1 {
				aggregated := putInCacheAmount(fsm.Cache.Input.Gen1, preloadType.Input.Id, preloadType.Input.Value)
				putInCacheAmount(fsm.Cache.Input.Gen0, preloadType.Input.Id, aggregated)
			} else {
				putInCacheAmount(fsm.Cache.Input.Gen0, preloadType.Input.Id, preloadType.Input.Value)
			}

		case *raftcmdpb.Preload_Output:
			if preloadSet.LastPersistedIndex == fsm.Cache.BaseIndex.Gen1 {
				aggregated := putInCacheAmount(fsm.Cache.Output.Gen1, preloadType.Output.Id, preloadType.Output.Value)
				putInCacheAmount(fsm.Cache.Output.Gen0, preloadType.Output.Id, aggregated)
			} else {
				putInCacheAmount(fsm.Cache.Output.Gen0, preloadType.Output.Id, preloadType.Output.Value)
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
		}
	}

	return nil
}

// applyProposal processes all orders in a proposal atomically.
// Uses RequestProcessor which handles rollback internally via Buffered.
func (fsm *Machine) applyProposal(ctx context.Context, raftIndex uint64, batch *data.Batch, proposal *raftcmdpb.Proposal) (*ApplyResult, error) {
	if err := fsm.Preload(proposal.Preload); err != nil {
		return nil, err
	}

	// Create buffer for this proposal
	buffer := NewBuffer(proposal.Date, fsm)

	// Process the proposal
	response, err := fsm.processor.ProcessProposal(proposal, buffer)
	if err != nil {
		return &ApplyResult{
			ProposalID: proposal.Id,
			Error:      err,
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
		NextLedgerId:      fsm.nextLedgerID,
		NextSequenceId:    fsm.nextSequenceID,
		Gen0:              serializeCacheGeneration(fsm.Cache, 0),
		Gen1:              serializeCacheGeneration(fsm.Cache, 1),
		CheckpointId:      checkpointID,
		CurrentGeneration: fsm.Cache.CurrentGeneration,
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
		inputStore          kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]]
		outputStore         kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]]
		metadataStore       kv.KV[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerMetadataStore kv.KV[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerStore         kv.KV[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]]
		boundaryStore       kv.KV[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]]
	)

	if genIndex == 0 {
		baseIndex = cache.BaseIndex.Gen0
		inputStore = cache.Input.Gen0
		outputStore = cache.Output.Gen0
		metadataStore = cache.AccountMetadata.Gen0
		ledgerMetadataStore = cache.LedgerMetadata.Gen0
		ledgerStore = cache.Ledgers.Gen0
		boundaryStore = cache.Boundaries.Gen0
	} else {
		baseIndex = cache.BaseIndex.Gen1
		inputStore = cache.Input.Gen1
		outputStore = cache.Output.Gen1
		metadataStore = cache.AccountMetadata.Gen1
		ledgerMetadataStore = cache.LedgerMetadata.Gen1
		ledgerStore = cache.Ledgers.Gen1
		boundaryStore = cache.Boundaries.Gen1
	}

	snapshot := &raftcmdpb.GenerationSnapshot{
		BaseIndex:      baseIndex,
		Input:          make([]*raftcmdpb.VolumeAttributeEntry, 0, inputStore.Size()),
		Output:         make([]*raftcmdpb.VolumeAttributeEntry, 0, outputStore.Size()),
		Metadata:       make([]*raftcmdpb.MetadataAttributeEntry, 0, metadataStore.Size()),
		LedgerMetadata: make([]*raftcmdpb.MetadataAttributeEntry, 0, ledgerMetadataStore.Size()),
		Ledgers:        make([]*raftcmdpb.LedgerAttributeEntry, 0, ledgerStore.Size()),
		Boundaries:     make([]*raftcmdpb.BoundaryAttributeEntry, 0, boundaryStore.Size()),
	}

	// Serialize Input KeyStore
	// todo: clean the casts
	if inputMap, ok := inputStore.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]]); ok {
		for u128, entry := range inputMap.Iter() {
			ksEntry := &raftcmdpb.VolumeAttributeEntry{
				Id: &raftcmdpb.AttributeID{
					Id:  u128[:],
					Tag: entry.Tag,
				},
			}
			if entry.Data != nil {
				ksEntry.Known = entry.Data.Known
				ksEntry.DiffSinceBaseIndex = entry.Data.DiffSinceBaseIndex
			}
			snapshot.Input = append(snapshot.Input, ksEntry)
		}
	}

	// Serialize Output KeyStore
	if outputMap, ok := outputStore.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]]); ok {
		for u128, entry := range outputMap.Iter() {
			ksEntry := &raftcmdpb.VolumeAttributeEntry{
				Id: &raftcmdpb.AttributeID{
					Id:  u128[:],
					Tag: entry.Tag,
				},
			}
			if entry.Data != nil {
				ksEntry.Known = entry.Data.Known
				ksEntry.DiffSinceBaseIndex = entry.Data.DiffSinceBaseIndex
			}
			snapshot.Output = append(snapshot.Output, ksEntry)
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
	fsm.lastCheckpointID = memSnapshot.CheckpointId

	// Reset the cache and deserialize both generations into it
	// Ledger info and boundaries are restored via deserializeCacheGeneration (from cache generations)
	fsm.Cache.Reset()
	deserializeCacheGeneration(fsm.Cache, memSnapshot.Gen0, 0)
	deserializeCacheGeneration(fsm.Cache, memSnapshot.Gen1, 1)

	// Update currentGeneration to match the snapshot
	fsm.Cache.CurrentGeneration = memSnapshot.CurrentGeneration

	return nil
}

// deserializeCacheGeneration deserializes a GenerationSnapshot into either Gen0 (genIndex=0) or Gen1 (genIndex=1)
func deserializeCacheGeneration(cache *cache.Cache, snapshot *raftcmdpb.GenerationSnapshot, genIndex int) {
	if snapshot == nil || cache == nil {
		return
	}

	var (
		inputStore          kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]]
		outputStore         kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]]
		metadataStore       kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerMetadataStore kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]]
		ledgerStore         kv.Map[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]]
		boundaryStore       kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]]
	)

	if genIndex == 0 {
		cache.BaseIndex.Gen0 = snapshot.BaseIndex
		inputStore = cache.Input.Gen0.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]])
		outputStore = cache.Output.Gen0.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]])
		metadataStore = cache.AccountMetadata.Gen0.(kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]])
		ledgerMetadataStore = cache.LedgerMetadata.Gen0.(kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]])
		ledgerStore = cache.Ledgers.Gen0.(kv.Map[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]])
		boundaryStore = cache.Boundaries.Gen0.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]])
	} else {
		cache.BaseIndex.Gen1 = snapshot.BaseIndex
		inputStore = cache.Input.Gen1.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]])
		outputStore = cache.Output.Gen1.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.VolumeHolder]])
		metadataStore = cache.AccountMetadata.Gen1.(kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]])
		ledgerMetadataStore = cache.LedgerMetadata.Gen1.(kv.Map[attributes.U128, attributes.Entry[*commonpb.MetadataValue]])
		ledgerStore = cache.Ledgers.Gen1.(kv.Map[attributes.U128, attributes.Entry[*commonpb.LedgerInfo]])
		boundaryStore = cache.Boundaries.Gen1.(kv.Map[attributes.U128, attributes.Entry[*raftcmdpb.LedgerBoundaries]])
	}

	// Deserialize Input KeyStore
	for _, ksEntry := range snapshot.Input {
		u128 := attributes.U128FromBytes(ksEntry.Id.Id)
		holder := &raftcmdpb.VolumeHolder{
			Known:              ksEntry.Known,
			DiffSinceBaseIndex: ksEntry.DiffSinceBaseIndex,
		}
		inputStore.Put(u128, attributes.Entry[*raftcmdpb.VolumeHolder]{
			Tag:  ksEntry.Id.Tag,
			Data: holder,
		})
	}

	// Deserialize Output KeyStore
	for _, ksEntry := range snapshot.Output {
		u128 := attributes.U128FromBytes(ksEntry.Id.Id)
		holder := &raftcmdpb.VolumeHolder{
			Known:              ksEntry.Known,
			DiffSinceBaseIndex: ksEntry.DiffSinceBaseIndex,
		}
		outputStore.Put(u128, attributes.Entry[*raftcmdpb.VolumeHolder]{
			Tag:  ksEntry.Id.Tag,
			Data: holder,
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
