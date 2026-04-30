package state

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/bloom"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/kv"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// CacheSnapshotter handles persisting and restoring the in-memory cache
// (generations, reversions, bloom filters) to/from Pebble under the 0xFF prefix.
// Extracted from Machine to isolate pure IO serialization logic.
type CacheSnapshotter struct {
	logger       logging.Logger
	dataStore    *dal.Store
	registry     *StateRegistry
	bloomFilters *bloom.FilterSet

	// bloomPopulateDone is closed when the background bloom populate goroutine
	// finishes. Nil when no background work is running.
	bloomPopulateDone chan struct{}
}

// NewCacheSnapshotter creates a CacheSnapshotter.
func NewCacheSnapshotter(logger logging.Logger, dataStore *dal.Store, registry *StateRegistry, bloomFilters *bloom.FilterSet) *CacheSnapshotter {
	return &CacheSnapshotter{
		logger:       logger,
		dataStore:    dataStore,
		registry:     registry,
		bloomFilters: bloomFilters,
	}
}

// PersistToStore writes cache and reversions to Pebble in a single batch.
// Called before creating a checkpoint so the checkpoint includes both.
func (s *CacheSnapshotter) PersistToStore() error {
	batch := s.dataStore.NewBatch()
	defer func() { _ = batch.Cancel() }()

	// Reversions: write each ledger's bitset.
	for ledger, bs := range s.registry.Reversions {
		if err := SaveReversions(batch, ledger, bs.MarshalWords()); err != nil {
			return fmt.Errorf("saving reversions for %s: %w", ledger, err)
		}
	}

	// Cache: clear previous snapshot data then re-write.
	if err := batch.DeleteRangeNoSync(
		[]byte{dal.KeyPrefixCacheSnapshot},
		[]byte{dal.KeyPrefixCacheSnapshot, dal.CacheMetaKey, 0x01},
	); err != nil {
		return fmt.Errorf("clearing cache snapshot range: %w", err)
	}

	for genIndex := range 2 {
		if err := s.persistGeneration(batch, byte(genIndex)); err != nil {
			return fmt.Errorf("persisting cache gen%d: %w", genIndex, err)
		}
	}

	if err := batch.SetProto(
		[]byte{dal.KeyPrefixCacheSnapshot, dal.CacheMetaKey},
		&raftcmdpb.CacheSnapshotMeta{
			CurrentGeneration: s.registry.Cache.CurrentGeneration(),
		},
	); err != nil {
		return fmt.Errorf("writing cache snapshot meta: %w", err)
	}

	// Bloom filters are NOT persisted in the checkpoint. They are large (can
	// exceed 1 GiB) and are fully rebuildable from Pebble attributes via
	// PopulateFromStore. At restart, the missing data triggers a fast background
	// rebuild that doesn't block the node.

	return batch.Commit()
}

// persistGeneration writes a single cache generation to Pebble.
func (s *CacheSnapshotter) persistGeneration(batch *dal.Batch, genByte byte) error {
	c := s.registry.Cache
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

		if err := batch.SetProto(makeKey(dal.AttributePrefixVolume, u128), e); err != nil {
			return err
		}
	}

	// Metadata
	for u128, entry := range metadataStore.Iter() {
		e := &raftcmdpb.MetadataAttributeEntry{
			Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Value: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.AttributePrefixMetadata, u128), e); err != nil {
			return err
		}
	}

	// Ledgers
	for u128, entry := range ledgerStore.Iter() {
		e := &raftcmdpb.LedgerAttributeEntry{
			Id:   &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Info: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.AttributePrefixLedger, u128), e); err != nil {
			return err
		}
	}

	// Boundaries
	for u128, entry := range boundaryStore.Iter() {
		e := &raftcmdpb.BoundaryAttributeEntry{
			Id:         &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Boundaries: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.AttributePrefixBoundary, u128), e); err != nil {
			return err
		}
	}

	// References
	for u128, entry := range referenceStore.Iter() {
		e := &raftcmdpb.TransactionReferenceAttributeEntry{
			Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Value: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.AttributePrefixReference, u128), e); err != nil {
			return err
		}
	}

	// Transactions
	for u128, entry := range transactionStore.Iter() {
		e := &raftcmdpb.TransactionStateAttributeEntry{
			Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			State: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.AttributePrefixTransaction, u128), e); err != nil {
			return err
		}
	}

	// NumscriptParsed
	for u128, entry := range numscriptParsedStore.Iter() {
		e := &raftcmdpb.NumscriptParsedAttributeEntry{
			Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Plain: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.AttributePrefixNumscript, u128), e); err != nil {
			return err
		}
	}

	// IdempotencyKeys
	for u128, entry := range idempotencyStore.Iter() {
		e := &raftcmdpb.IdempotencyKeyAttributeEntry{
			Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: entry.Tag},
			Value: entry.Data,
		}
		if err := batch.SetProto(makeKey(dal.AttributePrefixIdempotency, u128), e); err != nil {
			return err
		}
	}

	return nil
}

// RestoreFromStore rebuilds the in-memory cache from Pebble (0xFF prefix).
// Called on restart (when store is up to date) and after follower sync.
func (s *CacheSnapshotter) RestoreFromStore() error {
	restoreStart := time.Now()

	// Read cache-level metadata
	metaVal, closer, err := s.dataStore.Get([]byte{dal.KeyPrefixCacheSnapshot, dal.CacheMetaKey})
	if err != nil {
		// No cache data in Pebble — leave cache empty (fresh node)
		s.logger.Infof("No cache snapshot found in Pebble, starting with empty cache")

		return nil
	}

	meta := &raftcmdpb.CacheSnapshotMeta{}
	if err := meta.UnmarshalVT(metaVal); err != nil {
		_ = closer.Close()

		return fmt.Errorf("unmarshaling cache snapshot meta: %w", err)
	}

	_ = closer.Close()

	s.registry.Cache.Reset()

	// Restore both generations
	for genIndex := range 2 {
		genByte := byte(genIndex)

		if err := s.restoreGeneration(genByte); err != nil {
			return fmt.Errorf("restoring cache gen%d: %w", genIndex, err)
		}
	}

	s.registry.Cache.SetCurrentGeneration(meta.GetCurrentGeneration())

	s.logger.WithFields(map[string]any{
		"duration":          time.Since(restoreStart).String(),
		"currentGeneration": meta.GetCurrentGeneration(),
	}).Infof("Restored cache from Pebble")

	// Bloom filters are never persisted in checkpoints (too large). Always
	// rebuild from a full attribute scan in the background. The preloader
	// falls back to Pebble Gets while IsReady() returns false.
	if s.bloomFilters != nil {
		s.startAsyncBloomPopulate("bloom filters not persisted in checkpoint")
	}

	return nil
}

// restoreGeneration restores a single cache generation from Pebble.
func (s *CacheSnapshotter) restoreGeneration(genByte byte) error {
	genIndex := int(genByte)

	// Read generation metadata
	genMetaKey := []byte{dal.KeyPrefixCacheSnapshot, genByte, dal.CacheGenMeta}

	genMetaVal, closer, err := s.dataStore.Get(genMetaKey)
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
		s.registry.Cache.BaseIndex.Gen0 = genMeta.GetBaseIndex()
	} else {
		s.registry.Cache.BaseIndex.Gen1 = genMeta.GetBaseIndex()
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
		volumeStore = s.registry.Cache.Volumes.Gen0()
		metadataStore = s.registry.Cache.AccountMetadata.Gen0()
		ledgerStore = s.registry.Cache.Ledgers.Gen0()
		boundaryStore = s.registry.Cache.Boundaries.Gen0()
		referenceStore = s.registry.Cache.References.Gen0()
		transactionStore = s.registry.Cache.Transactions.Gen0()
		numscriptParsedStore = s.registry.Cache.NumscriptParsed.Gen0()
		idempotencyStore = s.registry.Cache.IdempotencyKeys.Gen0()
	} else {
		volumeStore = s.registry.Cache.Volumes.Gen1()
		metadataStore = s.registry.Cache.AccountMetadata.Gen1()
		ledgerStore = s.registry.Cache.Ledgers.Gen1()
		boundaryStore = s.registry.Cache.Boundaries.Gen1()
		referenceStore = s.registry.Cache.References.Gen1()
		transactionStore = s.registry.Cache.Transactions.Gen1()
		numscriptParsedStore = s.registry.Cache.NumscriptParsed.Gen1()
		idempotencyStore = s.registry.Cache.IdempotencyKeys.Gen1()
	}

	// Restore each cache type by iterating over its prefix
	type restoreSpec struct {
		cacheType byte
		restore   func(u128 attributes.U128, value []byte) error
	}

	specs := []restoreSpec{
		{dal.AttributePrefixVolume, func(u128 attributes.U128, value []byte) error {
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
		{dal.AttributePrefixMetadata, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.MetadataAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			metadataStore.Put(u128, attributes.Entry[*commonpb.MetadataValue]{
				Tag: e.GetId().GetTag(), Data: e.GetValue(),
			})

			return nil
		}},
		{dal.AttributePrefixLedger, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.LedgerAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			ledgerStore.Put(u128, attributes.Entry[*commonpb.LedgerInfo]{
				Tag: e.GetId().GetTag(), Data: e.GetInfo(),
			})

			return nil
		}},
		{dal.AttributePrefixBoundary, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.BoundaryAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			boundaryStore.Put(u128, attributes.Entry[*raftcmdpb.LedgerBoundaries]{
				Tag: e.GetId().GetTag(), Data: e.GetBoundaries(),
			})

			return nil
		}},
		{dal.AttributePrefixReference, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.TransactionReferenceAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			referenceStore.Put(u128, attributes.Entry[*commonpb.TransactionReferenceValue]{
				Tag: e.GetId().GetTag(), Data: e.GetValue(),
			})

			return nil
		}},
		{dal.AttributePrefixTransaction, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.TransactionStateAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			transactionStore.Put(u128, attributes.Entry[*commonpb.TransactionState]{
				Tag: e.GetId().GetTag(), Data: e.GetState(),
			})

			return nil
		}},
		{dal.AttributePrefixNumscript, func(u128 attributes.U128, value []byte) error {
			e := &raftcmdpb.NumscriptParsedAttributeEntry{}
			if err := e.UnmarshalVT(value); err != nil {
				return err
			}
			numscriptParsedStore.Put(u128, attributes.Entry[string]{
				Tag: e.GetId().GetTag(), Data: e.GetPlain(),
			})

			return nil
		}},
		{dal.AttributePrefixIdempotency, func(u128 attributes.U128, value []byte) error {
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

		iter, err := s.dataStore.NewIter(&pebble.IterOptions{
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

// startAsyncBloomPopulate launches a background goroutine to populate the bloom
// filters from a full Pebble attribute scan. While populating, IsReady()
// returns false and the preloader falls back to Pebble Gets.
func (s *CacheSnapshotter) startAsyncBloomPopulate(reason string) {
	s.logger.WithFields(map[string]any{
		"reason": reason,
	}).Infof("Populating bloom filters from attribute scan in background")

	s.bloomPopulateDone = make(chan struct{})

	go func() {
		defer close(s.bloomPopulateDone)

		start := time.Now()

		if err := s.bloomFilters.PopulateFromStore(s.dataStore); err != nil {
			s.logger.Errorf("Background bloom populate failed: %v", err)

			return
		}

		s.logger.WithFields(map[string]any{
			"duration": time.Since(start).String(),
		}).Infof("Background bloom populate complete")
	}()
}

// Stop waits for any background bloom populate goroutine to finish.
func (s *CacheSnapshotter) Stop() {
	if s.bloomPopulateDone != nil {
		<-s.bloomPopulateDone
	}
}
