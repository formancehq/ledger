package state

import (
	"encoding/binary"
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

// parseLeanValue extracts the tag and raw value bytes from a lean cache entry.
// Lean format: [8-byte tag LE][raw value bytes].
func parseLeanValue(value []byte) (uint64, []byte) {
	return binary.LittleEndian.Uint64(value[:8]), value[8:]
}

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

	// Reversions: write each word of each ledger's bitset.
	for ledger, bs := range s.registry.Reversions {
		for i := range bs.WordCount() {
			if err := SaveReversionWord(batch, ledger, uint64(i), bs.Word(uint64(i))); err != nil {
				return fmt.Errorf("saving reversion word for %s: %w", ledger, err)
			}
		}
	}

	// Cache: clear previous snapshot data then re-write.
	if err := batch.DeleteRangeNoSync(
		[]byte{dal.KeyPrefixCacheSnapshot},
		[]byte{dal.KeyPrefixCacheSnapshot, dal.CacheMetaKey, 0x01},
	); err != nil {
		return fmt.Errorf("clearing cache snapshot range: %w", err)
	}

	currentGen := s.registry.Cache.CurrentGeneration()
	gen0Byte := byte(currentGen % 2)
	gen1Byte := byte((currentGen + 1) % 2)

	if err := s.persistGeneration(batch, gen0Byte, 0); err != nil {
		return fmt.Errorf("persisting cache gen0: %w", err)
	}

	if err := s.persistGeneration(batch, gen1Byte, 1); err != nil {
		return fmt.Errorf("persisting cache gen1: %w", err)
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
// genByte is the byte written into 0xFF keys; genIndex selects which
// in-memory generation to read (0=gen0, 1=gen1).
func (s *CacheSnapshotter) persistGeneration(batch *dal.Batch, genByte byte, genIndex int) error {
	c := s.registry.Cache

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

	// All entries use lean format: [8-byte tag LE][raw value proto bytes].
	// This matches the format written incrementally by mergeSimpleWithCache.

	if err := persistLeanProtoEntries(batch, genByte, dal.AttributePrefixVolume, volumeStore); err != nil {
		return err
	}

	if err := persistLeanProtoEntries(batch, genByte, dal.AttributePrefixMetadata, metadataStore); err != nil {
		return err
	}

	if err := persistLeanProtoEntries(batch, genByte, dal.AttributePrefixLedger, ledgerStore); err != nil {
		return err
	}

	if err := persistLeanProtoEntries(batch, genByte, dal.AttributePrefixBoundary, boundaryStore); err != nil {
		return err
	}

	if err := persistLeanProtoEntries(batch, genByte, dal.AttributePrefixReference, referenceStore); err != nil {
		return err
	}

	if err := persistLeanProtoEntries(batch, genByte, dal.AttributePrefixTransaction, transactionStore); err != nil {
		return err
	}

	// NumscriptParsed stores a string, not a proto — write string bytes directly.
	for u128, entry := range numscriptParsedStore.Iter() {
		if err := writeCacheRaw(batch, genByte, dal.AttributePrefixNumscript, u128, entry.Tag, []byte(entry.Data)); err != nil {
			return err
		}
	}

	if err := persistLeanProtoEntries(batch, genByte, dal.AttributePrefixIdempotency, idempotencyStore); err != nil {
		return err
	}

	return nil
}

// persistLeanProtoEntries writes all entries from a KV store to 0xFF in lean format.
func persistLeanProtoEntries[V interface {
	MarshalVT() ([]byte, error)
}](batch *dal.Batch, genByte, cacheType byte, store kv.KV[attributes.U128, attributes.Entry[V]]) error {
	for u128, entry := range store.Iter() {
		valueBytes, err := entry.Data.MarshalVT()
		if err != nil {
			return fmt.Errorf("marshaling cache value: %w", err)
		}

		if err := writeCacheRaw(batch, genByte, cacheType, u128, entry.Tag, valueBytes); err != nil {
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

	currentGen := meta.GetCurrentGeneration()
	gen0Byte := byte(currentGen % 2)
	gen1Byte := byte((currentGen + 1) % 2)

	if err := s.restoreGeneration(gen0Byte, 0); err != nil {
		return fmt.Errorf("restoring cache gen0 from byte %d: %w", gen0Byte, err)
	}

	if err := s.restoreGeneration(gen1Byte, 1); err != nil {
		return fmt.Errorf("restoring cache gen1 from byte %d: %w", gen1Byte, err)
	}

	s.registry.Cache.SetCurrentGeneration(currentGen)

	s.logger.WithFields(map[string]any{
		"duration":          time.Since(restoreStart).String(),
		"currentGeneration": currentGen,
		"gen0Byte":          gen0Byte,
		"gen1Byte":          gen1Byte,
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
// genByte is the byte position in 0xFF keys; genIndex selects the
// in-memory generation to populate (0=gen0, 1=gen1).
func (s *CacheSnapshotter) restoreGeneration(genByte byte, genIndex int) error {
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

	// Restore each cache type by iterating over its prefix.
	// All entries use lean format: [8-byte tag LE][raw value proto bytes].
	type restoreSpec struct {
		cacheType byte
		restore   func(u128 attributes.U128, value []byte) error
	}

	specs := []restoreSpec{
		{dal.AttributePrefixVolume, func(u128 attributes.U128, value []byte) error {
			tag, valueBytes := parseLeanValue(value)
			v := &raftcmdpb.VolumePair{}
			if err := v.UnmarshalVT(valueBytes); err != nil {
				return err
			}
			volumeStore.Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{Tag: tag, Data: v})

			return nil
		}},
		{dal.AttributePrefixMetadata, func(u128 attributes.U128, value []byte) error {
			tag, valueBytes := parseLeanValue(value)
			v := &commonpb.MetadataValue{}
			if err := v.UnmarshalVT(valueBytes); err != nil {
				return err
			}
			metadataStore.Put(u128, attributes.Entry[*commonpb.MetadataValue]{Tag: tag, Data: v})

			return nil
		}},
		{dal.AttributePrefixLedger, func(u128 attributes.U128, value []byte) error {
			tag, valueBytes := parseLeanValue(value)
			v := &commonpb.LedgerInfo{}
			if err := v.UnmarshalVT(valueBytes); err != nil {
				return err
			}
			ledgerStore.Put(u128, attributes.Entry[*commonpb.LedgerInfo]{Tag: tag, Data: v})

			return nil
		}},
		{dal.AttributePrefixBoundary, func(u128 attributes.U128, value []byte) error {
			tag, valueBytes := parseLeanValue(value)
			v := &raftcmdpb.LedgerBoundaries{}
			if err := v.UnmarshalVT(valueBytes); err != nil {
				return err
			}
			boundaryStore.Put(u128, attributes.Entry[*raftcmdpb.LedgerBoundaries]{Tag: tag, Data: v})

			return nil
		}},
		{dal.AttributePrefixReference, func(u128 attributes.U128, value []byte) error {
			tag, valueBytes := parseLeanValue(value)
			v := &commonpb.TransactionReferenceValue{}
			if err := v.UnmarshalVT(valueBytes); err != nil {
				return err
			}
			referenceStore.Put(u128, attributes.Entry[*commonpb.TransactionReferenceValue]{Tag: tag, Data: v})

			return nil
		}},
		{dal.AttributePrefixTransaction, func(u128 attributes.U128, value []byte) error {
			tag, valueBytes := parseLeanValue(value)
			v := &commonpb.TransactionState{}
			if err := v.UnmarshalVT(valueBytes); err != nil {
				return err
			}
			transactionStore.Put(u128, attributes.Entry[*commonpb.TransactionState]{Tag: tag, Data: v})

			return nil
		}},
		{dal.AttributePrefixNumscript, func(u128 attributes.U128, value []byte) error {
			tag, valueBytes := parseLeanValue(value)
			numscriptParsedStore.Put(u128, attributes.Entry[string]{Tag: tag, Data: string(valueBytes)})

			return nil
		}},
		{dal.AttributePrefixIdempotency, func(u128 attributes.U128, value []byte) error {
			tag, valueBytes := parseLeanValue(value)
			v := &commonpb.IdempotencyKeyValue{}
			if err := v.UnmarshalVT(valueBytes); err != nil {
				return err
			}
			idempotencyStore.Put(u128, attributes.Entry[*commonpb.IdempotencyKeyValue]{Tag: tag, Data: v})

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
