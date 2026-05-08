package state

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/bloom"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
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

// putAttributeIfAbsent puts value at id only if store doesn't already have
// an entry, returning whichever value ends up in store. Used by MirrorPreload
// to avoid clobbering a fresher in-memory value with the boundary preload.
func putAttributeIfAbsent[V any](
	store kv.KV[attributes.U128, attributes.Entry[V]],
	id attributes.U128,
	tag uint64,
	value V,
) V {
	if existing, ok := store.Get(id); ok {
		return existing.Data
	}

	store.Put(id, attributes.Entry[V]{Tag: tag, Data: value})

	return value
}

// cacheSnapshotSlot captures the persist/restore logic for a single cache type.
// Implemented by protoSnapshotSlot[V] for proto-backed caches.
type cacheSnapshotSlot interface {
	// CacheType returns the Pebble attribute prefix byte for this cache type.
	CacheType() byte
	// Persist writes all entries from the given generation to the Pebble batch.
	Persist(batch *dal.Batch, genByte byte, genIndex int) error
	// RestoreEntry returns a function that restores a single entry into the given generation.
	RestoreEntry(genIndex int) func(u128 attributes.U128, rawValue []byte) error
	// MirrorTouch promotes id from gen1 to gen0 in-memory and mirrors the
	// new gen0 entry to 0xFF. No-op if gen0 already has the key or the key
	// is in neither generation.
	MirrorTouch(batch *dal.Batch, gen0Byte byte, id attributes.U128) error
	// MirrorPreload puts value into both in-memory generations and mirrors
	// to 0xFF at both byte positions. value is type-erased; the concrete
	// implementation re-asserts to V.
	MirrorPreload(batch *dal.Batch, gen0Byte, gen1Byte byte, attrID *raftcmdpb.AttributeID, value any) error
}

// protoSnapshotSlot implements cacheSnapshotSlot for a proto-backed AttributeCache.
type protoSnapshotSlot[V interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}] struct {
	cacheType byte
	ac        *cache.AttributeCache[V]
	newValue  func() V
}

func (s *protoSnapshotSlot[V]) CacheType() byte { return s.cacheType }

func (s *protoSnapshotSlot[V]) selectGen(genIndex int) kv.KV[attributes.U128, attributes.Entry[V]] {
	if genIndex == 0 {
		return s.ac.Gen0()
	}

	return s.ac.Gen1()
}

func (s *protoSnapshotSlot[V]) Persist(batch *dal.Batch, genByte byte, genIndex int) error {
	return persistLeanProtoEntries(batch, genByte, s.cacheType, s.selectGen(genIndex))
}

func (s *protoSnapshotSlot[V]) MirrorTouch(batch *dal.Batch, gen0Byte byte, id attributes.U128) error {
	// Skip when gen0 already holds the key — Touch is a no-op then, and
	// the 0xFF gen0Byte row may hold a fresher in-batch Merge value.
	if _, ok := s.ac.Gen0().Get(id); ok {
		return nil
	}

	s.ac.Touch(id)

	entry, ok := s.ac.Gen0().Get(id)
	if !ok {
		return nil
	}

	valueBytes, err := entry.Data.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling touched value: %w", err)
	}

	if err := writeCacheRaw(batch, gen0Byte, s.cacheType, id, entry.Tag, valueBytes); err != nil {
		return fmt.Errorf("persisting touched value: %w", err)
	}

	return nil
}

func (s *protoSnapshotSlot[V]) MirrorPreload(
	batch *dal.Batch,
	gen0Byte, gen1Byte byte,
	attrID *raftcmdpb.AttributeID,
	value any,
) error {
	typed, ok := value.(V)
	if !ok {
		return fmt.Errorf("MirrorPreload: value type %T does not match slot's V", value)
	}

	id := attributes.U128FromBytes(attrID.GetId())
	tag := attrID.GetTag()

	// gen1 wins if it already has a (post-rotation, possibly fresher)
	// value; otherwise the preload value populates both gens.
	effective := putAttributeIfAbsent(s.ac.Gen1(), id, tag, typed)
	putAttributeIfAbsent(s.ac.Gen0(), id, tag, effective)

	valueBytes, err := effective.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling preloaded value: %w", err)
	}

	if err := writeCacheRaw(batch, gen0Byte, s.cacheType, id, tag, valueBytes); err != nil {
		return fmt.Errorf("persisting preloaded gen0 value: %w", err)
	}

	if err := writeCacheRaw(batch, gen1Byte, s.cacheType, id, tag, valueBytes); err != nil {
		return fmt.Errorf("persisting preloaded gen1 value: %w", err)
	}

	return nil
}

func (s *protoSnapshotSlot[V]) RestoreEntry(genIndex int) func(u128 attributes.U128, rawValue []byte) error {
	store := s.selectGen(genIndex)

	return func(u128 attributes.U128, rawValue []byte) error {
		tag, valueBytes := parseLeanValue(rawValue)
		v := s.newValue()

		if err := v.UnmarshalVT(valueBytes); err != nil {
			return err
		}

		store.Put(u128, attributes.Entry[V]{Tag: tag, Data: v})

		return nil
	}
}

// newProtoSnapshotSlot creates a cacheSnapshotSlot for a proto-backed AttributeCache.
func newProtoSnapshotSlot[V interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}](cacheType byte, ac *cache.AttributeCache[V], newValue func() V) cacheSnapshotSlot {
	return &protoSnapshotSlot[V]{cacheType: cacheType, ac: ac, newValue: newValue}
}

// CacheSnapshotter handles persisting and restoring the in-memory cache
// (generations, reversions, bloom filters) to/from Pebble under the 0xFF prefix.
// Extracted from Machine to isolate pure IO serialization logic.
type CacheSnapshotter struct {
	logger       logging.Logger
	dataStore    *dal.Store
	registry     *StateRegistry
	bloomFilters *bloom.FilterSet
	slots        []cacheSnapshotSlot
	// touchSlots maps attribute code bytes to the corresponding slot,
	// for the FSM apply path's MirrorTouch dispatch.
	touchSlots map[byte]cacheSnapshotSlot

	// bloomPopulateDone is closed when the background bloom populate goroutine
	// finishes. Nil when no background work is running.
	bloomPopulateDone chan struct{}
}

// NewCacheSnapshotter creates a CacheSnapshotter.
func NewCacheSnapshotter(logger logging.Logger, dataStore *dal.Store, registry *StateRegistry, bloomFilters *bloom.FilterSet) *CacheSnapshotter {
	c := registry.Cache

	volumes := newProtoSnapshotSlot(dal.AttributeCodeVolume, c.Volumes, func() *raftcmdpb.VolumePair { return &raftcmdpb.VolumePair{} })
	metadata := newProtoSnapshotSlot(dal.AttributeCodeMetadata, c.AccountMetadata, func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} })
	ledgers := newProtoSnapshotSlot(dal.AttributeCodeLedger, c.Ledgers, func() *commonpb.LedgerInfo { return &commonpb.LedgerInfo{} })
	boundaries := newProtoSnapshotSlot(dal.AttributeCodeBoundary, c.Boundaries, func() *raftcmdpb.LedgerBoundaries { return &raftcmdpb.LedgerBoundaries{} })
	references := newProtoSnapshotSlot(dal.AttributeCodeReference, c.References, func() *commonpb.TransactionReferenceValue { return &commonpb.TransactionReferenceValue{} })
	transactions := newProtoSnapshotSlot(dal.AttributeCodeTransaction, c.Transactions, func() *commonpb.TransactionState { return &commonpb.TransactionState{} })
	sinks := newProtoSnapshotSlot(dal.AttributeCodeSinkConfig, c.SinkConfigs, func() *commonpb.SinkConfig { return &commonpb.SinkConfig{} })
	numscriptVersions := newProtoSnapshotSlot(dal.AttributeCodeNumscriptVersion, c.NumscriptVersions, func() *commonpb.NumscriptVersionValue { return &commonpb.NumscriptVersionValue{} })
	numscriptContents := newProtoSnapshotSlot(dal.AttributeCodeNumscriptContent, c.NumscriptContents, func() *commonpb.NumscriptInfo { return &commonpb.NumscriptInfo{} })
	preparedQueries := newProtoSnapshotSlot(dal.AttributeCodePreparedQuery, c.PreparedQueries, func() *commonpb.PreparedQuery { return &commonpb.PreparedQuery{} })

	return &CacheSnapshotter{
		logger:       logger,
		dataStore:    dataStore,
		registry:     registry,
		bloomFilters: bloomFilters,
		slots: []cacheSnapshotSlot{
			volumes, metadata, ledgers, boundaries, references,
			transactions, sinks, numscriptVersions, numscriptContents,
			preparedQueries,
		},
		touchSlots: map[byte]cacheSnapshotSlot{
			dal.AttributeCodeVolume:           volumes,
			dal.AttributeCodeMetadata:         metadata,
			dal.AttributeCodeLedger:           ledgers,
			dal.AttributeCodeBoundary:         boundaries,
			dal.AttributeCodeReference:        references,
			dal.AttributeCodeTransaction:      transactions,
			dal.AttributeCodeSinkConfig:       sinks,
			dal.AttributeCodeNumscriptVersion: numscriptVersions,
			dal.AttributeCodeNumscriptContent: numscriptContents,
			dal.AttributeCodePreparedQuery:    preparedQueries,
		},
	}
}

// MirrorTouch performs an in-memory Touch and mirrors the gen0 promotion
// to 0xFF, so a restart restores the entry into the same generation it
// occupies in memory. gen0Byte = currentGeneration % 2.
func (s *CacheSnapshotter) MirrorTouch(batch *dal.Batch, attrType byte, gen0Byte byte, id attributes.U128) error {
	slot, ok := s.touchSlots[attrType]
	if !ok {
		return nil
	}

	return slot.MirrorTouch(batch, gen0Byte, id)
}

// MirrorPreload populates both in-memory generations and mirrors to 0xFF
// at both byte positions, matching what preloadToCache used to do
// in-memory only.
func (s *CacheSnapshotter) MirrorPreload(batch *dal.Batch, gen0Byte, gen1Byte byte, preload *raftcmdpb.Preload) error {
	attrCode, attrID, value := extractPreload(preload)
	if attrID == nil {
		return nil
	}

	slot, ok := s.touchSlots[attrCode]
	if !ok {
		return nil
	}

	return slot.MirrorPreload(batch, gen0Byte, gen1Byte, attrID, value)
}

// extractPreload pulls the attribute code, AttributeID and typed value out
// of a Preload variant. One case per variant — no way to express this
// generically because each variant is a distinct concrete struct.
func extractPreload(p *raftcmdpb.Preload) (byte, *raftcmdpb.AttributeID, any) {
	switch v := p.GetType().(type) {
	case *raftcmdpb.Preload_Volume:
		return dal.AttributeCodeVolume, v.Volume.GetId(), v.Volume.GetValue()
	case *raftcmdpb.Preload_IdempotencyKey:
		// Idempotency keys are no longer stored in the cache system.
		// They are handled separately via the dedicated IdempotencyStore.
		return 0, nil, nil
	case *raftcmdpb.Preload_Ledger:
		return dal.AttributeCodeLedger, v.Ledger.GetId(), v.Ledger.GetValue()
	case *raftcmdpb.Preload_Boundary:
		return dal.AttributeCodeBoundary, v.Boundary.GetId(), v.Boundary.GetValue()
	case *raftcmdpb.Preload_TransactionReference:
		return dal.AttributeCodeReference, v.TransactionReference.GetId(), v.TransactionReference.GetValue()
	case *raftcmdpb.Preload_SinkConfig:
		return dal.AttributeCodeSinkConfig, v.SinkConfig.GetId(), v.SinkConfig.GetValue()
	case *raftcmdpb.Preload_AccountMetadata:
		return dal.AttributeCodeMetadata, v.AccountMetadata.GetId(), v.AccountMetadata.GetValue()
	case *raftcmdpb.Preload_NumscriptVersion:
		return dal.AttributeCodeNumscriptVersion, v.NumscriptVersion.GetId(), v.NumscriptVersion.GetValue()
	case *raftcmdpb.Preload_TransactionState:
		return dal.AttributeCodeTransaction, v.TransactionState.GetId(), v.TransactionState.GetValue()
	case *raftcmdpb.Preload_NumscriptContent:
		return dal.AttributeCodeNumscriptContent, v.NumscriptContent.GetId(), v.NumscriptContent.GetValue()
	case *raftcmdpb.Preload_PreparedQuery:
		return dal.AttributeCodePreparedQuery, v.PreparedQuery.GetId(), v.PreparedQuery.GetValue()
	}

	return 0, nil, nil
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
//
// The cache-level meta key ([0xFF][CacheMetaKey]) and per-generation meta keys
// ([0xFF][gen][CacheGenMeta]) are written only on rotation, but per-entry rows
// ([0xFF][gen][cacheType][U128]) are written every batch by mergeSimpleWithCache.
// Recovery therefore must iterate the entry rows directly and treat any missing
// meta as "no rotation has happened yet" (currentGeneration=0, BaseIndex=0)
// rather than gating restoration on the meta sentinel.
func (s *CacheSnapshotter) RestoreFromStore() error {
	restoreStart := time.Now()

	s.registry.Cache.Reset()
	s.registry.Idempotency.Reset()

	// Read cache-level metadata if present. Pre-rotation, this key does not
	// exist; default to currentGeneration=0.
	currentGen := uint64(0)

	metaVal, closer, err := s.dataStore.Get([]byte{dal.KeyPrefixCacheSnapshot, dal.CacheMetaKey})
	if err == nil {
		meta := &raftcmdpb.CacheSnapshotMeta{}
		if unmarshalErr := meta.UnmarshalVT(metaVal); unmarshalErr != nil {
			_ = closer.Close()

			return fmt.Errorf("unmarshaling cache snapshot meta: %w", unmarshalErr)
		}

		_ = closer.Close()

		currentGen = meta.GetCurrentGeneration()
	}

	// Gen-byte mapping: gen0 at currentGeneration % 2, gen1 at the other byte.
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
//
// The per-generation meta key ([0xFF][gen][CacheGenMeta]) is only written on
// rotation. When absent, BaseIndex defaults to 0 (the pre-rotation value) and
// we still iterate the per-entry rows that mergeSimpleWithCache emits every
// batch.
func (s *CacheSnapshotter) restoreGeneration(genByte byte, genIndex int) error {
	// Read generation metadata if present.
	baseIndex := uint64(0)

	genMetaKey := []byte{dal.KeyPrefixCacheSnapshot, genByte, dal.CacheGenMeta}

	genMetaVal, closer, err := s.dataStore.Get(genMetaKey)
	if err == nil {
		genMeta := &raftcmdpb.CacheGenerationMeta{}
		if unmarshalErr := genMeta.UnmarshalVT(genMetaVal); unmarshalErr != nil {
			_ = closer.Close()

			return fmt.Errorf("unmarshaling gen meta: %w", unmarshalErr)
		}

		_ = closer.Close()

		baseIndex = genMeta.GetBaseIndex()
	}

	if genIndex == 0 {
		s.registry.Cache.BaseIndex.Gen0 = baseIndex
	} else {
		s.registry.Cache.BaseIndex.Gen1 = baseIndex
	}

	// Restore each cache type by iterating over its Pebble prefix.
	// All entries use lean format: [8-byte tag LE][raw value proto bytes].
	for _, slot := range s.slots {
		restoreFn := slot.RestoreEntry(genIndex)

		lower := []byte{dal.KeyPrefixCacheSnapshot, genByte, slot.CacheType()}
		upper := []byte{dal.KeyPrefixCacheSnapshot, genByte, slot.CacheType() + 1}

		iter, err := s.dataStore.NewIter(&pebble.IterOptions{
			LowerBound: lower,
			UpperBound: upper,
		})
		if err != nil {
			return fmt.Errorf("creating cache iter for type 0x%02x: %w", slot.CacheType(), err)
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

			if err := restoreFn(u128, value); err != nil {
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
