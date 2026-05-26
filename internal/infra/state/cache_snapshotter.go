package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"time"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/bloom"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/kv"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

// parseLeanValue extracts the tag and raw value bytes from a lean cache entry.
// Lean format: [8-byte tag LE][raw value bytes].
func parseLeanValue(value []byte) (uint64, []byte) {
	return binary.LittleEndian.Uint64(value[:8]), value[8:]
}

// putAttributeIfAbsent puts value at id only if store doesn't already have
// an entry. Returns the resulting in-store value (existing data on a no-op,
// or value when the put happened) and whether the put occurred.
func putAttributeIfAbsent[V any](
	store kv.KV[attributes.U128, attributes.Entry[V]],
	id attributes.U128,
	tag uint64,
	value V,
) (V, bool) {
	if existing, ok := store.Get(id); ok {
		return existing.Data, false
	}

	store.Put(id, attributes.Entry[V]{Tag: tag, Data: value})

	return value, true
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
	// IterKeys iterates all U128 keys in the given generation.
	IterKeys(genIndex int) iter.Seq[attributes.U128]
}

// protoSnapshotSlot implements cacheSnapshotSlot for a proto-backed AttributeCache.
type protoSnapshotSlot[V interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
	SizeVT() int
	MarshalToVT([]byte) (int, error)
}] struct {
	cacheType     byte
	ac            *cache.AttributeCache[V]
	newValue      func() V
	marshalBuffer []byte // reusable buffer for MarshalToVT to avoid per-key allocations
}

func (s *protoSnapshotSlot[V]) CacheType() byte { return s.cacheType }

// marshalValue marshals v into s.marshalBuffer, growing it if needed.
// The returned slice is valid until the next marshalValue call.
func (s *protoSnapshotSlot[V]) marshalValue(v V) ([]byte, error) {
	size := v.SizeVT()
	if cap(s.marshalBuffer) >= size {
		s.marshalBuffer = s.marshalBuffer[:size]
	} else {
		s.marshalBuffer = make([]byte, size)
	}

	n, err := v.MarshalToVT(s.marshalBuffer)

	return s.marshalBuffer[:n], err
}

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
		// Touch was a no-op: key was NOT in gen1.
		// The leader thought this key was in gen1 (CacheNeedsTouch) but this
		// node doesn't have it.
		details := map[string]any{
			"id":        fmt.Sprintf("%x", id),
			"cacheType": s.cacheType,
			"gen0Size":  s.ac.Gen0().Size(),
			"gen1Size":  s.ac.Gen1().Size(),
		}
		lifecycle.SendEvent("touch_noop", details)
		assert.Unreachable("touch_noop: key missing from gen1 — cache divergence imminent", details)

		return fmt.Errorf("cache divergence: touch_noop for key %x (cacheType=%d) — key missing from gen1, gen0Size=%d gen1Size=%d",
			id, s.cacheType, s.ac.Gen0().Size(), s.ac.Gen1().Size())
	}

	valueBytes, err := s.marshalValue(entry.Data)
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
	effective, gen1Set := putAttributeIfAbsent(s.ac.Gen1(), id, tag, typed)
	_, gen0Set := putAttributeIfAbsent(s.ac.Gen0(), id, tag, effective)

	if !gen0Set && !gen1Set {
		if s.cacheType == dal.SubAttrLedger {
			lifecycle.SendEvent("preload_skip", map[string]any{
				"id": fmt.Sprintf("%x", id),
			})
		}

		return nil
	}

	if s.cacheType == dal.SubAttrLedger {
		lifecycle.SendEvent("preload_ok", map[string]any{
			"id":      fmt.Sprintf("%x", id),
			"gen0Set": gen0Set,
			"gen1Set": gen1Set,
		})
	}

	valueBytes, err := s.marshalValue(effective)
	if err != nil {
		return fmt.Errorf("marshaling preloaded value: %w", err)
	}

	if gen0Set {
		if err := writeCacheRaw(batch, gen0Byte, s.cacheType, id, tag, valueBytes); err != nil {
			return fmt.Errorf("persisting preloaded gen0 value: %w", err)
		}
	}

	if gen1Set {
		if err := writeCacheRaw(batch, gen1Byte, s.cacheType, id, tag, valueBytes); err != nil {
			return fmt.Errorf("persisting preloaded gen1 value: %w", err)
		}
	}

	return nil
}

func (s *protoSnapshotSlot[V]) IterKeys(genIndex int) iter.Seq[attributes.U128] {
	store := s.selectGen(genIndex)

	return func(yield func(attributes.U128) bool) {
		for u128 := range store.Iter() {
			if !yield(u128) {
				return
			}
		}
	}
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
	SizeVT() int
	MarshalToVT([]byte) (int, error)
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

	// bloomExecutor ensures at most one background bloom goroutine runs at a time.
	// Interrupt cancels the current goroutine and waits for it to finish before
	// starting a new one. This avoids multiple concurrent writers on bloom filters.
	bloomExecutor *worker.SingleTaskExecutor
}

// NewCacheSnapshotter creates a CacheSnapshotter.
func NewCacheSnapshotter(logger logging.Logger, dataStore *dal.Store, registry *StateRegistry, bloomFilters *bloom.FilterSet) *CacheSnapshotter {
	c := registry.Cache

	volumes := newProtoSnapshotSlot(dal.SubAttrVolume, c.Volumes, func() *raftcmdpb.VolumePair { return &raftcmdpb.VolumePair{} })
	metadata := newProtoSnapshotSlot(dal.SubAttrMetadata, c.AccountMetadata, func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} })
	ledgers := newProtoSnapshotSlot(dal.SubAttrLedger, c.Ledgers, func() *commonpb.LedgerInfo { return &commonpb.LedgerInfo{} })
	boundaries := newProtoSnapshotSlot(dal.SubAttrBoundary, c.Boundaries, func() *raftcmdpb.LedgerBoundaries { return &raftcmdpb.LedgerBoundaries{} })
	references := newProtoSnapshotSlot(dal.SubAttrReference, c.References, func() *commonpb.TransactionReferenceValue { return &commonpb.TransactionReferenceValue{} })
	transactions := newProtoSnapshotSlot(dal.SubAttrTransaction, c.Transactions, func() *commonpb.TransactionState { return &commonpb.TransactionState{} })
	sinks := newProtoSnapshotSlot(dal.SubAttrSinkConfig, c.SinkConfigs, func() *commonpb.SinkConfig { return &commonpb.SinkConfig{} })
	numscriptVersions := newProtoSnapshotSlot(dal.SubAttrNumscriptVersion, c.NumscriptVersions, func() *commonpb.NumscriptVersionValue { return &commonpb.NumscriptVersionValue{} })
	numscriptContents := newProtoSnapshotSlot(dal.SubAttrNumscriptContent, c.NumscriptContents, func() *commonpb.NumscriptInfo { return &commonpb.NumscriptInfo{} })
	preparedQueries := newProtoSnapshotSlot(dal.SubAttrPreparedQuery, c.PreparedQueries, func() *commonpb.PreparedQuery { return &commonpb.PreparedQuery{} })
	ledgerMetadata := newProtoSnapshotSlot(dal.SubAttrLedgerMetadata, c.LedgerMetadata, func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} })

	return &CacheSnapshotter{
		logger:       logger,
		dataStore:    dataStore,
		registry:     registry,
		bloomFilters: bloomFilters,
		slots: []cacheSnapshotSlot{
			volumes, metadata, ledgers, boundaries, references,
			transactions, sinks, numscriptVersions, numscriptContents,
			preparedQueries, ledgerMetadata,
		},
		touchSlots: map[byte]cacheSnapshotSlot{
			dal.SubAttrVolume:           volumes,
			dal.SubAttrMetadata:         metadata,
			dal.SubAttrLedger:           ledgers,
			dal.SubAttrBoundary:         boundaries,
			dal.SubAttrReference:        references,
			dal.SubAttrTransaction:      transactions,
			dal.SubAttrSinkConfig:       sinks,
			dal.SubAttrNumscriptVersion: numscriptVersions,
			dal.SubAttrNumscriptContent: numscriptContents,
			dal.SubAttrPreparedQuery:    preparedQueries,
			dal.SubAttrLedgerMetadata:   ledgerMetadata,
		},
		bloomExecutor: worker.NewSingleTaskExecutor(logger),
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
		return dal.SubAttrVolume, v.Volume.GetId(), v.Volume.GetValue()
	case *raftcmdpb.Preload_IdempotencyKey:
		// Idempotency keys are no longer stored in the cache system.
		// They are handled separately via the dedicated IdempotencyStore.
		return 0, nil, nil
	case *raftcmdpb.Preload_Ledger:
		return dal.SubAttrLedger, v.Ledger.GetId(), v.Ledger.GetValue()
	case *raftcmdpb.Preload_Boundary:
		return dal.SubAttrBoundary, v.Boundary.GetId(), v.Boundary.GetValue()
	case *raftcmdpb.Preload_TransactionReference:
		return dal.SubAttrReference, v.TransactionReference.GetId(), v.TransactionReference.GetValue()
	case *raftcmdpb.Preload_SinkConfig:
		return dal.SubAttrSinkConfig, v.SinkConfig.GetId(), v.SinkConfig.GetValue()
	case *raftcmdpb.Preload_AccountMetadata:
		return dal.SubAttrMetadata, v.AccountMetadata.GetId(), v.AccountMetadata.GetValue()
	case *raftcmdpb.Preload_NumscriptVersion:
		return dal.SubAttrNumscriptVersion, v.NumscriptVersion.GetId(), v.NumscriptVersion.GetValue()
	case *raftcmdpb.Preload_TransactionState:
		return dal.SubAttrTransaction, v.TransactionState.GetId(), v.TransactionState.GetValue()
	case *raftcmdpb.Preload_NumscriptContent:
		return dal.SubAttrNumscriptContent, v.NumscriptContent.GetId(), v.NumscriptContent.GetValue()
	case *raftcmdpb.Preload_PreparedQuery:
		return dal.SubAttrPreparedQuery, v.PreparedQuery.GetId(), v.PreparedQuery.GetValue()
	case *raftcmdpb.Preload_LedgerMetadata:
		return dal.SubAttrLedgerMetadata, v.LedgerMetadata.GetId(), v.LedgerMetadata.GetValue()
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

	metaVal, closer, err := s.dataStore.Get([]byte{dal.ZoneCache, dal.SubCacheMeta})
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

	// Restore bloom filters: load persisted blocks from Pebble, then replay
	// cache gen0+gen1 entries to fill the gap since the last rotation flush.
	// If no persisted blocks exist (first boot), fall back to a full attribute scan.
	if s.bloomFilters != nil {
		s.restoreBloomFilters()
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

	genMetaKey := []byte{dal.ZoneCache, genByte, dal.SubCacheGenMeta}

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

		lower := []byte{dal.ZoneCache, genByte, slot.CacheType()}
		upper := []byte{dal.ZoneCache, genByte, slot.CacheType() + 1}

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

// restoreBloomFilters launches a background task to restore bloom filters.
// If persisted blocks exist, loads them from Pebble and replays cache gen0+gen1
// to fill the gap. Otherwise, falls back to a full attribute scan.
// In both cases, IsReady() remains false until the background work completes.
func (s *CacheSnapshotter) restoreBloomFilters() {
	if s.hasPersistedBloomBlocks() {
		s.runBloomTask("restore from Pebble blocks", s.bloomFilters.RestoreFromStore)

		return
	}

	s.StartAsyncBloomPopulate("first boot: no persisted bloom blocks")
}

// StartAsyncBloomPopulate interrupts any running bloom task and launches a
// new one that populates the bloom filters from a full Pebble attribute scan.
// Used on first boot and after bloom config changes.
func (s *CacheSnapshotter) StartAsyncBloomPopulate(reason string) {
	s.runBloomTask(reason, s.bloomFilters.PopulateFromStore)
}

// runBloomTask is the common entry point for background bloom work.
// It interrupts any in-flight task, captures the current epoch, and runs
// loadFn (restore or populate) via the SingleTaskExecutor. After loadFn,
// it replays cache gen0+gen1 and marks the bloom as ready only if no
// Rebuild occurred during the work.
func (s *CacheSnapshotter) runBloomTask(reason string, loadFn func(context.Context, dal.PebbleReader) error) {
	s.bloomExecutor.Interrupt()

	s.logger.WithFields(map[string]any{
		"reason": reason,
	}).Infof("Bloom background task starting")

	epoch := s.bloomFilters.Epoch()

	s.bloomExecutor.Run(context.Background(), func(ctx context.Context) error {
		start := time.Now()

		if err := loadFn(ctx, s.dataStore); err != nil {
			return err
		}

		if err := s.replayBloomFromCache(ctx); err != nil {
			return err
		}

		if !s.bloomFilters.SetReadyIfEpoch(epoch) {
			s.logger.WithFields(map[string]any{
				"reason": reason,
			}).Infof("Bloom background task skipped SetReady: Rebuild occurred")

			return nil
		}

		s.logger.WithFields(map[string]any{
			"reason":   reason,
			"duration": time.Since(start).String(),
		}).Infof("Bloom background task complete")

		return nil
	})
}

// hasPersistedBloomBlocks checks if any bloom block keys exist in Pebble.
func (s *CacheSnapshotter) hasPersistedBloomBlocks() bool {
	lower := []byte{dal.ZoneGlobal, dal.SubGlobBloom}
	upper := []byte{dal.ZoneGlobal, dal.SubGlobBloom + 1}

	iter, err := s.dataStore.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return false
	}

	defer func() { _ = iter.Close() }()

	return iter.First()
}

// replayBloomFromCache iterates all entries in cache gen0 and gen1 and adds
// their canonical keys to the bloom filters. This fills the gap between the
// last rotation flush (when bloom blocks were persisted) and the current state.
// Adding duplicates is harmless for a bloom filter.
//
// The filter snapshot is captured once so that all attribute types are resolved
// from the same snapshot. Without this, a concurrent Rebuild could swap the
// pointer between slots, causing some replays to target the old snapshot and
// others the new one.
//
// The context is checked periodically so that Interrupt() can cancel the
// replay quickly without blocking the FSM goroutine.
func (s *CacheSnapshotter) replayBloomFromCache(ctx context.Context) error {
	snap := s.bloomFilters.Snapshot()

	for _, slot := range s.slots {
		f := snap.FilterForAttrType(slot.CacheType())
		if f == nil {
			continue
		}

		for _, genIndex := range []int{0, 1} {
			for u128 := range slot.IterKeys(genIndex) {
				if err := ctx.Err(); err != nil {
					return err
				}

				f.Add(u128)
			}
		}
	}

	return nil
}

// Stop interrupts any running bloom task and waits for it to finish.
func (s *CacheSnapshotter) Stop() {
	s.bloomExecutor.Interrupt()
}
