package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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
	if existing, ok := store.Get(id); ok && !existing.Deleted {
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
	Persist(batch *dal.WriteSession, genByte byte, genIndex int) error
	// RestoreEntry returns a function that restores a single entry into the given generation.
	RestoreEntry(genIndex int) func(u128 attributes.U128, rawValue []byte) error
	// MirrorTouch promotes id from gen1 to gen0 in-memory and mirrors the
	// new gen0 entry to 0xFF. No-op if gen0 already has the key or the key
	// is in neither generation.
	MirrorTouch(batch *dal.WriteSession, gen0Byte byte, id attributes.U128) error
	// MirrorPreload puts value into both in-memory generations and mirrors
	// to 0xFF at both byte positions. rawValue is the vtproto-marshaled
	// blob carried by Preload.raw_value; the concrete implementation
	// unmarshals it into its V before applying the tombstone/Gen1-wins
	// rules.
	MirrorPreload(batch *dal.WriteSession, gen0Byte, gen1Byte byte, attrID *raftcmdpb.AttributeID, rawValue []byte) error
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

func (s *protoSnapshotSlot[V]) Persist(batch *dal.WriteSession, genByte byte, genIndex int) error {
	return persistLeanProtoEntries(batch, genByte, s.cacheType, s.selectGen(genIndex))
}

func (s *protoSnapshotSlot[V]) MirrorTouch(batch *dal.WriteSession, gen0Byte byte, id attributes.U128) error {
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

	// A tombstone keeps the pre-delete value in entry.Data (AttributeCache.Del
	// only flips Deleted), so marshalValue would persist a live value to 0xFF and
	// resurrect the key when a node rehydrates its cache from the snapshot zone.
	// Empty bytes are the tombstone form RestoreEntry reads back as Deleted.
	var valueBytes []byte
	if !entry.Deleted {
		var err error
		valueBytes, err = s.marshalValue(entry.Data)
		if err != nil {
			return fmt.Errorf("marshaling touched value: %w", err)
		}
	}

	if err := writeCacheRaw(batch, gen0Byte, s.cacheType, id, entry.Tag, valueBytes); err != nil {
		return fmt.Errorf("persisting touched value: %w", err)
	}

	return nil
}

func (s *protoSnapshotSlot[V]) MirrorPreload(
	batch *dal.WriteSession,
	gen0Byte, gen1Byte byte,
	attrID *raftcmdpb.AttributeID,
	rawValue []byte,
) error {
	// Empty rawValue is the bloom-confirmed-absent signal: the proposer told
	// the FSM "this key has no value in Pebble". The cache must hold a
	// typed-nil so read paths that gate on `entry.Data != nil` (e.g.
	// ResolveLedgerID) fall through to Pebble — a non-nil zero-proto would
	// instead surface a phantom "id=0" hit.
	//
	// The dual on disk is a tombstone row: writeCacheRaw with empty bytes is
	// what RestoreEntry interprets as Deleted=true, so a restart rebuilds
	// the same nil-entry semantics.
	var typed V
	if len(rawValue) > 0 {
		typed = s.newValue()
		if err := typed.UnmarshalVT(rawValue); err != nil {
			return fmt.Errorf("MirrorPreload: unmarshal cacheType=0x%x (%d bytes): %w", s.cacheType, len(rawValue), err)
		}
	}

	id := attributes.U128FromBytes(attrID.GetId())
	tag := attrID.GetTag()

	// Preserve tombstones. A delete that landed in Raft order after the
	// preload was scanned writes a tombstone here; overwriting it with
	// the stale scanned value would silently resurrect metadata the
	// user deleted, and the per-entry CAS in applyMetadataConversionBatch
	// would then see a live entry and write the converted value. See
	// the metadata-conversion race surfaced on #359.
	//
	// The tag check matters in the (improbable but non-zero) case of a
	// U128 collision: a tombstone for canonical-key A would otherwise
	// suppress the preload for canonical-key B that hashes to the same
	// U128 id. With the tag check, only a tombstone for THIS key
	// short-circuits.
	if existing, ok := s.ac.Gen1().Get(id); ok && existing.Deleted && existing.Tag == tag {
		return nil
	}

	if existing, ok := s.ac.Gen0().Get(id); ok && existing.Deleted && existing.Tag == tag {
		return nil
	}

	// gen1 wins if it already has a (post-rotation, possibly fresher)
	// value; otherwise the preload value populates both gens.
	effective, gen1Set := putAttributeIfAbsent(s.ac.Gen1(), id, tag, typed)
	_, gen0Set := putAttributeIfAbsent(s.ac.Gen0(), id, tag, effective)

	if !gen0Set && !gen1Set {
		return nil
	}

	// Common path (Gen1 was empty, effective == typed): reuse the rawValue
	// bytes that arrived on the wire instead of re-marshaling. The cold
	// path (Gen1 won, effective is its older entry) re-serializes.
	valueBytes := rawValue
	if !gen1Set {
		var err error
		valueBytes, err = s.marshalValue(effective)
		if err != nil {
			return fmt.Errorf("marshaling preloaded value: %w", err)
		}
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

		// Empty value bytes = tombstone (key was deleted but kept in cache
		// to prevent pipelined MirrorTouch failures).
		if len(valueBytes) == 0 {
			var zero V
			store.Put(u128, attributes.Entry[V]{Tag: tag, Data: zero, Deleted: true})

			return nil
		}

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
//
// The snapshotter does NOT retain a dal.RecoveryReader: Machine holds a
// snapshotter as a hot-path field (for MirrorTouch / MirrorPreload, which are
// pure write operations), so a reader stored here would re-introduce indirect
// Pebble-read access from the hot path. Reader-bearing methods
// (RestoreFromStore, StartAsyncBloomPopulate, hasPersistedBloomBlocks) accept
// the reader as a parameter and are only called from non-hot-path contexts
// (Recovery, bootstrap).
type CacheSnapshotter struct {
	logger       logging.Logger
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
func NewCacheSnapshotter(logger logging.Logger, registry *StateRegistry, bloomFilters *bloom.FilterSet) *CacheSnapshotter {
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
func (s *CacheSnapshotter) MirrorTouch(batch *dal.WriteSession, attrType byte, gen0Byte byte, id attributes.U128) error {
	slot, ok := s.touchSlots[attrType]
	if !ok {
		return nil
	}

	return slot.MirrorTouch(batch, gen0Byte, id)
}

// MirrorPreload populates both in-memory generations and mirrors to 0xFF
// at both byte positions. attrCode (from the parent AttributePlan) picks
// the slot; value.raw_value carries the typed value bytes (vtproto-
// marshaled), and attrID carries the U128 + the xxh3 collision tag.
func (s *CacheSnapshotter) MirrorPreload(batch *dal.WriteSession, gen0Byte, gen1Byte byte, attrID *raftcmdpb.AttributeID, attrCode byte, value *raftcmdpb.AttributeValue) error {
	if attrID == nil || value == nil {
		return nil
	}

	slot, ok := s.touchSlots[attrCode]
	if !ok {
		return nil
	}

	return slot.MirrorPreload(batch, gen0Byte, gen1Byte, attrID, value.GetRawValue())
}

// persistLeanProtoEntries writes all entries from a KV store to 0xFF in lean format.
func persistLeanProtoEntries[V interface {
	MarshalVT() ([]byte, error)
}](batch *dal.WriteSession, genByte, cacheType byte, store kv.KV[attributes.U128, attributes.Entry[V]]) error {
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
func (s *CacheSnapshotter) RestoreFromStore(store dal.RecoveryReader) error {
	restoreStart := time.Now()

	s.registry.Cache.Reset()
	s.registry.Idempotency.Reset()
	s.registry.BackupJobs.Reset()

	reader, err := store.NewDirectReadHandle()
	if err != nil {
		return fmt.Errorf("creating read handle for restore: %w", err)
	}

	defer func() { _ = reader.Close() }()

	// Rebuild the idempotency bridge from Pebble. The Reset above cleared the
	// in-memory map, but the underlying Pebble entries survive snapshot
	// restoration — without this scan the FSM would re-accept already-applied
	// idempotent operations until the bridge naturally refilled, breaking the
	// at-most-once guarantee. See issue #300.
	if err := s.registry.Idempotency.RestoreFromStore(reader); err != nil {
		return fmt.Errorf("restoring idempotency bridge: %w", err)
	}

	// Rebuild the backup-jobs map from Pebble — same rationale as
	// Idempotency above. Active jobs survive across snapshot/restore and
	// the in-memory map must match what's on disk before the FSM accepts
	// a new BackupOrderStart.
	if err := s.registry.BackupJobs.RestoreFromStore(reader); err != nil {
		return fmt.Errorf("restoring backup jobs: %w", err)
	}

	// Read cache-level metadata if present. Pre-rotation, this key does not
	// exist; default to currentGeneration=0.
	currentGen := uint64(0)

	metaVal, closer, err := store.Get([]byte{dal.ZoneCache, dal.SubCacheMeta})
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

	if err := s.restoreGeneration(reader, gen0Byte, 0); err != nil {
		return fmt.Errorf("restoring cache gen0 from byte %d: %w", gen0Byte, err)
	}

	if err := s.restoreGeneration(reader, gen1Byte, 1); err != nil {
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
		s.restoreBloomFilters(store)
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
func (s *CacheSnapshotter) restoreGeneration(reader dal.PebbleReader, genByte byte, genIndex int) error {
	// Read generation metadata if present.
	baseIndex := uint64(0)

	genMetaKey := []byte{dal.ZoneCache, genByte, dal.SubCacheGenMeta}

	genMetaVal, closer, err := reader.Get(genMetaKey)
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

		iter, err := reader.NewIter(&pebble.IterOptions{
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
// The store is captured by the background goroutine so it can open a fresh
// read handle each invocation.
func (s *CacheSnapshotter) restoreBloomFilters(store dal.RecoveryReader) {
	if s.hasPersistedBloomBlocks(store) {
		s.runBloomTask(store, "restore from Pebble blocks", s.bloomFilters.RestoreFromStore)

		return
	}

	s.StartAsyncBloomPopulate(store, "first boot: no persisted bloom blocks")
}

// StartAsyncBloomPopulate interrupts any running bloom task and launches a
// new one that populates the bloom filters from a full Pebble attribute scan.
// Used on first boot and after bloom config changes. The reader is captured
// by the goroutine; the snapshotter itself does not retain it.
func (s *CacheSnapshotter) StartAsyncBloomPopulate(store dal.RecoveryReader, reason string) {
	s.runBloomTask(store, reason, s.bloomFilters.PopulateFromStore)
}

// runBloomTask is the common entry point for background bloom work.
// It interrupts any in-flight task, captures the current epoch, and runs
// loadFn (restore or populate) via the SingleTaskExecutor. After loadFn,
// it replays cache gen0+gen1 and marks the bloom as ready only if no
// Rebuild occurred during the work.
func (s *CacheSnapshotter) runBloomTask(store dal.RecoveryReader, reason string, loadFn func(context.Context, dal.PebbleReader) error) {
	s.bloomExecutor.Interrupt()

	s.logger.WithFields(map[string]any{
		"reason": reason,
	}).Infof("Bloom background task starting")

	epoch := s.bloomFilters.Epoch()

	s.bloomExecutor.Run(context.Background(), func(ctx context.Context) error {
		start := time.Now()

		// Hold dbMu.RLock for the entire bloom load to prevent RestoreCheckpoint
		// from closing the DB while iterators are open. StopBackgroundTasks cancels
		// the context first, so the bloom iteration exits and releases the lock
		// before RestoreCheckpoint acquires the exclusive lock.
		reader, err := store.NewDirectReadHandle()
		if err != nil {
			return fmt.Errorf("creating read handle for bloom task: %w", err)
		}

		loadErr := loadFn(ctx, reader)
		_ = reader.Close()

		if loadErr != nil {
			return loadErr
		}

		if err := s.replayBloomFromCache(ctx); err != nil {
			return err
		}

		if !s.bloomFilters.SetReadyIfEpoch(epoch) {
			// The rarest interleaving (#391-class): a Rebuild bumped the epoch
			// while this task was scanning, so publishing readiness would have
			// exposed a half-populated filter. Skipping is the correct path —
			// the rebuild's own task republishes readiness.
			assert.Reachable("bloom SetReady skipped: rebuild raced populate", map[string]any{
				"reason": reason,
				"epoch":  epoch,
			})

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
func (s *CacheSnapshotter) hasPersistedBloomBlocks(store dal.RecoveryReader) bool {
	handle, err := store.NewDirectReadHandle()
	if err != nil {
		return false
	}

	defer func() { _ = handle.Close() }()

	lower := []byte{dal.ZoneGlobal, dal.SubGlobBloom}
	upper := []byte{dal.ZoneGlobal, dal.SubGlobBloom + 1}

	iter, err := handle.NewIter(&pebble.IterOptions{
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
