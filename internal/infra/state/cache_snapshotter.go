package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
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

// parseLeanValue extracts the tag, tombstone flag, and raw value bytes from a
// lean cache entry. Lean format: [8-byte tag LE][1-byte flag][raw value bytes].
// Panics when the buffer is shorter than cacheValueHeaderLen or when the flag
// byte is neither cacheValueFlagLive nor cacheValueFlagTombstone — every 0xFF
// row is produced by writeCacheRaw, so any other shape means a corrupted store
// or a forward-incompatible binary, and silently treating an unknown flag as
// live would let either case resurrect deleted keys.
func parseLeanValue(value []byte) (tag uint64, deleted bool, valueBytes []byte) {
	if len(value) < cacheValueHeaderLen {
		panic(fmt.Sprintf("cache snapshotter: 0xFF value of %d bytes is shorter than the %d-byte lean header — store corrupted",
			len(value), cacheValueHeaderLen))
	}

	tag = binary.LittleEndian.Uint64(value[:8])

	switch value[8] {
	case cacheValueFlagLive:
		deleted = false
	case cacheValueFlagTombstone:
		deleted = true
		// Tombstones are always 9 bytes on the wire: writeCacheRaw ignores
		// valueBytes when deleted=true. Trailing payload after the flag
		// means the row was written by a corrupt or forward-incompatible
		// producer — silently dropping it would let an attacker mask a
		// live row as a tombstone with a one-byte flip.
		if len(value) != cacheValueHeaderLen {
			panic(fmt.Sprintf("cache snapshotter: 0xFF tombstone row has %d trailing bytes after the %d-byte lean header — store corrupted or written by an incompatible binary",
				len(value)-cacheValueHeaderLen, cacheValueHeaderLen))
		}
	default:
		panic(fmt.Sprintf("cache snapshotter: 0xFF value has unknown tombstone flag 0x%02x at offset 8 (expected 0x%02x or 0x%02x) — store corrupted or written by an incompatible binary",
			value[8], cacheValueFlagLive, cacheValueFlagTombstone))
	}

	return tag, deleted, value[cacheValueHeaderLen:]
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

func (s *protoSnapshotSlot[V]) MirrorPreload(
	batch *dal.WriteSession,
	gen0Byte, gen1Byte byte,
	attrID *raftcmdpb.AttributeID,
	rawValue []byte,
) error {
	// rawValue is the vtproto-marshaled blob of the (live) attribute value.
	// It can legitimately be empty — a presence-only marker or an all-default
	// proto marshals to zero bytes (EN-1377). The tombstone signal lives in
	// the on-disk lean format's flag byte, not in len(rawValue); admission
	// emits a Declare plan for absent keys (EN-1378) and never reaches this
	// code path for tombstones. Unmarshal unconditionally: vtproto produces
	// a valid zero-value proto from an empty buffer.
	typed := s.newValue()
	if err := typed.UnmarshalVT(rawValue); err != nil {
		return fmt.Errorf("MirrorPreload: unmarshal cacheType=0x%x (%d bytes): %w", s.cacheType, len(rawValue), err)
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
		if err := writeCacheRaw(batch, gen0Byte, s.cacheType, id, tag, false, valueBytes); err != nil {
			return fmt.Errorf("persisting preloaded gen0 value: %w", err)
		}
	}

	if gen1Set {
		if err := writeCacheRaw(batch, gen1Byte, s.cacheType, id, tag, false, valueBytes); err != nil {
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
		tag, deleted, valueBytes := parseLeanValue(rawValue)

		// The flag byte at offset 8 distinguishes tombstones from live entries.
		// Tombstones are kept in cache to shadow any live row in gen1 (see
		// AttributeCache.Del's lazy fabrication). A live entry whose proto
		// marshals to zero bytes is valid and must round-trip as live
		// (EN-1377).
		if deleted {
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
// snapshotter as a hot-path field (for MirrorPreload, which is a pure
// write operation), so a reader stored here would re-introduce indirect
// Pebble-read access from the hot path. Reader-bearing methods
// (RestoreFromStore, StartAsyncBloomPopulate, hasPersistedBloomBlocks) accept
// the reader as a parameter and are only called from non-hot-path contexts
// (Recovery, bootstrap).
type CacheSnapshotter struct {
	logger       logging.Logger
	registry     *StateRegistry
	bloomFilters *bloom.FilterSet
	slots        []cacheSnapshotSlot
	// slotByAttrCode maps attribute code bytes to the corresponding slot,
	// for the FSM apply path's MirrorPreload dispatch.
	slotByAttrCode map[byte]cacheSnapshotSlot

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
	indexEntries := newProtoSnapshotSlot(dal.SubAttrIndex, c.Indexes, func() *commonpb.Index { return &commonpb.Index{} })

	return &CacheSnapshotter{
		logger:       logger,
		registry:     registry,
		bloomFilters: bloomFilters,
		slots: []cacheSnapshotSlot{
			volumes, metadata, ledgers, boundaries, references,
			transactions, sinks, numscriptVersions, numscriptContents,
			preparedQueries, ledgerMetadata, indexEntries,
		},
		slotByAttrCode: map[byte]cacheSnapshotSlot{
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
			dal.SubAttrIndex:            indexEntries,
		},
		bloomExecutor: worker.NewSingleTaskExecutor(logger),
	}
}

// MirrorPreload populates both in-memory generations and mirrors to 0xFF
// at both byte positions. attrCode (from the parent AttributeCoverage) picks
// the slot; value.raw_value carries the typed value bytes (vtproto-
// marshaled), and attrID carries the U128 + the xxh3 collision tag.
func (s *CacheSnapshotter) MirrorPreload(batch *dal.WriteSession, gen0Byte, gen1Byte byte, attrID *raftcmdpb.AttributeID, attrCode byte, value *raftcmdpb.AttributeValue) error {
	if attrID == nil || value == nil {
		return nil
	}

	slot, ok := s.slotByAttrCode[attrCode]
	if !ok {
		return nil
	}

	return slot.MirrorPreload(batch, gen0Byte, gen1Byte, attrID, value.GetRawValue())
}

// persistLeanProtoEntries writes all entries from a KV store to 0xFF in lean format.
// Tombstones (entry.Deleted) are written as a 9-byte row with the flag byte
// set; their pre-delete entry.Data is intentionally not marshaled (it would
// resurrect on restore — AttributeCache.Del only flips Deleted and keeps the
// payload around).
func persistLeanProtoEntries[V interface {
	MarshalVT() ([]byte, error)
}](batch *dal.WriteSession, genByte, cacheType byte, store kv.KV[attributes.U128, attributes.Entry[V]]) error {
	for u128, entry := range store.Iter() {
		if entry.Deleted {
			if err := writeCacheRaw(batch, genByte, cacheType, u128, entry.Tag, true, nil); err != nil {
				return err
			}

			continue
		}

		valueBytes, err := entry.Data.MarshalVT()
		if err != nil {
			return fmt.Errorf("marshaling cache value: %w", err)
		}

		if err := writeCacheRaw(batch, genByte, cacheType, u128, entry.Tag, false, valueBytes); err != nil {
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
	// On the persisted-blocks (restart) path, restoreBloomFilters runs the
	// rebuild synchronously and returns once IsReady() is true -- closes
	// the EN-1410 window where replayWAL would race the rebuild and drive
	// rotations while !IsReady.
	if s.bloomFilters != nil {
		if err := s.restoreBloomFilters(store); err != nil {
			return fmt.Errorf("restoring bloom filters: %w", err)
		}
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
	// All entries use lean format: [8-byte tag LE][1-byte flag][raw value proto bytes].
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

// restoreBloomFilters publishes a ready bloom on top of Pebble's
// persisted state. On a simple restart (persisted bloom blocks exist),
// the rebuild runs SYNCHRONOUSLY before this function returns: the cost
// is bounded (O(blocks) + O(cache gen0+gen1)) and running it inline
// closes the EN-1410 window where the replayWAL goroutine would
// otherwise race the background rebuild and drive cache rotations
// while !IsReady. On cold start / post-Rebuild (no persisted blocks),
// the rebuild stays async because the full attribute scan via
// PopulateFromStore can take minutes on a large database -- blocking
// boot is unacceptable. The cold-start path's correctness is guarded
// separately at the cache-rotation site, which inhibits rotation while
// the bloom is still populating (see machine.go).
func (s *CacheSnapshotter) restoreBloomFilters(store dal.RecoveryReader) error {
	if s.hasPersistedBloomBlocks(store) {
		return s.runBloomTaskSync(store, "restore from Pebble blocks", s.bloomFilters.RestoreFromStore)
	}

	s.StartAsyncBloomPopulate(store, "first boot: no persisted bloom blocks")

	return nil
}

// StartAsyncBloomPopulate interrupts any running bloom task and launches a
// new one that populates the bloom filters from a full Pebble attribute scan.
// Used on first boot and after bloom config changes. The reader is captured
// by the goroutine; the snapshotter itself does not retain it.
func (s *CacheSnapshotter) StartAsyncBloomPopulate(store dal.RecoveryReader, reason string) {
	s.runBloomTask(store, reason, s.bloomFilters.PopulateFromStore)
}

// runBloomTaskBody runs loadFn + replayBloomFromCache + SetReadyIfEpoch
// in the calling goroutine using the provided context. Shared between
// runBloomTask (wraps it in a SingleTaskExecutor) and runBloomTaskSync
// (drives it inline on the boot path).
func (s *CacheSnapshotter) runBloomTaskBody(ctx context.Context, store dal.RecoveryReader, reason string, epoch uint64, loadFn func(context.Context, dal.PebbleReader) error) error {
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
		}).Infof("Bloom task skipped SetReady: Rebuild occurred")

		return nil
	}

	s.logger.WithFields(map[string]any{
		"reason":   reason,
		"duration": time.Since(start).String(),
	}).Infof("Bloom task complete")

	return nil
}

// runBloomTask is the common entry point for background bloom work.
// It interrupts any in-flight task, captures the current epoch, and runs
// loadFn via the SingleTaskExecutor. See runBloomTaskBody for the actual
// load + replay + SetReady sequence.
func (s *CacheSnapshotter) runBloomTask(store dal.RecoveryReader, reason string, loadFn func(context.Context, dal.PebbleReader) error) {
	s.bloomExecutor.Interrupt()

	s.logger.WithFields(map[string]any{
		"reason": reason,
	}).Infof("Bloom background task starting")

	epoch := s.bloomFilters.Epoch()

	s.bloomExecutor.Run(context.Background(), func(ctx context.Context) error {
		return s.runBloomTaskBody(ctx, store, reason, epoch, loadFn)
	})
}

// runBloomTaskSync is the synchronous variant used on the restart path
// (persisted bloom blocks exist). It interrupts any in-flight async
// task, captures the current epoch, and runs the body inline against a
// background context. By the time it returns, the bloom is ready (or
// the call errored out), so the caller can safely begin work that
// relies on bloom completeness -- in particular replayWAL.
func (s *CacheSnapshotter) runBloomTaskSync(store dal.RecoveryReader, reason string, loadFn func(context.Context, dal.PebbleReader) error) error {
	s.bloomExecutor.Interrupt()

	s.logger.WithFields(map[string]any{
		"reason": reason,
	}).Infof("Bloom sync task starting")

	epoch := s.bloomFilters.Epoch()

	return s.runBloomTaskBody(context.Background(), store, reason, epoch, loadFn)
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
