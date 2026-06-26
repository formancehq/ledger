package state

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newVolumeKey(ak domain.AccountKey, asset string) domain.VolumeKey {
	base, prec := domain.ParseAssetPrecision(asset)

	return domain.VolumeKey{
		AccountKey:     ak,
		Asset:          asset,
		AssetBase:      base,
		AssetPrecision: prec,
	}
}

func persistGeneration(s *CacheSnapshotter, batch *dal.WriteSession, genByte byte, genIndex int) error {
	var baseIndex uint64
	if genIndex == 0 {
		baseIndex = s.registry.Cache.BaseIndex.Gen0
	} else {
		baseIndex = s.registry.Cache.BaseIndex.Gen1
	}

	if err := batch.SetProto(
		[]byte{dal.ZoneCache, genByte, dal.SubCacheGenMeta},
		&raftcmdpb.CacheGenerationMeta{BaseIndex: baseIndex},
	); err != nil {
		return fmt.Errorf("writing gen meta: %w", err)
	}

	for _, slot := range s.slots {
		if err := slot.Persist(batch, genByte, genIndex); err != nil {
			return err
		}
	}

	return nil
}

// persistToStore is a test helper that writes cache and reversions to Pebble
// in a single batch, so that RestoreFromStore can be tested. The session
// factory is passed explicitly — CacheSnapshotter itself only holds the
// narrow LifecycleHandle capability and cannot open write sessions.
func persistToStore(s *CacheSnapshotter, sessions dal.WriteSessionFactory) error {
	batch := sessions.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	for ledger, bs := range s.registry.Reversions {
		for i := range bs.WordCount() {
			if err := saveReversionWord(batch, ledger, i, bs.Word(i)); err != nil {
				return fmt.Errorf("saving reversion word for %q: %w", ledger, err)
			}
		}
	}

	if err := batch.DeleteRangeNoSync(
		[]byte{dal.ZoneCache},
		[]byte{dal.ZoneCache, dal.SubCacheMeta, 0x01},
	); err != nil {
		return fmt.Errorf("clearing cache snapshot range: %w", err)
	}

	currentGen := s.registry.Cache.CurrentGeneration()
	gen0Byte := byte(currentGen % 2)
	gen1Byte := byte((currentGen + 1) % 2)

	if err := persistGeneration(s, batch, gen0Byte, 0); err != nil {
		return fmt.Errorf("persisting cache gen0: %w", err)
	}

	if err := persistGeneration(s, batch, gen1Byte, 1); err != nil {
		return fmt.Errorf("persisting cache gen1: %w", err)
	}

	if err := batch.SetProto(
		[]byte{dal.ZoneCache, dal.SubCacheMeta},
		&raftcmdpb.CacheSnapshotMeta{
			CurrentGeneration: s.registry.Cache.CurrentGeneration(),
		},
	); err != nil {
		return fmt.Errorf("writing cache snapshot meta: %w", err)
	}

	return batch.Commit()
}

func newTestCacheSnapshotter(t *testing.T, bloomFilters *bloom.FilterSet) (*CacheSnapshotter, *dal.Store, *StateRegistry) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	attrs := attributes.New()

	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	registry := NewStateRegistry(c, attrs, 0)

	snapshotter := NewCacheSnapshotter(logger, registry, bloomFilters)

	return snapshotter, dataStore, registry
}

func TestCacheSnapshotter_PersistAndRestoreEmpty(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	// Persist empty cache
	require.NoError(t, persistToStore(snapshotter, dataStore))

	// Reset and restore
	registry.Cache.Reset()

	require.NoError(t, snapshotter.RestoreFromStore(dataStore))
}

func TestCacheSnapshotter_PersistAndRestoreVolumes(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	// Populate cache with volume data in gen0
	volumeKey := newVolumeKey(domain.AccountKey{LedgerName: "test", Account: "bank"}, "USD")
	u128 := attributes.HashU128(volumeKey.Bytes())
	pair := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(50),
	}
	registry.Cache.Volumes.Gen0().Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 1, Data: pair,
	})

	// Persist
	require.NoError(t, persistToStore(snapshotter, dataStore))

	// Save generation for comparison
	savedGen := registry.Cache.CurrentGeneration()

	// Reset and restore
	registry.Cache.Reset()

	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	// Verify restored data
	require.Equal(t, savedGen, registry.Cache.CurrentGeneration())

	restored, ok := registry.Cache.Volumes.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, int64(100), restored.Data.GetInput().ToBigInt().Int64())
	require.Equal(t, int64(50), restored.Data.GetOutput().ToBigInt().Int64())
	require.Equal(t, uint64(1), restored.Tag)
}

func TestCacheSnapshotter_PersistAndRestoreMetadata(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	// Populate cache with metadata in gen0
	metaKey := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "bank"}, Key: "label"}
	u128 := attributes.HashU128(metaKey.Bytes())
	metaValue := commonpb.NewStringValue("test-value")
	registry.Cache.AccountMetadata.Gen0().Put(u128, attributes.Entry[*commonpb.MetadataValue]{
		Tag: 2, Data: metaValue,
	})

	// Persist and restore
	require.NoError(t, persistToStore(snapshotter, dataStore))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	// Verify
	restored, ok := registry.Cache.AccountMetadata.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, "test-value", restored.Data.GetStringValue())
	require.Equal(t, uint64(2), restored.Tag)
}

func TestCacheSnapshotter_PersistAndRestoreLedgers(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	// Populate cache with ledger info in gen0
	ledgerKey := domain.LedgerKey{Name: "my-ledger"}
	u128 := attributes.HashU128(ledgerKey.Bytes())
	ledgerInfo := &commonpb.LedgerInfo{Name: "my-ledger"}
	registry.Cache.Ledgers.Gen0().Put(u128, attributes.Entry[*commonpb.LedgerInfo]{
		Tag: 3, Data: ledgerInfo,
	})

	// Persist and restore
	require.NoError(t, persistToStore(snapshotter, dataStore))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	// Verify
	restored, ok := registry.Cache.Ledgers.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, "my-ledger", restored.Data.GetName())
	require.Equal(t, uint64(3), restored.Tag)
}

func TestCacheSnapshotter_PersistAndRestoreReversions(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	// Populate reversions
	bs := bitset.New(10)
	bs.Set(3)
	bs.Set(7)
	registry.Reversions["test"] = bs

	// Persist — should succeed (reversions are written to Pebble)
	require.NoError(t, persistToStore(snapshotter, dataStore))
}

func TestCacheSnapshotter_PersistAndRestoreBothGenerations(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	// Populate gen0 with a volume
	volKey0 := newVolumeKey(domain.AccountKey{LedgerName: "test", Account: "alice"}, "USD")
	u128_0 := attributes.HashU128(volKey0.Bytes())
	registry.Cache.Volumes.Gen0().Put(u128_0, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 1, Data: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(200),
			Output: commonpb.NewUint256FromUint64(0),
		},
	})

	// Populate gen1 with a different volume
	volKey1 := newVolumeKey(domain.AccountKey{LedgerName: "test", Account: "bob"}, "EUR")
	u128_1 := attributes.HashU128(volKey1.Bytes())
	registry.Cache.Volumes.Gen1().Put(u128_1, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 2, Data: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(0),
			Output: commonpb.NewUint256FromUint64(300),
		},
	})

	// Set base indexes
	registry.Cache.BaseIndex.Gen0 = 10
	registry.Cache.BaseIndex.Gen1 = 20

	// Persist and restore
	require.NoError(t, persistToStore(snapshotter, dataStore))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	// Verify gen0
	require.Equal(t, uint64(10), registry.Cache.BaseIndex.Gen0)
	restoredGen0, ok := registry.Cache.Volumes.Gen0().Get(u128_0)
	require.True(t, ok)
	require.Equal(t, int64(200), restoredGen0.Data.GetInput().ToBigInt().Int64())

	// Verify gen1
	require.Equal(t, uint64(20), registry.Cache.BaseIndex.Gen1)
	restoredGen1, ok := registry.Cache.Volumes.Gen1().Get(u128_1)
	require.True(t, ok)
	require.Equal(t, int64(300), restoredGen1.Data.GetOutput().ToBigInt().Int64())
}

func TestCacheSnapshotter_RestoreFromEmptyStore(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, _ := newTestCacheSnapshotter(t, nil)

	// RestoreFromStore on an empty Pebble should succeed silently
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))
}

func TestCacheSnapshotter_PersistAndRestoreWithBloomFilters(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	bloomCfg := &commonpb.ClusterConfig{
		BloomVolumes:  &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
		BloomMetadata: &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
	}
	bloomFilters := bloom.NewFilterSet(bloomCfg, meter)
	require.NotNil(t, bloomFilters)

	bloomFilters.SetReady(true)

	snapshotter, dataStore, _ := newTestCacheSnapshotter(t, bloomFilters)
	defer snapshotter.Stop()

	// Persist (config only — full filter data is never checkpointed)
	require.NoError(t, persistToStore(snapshotter, dataStore))

	bloomFilters.SetReady(false)

	// Restore: detects config-only snapshot, starts async populate.
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	// Bloom is not ready immediately (async populate in background).
	// Wait for the background goroutine to finish.
	snapshotter.Stop()
	require.True(t, bloomFilters.IsReady())
}

func TestCacheSnapshotter_PersistNotReadyBloom(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	bloomCfg := &commonpb.ClusterConfig{
		BloomVolumes:  &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
		BloomMetadata: &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
	}
	bloomFilters := bloom.NewFilterSet(bloomCfg, meter)
	require.NotNil(t, bloomFilters)

	// Do NOT mark ready — simulates mid-population state.
	snapshotter, dataStore, _ := newTestCacheSnapshotter(t, bloomFilters)
	defer snapshotter.Stop()

	// Persist: should only write config, not filter data.
	require.NoError(t, persistToStore(snapshotter, dataStore))
	require.False(t, bloomFilters.IsReady())

	// Restore: should detect config-only snapshot and start async populate.
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	// Bloom is not ready immediately (async populate in progress).
	// Wait for the background goroutine to finish.
	snapshotter.Stop()
	require.True(t, bloomFilters.IsReady())
}

func TestCacheSnapshotter_PersistAndRestoreReferences(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	refKey := domain.TransactionReferenceKey{LedgerName: "test", Reference: "ref-1"}
	u128 := attributes.HashU128(refKey.Bytes())
	value := &commonpb.TransactionReferenceValue{TransactionId: 99}
	registry.Cache.References.Gen0().Put(u128, attributes.Entry[*commonpb.TransactionReferenceValue]{
		Tag: 6, Data: value,
	})

	require.NoError(t, persistToStore(snapshotter, dataStore))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	restored, ok := registry.Cache.References.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, uint64(99), restored.Data.GetTransactionId())
	require.Equal(t, uint64(6), restored.Tag)
}

func TestCacheSnapshotter_PersistAndRestoreTransactions(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	txKey := domain.TransactionKey{LedgerName: "test", ID: 42}
	u128 := attributes.HashU128(txKey.Bytes())
	value := &commonpb.TransactionState{CreatedByLog: 10, RevertedByTransaction: 5}
	registry.Cache.Transactions.Gen0().Put(u128, attributes.Entry[*commonpb.TransactionState]{
		Tag: 7, Data: value,
	})

	require.NoError(t, persistToStore(snapshotter, dataStore))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	restored, ok := registry.Cache.Transactions.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, uint64(10), restored.Data.GetCreatedByLog())
	require.Equal(t, uint64(5), restored.Data.GetRevertedByTransaction())
	require.Equal(t, uint64(7), restored.Tag)
}

// TestCacheSnapshotter_PersistAndRestoreAccounts verifies an account existence
// marker survives a snapshot/restore round-trip as a LIVE (non-deleted) entry,
// so a node that rehydrates its cache from the 0xFF zone still sees a
// pre-existing account as existing — never re-applying default metadata to it.
func TestCacheSnapshotter_PersistAndRestoreAccounts(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	acctKey := domain.AccountKey{LedgerName: "test", Account: "users:alice"}
	u128, tag := attributes.MakeKey(acctKey.Bytes())
	registry.Cache.Accounts.Gen0().Put(u128, attributes.Entry[*commonpb.AccountState]{
		Tag: tag, Data: &commonpb.AccountState{InsertionDate: &commonpb.Timestamp{Data: 1234567890}},
	})

	require.NoError(t, persistToStore(snapshotter, dataStore))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	restored, ok := registry.Cache.Accounts.Gen0().Get(u128)
	require.True(t, ok, "account marker missing from cache after restore")
	require.False(t, restored.Deleted, "account marker restored as deleted — existing account would be treated as new")
}

func TestCacheSnapshotter_PersistOverwritesPrevious(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	// First persist with one volume
	volKey := newVolumeKey(domain.AccountKey{LedgerName: "test", Account: "alice"}, "USD")
	u128 := attributes.HashU128(volKey.Bytes())
	registry.Cache.Volumes.Gen0().Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 1, Data: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(100),
			Output: commonpb.NewUint256FromUint64(0),
		},
	})

	require.NoError(t, persistToStore(snapshotter, dataStore))

	// Update volume and persist again
	registry.Cache.Volumes.Gen0().Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 1, Data: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(500),
			Output: commonpb.NewUint256FromUint64(200),
		},
	})

	require.NoError(t, persistToStore(snapshotter, dataStore))

	// Restore and verify we get the latest values
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	restored, ok := registry.Cache.Volumes.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, int64(500), restored.Data.GetInput().ToBigInt().Int64())
	require.Equal(t, int64(200), restored.Data.GetOutput().ToBigInt().Int64())
}

func TestCacheSnapshotter_PersistAndRestoreCurrentGeneration(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	// Set a non-default current generation
	registry.Cache.SetCurrentGeneration(42)

	require.NoError(t, persistToStore(snapshotter, dataStore))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	require.Equal(t, uint64(42), registry.Cache.CurrentGeneration())
}

// TestCacheSnapshotter_RestorePreRotation simulates a crash before any cache
// rotation has occurred: per-entry rows are written every batch by
// mergeSimpleWithCache, but [0xFF][CacheMetaKey] and [0xFF][gen][CacheGenMeta]
// are produced only by writeCacheRotation. Recovery must still rebuild the
// cache from the per-entry rows that exist on disk.
func TestCacheSnapshotter_RestorePreRotation(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	// Simulate the writes mergeSimpleWithCache emits during normal apply,
	// without invoking writeCacheRotation. genByte=0 is the natural choice
	// pre-rotation because CurrentGeneration() defaults to 0.
	const genByte byte = 0

	boundaryKey := domain.LedgerKey{Name: "default"}
	boundaryU128 := attributes.HashU128(boundaryKey.Bytes())
	boundaryValue := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1}
	boundaryBytes, err := boundaryValue.MarshalVT()
	require.NoError(t, err)

	volKey := newVolumeKey(domain.AccountKey{LedgerName: "test", Account: "world"}, "USD")
	volU128 := attributes.HashU128(volKey.Bytes())
	volValue := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}
	volBytes, err := volValue.MarshalVT()
	require.NoError(t, err)

	batch := dataStore.OpenWriteSession()
	require.NoError(t, writeCacheRaw(batch, genByte, dal.SubAttrBoundary, boundaryU128, 11, false, boundaryBytes))
	require.NoError(t, writeCacheRaw(batch, genByte, dal.SubAttrVolume, volU128, 22, false, volBytes))
	require.NoError(t, batch.Commit())

	// Sanity: no meta keys written.
	_, _, err = dataStore.Get([]byte{dal.ZoneCache, dal.SubCacheMeta})
	require.Error(t, err, "CacheMetaKey must be absent pre-rotation")

	_, _, err = dataStore.Get([]byte{dal.ZoneCache, genByte, dal.SubCacheGenMeta})
	require.Error(t, err, "CacheGenMeta must be absent pre-rotation")

	// Recovery must pick up the entries despite the missing sentinels.
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	require.Equal(t, uint64(0), registry.Cache.CurrentGeneration())
	require.Equal(t, uint64(0), registry.Cache.BaseIndex.Gen0)

	restoredBoundary, ok := registry.Cache.Boundaries.Gen0().Get(boundaryU128)
	require.True(t, ok, "boundary must be restored from per-entry row")
	require.Equal(t, uint64(1), restoredBoundary.Data.GetNextTransactionId())
	require.Equal(t, uint64(11), restoredBoundary.Tag)

	restoredVol, ok := registry.Cache.Volumes.Gen0().Get(volU128)
	require.True(t, ok, "volume must be restored from per-entry row")
	require.Equal(t, uint64(22), restoredVol.Tag)
}

func TestCacheSnapshotter_MachineIntegration(t *testing.T) {
	t.Parallel()

	// Verify Machine.RestoreCacheFromStore delegates to CacheSnapshotter
	machine, dataStore, _ := newTestMachine(t)

	// Populate some data via the machine's registry
	volKey := newVolumeKey(domain.AccountKey{LedgerName: "test", Account: "alice"}, "USD")
	u128 := attributes.HashU128(volKey.Bytes())
	machine.Registry.Cache.Volumes.Gen0().Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 1, Data: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(100),
			Output: commonpb.NewUint256FromUint64(0),
		},
	})

	// Persist via the machine's internal snapshotter
	require.NoError(t, persistToStore(machine.cacheSnapshotter, dataStore))

	// Reset and restore via Recovery (RestoreCacheFromStore moved off Machine).
	machine.Registry.Cache.Reset()
	recovery := NewRecovery(machine, dataStore)
	require.NoError(t, recovery.RestoreCacheFromStore())

	// Verify
	restored, ok := machine.Registry.Cache.Volumes.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, int64(100), restored.Data.GetInput().ToBigInt().Int64())
}

// Regression for the delete-resurrection-across-recovery bug found via the
// singleton_driver_model antithesis workload: a deleted ledger-metadata key
// must not reappear after a node rebuilds its cache from the 0xFF snapshot zone.
//
// AttributeCache.Del marks a tombstone but keeps the old value in entry.Data.
// MirrorTouch promotes a gen1 entry to gen0 and mirrors it to 0xFF; without the
// Deleted guard it persists marshalValue(entry.Data) — a NON-EMPTY value — so a
// CacheNeedsTouch on a gen1-only deleted key writes the deleted key's old value
// back as a live 0xFF entry. The live cache still reads "deleted" (it keeps the
// flag), so it only surfaces when a node rehydrates from 0xFF (RestoreFromStore):
// RestoreEntry sees non-empty bytes and restores a live value.
//
// This drives that exact sequence against a real Pebble store: a gen1 tombstone
// (the deleted-then-rotated state) + the 0xFF tombstone a delete writes, then
// MirrorTouch, then RestoreFromStore — and asserts the key stays deleted.
func TestCacheSnapshotter_MirrorTouchDoesNotResurrectDeletedMetadata(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	lmk := domain.LedgerMetadataKey{LedgerName: "test-ledger", Key: "k0"}
	u128, tag := attributes.MakeKey(lmk.Bytes())
	deletedValue := commonpb.NewStringValue("should-stay-deleted")

	// In-memory state after a delete followed by a rotation: the entry is a
	// tombstone in gen1 (Deleted, but Del retained the old value in Data) and
	// absent from gen0 — i.e. gen1-only, which CheckCache reports as CacheNeedsTouch.
	registry.Cache.LedgerMetadata.Gen1().Put(u128, attributes.Entry[*commonpb.MetadataValue]{
		Tag:     tag,
		Data:    deletedValue,
		Deleted: true,
	})

	gen0Byte := byte(registry.Cache.CurrentGeneration() % 2)

	batch := dataStore.OpenWriteSession()
	// The delete wrote a tombstone to the 0xFF zone (both generations).
	require.NoError(t, writeCacheTombstone(batch, dal.SubAttrLedgerMetadata, u128, tag))
	// The leader's CacheNeedsTouch promotes the gen1 tombstone to gen0. MirrorTouch
	// must persist a tombstone for it — not the retained value.
	require.NoError(t, snapshotter.MirrorTouch(batch, dal.SubAttrLedgerMetadata, gen0Byte, u128))
	require.NoError(t, batch.Commit())

	// Rehydrate the cache from the 0xFF zone, exactly as a node does on restart.
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	// The key must still be deleted in the restored cache. On the buggy code the
	// restored gen0 entry is a live value (Deleted=false): the deleted key
	// resurrected to its pre-delete value.
	if e, ok := registry.Cache.LedgerMetadata.Gen0().Get(u128); ok {
		require.True(t, e.Deleted,
			"deleted ledger-metadata key resurrected after restore: gen0 holds live value %q",
			e.Data.GetStringValue())
	}
	if e, ok := registry.Cache.LedgerMetadata.Gen1().Get(u128); ok && !e.Deleted {
		t.Fatalf("deleted ledger-metadata key resurrected after restore: gen1 holds live value %q",
			e.Data.GetStringValue())
	}
}

// TestCacheSnapshotter_EN1377_LiveZeroByteProtoRoundTrip is the main
// regression test for EN-1377. Before the fix, an attribute whose proto
// marshals to zero bytes (here LedgerBoundaries with all-default fixed64
// counters) round-tripped as a tombstone because the snapshotter used
// len(value) == 0 as the in-band tombstone signal. With the explicit flag
// byte the entry restores as live.
func TestCacheSnapshotter_EN1377_LiveZeroByteProtoRoundTrip(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	emptyBoundaries := &raftcmdpb.LedgerBoundaries{}
	// Sanity: the proto we picked really does marshal to zero bytes — the
	// precondition that made the bug exist in the first place.
	require.Equal(t, 0, emptyBoundaries.SizeVT(),
		"LedgerBoundaries with all-default fixed64 must marshal to zero bytes (regression precondition)")

	u128 := attributes.HashU128([]byte("ledger:empty-boundaries"))
	const tag uint64 = 42
	registry.Cache.Boundaries.Gen0().Put(u128, attributes.Entry[*raftcmdpb.LedgerBoundaries]{
		Tag:  tag,
		Data: emptyBoundaries,
	})

	require.NoError(t, persistToStore(snapshotter, dataStore))

	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	restored, ok := registry.Cache.Boundaries.Gen0().Get(u128)
	require.True(t, ok, "live zero-byte entry must restore")
	require.False(t, restored.Deleted, "live zero-byte entry must NOT restore as tombstone (EN-1377)")
	require.Equal(t, tag, restored.Tag)
}

// TestCacheSnapshotter_EN1377_PersistRotationDoesNotResurrectDeletedEntry
// guards the latent bug in persistLeanProtoEntries that EN-1377 fixes
// alongside the format change: AttributeCache.Del keeps the pre-delete
// payload in entry.Data with Deleted=true. Before the fix, persist marshaled
// entry.Data unconditionally — restoring the deleted key as live with its
// pre-delete value. The explicit flag byte makes persist emit a tombstone row.
func TestCacheSnapshotter_EN1377_PersistRotationDoesNotResurrectDeletedEntry(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	lmk := domain.LedgerMetadataKey{LedgerName: "ledger", Key: "to-delete"}
	u128 := attributes.HashU128(lmk.Bytes())
	value := commonpb.NewStringValue("pre-delete-payload")
	const tag uint64 = 7

	// Live entry, then deleted in place (Del flips Deleted but keeps Data).
	registry.Cache.LedgerMetadata.Gen0().Put(u128, attributes.Entry[*commonpb.MetadataValue]{
		Tag:     tag,
		Data:    value,
		Deleted: true,
	})

	require.NoError(t, persistToStore(snapshotter, dataStore))

	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	restored, ok := registry.Cache.LedgerMetadata.Gen0().Get(u128)
	require.True(t, ok, "tombstone must restore")
	require.True(t, restored.Deleted,
		"persisted tombstone must round-trip as deleted, not as live with pre-delete payload (EN-1377)")
	require.Equal(t, tag, restored.Tag)
}

// TestCacheSnapshotter_EN1377_MirrorPreloadEmptyRawValue confirms that an
// empty rawValue passed to MirrorPreload — the wire form of a presence-only
// proto — populates the cache as a LIVE zero-value entry, not as a tombstone.
// Before the fix, MirrorPreload guarded on len(rawValue) > 0 and would have
// left the typed value zero, but the on-disk write was still len==0 and
// therefore re-read as a tombstone on the next restart.
func TestCacheSnapshotter_EN1377_MirrorPreloadEmptyRawValue(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	u128 := attributes.HashU128([]byte("preload:empty-boundaries"))
	const tag uint64 = 99

	gen0Byte := byte(registry.Cache.CurrentGeneration() % 2)
	gen1Byte := byte((registry.Cache.CurrentGeneration() + 1) % 2)

	batch := dataStore.OpenWriteSession()
	require.NoError(t, snapshotter.MirrorPreload(
		batch, gen0Byte, gen1Byte,
		&raftcmdpb.AttributeID{Id: u128[:], Tag: tag},
		dal.SubAttrBoundary,
		&raftcmdpb.AttributeValue{RawValue: nil},
	))
	require.NoError(t, batch.Commit())

	// In-memory: live entry with zero-value proto, not a tombstone.
	inMem, ok := registry.Cache.Boundaries.Gen0().Get(u128)
	require.True(t, ok)
	require.False(t, inMem.Deleted, "MirrorPreload of empty rawValue must populate a live entry (EN-1377)")
	require.NotNil(t, inMem.Data, "live entry must have a non-nil zero-value proto")

	// On-disk round-trip: restore must agree.
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	restored, ok := registry.Cache.Boundaries.Gen0().Get(u128)
	require.True(t, ok)
	require.False(t, restored.Deleted, "presence-only marker must round-trip as live")
	require.Equal(t, tag, restored.Tag)
}

// TestCacheSnapshotter_EN1377_MirrorTouchOfZeroByteValue mirrors the
// previous test on the Touch path. A live gen1 entry whose proto marshals
// to zero bytes is promoted to gen0 by MirrorTouch; the new gen0 row must
// be readable as live after a restart.
func TestCacheSnapshotter_EN1377_MirrorTouchOfZeroByteValue(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	u128 := attributes.HashU128([]byte("touch:empty-boundaries"))
	const tag uint64 = 11

	registry.Cache.Boundaries.Gen1().Put(u128, attributes.Entry[*raftcmdpb.LedgerBoundaries]{
		Tag:  tag,
		Data: &raftcmdpb.LedgerBoundaries{},
	})

	gen0Byte := byte(registry.Cache.CurrentGeneration() % 2)

	batch := dataStore.OpenWriteSession()
	require.NoError(t, snapshotter.MirrorTouch(batch, dal.SubAttrBoundary, gen0Byte, u128))
	require.NoError(t, batch.Commit())

	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	restored, ok := registry.Cache.Boundaries.Gen0().Get(u128)
	require.True(t, ok, "touched zero-byte entry must restore in gen0")
	require.False(t, restored.Deleted, "touched live entry must NOT restore as tombstone (EN-1377)")
	require.Equal(t, tag, restored.Tag)
}

// TestCacheSnapshotter_EN1377_RestoreRejectsShortValue asserts the
// snapshotter panics on a 0xFF row shorter than the lean header. Every
// row is produced by writeCacheRaw which writes at least cacheValueHeaderLen
// bytes; a shorter row implies external corruption and silent
// interpretation would be unsafe.
func TestCacheSnapshotter_EN1377_RestoreRejectsShortValue(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	u128 := attributes.HashU128([]byte("corrupt:short-value"))
	gen0Byte := byte(registry.Cache.CurrentGeneration() % 2)

	key := []byte{dal.ZoneCache, gen0Byte, dal.SubAttrBoundary}
	key = append(key, u128[:]...)

	// Write a value that is shorter than the lean header (8-byte tag + 1-byte flag).
	batch := dataStore.OpenWriteSession()
	require.NoError(t, batch.Set(key, []byte{0x00, 0x00, 0x00}, nil))
	require.NoError(t, batch.Commit())

	require.Panics(t, func() {
		_ = snapshotter.RestoreFromStore(dataStore)
	}, "restore must panic on a 0xFF row shorter than the lean header")
}

// TestCacheSnapshotter_EN1377_RestoreRejectsTombstoneWithPayload asserts the
// snapshotter panics on a 0xFF row tagged as tombstone (flag 0x01) that
// carries trailing bytes after the lean header. writeCacheRaw never emits
// such a row — a single-byte flip on a live row could turn it into this
// shape and silently mask the original value, so we reject it loudly.
func TestCacheSnapshotter_EN1377_RestoreRejectsTombstoneWithPayload(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	u128 := attributes.HashU128([]byte("corrupt:tombstone-with-payload"))
	gen0Byte := byte(registry.Cache.CurrentGeneration() % 2)

	key := []byte{dal.ZoneCache, gen0Byte, dal.SubAttrBoundary}
	key = append(key, u128[:]...)

	// Header (9 bytes) with tombstone flag, plus stray trailing bytes.
	value := make([]byte, cacheValueHeaderLen+4)
	value[8] = cacheValueFlagTombstone
	batch := dataStore.OpenWriteSession()
	require.NoError(t, batch.Set(key, value, nil))
	require.NoError(t, batch.Commit())

	require.Panics(t, func() {
		_ = snapshotter.RestoreFromStore(dataStore)
	}, "restore must panic on a tombstone row that carries trailing payload bytes")
}

// TestCacheSnapshotter_EN1377_RestoreRejectsUnknownFlagByte asserts the
// snapshotter panics on a 0xFF row whose flag byte at offset 8 is neither
// cacheValueFlagLive (0x00) nor cacheValueFlagTombstone (0x01). Silently
// treating unknown flags as live would let a corrupted store or a
// forward-incompatible binary resurrect deleted keys.
func TestCacheSnapshotter_EN1377_RestoreRejectsUnknownFlagByte(t *testing.T) {
	t.Parallel()

	snapshotter, dataStore, registry := newTestCacheSnapshotter(t, nil)

	u128 := attributes.HashU128([]byte("corrupt:unknown-flag"))
	gen0Byte := byte(registry.Cache.CurrentGeneration() % 2)

	key := []byte{dal.ZoneCache, gen0Byte, dal.SubAttrBoundary}
	key = append(key, u128[:]...)

	// Header-shaped value (9 bytes) but with an out-of-range flag byte.
	value := make([]byte, cacheValueHeaderLen)
	value[8] = 0x42
	batch := dataStore.OpenWriteSession()
	require.NoError(t, batch.Set(key, value, nil))
	require.NoError(t, batch.Commit())

	require.Panics(t, func() {
		_ = snapshotter.RestoreFromStore(dataStore)
	}, "restore must panic on a 0xFF row with an unknown tombstone flag byte")
}
