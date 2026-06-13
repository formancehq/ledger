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

func persistGeneration(s *CacheSnapshotter, batch *dal.Batch, genByte byte, genIndex int) error {
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
// in a single batch, so that RestoreFromStore can be tested.
func persistToStore(s *CacheSnapshotter) error {
	batch := s.dataStore.NewBatch()
	defer func() { _ = batch.Cancel() }()

	for ledger, bs := range s.registry.Reversions {
		for i := range bs.WordCount() {
			if err := saveReversionWord(batch, ledger, i, bs.Word(i)); err != nil {
				return fmt.Errorf("saving reversion word for %d: %w", ledger, err)
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

	snapshotter := NewCacheSnapshotter(logger, dataStore, registry, bloomFilters)

	return snapshotter, dataStore, registry
}

func TestCacheSnapshotter_PersistAndRestoreEmpty(t *testing.T) {
	t.Parallel()

	snapshotter, _, registry := newTestCacheSnapshotter(t, nil)

	// Persist empty cache
	require.NoError(t, persistToStore(snapshotter))

	// Reset and restore
	registry.Cache.Reset()

	require.NoError(t, snapshotter.RestoreFromStore())
}

func TestCacheSnapshotter_PersistAndRestoreVolumes(t *testing.T) {
	t.Parallel()

	snapshotter, _, registry := newTestCacheSnapshotter(t, nil)

	// Populate cache with volume data in gen0
	volumeKey := newVolumeKey(domain.AccountKey{LedgerID: 1, Account: "bank"}, "USD")
	u128 := attributes.HashU128(volumeKey.Bytes())
	pair := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(50),
	}
	registry.Cache.Volumes.Gen0().Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 1, Data: pair,
	})

	// Persist
	require.NoError(t, persistToStore(snapshotter))

	// Save generation for comparison
	savedGen := registry.Cache.CurrentGeneration()

	// Reset and restore
	registry.Cache.Reset()

	require.NoError(t, snapshotter.RestoreFromStore())

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

	snapshotter, _, registry := newTestCacheSnapshotter(t, nil)

	// Populate cache with metadata in gen0
	metaKey := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "bank"}, Key: "label"}
	u128 := attributes.HashU128(metaKey.Bytes())
	metaValue := commonpb.NewStringValue("test-value")
	registry.Cache.AccountMetadata.Gen0().Put(u128, attributes.Entry[*commonpb.MetadataValue]{
		Tag: 2, Data: metaValue,
	})

	// Persist and restore
	require.NoError(t, persistToStore(snapshotter))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore())

	// Verify
	restored, ok := registry.Cache.AccountMetadata.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, "test-value", restored.Data.GetStringValue())
	require.Equal(t, uint64(2), restored.Tag)
}

func TestCacheSnapshotter_PersistAndRestoreLedgers(t *testing.T) {
	t.Parallel()

	snapshotter, _, registry := newTestCacheSnapshotter(t, nil)

	// Populate cache with ledger info in gen0
	ledgerKey := domain.LedgerKey{Name: "my-ledger"}
	u128 := attributes.HashU128(ledgerKey.Bytes())
	ledgerInfo := &commonpb.LedgerInfo{Name: "my-ledger"}
	registry.Cache.Ledgers.Gen0().Put(u128, attributes.Entry[*commonpb.LedgerInfo]{
		Tag: 3, Data: ledgerInfo,
	})

	// Persist and restore
	require.NoError(t, persistToStore(snapshotter))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore())

	// Verify
	restored, ok := registry.Cache.Ledgers.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, "my-ledger", restored.Data.GetName())
	require.Equal(t, uint64(3), restored.Tag)
}

func TestCacheSnapshotter_PersistAndRestoreReversions(t *testing.T) {
	t.Parallel()

	snapshotter, _, registry := newTestCacheSnapshotter(t, nil)

	// Populate reversions
	bs := bitset.New(10)
	bs.Set(3)
	bs.Set(7)
	registry.Reversions[1] = bs

	// Persist — should succeed (reversions are written to Pebble)
	require.NoError(t, persistToStore(snapshotter))
}

func TestCacheSnapshotter_PersistAndRestoreBothGenerations(t *testing.T) {
	t.Parallel()

	snapshotter, _, registry := newTestCacheSnapshotter(t, nil)

	// Populate gen0 with a volume
	volKey0 := newVolumeKey(domain.AccountKey{LedgerID: 1, Account: "alice"}, "USD")
	u128_0 := attributes.HashU128(volKey0.Bytes())
	registry.Cache.Volumes.Gen0().Put(u128_0, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 1, Data: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(200),
			Output: commonpb.NewUint256FromUint64(0),
		},
	})

	// Populate gen1 with a different volume
	volKey1 := newVolumeKey(domain.AccountKey{LedgerID: 1, Account: "bob"}, "EUR")
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
	require.NoError(t, persistToStore(snapshotter))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore())

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

	snapshotter, _, _ := newTestCacheSnapshotter(t, nil)

	// RestoreFromStore on an empty Pebble should succeed silently
	require.NoError(t, snapshotter.RestoreFromStore())
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

	snapshotter, _, _ := newTestCacheSnapshotter(t, bloomFilters)
	defer snapshotter.Stop()

	// Persist (config only — full filter data is never checkpointed)
	require.NoError(t, persistToStore(snapshotter))

	bloomFilters.SetReady(false)

	// Restore: detects config-only snapshot, starts async populate.
	require.NoError(t, snapshotter.RestoreFromStore())

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
	snapshotter, _, _ := newTestCacheSnapshotter(t, bloomFilters)
	defer snapshotter.Stop()

	// Persist: should only write config, not filter data.
	require.NoError(t, persistToStore(snapshotter))
	require.False(t, bloomFilters.IsReady())

	// Restore: should detect config-only snapshot and start async populate.
	require.NoError(t, snapshotter.RestoreFromStore())

	// Bloom is not ready immediately (async populate in progress).
	// Wait for the background goroutine to finish.
	snapshotter.Stop()
	require.True(t, bloomFilters.IsReady())
}

func TestCacheSnapshotter_PersistAndRestoreReferences(t *testing.T) {
	t.Parallel()

	snapshotter, _, registry := newTestCacheSnapshotter(t, nil)

	refKey := domain.TransactionReferenceKey{LedgerID: 1, Reference: "ref-1"}
	u128 := attributes.HashU128(refKey.Bytes())
	value := &commonpb.TransactionReferenceValue{TransactionId: 99}
	registry.Cache.References.Gen0().Put(u128, attributes.Entry[*commonpb.TransactionReferenceValue]{
		Tag: 6, Data: value,
	})

	require.NoError(t, persistToStore(snapshotter))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore())

	restored, ok := registry.Cache.References.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, uint64(99), restored.Data.GetTransactionId())
	require.Equal(t, uint64(6), restored.Tag)
}

func TestCacheSnapshotter_PersistAndRestoreTransactions(t *testing.T) {
	t.Parallel()

	snapshotter, _, registry := newTestCacheSnapshotter(t, nil)

	txKey := domain.TransactionKey{LedgerID: 1, ID: 42}
	u128 := attributes.HashU128(txKey.Bytes())
	value := &commonpb.TransactionState{CreatedByLog: 10, RevertedByTransaction: 5}
	registry.Cache.Transactions.Gen0().Put(u128, attributes.Entry[*commonpb.TransactionState]{
		Tag: 7, Data: value,
	})

	require.NoError(t, persistToStore(snapshotter))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore())

	restored, ok := registry.Cache.Transactions.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, uint64(10), restored.Data.GetCreatedByLog())
	require.Equal(t, uint64(5), restored.Data.GetRevertedByTransaction())
	require.Equal(t, uint64(7), restored.Tag)
}

func TestCacheSnapshotter_PersistOverwritesPrevious(t *testing.T) {
	t.Parallel()

	snapshotter, _, registry := newTestCacheSnapshotter(t, nil)

	// First persist with one volume
	volKey := newVolumeKey(domain.AccountKey{LedgerID: 1, Account: "alice"}, "USD")
	u128 := attributes.HashU128(volKey.Bytes())
	registry.Cache.Volumes.Gen0().Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 1, Data: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(100),
			Output: commonpb.NewUint256FromUint64(0),
		},
	})

	require.NoError(t, persistToStore(snapshotter))

	// Update volume and persist again
	registry.Cache.Volumes.Gen0().Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 1, Data: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(500),
			Output: commonpb.NewUint256FromUint64(200),
		},
	})

	require.NoError(t, persistToStore(snapshotter))

	// Restore and verify we get the latest values
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore())

	restored, ok := registry.Cache.Volumes.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, int64(500), restored.Data.GetInput().ToBigInt().Int64())
	require.Equal(t, int64(200), restored.Data.GetOutput().ToBigInt().Int64())
}

func TestCacheSnapshotter_PersistAndRestoreCurrentGeneration(t *testing.T) {
	t.Parallel()

	snapshotter, _, registry := newTestCacheSnapshotter(t, nil)

	// Set a non-default current generation
	registry.Cache.SetCurrentGeneration(42)

	require.NoError(t, persistToStore(snapshotter))
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore())

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

	volKey := newVolumeKey(domain.AccountKey{LedgerID: 1, Account: "world"}, "USD")
	volU128 := attributes.HashU128(volKey.Bytes())
	volValue := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}
	volBytes, err := volValue.MarshalVT()
	require.NoError(t, err)

	batch := dataStore.NewBatch()
	require.NoError(t, writeCacheRaw(batch, genByte, dal.SubAttrBoundary, boundaryU128, 11, boundaryBytes))
	require.NoError(t, writeCacheRaw(batch, genByte, dal.SubAttrVolume, volU128, 22, volBytes))
	require.NoError(t, batch.Commit())

	// Sanity: no meta keys written.
	_, _, err = dataStore.Get([]byte{dal.ZoneCache, dal.SubCacheMeta})
	require.Error(t, err, "CacheMetaKey must be absent pre-rotation")

	_, _, err = dataStore.Get([]byte{dal.ZoneCache, genByte, dal.SubCacheGenMeta})
	require.Error(t, err, "CacheGenMeta must be absent pre-rotation")

	// Recovery must pick up the entries despite the missing sentinels.
	registry.Cache.Reset()
	require.NoError(t, snapshotter.RestoreFromStore())

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
	machine, _, _ := newTestMachine(t)

	// Populate some data via the machine's registry
	volKey := newVolumeKey(domain.AccountKey{LedgerID: 1, Account: "alice"}, "USD")
	u128 := attributes.HashU128(volKey.Bytes())
	machine.Registry.Cache.Volumes.Gen0().Put(u128, attributes.Entry[*raftcmdpb.VolumePair]{
		Tag: 1, Data: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(100),
			Output: commonpb.NewUint256FromUint64(0),
		},
	})

	// Persist via the machine's internal snapshotter
	require.NoError(t, persistToStore(machine.cacheSnapshotter))

	// Reset and restore via the Machine delegation method
	machine.Registry.Cache.Reset()
	require.NoError(t, machine.RestoreCacheFromStore())

	// Verify
	restored, ok := machine.Registry.Cache.Volumes.Gen0().Get(u128)
	require.True(t, ok)
	require.Equal(t, int64(100), restored.Data.GetInput().ToBigInt().Int64())
}
