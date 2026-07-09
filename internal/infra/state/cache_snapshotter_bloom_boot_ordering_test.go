package state

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestCacheSnapshotter_BloomBootOrdering_RestoreIsSynchronous is the
// EN-1410 regression test for the boot-ordering bug.
//
// The bug: on a simple restart, RestoreFromStore restored the cache
// synchronously but launched the bloom rebuild in a background
// goroutine and returned immediately. The caller (applier.recoverState)
// then ran replayWAL without waiting on IsReady -- so the FSM could
// drive cache rotations while !IsReady. writeCacheRotation wiped the
// outgoing cache generation from 0xFF unconditionally while
// PersistDirtyBlocks was gated on IsReady; keys whose volume entry
// made it to Pebble 0x01 but whose bloom block was never persisted
// ended up in neither the post-rotation 0xFF cache nor the persisted
// bloom blocks. The next crash dropped them; the subsequent restart
// rebuilt an incomplete bloom; MayContain returned false for keys
// still present in Pebble; the resolver injected a zero VolumePair
// (the includeZeroValue=true branch in plan/resolve.go); the FSM
// apply path returned "insufficient funds available=0".
//
// The fix lives in CacheSnapshotter.restoreBloomFilters: when the
// store carries persisted bloom blocks (every restart after the very
// first boot), the rebuild runs synchronously inside RestoreFromStore.
// The cold-start / Rebuild path keeps its async PopulateFromStore,
// which is safe by construction: a crash there leaves
// hasPersistedBloomBlocks == false, so the next boot rescans 0x01 in
// full and reconstructs the bloom from scratch.
//
// This test asserts the synchronous contract directly: after
// RestoreFromStore returns, IsReady() must already be true. Before
// the fix, IsReady() returns false until the background goroutine
// completes -- the test would race the executor.
func TestCacheSnapshotter_BloomBootOrdering_RestoreIsSynchronous(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	bloomCfg := &commonpb.ClusterConfig{
		BloomVolumes: &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
	}

	// Shared attrs across the two "process incarnations" -- attrs carry
	// per-attribute persistence helpers only, no live state.
	attrs := attributes.New()

	// ---- Process incarnation #1: populate the persisted state ----
	//
	// We need hasPersistedBloomBlocks to return true on the restart
	// path. The fastest way is to add a key, mark IsReady true, and
	// flush dirty blocks to Pebble explicitly.
	bloomFilters := bloom.NewFilterSet(bloomCfg, meter)
	require.NotNil(t, bloomFilters)
	bloomFilters.SetReady(true)

	registry, snapshotter := newRegistryAndSnapshotter(t, logger, attrs, bloomFilters, 1000)
	defer snapshotter.Stop()

	volumeKey := domain.VolumeKey{
		AccountKey:     domain.AccountKey{LedgerName: "test", Account: "k-old"},
		Asset:          "USD/2",
		AssetBase:      "USD",
		AssetPrecision: 2,
	}
	id := attributes.HashU128(volumeKey.Bytes())
	pair := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(1_000_000),
		Output: commonpb.NewUint256FromUint64(0),
	}

	{
		batch := dataStore.OpenWriteSession()
		updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{{
			ID:           id,
			Tag:          1,
			CanonicalKey: volumeKey.Bytes(),
			New:          pair,
		}}
		require.NoError(t, mergeSimpleWithCache(attrs.Volume, batch, 0, dal.SubAttrVolume, updates))

		registry.Cache.Volumes.Gen0().Put(id, attributes.Entry[*raftcmdpb.VolumePair]{Tag: 1, Data: pair})
		bloomFilters.AddCanonicalKeys(&bloom.BloomUpdates{Volumes: []attributes.U128{id}})

		// Persist the bloom block so the next incarnation sees
		// hasPersistedBloomBlocks == true (the restart-path branch).
		require.NoError(t, bloomFilters.PersistDirtyBlocks(batch))
		require.NoError(t, batch.Commit())
	}

	// ---- Process incarnation #2: fresh in-memory state, reuse Pebble ----
	freshBloom := bloom.NewFilterSet(bloomCfg, meter)
	require.NotNil(t, freshBloom)

	_, freshSnapshotter := newRegistryAndSnapshotter(t, logger, attrs, freshBloom, 1000)
	defer freshSnapshotter.Stop()

	// Sanity: before RestoreFromStore the fresh bloom must not be ready.
	require.False(t, freshBloom.IsReady())

	// The EN-1410 contract: RestoreFromStore on the restart path
	// (persisted bloom blocks exist) must run the rebuild inline and
	// return only once IsReady is true. Before the fix, IsReady stayed
	// false here -- the background goroutine had not finished yet.
	require.NoError(t, freshSnapshotter.RestoreFromStore(dataStore))
	require.True(t, freshBloom.IsReady(),
		"EN-1410: bloom must be ready synchronously on the restart path "+
			"(otherwise replayWAL would race cache rotations while !IsReady)")

	// And the rebuild must actually have loaded the persisted block --
	// MayContain returns true for the key we persisted in incarnation 1.
	volFilter := freshBloom.FilterForAttrType(dal.SubAttrVolume)
	require.NotNil(t, volFilter)
	require.True(t, volFilter.MayContain(id),
		"rebuilt bloom must contain the persisted key")
}

// TestCacheSnapshotter_BloomBootOrdering_PopulatePathStaysAsync is the
// counterpart contract: when there are no persisted bloom blocks (cold
// start, or right after a Rebuild that purged them), the full attribute
// scan runs in the background. Blocking boot on this scan would be
// unacceptable -- on a large database it can take minutes.
//
// This path is safe even though the rebuild is async: a crash leaves
// hasPersistedBloomBlocks == false, so the next boot re-enters this
// same path and re-scans 0x01 from scratch.
func TestCacheSnapshotter_BloomBootOrdering_PopulatePathStaysAsync(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	bloomCfg := &commonpb.ClusterConfig{
		BloomVolumes: &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
	}
	bloomFilters := bloom.NewFilterSet(bloomCfg, meter)
	require.NotNil(t, bloomFilters)

	attrs := attributes.New()
	_, snapshotter := newRegistryAndSnapshotter(t, logger, attrs, bloomFilters, 1000)
	defer snapshotter.Stop()

	// First boot: no persisted bloom blocks. RestoreFromStore must
	// return without waiting on the populate scan -- but the populate
	// is launched and will eventually publish ready.
	require.NoError(t, snapshotter.RestoreFromStore(dataStore))

	// Wait for the async populate to complete. The bounded budget here
	// is generous (the test store is empty so the scan is instantaneous).
	require.Eventually(t, func() bool {
		return bloomFilters.IsReady()
	}, 5*time.Second, 10*time.Millisecond,
		"async populate must eventually mark the bloom ready")
}

// ---------------------------------------------------------------------------
// Test helpers.

// newRegistryAndSnapshotter constructs a fresh in-memory cache + registry
// + snapshotter sharing the given attrs and bloomFilters.
func newRegistryAndSnapshotter(
	t *testing.T,
	logger logging.Logger,
	attrs *attributes.Attributes,
	bloomFilters *bloom.FilterSet,
	rotationThreshold uint64,
) (*StateRegistry, *CacheSnapshotter) {
	t.Helper()

	c, err := cache.New(rotationThreshold, noop.NewMeterProvider().Meter("test-cache"))
	require.NoError(t, err)

	registry := NewStateRegistry(c, attrs, 0)
	snapshotter := NewCacheSnapshotter(logger, registry, bloomFilters)

	return registry, snapshotter
}
