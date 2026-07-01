package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestApplyClusterConfig_ThresholdChangePersistsGeneration is the regression
// for NumaryBot's blocker on the admission-cache-horizon PR: after a
// rotation-threshold change fires CheckRotationNeeded to realign
// currentGeneration and BaseIndex in memory, the FSM must persist that
// realigned state to Pebble. Without persistence a node restart before the
// next organic rotation would rehydrate currentGeneration=0 from disk and
// admission's CheckCache would re-observe a stale horizon, falsely tripping
// CacheUnreachable for valid preloaded proposals.
//
// The test drives applyClusterConfig at a Raft index whose new-threshold gen
// is non-zero, then simulates the restart via Reset + RestoreFromStore and
// asserts the reconstructed (currentGeneration, BaseIndex) tuple matches the
// pre-restart in-memory state.
func TestApplyClusterConfig_ThresholdChangePersistsGeneration(t *testing.T) {
	t.Parallel()

	// Threshold 1000 initially — the default in newTestMachine.
	fsm, dataStore, _ := newTestMachine(t)

	// The threshold change we want to apply. With threshold=10 and
	// raftIndex=25, Gen(25, 10) = 2 → CheckRotationNeeded must jump
	// currentGeneration to 2 and BaseIndex.Gen0 to genEndIndex(1, 10) = 20.
	const (
		newThreshold uint64 = 10
		raftIndex    uint64 = 25
		expectedGen  uint64 = 2 // Gen(25, 10)
		expectedGen0 uint64 = 20
	)

	batch := dataStore.OpenWriteSession()
	require.NoError(t, fsm.applyClusterConfig(batch, raftIndex, &commonpb.ClusterConfig{
		RotationThreshold: newThreshold,
	}))
	require.NoError(t, batch.Commit())

	// Verify the in-memory state was realigned.
	require.Equal(t, expectedGen, fsm.Registry.Cache.CurrentGeneration())
	require.Equal(t, expectedGen0, fsm.Registry.Cache.BaseIndex.Gen0)
	require.Equal(t, uint64(0), fsm.Registry.Cache.BaseIndex.Gen1)

	// Verify the on-disk state matches — this is what a fresh RestoreFromStore
	// will see.
	meta := readCacheSnapshotMeta(t, dataStore)
	require.Equal(t, expectedGen, meta.GetCurrentGeneration(),
		"disk CacheMeta must carry the post-CheckRotationNeeded currentGeneration")

	gen0Byte := byte(expectedGen % 2)
	gen1Byte := byte((expectedGen + 1) % 2)

	require.Equal(t, expectedGen0, readCacheGenMeta(t, dataStore, gen0Byte).GetBaseIndex(),
		"disk gen0 CacheGenMeta must carry the post-rotation BaseIndex.Gen0")
	require.Equal(t, uint64(0), readCacheGenMeta(t, dataStore, gen1Byte).GetBaseIndex(),
		"disk gen1 CacheGenMeta must carry the post-rotation BaseIndex.Gen1")

	// Simulate a restart: reset the in-memory cache and restore from disk.
	fsm.Registry.Cache.Reset()
	require.NoError(t, fsm.cacheSnapshotter.RestoreFromStore(dataStore))

	require.Equal(t, expectedGen, fsm.Registry.Cache.CurrentGeneration(),
		"restart must reconstruct the post-threshold-change currentGeneration")
	require.Equal(t, expectedGen0, fsm.Registry.Cache.BaseIndex.Gen0,
		"restart must reconstruct the post-threshold-change BaseIndex.Gen0")
	require.Equal(t, uint64(0), fsm.Registry.Cache.BaseIndex.Gen1,
		"restart must reconstruct the post-threshold-change BaseIndex.Gen1")

	// A CheckCache query at any index within the current generation must NOT
	// return CacheUnreachable — the horizon must reflect the persisted-then-
	// restored generation, not currentGeneration=0.
	inGenIndex := raftIndex + 1
	require.NotEqual(t, cache.CacheUnreachable,
		fsm.Registry.Cache.LedgerMetadata.CheckCache(inGenIndex, attributes.NewU128(0, 0)),
		"CheckCache at a future index in the same generation must not fire the horizon guard")
}

func readCacheSnapshotMeta(t *testing.T, store *dal.Store) *raftcmdpb.CacheSnapshotMeta {
	t.Helper()

	val, closer, err := store.Get([]byte{dal.ZoneCache, dal.SubCacheMeta})
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	meta := &raftcmdpb.CacheSnapshotMeta{}
	require.NoError(t, meta.UnmarshalVT(val))

	return meta
}

func readCacheGenMeta(t *testing.T, store *dal.Store, genByte byte) *raftcmdpb.CacheGenerationMeta {
	t.Helper()

	val, closer, err := store.Get([]byte{dal.ZoneCache, genByte, dal.SubCacheGenMeta})
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	meta := &raftcmdpb.CacheGenerationMeta{}
	require.NoError(t, meta.UnmarshalVT(val))

	return meta
}
