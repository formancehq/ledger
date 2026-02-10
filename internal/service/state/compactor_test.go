package state

import (
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func newTestCompactor(t *testing.T) (*Compactor, *data.Store, *attributes.Attributes, *cache.Cache) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := data.NewStore(t.TempDir(), logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	compactor, err := NewCompactor(logger, dataStore, c, meter, DefaultCompactorConfig())
	require.NoError(t, err)

	return compactor, dataStore, attributes.New(), c
}

// TestCompactorPhase1IntermediateDiffRemoval verifies that Phase 1 removes
// intermediate diffs while preserving the latest base + latest diff.
func TestCompactorPhase1IntermediateDiffRemoval(t *testing.T) {
	t.Parallel()

	compactor, dataStore, attrs, _ := newTestCompactor(t)

	testKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:alice"},
		Asset:      "EUR",
	}.Bytes()

	// Write base at index 100
	batch := dataStore.NewBatch()
	err := attrs.Input.SetBase(batch, 100, testKey, commonpb.NewBigInt(big.NewInt(1000)))
	require.NoError(t, err)

	// Write 5 cumulative diffs at indexes 150, 200, 250, 300, 350
	for i, amount := range []int64{50, 80, 100, 150, 200} {
		err = attrs.Input.AddDiff(batch, uint64(150+i*50), testKey, commonpb.NewBigInt(big.NewInt(amount)))
		require.NoError(t, err)
	}
	require.NoError(t, batch.Commit())

	// Verify initial state: 6 entries (1 base + 5 diffs)
	entries := listRawAttributeEntries(t, dataStore, data.AttributePrefixInput, testKey)
	require.Len(t, entries, 6)

	// Run compaction
	err = compactor.compact()
	require.NoError(t, err)

	// Verify: after Phase 1 + Phase 2 (cold key), should be 1 entry (consolidated base)
	// because the key is not in cache
	entries = listRawAttributeEntries(t, dataStore, data.AttributePrefixInput, testKey)
	require.Len(t, entries, 1, "cold key should be consolidated to 1 entry")
	require.True(t, entries[0].IsBase, "consolidated entry should be a base")

	// Verify computed value is still correct: 1000 + 200 = 1200
	val, err := attrs.Input.ComputeValue(dataStore, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, int64(1200), val.Value().Int64())
}

// TestCompactorPhase2SkipsHotKeys verifies that Phase 2 does NOT consolidate
// keys that are present in the cache.
func TestCompactorPhase2SkipsHotKeys(t *testing.T) {
	t.Parallel()

	compactor, dataStore, attrs, c := newTestCompactor(t)

	testKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:hot"},
		Asset:      "EUR",
	}.Bytes()

	// Write base + diff
	batch := dataStore.NewBatch()
	err := attrs.Input.SetBase(batch, 100, testKey, commonpb.NewBigInt(big.NewInt(1000)))
	require.NoError(t, err)
	err = attrs.Input.AddDiff(batch, 200, testKey, commonpb.NewBigInt(big.NewInt(500)))
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Put the key in cache to make it "hot"
	hasher := attributes.NewKeyHasher(attributes.DefaultKeys)
	u128, tag := hasher.MakeKey(testKey)
	c.Input.Put(u128, attributes.Entry[*raftcmdpb.VolumeHolder]{
		Tag: tag,
		Data: &raftcmdpb.VolumeHolder{
			Known: commonpb.NewBigInt(big.NewInt(1500)),
		},
	})

	// Run compaction
	err = compactor.compact()
	require.NoError(t, err)

	// Verify: hot key should still have 2 entries (base + diff)
	entries := listRawAttributeEntries(t, dataStore, data.AttributePrefixInput, testKey)
	require.Len(t, entries, 2, "hot key should NOT be consolidated")

	// Verify computed value is still correct
	val, err := attrs.Input.ComputeValue(dataStore, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, int64(1500), val.Value().Int64())
}

// TestCompactorSkipsDiffOnlyKeys verifies that Phase 2 skips keys without a base
// (like @world which only has diffs).
func TestCompactorSkipsDiffOnlyKeys(t *testing.T) {
	t.Parallel()

	compactor, dataStore, attrs, _ := newTestCompactor(t)

	testKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "world"},
		Asset:      "EUR",
	}.Bytes()

	// Write only diffs (no base) - simulates @world
	batch := dataStore.NewBatch()
	for i, amount := range []int64{100, 200, 300} {
		err := attrs.Output.AddDiff(batch, uint64(10+i*10), testKey, commonpb.NewBigInt(big.NewInt(amount)))
		require.NoError(t, err)
	}
	require.NoError(t, batch.Commit())

	// Verify initial state: 3 diffs
	entries := listRawAttributeEntries(t, dataStore, data.AttributePrefixOutput, testKey)
	require.Len(t, entries, 3)

	// Run compaction
	err := compactor.compact()
	require.NoError(t, err)

	// Phase 1 should reduce to 1 diff (latest), Phase 2 skipped (no base)
	entries = listRawAttributeEntries(t, dataStore, data.AttributePrefixOutput, testKey)
	require.Len(t, entries, 1, "diff-only key should keep only latest diff")
	require.False(t, entries[0].IsBase, "should remain a diff")
	require.Equal(t, uint64(30), entries[0].RaftIndex, "should keep the latest diff")

	// Verify computed value is still correct: 0 + 300 = 300
	val, err := attrs.Output.ComputeValue(dataStore, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, int64(300), val.Value().Int64())
}

// TestCompactorSignalAndStop verifies the Start/Signal/Stop lifecycle.
func TestCompactorSignalAndStop(t *testing.T) {
	t.Parallel()

	compactor, dataStore, attrs, _ := newTestCompactor(t)

	testKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:lifecycle"},
		Asset:      "EUR",
	}.Bytes()

	// Write base + 3 diffs
	batch := dataStore.NewBatch()
	err := attrs.Input.SetBase(batch, 10, testKey, commonpb.NewBigInt(big.NewInt(1000)))
	require.NoError(t, err)
	for i, amount := range []int64{100, 200, 300} {
		err = attrs.Input.AddDiff(batch, uint64(20+i*10), testKey, commonpb.NewBigInt(big.NewInt(amount)))
		require.NoError(t, err)
	}
	require.NoError(t, batch.Commit())

	// Start the compactor
	compactor.Start()

	// Signal compaction
	compactor.Signal()

	// Wait for compaction to complete
	require.Eventually(t, func() bool {
		entries := listRawAttributeEntries(t, dataStore, data.AttributePrefixInput, testKey)
		return len(entries) <= 2 // Phase 1 + Phase 2 should reduce entries
	}, 5*time.Second, 50*time.Millisecond, "compaction should reduce entries")

	// Stop the compactor
	compactor.Stop()

	// Verify computed value is correct: 1000 + 300 = 1300
	val, err := attrs.Input.ComputeValue(dataStore, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, int64(1300), val.Value().Int64())
}

// TestCompactorSkipsSingleEntry verifies that keys with only 1 entry are skipped.
func TestCompactorSkipsSingleEntry(t *testing.T) {
	t.Parallel()

	compactor, dataStore, attrs, _ := newTestCompactor(t)

	testKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:single"},
		Asset:      "EUR",
	}.Bytes()

	// Write a single base
	batch := dataStore.NewBatch()
	err := attrs.Input.SetBase(batch, 100, testKey, commonpb.NewBigInt(big.NewInt(1000)))
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Run compaction
	err = compactor.compact()
	require.NoError(t, err)

	// Should still have 1 entry (nothing to compact)
	entries := listRawAttributeEntries(t, dataStore, data.AttributePrefixInput, testKey)
	require.Len(t, entries, 1)
	require.True(t, entries[0].IsBase)
}
