package state

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func newTestCompactor(t *testing.T) (*Compactor, *data.Store, *attributes.Attributes) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := data.NewStore(t.TempDir(), logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	compactor, err := NewCompactor(logger, dataStore, meter, DefaultCompactorConfig())
	require.NoError(t, err)

	return compactor, dataStore, attributes.New()
}

// TestCompactorDeletesOldEntries verifies that the compactor removes
// entries older than the compaction index for dirty keys.
func TestCompactorDeletesOldEntries(t *testing.T) {
	t.Parallel()

	compactor, dataStore, attrs := newTestCompactor(t)

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

	// Run compaction with dirty keys at compaction index 300
	dirtyKeys := map[string]struct{}{string(testKey): {}}
	err = compactor.compact(compactionRequest{
		compactionIndex: 300,
		dirtyKeys:       dirtyKeys,
	})
	require.NoError(t, err)

	// Entries before index 300 should be deleted (base@100, diffs@150,200,250)
	// Remaining: diffs at 300 and 350
	entries = listRawAttributeEntries(t, dataStore, data.AttributePrefixInput, testKey)
	require.Len(t, entries, 2, "should have 2 entries remaining after compaction")
	require.Equal(t, uint64(300), entries[0].RaftIndex)
	require.Equal(t, uint64(350), entries[1].RaftIndex)
}

// TestCompactorSkipsKeysNotInDirtySet verifies that keys NOT in the dirty
// set are not affected by compaction.
func TestCompactorSkipsKeysNotInDirtySet(t *testing.T) {
	t.Parallel()

	compactor, dataStore, attrs := newTestCompactor(t)

	dirtyKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:dirty"},
		Asset:      "EUR",
	}.Bytes()

	cleanKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:clean"},
		Asset:      "EUR",
	}.Bytes()

	// Write diffs for both keys
	batch := dataStore.NewBatch()
	for i, amount := range []int64{100, 200, 300} {
		err := attrs.Input.AddDiff(batch, uint64(10+i*10), dirtyKey, commonpb.NewBigInt(big.NewInt(amount)))
		require.NoError(t, err)
		err = attrs.Input.AddDiff(batch, uint64(10+i*10), cleanKey, commonpb.NewBigInt(big.NewInt(amount)))
		require.NoError(t, err)
	}
	require.NoError(t, batch.Commit())

	// Only include dirtyKey in the dirty set
	dirtyKeys := map[string]struct{}{string(dirtyKey): {}}
	err := compactor.compact(compactionRequest{
		compactionIndex: 20,
		dirtyKeys:       dirtyKeys,
	})
	require.NoError(t, err)

	// dirtyKey should have entries pruned (only index 20 and 30 remain)
	entries := listRawAttributeEntries(t, dataStore, data.AttributePrefixInput, dirtyKey)
	require.Len(t, entries, 2, "dirty key should have entries pruned")

	// cleanKey should be untouched (all 3 entries remain)
	entries = listRawAttributeEntries(t, dataStore, data.AttributePrefixInput, cleanKey)
	require.Len(t, entries, 3, "clean key should NOT be compacted")
}

// TestCompactorSignalAndStop verifies the Start/Signal/Stop lifecycle.
func TestCompactorSignalAndStop(t *testing.T) {
	t.Parallel()

	compactor, dataStore, attrs := newTestCompactor(t)

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

	// Signal compaction with dirty keys at compaction index 30
	dirtyKeys := map[string]struct{}{string(testKey): {}}
	compactor.Signal(30, dirtyKeys)

	// Wait for compaction to complete
	require.Eventually(t, func() bool {
		entries := listRawAttributeEntries(t, dataStore, data.AttributePrefixInput, testKey)
		return len(entries) <= 2 // base@10 and diffs@20 pruned, remaining: diff@30 + diff@40
	}, 5*time.Second, 50*time.Millisecond, "compaction should reduce entries")

	// Stop the compactor
	compactor.Stop()

	// After compaction at index 30: base@10 and diff@20 deleted.
	// Remaining: diff@30(200), diff@40(300). Latest cumulative diff = 300.
	// The base was pruned, so computed value = 0 + 300 = 300.
	val, err := attrs.Input.ComputeValue(dataStore, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, int64(300), val.Value().Int64())
}

// TestCompactorBatching verifies that keys are committed in batches.
func TestCompactorBatching(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := data.NewStore(t.TempDir(), logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	// Create compactor with batch size of 2
	compactor, err := NewCompactor(logger, dataStore, meter, CompactorConfig{
		Enabled:   true,
		BatchSize: 2,
	})
	require.NoError(t, err)

	attrs := attributes.New()
	dirtyKeys := make(map[string]struct{})

	// Write 5 keys with diffs
	for i := 0; i < 5; i++ {
		key := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: 1, Account: fmt.Sprintf("user:%d", i)},
			Asset:      "EUR",
		}.Bytes()
		dirtyKeys[string(key)] = struct{}{}

		batch := dataStore.NewBatch()
		for j, amount := range []int64{100, 200} {
			err := attrs.Input.AddDiff(batch, uint64(10+j*10), key, commonpb.NewBigInt(big.NewInt(amount)))
			require.NoError(t, err)
		}
		require.NoError(t, batch.Commit())
	}

	// Run compaction at index 15 (should delete diffs at index 10)
	err = compactor.compact(compactionRequest{
		compactionIndex: 15,
		dirtyKeys:       dirtyKeys,
	})
	require.NoError(t, err)

	// Verify each key has only 1 entry remaining (diff at index 20)
	for i := 0; i < 5; i++ {
		key := data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: 1, Account: fmt.Sprintf("user:%d", i)},
			Asset:      "EUR",
		}.Bytes()
		entries := listRawAttributeEntries(t, dataStore, data.AttributePrefixInput, key)
		require.Len(t, entries, 1, "key user:%d should have 1 entry after compaction", i)
		require.Equal(t, uint64(20), entries[0].RaftIndex)
	}
}
