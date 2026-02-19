package attributes

import (
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func createTestStore(t *testing.T) *data.Store {
	tmpDir := t.TempDir()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := data.NewStore(tmpDir, logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func TestSetBaseAndComputeValue(t *testing.T) {
	t.Parallel()

	// Create a data store
	store := createTestStore(t)
	attrs := New()

	// Create a batch
	batch := store.NewBatch()
	defer func() {
		_ = batch.Cancel()
	}()

	// Create a test canonical key (ledger:account:asset format)
	testKey := []byte("test-ledger\x00test-account\x00USD")

	// Test value: input=1000, output=0
	testValue := &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(1000)}

	// Set base at index 5
	err := attrs.Volume.SetBase(batch, 5, testKey, testValue)
	require.NoError(t, err)

	// Commit the batch
	err = batch.Commit()
	require.NoError(t, err)

	// Compute value
	result, err := attrs.Volume.ComputeValue(store, 100, testKey)
	require.NoError(t, err)

	// Verify the result
	require.NotNil(t, result)
	require.Equal(t, int64(1000), result.InputKnown.ToBigInt().Int64())
	require.Equal(t, int64(0), result.OutputKnown.ToBigInt().Int64())
}

func TestComputeValueWithCumulativeDiffs(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	batch := store.NewBatch()
	defer func() {
		_ = batch.Cancel()
	}()

	testKey := []byte("test-ledger\x00cumul-account\x00USD")

	// Set base at index 5: input = 1000
	err := attrs.Volume.SetBase(batch, 5, testKey, &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(1000)})
	require.NoError(t, err)

	// Write 3 cumulative diffs (each represents the total cumul since base)
	// Diff at index 10: cumul input = 100
	err = attrs.Volume.AddDiff(batch, 10, testKey, &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(100)})
	require.NoError(t, err)

	// Diff at index 15: cumul input = 250
	err = attrs.Volume.AddDiff(batch, 15, testKey, &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(250)})
	require.NoError(t, err)

	// Diff at index 20: cumul input = 500
	err = attrs.Volume.AddDiff(batch, 20, testKey, &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(500)})
	require.NoError(t, err)

	err = batch.Commit()
	require.NoError(t, err)

	// ComputeValue should return base + last cumul = 1000 + 500 = 1500
	// (not base + sum of all = 1000 + 100 + 250 + 500 = 1850)
	result, err := attrs.Volume.ComputeValue(store, 100, testKey)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(1500), result.InputKnown.ToBigInt().Int64())
}

func TestDeleteRemovesAllEntries(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("test-ledger\x00delete-account\x00status")

	// Set a base value and some diffs for metadata
	batch := store.NewBatch()
	err := attrs.Metadata.SetBase(batch, 5, testKey, &commonpb.MetadataValue{Value: "active"})
	require.NoError(t, err)
	err = attrs.Metadata.AddDiff(batch, 10, testKey, &commonpb.MetadataValue{Value: "inactive"})
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)

	// Verify data exists before deletion
	result, err := attrs.Metadata.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "inactive", result.Value)

	// Delete all entries for this key
	batch = store.NewBatch()
	err = attrs.Metadata.Delete(batch, testKey)
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)

	// Verify data is gone
	result, err = attrs.Metadata.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Nil(t, result)

	// Verify List no longer returns this key
	entries, err := attrs.Metadata.List(store)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestDeleteThenReAdd(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("test-ledger\x00readd-account\x00status")

	// Set initial value
	batch := store.NewBatch()
	err := attrs.Metadata.SetBase(batch, 5, testKey, &commonpb.MetadataValue{Value: "original"})
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)

	// Delete all entries
	batch = store.NewBatch()
	err = attrs.Metadata.Delete(batch, testKey)
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)

	// Re-add with a new value
	batch = store.NewBatch()
	err = attrs.Metadata.AddDiff(batch, 20, testKey, &commonpb.MetadataValue{Value: "new-value"})
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)

	// Verify new value is returned
	result, err := attrs.Metadata.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "new-value", result.Value)
}

func TestDeleteNonExistentKey(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("test-ledger\x00nonexistent\x00key")

	// Delete on a key with no entries should not error
	batch := store.NewBatch()
	err := attrs.Metadata.Delete(batch, testKey)
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)

	// ComputeValue should still return nil
	result, err := attrs.Metadata.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestScanEntriesBaseAndDiffs(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("test-ledger\x00scan-account\x00USD")

	batch := store.NewBatch()
	defer func() { _ = batch.Cancel() }()

	// Set base at index 5: input = 1000
	err := attrs.Volume.SetBase(batch, 5, testKey, &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(1000)})
	require.NoError(t, err)

	// Write 3 cumulative diffs
	err = attrs.Volume.AddDiff(batch, 10, testKey, &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(100)})
	require.NoError(t, err)
	err = attrs.Volume.AddDiff(batch, 15, testKey, &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(250)})
	require.NoError(t, err)
	err = attrs.Volume.AddDiff(batch, 20, testKey, &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(500)})
	require.NoError(t, err)

	err = batch.Commit()
	require.NoError(t, err)

	scan, err := attrs.Volume.ScanEntries(store, testKey)
	require.NoError(t, err)
	require.NotNil(t, scan)
	require.True(t, scan.HasBase)
	require.Equal(t, uint64(5), scan.LatestBaseIndex)
	require.Equal(t, int64(1000), scan.LatestBase.InputKnown.ToBigInt().Int64())
	require.True(t, scan.HasDiff)
	require.Equal(t, uint64(20), scan.LatestDiffIndex)
	require.Equal(t, 4, scan.TotalEntries)
}

func TestScanEntriesDiffsOnly(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("test-ledger\x00scan-diff-only\x00USD")

	batch := store.NewBatch()
	defer func() { _ = batch.Cancel() }()

	// Only diffs, no base (like @world)
	err := attrs.Volume.AddDiff(batch, 10, testKey, &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(100)})
	require.NoError(t, err)
	err = attrs.Volume.AddDiff(batch, 20, testKey, &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(200)})
	require.NoError(t, err)

	err = batch.Commit()
	require.NoError(t, err)

	scan, err := attrs.Volume.ScanEntries(store, testKey)
	require.NoError(t, err)
	require.NotNil(t, scan)
	require.False(t, scan.HasBase)
	require.True(t, scan.HasDiff)
	require.Equal(t, uint64(20), scan.LatestDiffIndex)
	require.Equal(t, 2, scan.TotalEntries)
}

func TestScanEntriesEmpty(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("test-ledger\x00nonexistent\x00USD")

	scan, err := attrs.Volume.ScanEntries(store, testKey)
	require.NoError(t, err)
	require.NotNil(t, scan)
	require.False(t, scan.HasBase)
	require.False(t, scan.HasDiff)
	require.Equal(t, 0, scan.TotalEntries)
}

func TestSetBaseWithZeroValue(t *testing.T) {
	t.Parallel()

	// Create a data store
	store := createTestStore(t)
	attrs := New()

	// Create a batch
	batch := store.NewBatch()
	defer func() {
		_ = batch.Cancel()
	}()

	// Create a test canonical key (different from the other test for isolation)
	testKey := []byte("test-ledger\x00another-account\x00EUR")

	// Test value: input=0, output=0
	testValue := &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(0)}

	// Set base at index 5
	err := attrs.Volume.SetBase(batch, 5, testKey, testValue)
	require.NoError(t, err)

	// Commit the batch
	err = batch.Commit()
	require.NoError(t, err)

	// Compute value
	result, err := attrs.Volume.ComputeValue(store, 100, testKey)
	require.NoError(t, err)

	// Verify the result
	require.NotNil(t, result)
	require.Equal(t, int64(0), result.InputKnown.ToBigInt().Int64())
}

