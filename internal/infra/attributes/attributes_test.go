package attributes

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func createTestStore(t *testing.T) *dal.Store {
	tmpDir := t.TempDir()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func TestSetAndComputeValue(t *testing.T) {
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
	testValue := &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(1000)}

	// Set at index 5
	_, err := attrs.Volume.Set(batch, testKey, testValue)
	require.NoError(t, err)

	// Commit the batch
	err = batch.Commit()
	require.NoError(t, err)

	// Compute value
	result, err := attrs.Volume.Get(store, testKey)
	require.NoError(t, err)

	// Verify the result
	require.NotNil(t, result)
	require.Equal(t, int64(1000), result.GetInput().ToBigInt().Int64())
	require.Equal(t, int64(0), result.GetOutput().ToBigInt().Int64())
}

func TestComputeValueWithMultipleSets(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	batch := store.NewBatch()

	defer func() {
		_ = batch.Cancel()
	}()

	testKey := []byte("test-ledger\x00cumul-account\x00USD")

	// Set at index 5: input = 1000
	_, err := attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(1000)})
	require.NoError(t, err)

	// Set at index 10: input = 100
	_, err = attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(100)})
	require.NoError(t, err)

	// Set at index 15: input = 250
	_, err = attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(250)})
	require.NoError(t, err)

	// Set at index 20: input = 500
	_, err = attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(500)})
	require.NoError(t, err)

	err = batch.Commit()
	require.NoError(t, err)

	// ComputeValue should return the latest Set value (last-write-wins) = 500
	result, err := attrs.Volume.Get(store, testKey)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(500), result.GetInput().ToBigInt().Int64())
}

func TestDeleteRemovesAllEntries(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("test-ledger\x00delete-account\x00status")

	// Set metadata values at two indexes
	batch := store.NewBatch()
	_, err := attrs.Metadata.Set(batch, testKey, commonpb.NewStringValue("active"))
	require.NoError(t, err)
	_, err = attrs.Metadata.Set(batch, testKey, commonpb.NewStringValue("inactive"))
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)

	// Verify data exists before deletion
	result, err := attrs.Metadata.Get(store, testKey)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "inactive", commonpb.MetadataValueToString(result))

	// Delete all entries for this key
	batch = store.NewBatch()
	err = attrs.Metadata.Delete(batch, testKey)
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)

	// Verify data is gone
	result, err = attrs.Metadata.Get(store, testKey)
	require.NoError(t, err)
	require.Nil(t, result)

	// Verify no entries remain for this prefix
	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	entries, err := attrs.Metadata.ComputeAllForPrefix(handle, testKey)
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
	_, err := attrs.Metadata.Set(batch, testKey, commonpb.NewStringValue("original"))
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
	_, err = attrs.Metadata.Set(batch, testKey, commonpb.NewStringValue("new-value"))
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)

	// Verify new value is returned
	result, err := attrs.Metadata.Get(store, testKey)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "new-value", commonpb.MetadataValueToString(result))
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
	result, err := attrs.Metadata.Get(store, testKey)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestScanEntriesMultipleSets(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("test-ledger\x00scan-account\x00USD")

	batch := store.NewBatch()

	defer func() { _ = batch.Cancel() }()

	// Set at index 5: input = 1000
	_, err := attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(1000)})
	require.NoError(t, err)

	// Set at index 10
	_, err = attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(100)})
	require.NoError(t, err)
	// Set at index 15
	_, err = attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(250)})
	require.NoError(t, err)
	// Set at index 20
	_, err = attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(500)})
	require.NoError(t, err)

	err = batch.Commit()
	require.NoError(t, err)

	scan, err := attrs.Volume.ScanEntries(store, testKey)
	require.NoError(t, err)
	require.NotNil(t, scan)
	require.True(t, scan.HasBase)
	require.Equal(t, int64(500), scan.LatestBase.GetInput().ToBigInt().Int64())
}

func TestScanEntriesMultipleSetsNoPrior(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("test-ledger\x00scan-diff-only\x00USD")

	batch := store.NewBatch()

	defer func() { _ = batch.Cancel() }()

	// Two sets, no prior data
	_, err := attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(100)})
	require.NoError(t, err)
	_, err = attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(200)})
	require.NoError(t, err)

	err = batch.Commit()
	require.NoError(t, err)

	scan, err := attrs.Volume.ScanEntries(store, testKey)
	require.NoError(t, err)
	require.NotNil(t, scan)
	require.True(t, scan.HasBase)
	require.Equal(t, int64(200), scan.LatestBase.GetInput().ToBigInt().Int64())
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
}

func TestSetWithZeroValue(t *testing.T) {
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
	testValue := &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(0)}

	// Set at index 5
	_, err := attrs.Volume.Set(batch, testKey, testValue)
	require.NoError(t, err)

	// Commit the batch
	err = batch.Commit()
	require.NoError(t, err)

	// Compute value
	result, err := attrs.Volume.Get(store, testKey)
	require.NoError(t, err)

	// Verify the result
	require.NotNil(t, result)
	require.Equal(t, int64(0), result.GetInput().ToBigInt().Int64())
}
