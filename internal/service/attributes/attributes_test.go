package attributes

import (
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
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

	// Test value: 1000
	testValue := commonpb.NewBigInt(big.NewInt(1000))

	// Set base at index 5
	err := attrs.Input.SetBase(batch, 5, testKey, testValue)
	require.NoError(t, err)

	// Commit the batch
	err = batch.Commit()
	require.NoError(t, err)

	// Compute value
	result, err := attrs.Input.ComputeValue(store, 100, testKey)
	require.NoError(t, err)

	// Verify the result
	require.NotNil(t, result)
	require.Equal(t, int64(1000), result.Value().Int64())
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

	// Set base at index 5: value = 1000
	err := attrs.Input.SetBase(batch, 5, testKey, commonpb.NewBigInt(big.NewInt(1000)))
	require.NoError(t, err)

	// Write 3 cumulative diffs (each represents the total cumul since base)
	// Diff at index 10: cumul = 100
	err = attrs.Input.AddDiff(batch, 10, testKey, commonpb.NewBigInt(big.NewInt(100)))
	require.NoError(t, err)

	// Diff at index 15: cumul = 250
	err = attrs.Input.AddDiff(batch, 15, testKey, commonpb.NewBigInt(big.NewInt(250)))
	require.NoError(t, err)

	// Diff at index 20: cumul = 500
	err = attrs.Input.AddDiff(batch, 20, testKey, commonpb.NewBigInt(big.NewInt(500)))
	require.NoError(t, err)

	err = batch.Commit()
	require.NoError(t, err)

	// ComputeValue should return base + last cumul = 1000 + 500 = 1500
	// (not base + sum of all = 1000 + 100 + 250 + 500 = 1850)
	result, err := attrs.Input.ComputeValue(store, 100, testKey)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(1500), result.Value().Int64())
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

	// Test value: 0
	testValue := commonpb.NewBigInt(big.NewInt(0))

	// Set base at index 5
	err := attrs.Input.SetBase(batch, 5, testKey, testValue)
	require.NoError(t, err)

	// Commit the batch
	err = batch.Commit()
	require.NoError(t, err)

	// Compute value
	result, err := attrs.Input.ComputeValue(store, 100, testKey)
	require.NoError(t, err)

	// Verify the result
	require.NotNil(t, result)
	require.Equal(t, int64(0), result.Value().Int64())
}
