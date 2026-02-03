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

	// Create a test ID
	testID := U128{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0}

	// Test value: 1000
	testValue := commonpb.NewBigInt(big.NewInt(1000))

	// Set base at index 5
	err := attrs.Input.SetBase(batch, 5, testID, testValue)
	require.NoError(t, err)

	// Commit the batch
	err = batch.Commit()
	require.NoError(t, err)

	// Compute value
	result, err := attrs.Input.ComputeValue(store, 100, testID)
	require.NoError(t, err)

	// Verify the result
	require.NotNil(t, result)
	require.Equal(t, int64(1000), result.Value().Int64())
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

	// Create a test ID
	testID := U128{0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89}

	// Test value: 0
	testValue := commonpb.NewBigInt(big.NewInt(0))

	// Set base at index 5
	err := attrs.Input.SetBase(batch, 5, testID, testValue)
	require.NoError(t, err)

	// Commit the batch
	err = batch.Commit()
	require.NoError(t, err)

	// Compute value
	result, err := attrs.Input.ComputeValue(store, 100, testID)
	require.NoError(t, err)

	// Verify the result
	require.NotNil(t, result)
	require.Equal(t, int64(0), result.Value().Int64())
}
