package dal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/go-libs/v4/logging"
)

func TestStoreInterceptor_Delegate(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	interceptor := NewStoreInterceptor(s)

	require.Same(t, s, interceptor.Delegate())
}

func TestStoreInterceptor_NewBatch_Passthrough(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	interceptor := NewStoreInterceptor(s)

	batch := interceptor.NewBatch()
	require.NotNil(t, batch)

	require.NoError(t, batch.SetBytes([]byte("k"), []byte("v")))
	require.NoError(t, batch.Commit())

	val, closer, err := s.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
	require.NoError(t, closer.Close())
}

func TestStoreInterceptor_NewBatch_Intercepted(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	interceptor := NewStoreInterceptor(s)

	var interceptCalled bool

	interceptor.SetNewBatchInterceptor(func(delegate *Store) *Batch {
		interceptCalled = true

		return delegate.NewBatch()
	})

	batch := interceptor.NewBatch()

	require.True(t, interceptCalled)
	require.NotNil(t, batch)
	_ = batch.Cancel()
}

func TestStoreInterceptor_CreateSnapshot_Passthrough(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	interceptor := NewStoreInterceptor(s)

	id, err := interceptor.CreateSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)
}

func TestStoreInterceptor_CreateSnapshot_Intercepted(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	interceptor := NewStoreInterceptor(s)

	errInjected := errors.New("snapshot blocked")

	interceptor.SetCreateSnapshotInterceptor(func(delegate *Store) (uint64, error) {
		return 0, errInjected
	})

	_, err := interceptor.CreateSnapshot()
	require.ErrorIs(t, err, errInjected)
}

func TestStoreInterceptor_Close_Passthrough(t *testing.T) {
	t.Parallel()

	// Manually create a store without the cleanup closure to avoid double-close panic.
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := NewStore(t.TempDir(), logger, meter, DefaultConfig())
	require.NoError(t, err)

	interceptor := NewStoreInterceptor(s)

	require.NoError(t, interceptor.Close())
}

func TestStoreInterceptor_Close_Intercepted(t *testing.T) {
	t.Parallel()

	// Manually create a store without the cleanup closure to avoid double-close panic.
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := NewStore(t.TempDir(), logger, meter, DefaultConfig())
	require.NoError(t, err)

	interceptor := NewStoreInterceptor(s)

	var closeCalled bool

	interceptor.SetCloseInterceptor(func(delegate *Store) error {
		closeCalled = true

		return delegate.Close()
	})

	require.NoError(t, interceptor.Close())
	require.True(t, closeCalled)
}

func TestStoreInterceptor_ClearInterceptors(t *testing.T) {
	t.Parallel()

	// Manually create a store without cleanup to avoid double-close panic
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := NewStore(t.TempDir(), logger, meter, DefaultConfig())
	require.NoError(t, err)

	interceptor := NewStoreInterceptor(s)

	// Set interceptors
	interceptor.SetNewBatchInterceptor(func(delegate *Store) *Batch {
		t.Fatal("should not be called after clear")

		return nil
	})
	interceptor.SetCreateSnapshotInterceptor(func(delegate *Store) (uint64, error) {
		t.Fatal("should not be called after clear")

		return 0, nil
	})
	interceptor.SetCloseInterceptor(func(delegate *Store) error {
		t.Fatal("should not be called after clear")

		return nil
	})

	// Clear all
	interceptor.ClearInterceptors()

	// Now calls should pass through
	batch := interceptor.NewBatch()
	require.NotNil(t, batch)
	_ = batch.Cancel()

	require.NoError(t, interceptor.Close())
}
