package futures

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFuture_ResolveAndWait(t *testing.T) {
	t.Parallel()

	f := New[string]()
	f.Resolve("hello", nil)

	val, err := f.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, "hello", val)
}

func TestFuture_ResolveWithError(t *testing.T) {
	t.Parallel()

	expected := errors.New("something failed")
	f := New[int]()
	f.Resolve(0, expected)

	val, err := f.Wait(t.Context())
	require.ErrorIs(t, err, expected)
	require.Equal(t, 0, val)
}

func TestFuture_WaitBlocksUntilResolve(t *testing.T) {
	t.Parallel()

	f := New[int]()
	ctx := t.Context()

	var (
		wg  sync.WaitGroup
		got int
		err error
	)

	wg.Go(func() {
		got, err = f.Wait(ctx)
	})

	f.Resolve(42, nil)
	wg.Wait()

	require.NoError(t, err)
	require.Equal(t, 42, got)
}

func TestFuture_MultipleWaiters(t *testing.T) {
	t.Parallel()

	f := New[string]()
	ctx := t.Context()

	const n = 10

	var wg sync.WaitGroup

	results := make([]string, n)
	errs := make([]error, n)

	for i := range n {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			results[idx], errs[idx] = f.Wait(ctx)
		}(i)
	}

	f.Resolve("shared", nil)
	wg.Wait()

	for i := range n {
		require.NoError(t, errs[i])
		require.Equal(t, "shared", results[i])
	}
}

func TestFuture_ResolveBeforeWait(t *testing.T) {
	t.Parallel()

	f := New[int]()
	f.Resolve(99, nil)

	val, err := f.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, 99, val)
}

func TestFuture_ZeroValue(t *testing.T) {
	t.Parallel()

	f := New[string]()
	f.Resolve("", nil)

	val, err := f.Wait(t.Context())
	require.NoError(t, err)
	require.Empty(t, val)
}

func TestFuture_Wait_CancelBeforeResolve(t *testing.T) {
	t.Parallel()

	f := New[int]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	val, err := f.Wait(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, val)
}

func TestFuture_Wait_CancelDuringWait(t *testing.T) {
	t.Parallel()

	f := New[int]()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	val, err := f.Wait(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 0, val)
}

func TestFuture_Wait_ResolveDuringWait(t *testing.T) {
	t.Parallel()

	f := New[string]()
	ctx := t.Context()

	var (
		wg  sync.WaitGroup
		got string
		err error
	)

	wg.Go(func() {
		got, err = f.Wait(ctx)
	})

	// Resolve concurrently with the waiter. The scheduler decides whether the
	// goroutine is already parked in Wait or resolves first; both orderings
	// are valid and must yield the resolved value. wg.Wait barriers on
	// completion, so no fixed sleep is needed.
	f.Resolve("hello", nil)
	wg.Wait()

	require.NoError(t, err)
	require.Equal(t, "hello", got)
}

// TestFuture_Wait_NoLostWakeup exercises the cancellation path under heavy
// contention to catch a lost-wakeup regression: if the canceller broadcasts
// while the waiter is between its ctx.Err() check and cond.Wait()'s park,
// the waiter must still observe the cancellation. Without the lock around
// Broadcast in Wait(), this test deadlocks roughly 1/N iterations under -race.
func TestFuture_Wait_NoLostWakeup(t *testing.T) {
	t.Parallel()

	const iterations = 500

	for range iterations {
		f := New[int]()
		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)

		go func() {
			_, err := f.Wait(ctx)
			errCh <- err
		}()

		cancel()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("Wait did not return after ctx cancellation (lost wakeup)")
		}
	}
}

func TestFuture_Wait_NilContext(t *testing.T) {
	t.Parallel()

	f := New[int]()
	f.Resolve(1, nil)

	require.PanicsWithValue(t, "futures: Wait called with nil context", func() {
		//nolint:staticcheck // intentional nil context to assert the contract
		_, _ = f.Wait(nil)
	})
}
