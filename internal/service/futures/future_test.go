package futures

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFuture_ResolveAndWait(t *testing.T) {
	t.Parallel()

	f := New[string]()
	f.Resolve("hello", nil)

	val, err := f.Wait()
	require.NoError(t, err)
	require.Equal(t, "hello", val)
}

func TestFuture_ResolveWithError(t *testing.T) {
	t.Parallel()

	expected := errors.New("something failed")
	f := New[int]()
	f.Resolve(0, expected)

	val, err := f.Wait()
	require.ErrorIs(t, err, expected)
	require.Equal(t, 0, val)
}

func TestFuture_WaitBlocksUntilResolve(t *testing.T) {
	t.Parallel()

	f := New[int]()
	var (
		wg  sync.WaitGroup
		got int
		err error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		got, err = f.Wait()
	}()

	f.Resolve(42, nil)
	wg.Wait()

	require.NoError(t, err)
	require.Equal(t, 42, got)
}

func TestFuture_MultipleWaiters(t *testing.T) {
	t.Parallel()

	f := New[string]()
	const n = 10

	var wg sync.WaitGroup
	results := make([]string, n)
	errs := make([]error, n)

	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = f.Wait()
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

	// Wait should return immediately since it's already resolved
	val, err := f.Wait()
	require.NoError(t, err)
	require.Equal(t, 99, val)
}

func TestFuture_ZeroValue(t *testing.T) {
	t.Parallel()

	f := New[string]()
	f.Resolve("", nil)

	val, err := f.Wait()
	require.NoError(t, err)
	require.Equal(t, "", val)
}
