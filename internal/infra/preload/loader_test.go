package preload

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func TestAttributeLoader_LoadOrWait_FirstLoad(t *testing.T) {
	t.Parallel()

	loader := NewAttributeLoader[int]()
	key := attributes.NewU128(1, 2)

	loadCount := 0
	result, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
		loadCount++

		return 42, 0, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, result.Value)
	assert.True(t, result.FromLoad, "First load should set FromLoad=true")
	assert.Equal(t, 1, loadCount, "Load function should be called once")
}

func TestAttributeLoader_LoadOrWait_CachedResult(t *testing.T) {
	t.Parallel()

	loader := NewAttributeLoader[int]()
	key := attributes.NewU128(1, 2)

	// First load
	_, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
		return 42, 0, nil
	})
	require.NoError(t, err)

	// Second load with same boundary - should use cached value
	loadCount := 0
	result, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
		loadCount++

		return 999, 0, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, result.Value, "Should return cached value")
	assert.False(t, result.FromLoad, "Should not load from store")
	assert.Equal(t, 0, loadCount, "Load function should not be called")
}

func TestAttributeLoader_LoadOrWait_CachedResultWithLowerBoundary(t *testing.T) {
	t.Parallel()

	loader := NewAttributeLoader[int]()
	key := attributes.NewU128(1, 2)

	// First load at boundary 200
	_, err := loader.LoadOrWait(key, 200, func() (int, uint64, error) {
		return 42, 0, nil
	})
	require.NoError(t, err)

	// Second load with lower boundary - should use cached value
	loadCount := 0
	result, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
		loadCount++

		return 999, 0, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, result.Value, "Should return cached value (higher boundary is acceptable)")
	assert.False(t, result.FromLoad, "Should not load from store")
	assert.Equal(t, 0, loadCount, "Load function should not be called")
}

func TestAttributeLoader_LoadOrWait_ConcurrentLoads(t *testing.T) {
	t.Parallel()

	loader := NewAttributeLoader[int]()
	key := attributes.NewU128(1, 2)

	var loadCount atomic.Int32

	loadStarted := make(chan struct{})
	loadContinue := make(chan struct{})

	// Start first goroutine that will hold the load
	var wg sync.WaitGroup

	wg.Go(func() {
		result, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
			loadCount.Add(1)
			close(loadStarted)
			<-loadContinue // Wait for signal to continue

			return 42, 0, nil
		})
		require.NoError(t, err)
		assert.Equal(t, 42, result.Value)
		assert.True(t, result.FromLoad)
	})

	// Wait for first load to start
	<-loadStarted

	// Start multiple concurrent goroutines that should wait
	const numWaiters = 5
	wg.Add(numWaiters)
	results := make(chan *LoadResult[int], numWaiters)

	for range numWaiters {
		go func() {
			defer wg.Done()

			result, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
				loadCount.Add(1)

				return 999, 0, nil // Should never be called
			})
			require.NoError(t, err)

			results <- result
		}()
	}

	// Wait to confirm no extra loads are triggered while first load is in progress
	require.Never(t, func() bool { return loadCount.Load() > 1 }, 50*time.Millisecond, 10*time.Millisecond)

	// Release the first load
	close(loadContinue)

	wg.Wait()
	close(results)

	// All waiters should have received the cached value
	for result := range results {
		assert.Equal(t, 42, result.Value, "Waiters should get cached value")
		assert.False(t, result.FromLoad, "Waiters should not have loaded")
	}

	// Load function should only be called once
	assert.Equal(t, int32(1), loadCount.Load(), "Load function should be called exactly once")
}

func TestAttributeLoader_LoadOrWait_Error(t *testing.T) {
	t.Parallel()

	loader := NewAttributeLoader[int]()
	key := attributes.NewU128(1, 2)

	expectedErr := errors.New("load failed")
	result, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
		return 0, 0, expectedErr
	})

	require.ErrorIs(t, err, expectedErr)
	assert.Equal(t, 0, result.Value)
	assert.False(t, result.FromLoad)

	// Verify the key is not in the loaded map (error should not cache)
	loadCount := 0
	result, err = loader.LoadOrWait(key, 100, func() (int, uint64, error) {
		loadCount++

		return 42, 0, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, result.Value)
	assert.True(t, result.FromLoad, "Should load again after error")
	assert.Equal(t, 1, loadCount)
}

func TestAttributeLoader_LoadOrWait_ErrorReleasesWaiters(t *testing.T) {
	t.Parallel()

	loader := NewAttributeLoader[int]()
	key := attributes.NewU128(1, 2)

	loadStarted := make(chan struct{})
	loadContinue := make(chan struct{})

	// Start first goroutine that will fail
	var wg sync.WaitGroup

	wg.Go(func() {
		_, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
			close(loadStarted)
			<-loadContinue

			return 0, 0, errors.New("load failed")
		})
		require.Error(t, err)
	})

	// Wait for first load to start
	<-loadStarted

	// Start waiter
	wg.Add(1)

	waiterDone := make(chan struct{})

	go func() {
		defer wg.Done()
		// This should wait, then try to load again since the first one failed
		result, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
			return 42, 0, nil
		})
		require.NoError(t, err)
		assert.Equal(t, 42, result.Value)
		assert.True(t, result.FromLoad, "Should load since previous failed")
		close(waiterDone)
	}()

	// Wait to confirm waiter hasn't completed yet (it should be blocked)
	require.Never(t, func() bool {
		select {
		case <-waiterDone:
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, 10*time.Millisecond)

	// Release the first load (which will fail)
	close(loadContinue)

	// Wait for completion
	select {
	case <-waiterDone:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Waiter did not complete in time - may be deadlocked")
	}

	wg.Wait()
}

func TestAttributeLoader_Release(t *testing.T) {
	t.Parallel()

	loader := NewAttributeLoader[int]()
	key := attributes.NewU128(1, 2)

	// Load a value
	_, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
		return 42, 0, nil
	})
	require.NoError(t, err)

	// Verify it's cached
	loadCount := 0
	result, err := loader.LoadOrWait(key, 100, func() (int, uint64, error) {
		loadCount++

		return 999, 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 42, result.Value)
	assert.False(t, result.FromLoad)
	assert.Equal(t, 0, loadCount)

	// Release the key
	loader.Release(key)

	// Should load again
	result, err = loader.LoadOrWait(key, 100, func() (int, uint64, error) {
		loadCount++

		return 123, 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 123, result.Value)
	assert.True(t, result.FromLoad, "Should load after Release")
	assert.Equal(t, 1, loadCount)
}

func TestAttributeLoader_DifferentKeys(t *testing.T) {
	t.Parallel()

	loader := NewAttributeLoader[int]()
	key1 := attributes.NewU128(1, 1)
	key2 := attributes.NewU128(2, 2)

	// Load key1
	result1, err := loader.LoadOrWait(key1, 100, func() (int, uint64, error) {
		return 42, 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 42, result1.Value)
	assert.True(t, result1.FromLoad)

	// Load key2 - should not use key1's cached value
	result2, err := loader.LoadOrWait(key2, 100, func() (int, uint64, error) {
		return 99, 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 99, result2.Value)
	assert.True(t, result2.FromLoad, "Different key should load independently")
}

func TestNewLoaders(t *testing.T) {
	t.Parallel()

	loaders := NewLoaders()

	assert.NotNil(t, loaders.Volumes)
	assert.NotNil(t, loaders.IdempotencyKeys)
}

func TestCleanupToken_Release(t *testing.T) {
	t.Parallel()

	loaders := NewLoaders()

	// Load some values
	key1 := attributes.NewU128(1, 1)
	key2 := attributes.NewU128(2, 2)
	key4 := attributes.NewU128(4, 4)

	_, err := loaders.Volumes.LoadOrWait(key1, 100, func() (*raftcmdpb.VolumePair, uint64, error) {
		return &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(42)}, 0, nil
	})
	require.NoError(t, err)

	_, err = loaders.Volumes.LoadOrWait(key2, 100, func() (*raftcmdpb.VolumePair, uint64, error) {
		return &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(43)}, 0, nil
	})
	require.NoError(t, err)

	_, err = loaders.IdempotencyKeys.LoadOrWait(key4, 100, func() (*commonpb.IdempotencyKeyValue, uint64, error) {
		return &commonpb.IdempotencyKeyValue{LogSequence: 1, Hash: []byte("test")}, 0, nil
	})
	require.NoError(t, err)

	// Create tracker with the loaded keys
	token := &CleanupToken{
		Volumes:         []attributes.U128{key1, key2},
		IdempotencyKeys: []attributes.U128{key4},
	}

	// Release all tracked keys
	token.Release(loaders)

	// Verify all keys were removed - next load should actually load
	volumeLoadCount := 0
	_, err = loaders.Volumes.LoadOrWait(key1, 100, func() (*raftcmdpb.VolumePair, uint64, error) {
		volumeLoadCount++

		return &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(100)}, 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, volumeLoadCount, "Volumes key1 should reload after Release")

	volumeLoadCount2 := 0
	_, err = loaders.Volumes.LoadOrWait(key2, 100, func() (*raftcmdpb.VolumePair, uint64, error) {
		volumeLoadCount2++

		return &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(100)}, 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, volumeLoadCount2, "Volumes key2 should reload after Release")

	idempotencyLoadCount := 0
	_, err = loaders.IdempotencyKeys.LoadOrWait(key4, 100, func() (*commonpb.IdempotencyKeyValue, uint64, error) {
		idempotencyLoadCount++

		return &commonpb.IdempotencyKeyValue{LogSequence: 2, Hash: []byte("new")}, 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, idempotencyLoadCount, "IdempotencyKeys should reload after Release")
}
