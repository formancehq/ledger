package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCache_BasicGetAndRelease(t *testing.T) {
	initCount := 0
	cache := NewCache(func(ctx context.Context, key string) (int, error) {
		initCount++
		return len(key), nil
	})

	ctx := context.Background()

	// First get should initialize
	handle, err := cache.Get(ctx, "hello")
	require.NoError(t, err)
	require.Equal(t, 5, handle.Value())
	require.Equal(t, 1, initCount)
	require.Equal(t, 1, cache.Size())

	// Second get for same key should reuse
	handle2, err := cache.Get(ctx, "hello")
	require.NoError(t, err)
	require.Equal(t, 5, handle2.Value())
	require.Equal(t, 1, initCount) // Still 1, no new init
	require.Equal(t, 1, cache.Size())

	// Release first handle - should not evict (handle2 still holds reference)
	handle.Release()
	require.Equal(t, 1, cache.Size())

	// Release second handle - should evict
	handle2.Release()
	require.Equal(t, 0, cache.Size())

	// Getting again should reinitialize
	handle3, err := cache.Get(ctx, "hello")
	require.NoError(t, err)
	require.Equal(t, 5, handle3.Value())
	require.Equal(t, 2, initCount) // New init
	handle3.Release()
}

func TestCache_DifferentKeys(t *testing.T) {
	initCount := 0
	cache := NewCache(func(ctx context.Context, key string) (int, error) {
		initCount++
		return len(key), nil
	})

	ctx := context.Background()

	handle1, err := cache.Get(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, 1, handle1.Value())

	handle2, err := cache.Get(ctx, "bb")
	require.NoError(t, err)
	require.Equal(t, 2, handle2.Value())

	require.Equal(t, 2, initCount)
	require.Equal(t, 2, cache.Size())

	handle1.Release()
	require.Equal(t, 1, cache.Size())

	handle2.Release()
	require.Equal(t, 0, cache.Size())
}

func TestCache_InitError(t *testing.T) {
	expectedErr := errors.New("init failed")
	initCount := 0
	cache := NewCache(func(ctx context.Context, key string) (int, error) {
		initCount++
		if key == "fail" {
			return 0, expectedErr
		}
		return len(key), nil
	})

	ctx := context.Background()

	// Error should be returned and entry should not be cached
	handle, err := cache.Get(ctx, "fail")
	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, handle)
	require.Equal(t, 1, initCount)
	require.Equal(t, 0, cache.Size()) // Entry removed on error

	// Retry should attempt init again
	handle, err = cache.Get(ctx, "fail")
	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, handle)
	require.Equal(t, 2, initCount) // New attempt
}

func TestCache_ConcurrentGetSameKey(t *testing.T) {
	var initCount atomic.Int32
	initStarted := make(chan struct{})
	initCanProceed := make(chan struct{})

	cache := NewCache(func(ctx context.Context, key string) (int, error) {
		initCount.Add(1)
		close(initStarted)
		<-initCanProceed
		return len(key), nil
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	handles := make([]*Handle[int], 3)
	errs := make([]error, 3)

	// Start first goroutine - it will start init and block
	wg.Add(1)
	go func() {
		defer wg.Done()
		handles[0], errs[0] = cache.Get(ctx, "test")
	}()

	// Wait for init to start
	<-initStarted

	// Start two more goroutines - they should wait for init
	for i := 1; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			handles[idx], errs[idx] = cache.Get(ctx, "test")
		}(i)
	}

	// Wait for the cache to have recorded all 3 references (meaning all goroutines reached the waiting point)
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		if handle, ok := cache.cache["test"]; ok {
			return handle.references == 3
		}
		return false
	}, 500*time.Millisecond, 1*time.Millisecond, "all goroutines should reach the waiting point")

	// Allow init to complete
	close(initCanProceed)
	wg.Wait()

	// All should succeed with same value
	for i := 0; i < 3; i++ {
		require.NoError(t, errs[i], "goroutine %d", i)
		require.NotNil(t, handles[i], "goroutine %d", i)
		require.Equal(t, 4, handles[i].Value(), "goroutine %d", i)
	}

	// Init should only be called once
	require.Equal(t, int32(1), initCount.Load())

	// Cache should have one entry with 3 references
	require.Equal(t, 1, cache.Size())

	// Release all handles
	for i := 0; i < 3; i++ {
		handles[i].Release()
	}
	require.Equal(t, 0, cache.Size())
}

func TestCache_ConcurrentGetSameKeyWithError(t *testing.T) {
	expectedErr := errors.New("init failed")
	var initCount atomic.Int32
	initStarted := make(chan struct{})
	initCanProceed := make(chan struct{})

	cache := NewCache(func(ctx context.Context, key string) (int, error) {
		initCount.Add(1)
		close(initStarted)
		<-initCanProceed
		return 0, expectedErr
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	handles := make([]*Handle[int], 3)
	errs := make([]error, 3)

	// Start first goroutine - it will start init and block
	wg.Add(1)
	go func() {
		defer wg.Done()
		handles[0], errs[0] = cache.Get(ctx, "test")
	}()

	// Wait for init to start
	<-initStarted

	// Start two more goroutines - they should wait for init
	for i := 1; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			handles[idx], errs[idx] = cache.Get(ctx, "test")
		}(i)
	}

	// Wait for the cache to have recorded all 3 references (meaning all goroutines reached the waiting point)
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		if handle, ok := cache.cache["test"]; ok {
			return handle.references == 3
		}
		return false
	}, 500*time.Millisecond, 1*time.Millisecond, "all goroutines should reach the waiting point")

	// Allow init to complete
	close(initCanProceed)
	wg.Wait()

	// All should get the error
	for i := 0; i < 3; i++ {
		require.ErrorIs(t, errs[i], expectedErr, "goroutine %d", i)
		require.Nil(t, handles[i], "goroutine %d", i)
	}

	// Init should only be called once
	require.Equal(t, int32(1), initCount.Load())

	// Cache should be empty (error entry removed)
	require.Equal(t, 0, cache.Size())
}

func TestCache_ConcurrentGetDifferentKeys(t *testing.T) {
	var initCount atomic.Int32
	cache := NewCache(func(ctx context.Context, key string) (int, error) {
		initCount.Add(1)
		time.Sleep(10 * time.Millisecond) // Simulate some work
		return len(key), nil
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	keys := []string{"a", "bb", "ccc", "dddd", "eeeee"}
	handles := make([]*Handle[int], len(keys))
	errs := make([]error, len(keys))

	for i, key := range keys {
		wg.Add(1)
		go func(idx int, k string) {
			defer wg.Done()
			handles[idx], errs[idx] = cache.Get(ctx, k)
		}(i, key)
	}

	wg.Wait()

	// All should succeed
	for i := range keys {
		require.NoError(t, errs[i])
		require.Equal(t, len(keys[i]), handles[i].Value())
	}

	// Each key should have triggered one init
	require.Equal(t, int32(len(keys)), initCount.Load())
	require.Equal(t, len(keys), cache.Size())

	// Release all
	for _, h := range handles {
		h.Release()
	}
	require.Equal(t, 0, cache.Size())
}

func TestCache_NoDeadlock(t *testing.T) {
	// This test verifies there's no deadlock when Get and Release are called concurrently
	cache := NewCache(func(ctx context.Context, key int) (int, error) {
		return key * 2, nil
	})

	ctx := context.Background()
	var wg sync.WaitGroup

	// Run many concurrent operations
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := n % 10 // Use only 10 different keys to force contention

			handle, err := cache.Get(ctx, key)
			if err != nil {
				return
			}

			// Small random delay to increase chance of contention
			time.Sleep(time.Duration(n%5) * time.Millisecond)

			handle.Release()
		}(i)
	}

	// Use a timeout to detect deadlock
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - no deadlock
	case <-time.After(5 * time.Second):
		t.Fatal("Deadlock detected: test did not complete within timeout")
	}
}

func TestCache_ContextCancellation(t *testing.T) {
	cache := NewCache(func(ctx context.Context, key string) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return len(key), nil
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	handle, err := cache.Get(ctx, "test")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, handle)
	require.Equal(t, 0, cache.Size())
}

func TestCache_MultipleReleasesAreSafe(t *testing.T) {
	// This tests that calling Release multiple times doesn't cause issues
	// Note: This is technically a misuse of the API, but we should handle it gracefully
	cache := NewCache(func(ctx context.Context, key string) (int, error) {
		return len(key), nil
	})

	ctx := context.Background()

	handle, err := cache.Get(ctx, "test")
	require.NoError(t, err)

	handle.Release()
	require.Equal(t, 0, cache.Size())

	// Second release - references will go negative but shouldn't panic
	// The entry is already removed so this is a no-op for eviction
	handle.Release()
}
