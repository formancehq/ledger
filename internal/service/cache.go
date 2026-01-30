// Package service provides core ledger service functionality.
package service

import (
	"context"
	"sync"
)

// Cache is a generic, thread-safe cache with reference counting.
//
// The cache ensures that:
//   - Each key is initialized only once, even under concurrent access
//   - Entries are evicted only when all references have been released
//   - Failed initializations are not cached (allowing retry)
//
// This is useful for caching expensive resources (like database connections,
// compiled scripts, etc.) that should be shared across multiple consumers
// and cleaned up when no longer in use.
//
// Usage:
//
//	cache := NewCache(func(ctx context.Context, key string) (*Resource, error) {
//	    return loadResource(ctx, key)
//	})
//
//	handle, err := cache.Get(ctx, "my-key")
//	if err != nil {
//	    return err
//	}
//	defer handle.Release() // Always release when done
//
//	resource := handle.Value()
//	// Use resource...
type Cache[K comparable, V any] struct {
	mu     sync.Mutex
	cache  map[K]*Handle[V]
	initFn func(ctx context.Context, key K) (V, error)
}

// NewCache creates a new cache with the given initialization function.
// The initFn is called once per key when a value is first requested.
// If initFn returns an error, the error is returned to all waiting callers
// and the entry is not cached (subsequent calls will retry initialization).
func NewCache[K comparable, V any](initFn func(ctx context.Context, key K) (V, error)) *Cache[K, V] {
	return &Cache[K, V]{
		cache:  make(map[K]*Handle[V]),
		initFn: initFn,
	}
}

// Get retrieves or initializes a value for the given key.
//
// If the key exists in the cache, a new reference is acquired and the existing
// value is returned. If the key doesn't exist, initFn is called to initialize
// the value.
//
// Multiple concurrent calls for the same key will block until initialization
// completes, and all will receive the same value (or error).
//
// The caller MUST call Handle.Release() when done with the value to allow
// eventual eviction.
//
// Returns an error if initialization fails. Failed entries are not cached,
// allowing subsequent calls to retry.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (*Handle[V], error) {
	c.mu.Lock()
	handle, ok := c.cache[key]
	if ok {
		handle.references++
		c.mu.Unlock()

		<-handle.initTerminated
		if handle.err != nil {
			c.release(key, handle)
			return nil, handle.err
		}

		return handle, nil
	}

	handle = &Handle[V]{
		references:     1,
		initTerminated: make(chan struct{}),
	}
	// Set up the releaser closure that captures key
	handle.releaser = func() {
		c.release(key, handle)
	}
	c.cache[key] = handle
	c.mu.Unlock()

	value, err := c.initFn(ctx, key)

	c.mu.Lock()
	if err != nil {
		handle.err = err
		delete(c.cache, key)
	} else {
		handle.value = value
	}
	close(handle.initTerminated)
	c.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return handle, nil
}

// release decrements the reference count for a handle and evicts
// the entry from the cache if no references remain.
func (c *Cache[K, V]) release(key K, handle *Handle[V]) {
	c.mu.Lock()
	defer c.mu.Unlock()

	handle.references--
	if handle.references == 0 {
		delete(c.cache, key)
	}
}

// Size returns the number of entries currently in the cache.
// This is primarily useful for testing and monitoring.
func (c *Cache[K, V]) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

// Handle represents a reference to a cached value.
//
// A Handle is returned by Cache.Get() and must be released via Release()
// when the caller is done using the value. The cached entry will only be
// evicted when all handles have been released.
//
// Handles are safe to use concurrently from multiple goroutines.
type Handle[V any] struct {
	releaser       func()
	references     int
	value          V
	err            error
	initTerminated chan struct{}
}

// Release decrements the reference count and evicts the entry if no references remain.
func (h *Handle[V]) Release() {
	h.releaser()
}

// Value returns the cached value.
func (h *Handle[V]) Value() V {
	return h.value
}

// Err returns any error that occurred during initialization.
func (h *Handle[V]) Err() error {
	return h.err
}
