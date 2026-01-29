package service

import (
	"context"
	"sort"
	"sync"
)

// todo: add deadlocks detection
// KeySetLocker provides key-based locking for concurrent access.
// The caller is responsible for including any necessary prefixes (e.g., ledgerID) in the keys.
type KeySetLocker interface {
	LockKeys(ctx context.Context, keys ...string) (func(), error)
}

// DefaultKeySetLocker is a default implementation of KeySetLocker.
type DefaultKeySetLocker struct {
	// locks is a map of mutexes keyed by lock key.
	locks map[string]*lockEntry
	// locksMutex protects the locks map itself.
	locksMutex sync.RWMutex
}

type lockEntry struct {
	mutex    sync.Mutex
	refCount int
}

// NewDefaultKeySetLocker creates a new DefaultKeySetLocker.
func NewDefaultKeySetLocker() *DefaultKeySetLocker {
	return &DefaultKeySetLocker{
		locks: make(map[string]*lockEntry),
	}
}

// LockKeys locks the requested keys and returns a release function.
func (s *DefaultKeySetLocker) LockKeys(_ context.Context, keys ...string) (func(), error) {

	// Sort lock keys to avoid deadlocks (always lock in the same order).
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)

	// Lock all requested keys.
	for _, lockKey := range sortedKeys {
		entry := s.getOrCreateLock(lockKey)
		entry.mutex.Lock()
	}

	return func() {
		s.releaseLocks(sortedKeys)
	}, nil
}

// getOrCreateLock gets or creates a mutex for the given lock key (account:asset)
func (s *DefaultKeySetLocker) getOrCreateLock(lockKey string) *lockEntry {
	s.locksMutex.Lock()
	defer s.locksMutex.Unlock()

	if lock, ok := s.locks[lockKey]; ok {
		lock.refCount++
		return lock
	}

	// Create new lock
	lock := &lockEntry{refCount: 1}
	s.locks[lockKey] = lock
	return lock
}

// releaseLocks releases all locks for the given lock keys (account:asset combinations)
func (s *DefaultKeySetLocker) releaseLocks(lockKeys []string) {
	// Release locks in reverse order (best practice for nested locks)
	for i := len(lockKeys) - 1; i >= 0; i-- {
		lockKey := lockKeys[i]
		s.locksMutex.RLock()
		lock, ok := s.locks[lockKey]
		s.locksMutex.RUnlock()

		if ok && lock != nil {
			lock.mutex.Unlock()
			s.decrementLockRef(lockKey)
		}
	}
}

func (s *DefaultKeySetLocker) decrementLockRef(lockKey string) {
	s.locksMutex.Lock()
	defer s.locksMutex.Unlock()
	lock, ok := s.locks[lockKey]
	if !ok {
		return
	}
	lock.refCount--
	if lock.refCount <= 0 {
		delete(s.locks, lockKey)
	}
}
