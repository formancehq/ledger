package service

import (
	"context"
	"fmt"
	"sort"
	"sync"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// DefaultLockedVolumesStore is a default implementation of LockedVolumesStore
// that wraps a VolumesStore and provides locking for concurrent access
type DefaultLockedVolumesStore struct {
	volumesStore VolumesStore
	// locks is a map of mutexes keyed by "account:asset" combination
	locks map[string]*sync.Mutex
	// locksMutex protects the locks map itself
	locksMutex sync.RWMutex
}

// makeLockKey creates a composite key for account and asset
func makeLockKey(account, asset string) string {
	return fmt.Sprintf("%s:%s", account, asset)
}

// NewDefaultLockedVolumesStore creates a new DefaultLockedVolumesStore
func NewDefaultLockedVolumesStore(volumesStore VolumesStore) *DefaultLockedVolumesStore {
	return &DefaultLockedVolumesStore{
		volumesStore: volumesStore,
		locks:        make(map[string]*sync.Mutex),
	}
}

// LockBalances locks the requested account:asset combinations and returns balances with a release function
func (s *DefaultLockedVolumesStore) LockBalances(ctx context.Context, balanceQuery map[string][]string) (ledger.Balances, func(), error) {
	// Extract all account:asset combinations from the query
	lockKeys := make([]string, 0)
	for account, assets := range balanceQuery {
		for _, asset := range assets {
			lockKey := makeLockKey(account, asset)
			lockKeys = append(lockKeys, lockKey)
		}
	}

	// Sort lock keys to avoid deadlocks (always lock in the same order)
	sort.Strings(lockKeys)

	// Lock all requested account:asset combinations
	lockedKeys := make([]string, 0, len(lockKeys))
	for _, lockKey := range lockKeys {
		lock := s.getOrCreateLock(lockKey)
		lock.Lock()
		lockedKeys = append(lockedKeys, lockKey)
	}

	// Get balances from the underlying store
	balances, err := s.volumesStore.GetBalance(ctx, balanceQuery)
	if err != nil {
		// Release all locks on error
		s.releaseLocks(lockedKeys)
		// Return empty balances, a no-op release function, and the error
		return ledger.Balances{}, func() {}, err
	}

	// Return balances and a release function
	release := func() {
		s.releaseLocks(lockedKeys)
	}

	return balances, release, nil
}

// getOrCreateLock gets or creates a mutex for the given lock key (account:asset)
func (s *DefaultLockedVolumesStore) getOrCreateLock(lockKey string) *sync.Mutex {
	// First, try to read without locking
	s.locksMutex.RLock()
	if lock, ok := s.locks[lockKey]; ok {
		s.locksMutex.RUnlock()
		return lock
	}
	s.locksMutex.RUnlock()

	// Need to create a new lock, acquire write lock
	s.locksMutex.Lock()
	defer s.locksMutex.Unlock()

	// Double-check after acquiring write lock (another goroutine might have created it)
	if lock, ok := s.locks[lockKey]; ok {
		return lock
	}

	// Create new lock
	lock := &sync.Mutex{}
	s.locks[lockKey] = lock
	return lock
}

// releaseLocks releases all locks for the given lock keys (account:asset combinations)
func (s *DefaultLockedVolumesStore) releaseLocks(lockKeys []string) {
	// Release locks in reverse order (best practice for nested locks)
	for i := len(lockKeys) - 1; i >= 0; i-- {
		lockKey := lockKeys[i]
		s.locksMutex.RLock()
		lock, ok := s.locks[lockKey]
		s.locksMutex.RUnlock()

		if ok && lock != nil {
			lock.Unlock()
		}
	}
}
