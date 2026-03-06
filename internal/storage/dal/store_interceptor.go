package dal

import (
	"sync"
)

// StoreInterceptor wraps a Store and allows intercepting method calls.
type StoreInterceptor struct {
	delegate *Store
	mu       sync.RWMutex

	OnNewBatch       func(delegate *Store) *Batch
	OnCreateSnapshot func(delegate *Store) (uint64, error)
	OnClose          func(delegate *Store) error
}

// NewStoreInterceptor creates a new StoreInterceptor wrapping the given Store.
func NewStoreInterceptor(delegate *Store) *StoreInterceptor {
	return &StoreInterceptor{
		delegate: delegate,
	}
}

func (s *StoreInterceptor) Delegate() *Store {
	return s.delegate
}

func (s *StoreInterceptor) NewBatch() *Batch {
	s.mu.RLock()
	interceptor := s.OnNewBatch
	s.mu.RUnlock()

	if interceptor != nil {
		return interceptor(s.delegate)
	}

	return s.delegate.NewBatch()
}

func (s *StoreInterceptor) CreateSnapshot() (uint64, error) {
	s.mu.RLock()
	interceptor := s.OnCreateSnapshot
	s.mu.RUnlock()

	if interceptor != nil {
		return interceptor(s.delegate)
	}

	return s.delegate.CreateSnapshot()
}

func (s *StoreInterceptor) Close() error {
	s.mu.RLock()
	interceptor := s.OnClose
	s.mu.RUnlock()

	if interceptor != nil {
		return interceptor(s.delegate)
	}

	return s.delegate.Close()
}

// Setter methods for Store interceptors

func (s *StoreInterceptor) SetNewBatchInterceptor(fn func(delegate *Store) *Batch) {
	s.mu.Lock()
	s.OnNewBatch = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetCreateSnapshotInterceptor(fn func(delegate *Store) (uint64, error)) {
	s.mu.Lock()
	s.OnCreateSnapshot = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetCloseInterceptor(fn func(delegate *Store) error) {
	s.mu.Lock()
	s.OnClose = fn
	s.mu.Unlock()
}

// ClearInterceptors removes all interceptors.
func (s *StoreInterceptor) ClearInterceptors() {
	s.mu.Lock()
	s.OnNewBatch = nil
	s.OnCreateSnapshot = nil
	s.OnClose = nil
	s.mu.Unlock()
}
