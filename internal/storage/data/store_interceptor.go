package data

import (
	"sync"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// StoreInterceptor wraps a Store and allows intercepting method calls.
type StoreInterceptor struct {
	delegate *Store
	mu       sync.RWMutex

	// LogReader interceptors (global logs by sequence)
	OnGetLogBySequence func(delegate *Store, sequence uint64) (*commonpb.Log, error)

	// Store interceptors
	OnListLedgers                  func(delegate *Store) (Cursor[*commonpb.LedgerInfo], error)
	OnNewBatch func(delegate *Store) *Batch
	OnCreateSnapshot               func(delegate *Store) (uint64, error)
	OnGetLastAppliedIndex          func(delegate *Store) (uint64, error)
	OnGetLastSequence              func(delegate *Store) (uint64, error)
	OnGetLedgerByName              func(delegate *Store, name string) (*commonpb.LedgerInfo, error)
	OnClose                        func(delegate *Store) error
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

func (s *StoreInterceptor) GetLogBySequence(sequence uint64) (*commonpb.Log, error) {
	s.mu.RLock()
	interceptor := s.OnGetLogBySequence
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, sequence)
	}
	return s.delegate.GetLogBySequence(sequence)
}

func (s *StoreInterceptor) ListLedgers() (Cursor[*commonpb.LedgerInfo], error) {
	s.mu.RLock()
	interceptor := s.OnListLedgers
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate)
	}
	return s.delegate.ListLedgers()
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

func (s *StoreInterceptor) GetLastAppliedIndex() (uint64, error) {
	s.mu.RLock()
	interceptor := s.OnGetLastAppliedIndex
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate)
	}
	return s.delegate.GetLastAppliedIndex()
}

func (s *StoreInterceptor) GetLastSequence() (uint64, error) {
	s.mu.RLock()
	interceptor := s.OnGetLastSequence
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate)
	}
	return s.delegate.GetLastSequence()
}

func (s *StoreInterceptor) GetLedgerByName(name string) (*commonpb.LedgerInfo, error) {
	s.mu.RLock()
	interceptor := s.OnGetLedgerByName
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, name)
	}
	return s.delegate.GetLedgerByName(name)
}

func (s *StoreInterceptor) GetLedgerByID(id uint32) (*commonpb.LedgerInfo, error) {
	return s.delegate.GetLedgerByID(id)
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

func (s *StoreInterceptor) SetGetLogBySequenceInterceptor(fn func(delegate *Store, sequence uint64) (*commonpb.Log, error)) {
	s.mu.Lock()
	s.OnGetLogBySequence = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetListLedgersInterceptor(fn func(delegate *Store) (Cursor[*commonpb.LedgerInfo], error)) {
	s.mu.Lock()
	s.OnListLedgers = fn
	s.mu.Unlock()
}

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

func (s *StoreInterceptor) SetGetLastAppliedIndexInterceptor(fn func(delegate *Store) (uint64, error)) {
	s.mu.Lock()
	s.OnGetLastAppliedIndex = fn
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
	s.OnGetLogBySequence = nil
	s.OnListLedgers = nil
	s.OnNewBatch = nil
	s.OnCreateSnapshot = nil
	s.OnGetLastAppliedIndex = nil
	s.OnGetLastSequence = nil
	s.OnGetLedgerByName = nil
	s.OnClose = nil
	s.mu.Unlock()
}
