package store

import (
	"sync"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// StoreInterceptor wraps a Store and allows intercepting method calls.
type StoreInterceptor struct {
	delegate *Store
	mu       sync.RWMutex

	// LogReader interceptors (global logs by sequence)
	OnGetAllLogs       func(delegate *Store, from, to uint64) (Cursor[*commonpb.Log], error)
	OnGetLogBySequence func(delegate *Store, sequence uint64) (*commonpb.Log, error)

	// Store interceptors
	OnListLedgers                  func(delegate *Store) (Cursor[*commonpb.LedgerInfo], error)
	OnGetBalanceDiffs              func(delegate *Store, ledgerName string, query BalanceDiffsQuery) (BalanceDiffsResult, error)
	OnGetBalanceBase               func(delegate *Store, ledgerName string, account, asset string, maxRaftIndex uint64) (*StoredBalanceBase, error)
	OnGetAccountMetadata           func(delegate *Store, ledgerName string, accounts []string) (map[string]metadata.Metadata, error)
	OnGetAccountVolumes            func(delegate *Store, ledgerName string, account string) (map[string]*commonpb.VolumesWithBalance, error)
	OnGetSequenceForIdempotencyKey func(delegate *Store, idempotencyKey string) (uint64, error)
	OnGetSequenceForTransactionID  func(delegate *Store, ledgerName string, transactionID uint64) (uint64, error)
	OnIsTransactionReverted        func(delegate *Store, ledgerName string, transactionID uint64) (bool, error)
	OnNewBatch                     func(delegate *Store) *Batch
	OnCreateSnapshot               func(delegate *Store) error
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

func (s *StoreInterceptor) GetAllLogs(from, to uint64) (Cursor[*commonpb.Log], error) {
	s.mu.RLock()
	interceptor := s.OnGetAllLogs
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, from, to)
	}
	return s.delegate.GetAllLogs(from, to)
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

func (s *StoreInterceptor) GetBalanceDiffs(ledgerName string, query BalanceDiffsQuery) (BalanceDiffsResult, error) {
	s.mu.RLock()
	interceptor := s.OnGetBalanceDiffs
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerName, query)
	}
	return s.delegate.GetBalanceDiffs(ledgerName, query)
}

func (s *StoreInterceptor) GetBalanceBase(ledgerName string, account, asset string, maxRaftIndex uint64) (*StoredBalanceBase, error) {
	s.mu.RLock()
	interceptor := s.OnGetBalanceBase
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerName, account, asset, maxRaftIndex)
	}
	return s.delegate.GetBalanceBase(ledgerName, account, asset, maxRaftIndex)
}

func (s *StoreInterceptor) GetAccountMetadata(ledgerName string, accounts []string) (map[string]metadata.Metadata, error) {
	s.mu.RLock()
	interceptor := s.OnGetAccountMetadata
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerName, accounts)
	}
	return s.delegate.GetAccountMetadata(ledgerName, accounts)
}

func (s *StoreInterceptor) GetAccountVolumes(ledgerName string, account string) (map[string]*commonpb.VolumesWithBalance, error) {
	s.mu.RLock()
	interceptor := s.OnGetAccountVolumes
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerName, account)
	}
	return s.delegate.GetAccountVolumes(ledgerName, account)
}

func (s *StoreInterceptor) GetSequenceForIdempotencyKey(idempotencyKey string) (uint64, error) {
	s.mu.RLock()
	interceptor := s.OnGetSequenceForIdempotencyKey
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, idempotencyKey)
	}
	return s.delegate.GetSequenceForIdempotencyKey(idempotencyKey)
}

func (s *StoreInterceptor) GetSequenceForTransactionID(ledgerName string, transactionID uint64) (uint64, error) {
	s.mu.RLock()
	interceptor := s.OnGetSequenceForTransactionID
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerName, transactionID)
	}
	return s.delegate.GetSequenceForTransactionID(ledgerName, transactionID)
}

func (s *StoreInterceptor) IsTransactionReverted(ledgerName string, transactionID uint64) (bool, error) {
	s.mu.RLock()
	interceptor := s.OnIsTransactionReverted
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerName, transactionID)
	}
	return s.delegate.IsTransactionReverted(ledgerName, transactionID)
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

func (s *StoreInterceptor) CreateSnapshot() error {
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

func (s *StoreInterceptor) SetGetAllLogsInterceptor(fn func(delegate *Store, from, to uint64) (Cursor[*commonpb.Log], error)) {
	s.mu.Lock()
	s.OnGetAllLogs = fn
	s.mu.Unlock()
}

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

func (s *StoreInterceptor) SetGetBalanceDiffsInterceptor(fn func(delegate *Store, ledgerName string, query BalanceDiffsQuery) (BalanceDiffsResult, error)) {
	s.mu.Lock()
	s.OnGetBalanceDiffs = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetNewBatchInterceptor(fn func(delegate *Store) *Batch) {
	s.mu.Lock()
	s.OnNewBatch = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetCreateSnapshotInterceptor(fn func(delegate *Store) error) {
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
	s.OnGetAllLogs = nil
	s.OnGetLogBySequence = nil
	s.OnListLedgers = nil
	s.OnGetBalanceDiffs = nil
	s.OnGetBalanceBase = nil
	s.OnGetAccountMetadata = nil
	s.OnGetAccountVolumes = nil
	s.OnGetSequenceForIdempotencyKey = nil
	s.OnGetSequenceForTransactionID = nil
	s.OnIsTransactionReverted = nil
	s.OnNewBatch = nil
	s.OnCreateSnapshot = nil
	s.OnGetLastAppliedIndex = nil
	s.OnGetLastSequence = nil
	s.OnGetLedgerByName = nil
	s.OnClose = nil
	s.mu.Unlock()
}
