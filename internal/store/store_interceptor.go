package store

import (
	"sync"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// BatchInterceptor wraps a Batch and allows intercepting method calls.
type BatchInterceptor struct {
	delegate Batch
	mu       sync.RWMutex

	OnAppendLogs                 func(delegate Batch, logs []*commonpb.Log) error
	OnSaveLedger                 func(delegate Batch, info *commonpb.LedgerInfo) error
	OnDeleteLedger               func(delegate Batch, id uint32) error
	OnAppendBalanceDiff          func(delegate Batch, diff BalanceDiff) error
	OnSetBalanceBase             func(delegate Batch, base BalanceBase) error
	OnSaveAccountMetadata        func(delegate Batch, ledger uint32, account string, metadata *commonpb.Metadata) error
	OnDeleteAccountMetadata      func(delegate Batch, ledger uint32, account string, keys []string) error
	OnStoreTransactionID         func(delegate Batch, ledger uint32, transactionID uint64, sequence uint64) error
	OnStoreRevertedTransactionID func(delegate Batch, ledger uint32, transactionID uint64, sequence uint64) error
	OnCancel                     func(delegate Batch) error
	OnCommit                     func(delegate Batch) error
}

// NewBatchInterceptor creates a new BatchInterceptor wrapping the given Batch.
func NewBatchInterceptor(delegate Batch) *BatchInterceptor {
	return &BatchInterceptor{
		delegate: delegate,
	}
}

func (b *BatchInterceptor) Delegate() Batch {
	return b.delegate
}

func (b *BatchInterceptor) AppendLogs(logs ...*commonpb.Log) error {
	b.mu.RLock()
	interceptor := b.OnAppendLogs
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate, logs)
	}
	return b.delegate.AppendLogs(logs...)
}

func (b *BatchInterceptor) SaveLedger(info *commonpb.LedgerInfo) error {
	b.mu.RLock()
	interceptor := b.OnSaveLedger
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate, info)
	}
	return b.delegate.SaveLedger(info)
}

func (b *BatchInterceptor) DeleteLedger(id uint32) error {
	b.mu.RLock()
	interceptor := b.OnDeleteLedger
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate, id)
	}
	return b.delegate.DeleteLedger(id)
}

func (b *BatchInterceptor) AppendBalanceDiff(diff BalanceDiff) error {
	b.mu.RLock()
	interceptor := b.OnAppendBalanceDiff
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate, diff)
	}
	return b.delegate.AppendBalanceDiff(diff)
}

func (b *BatchInterceptor) SetBalanceBase(base BalanceBase) error {
	b.mu.RLock()
	interceptor := b.OnSetBalanceBase
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate, base)
	}
	return b.delegate.SetBalanceBase(base)
}

func (b *BatchInterceptor) SaveAccountMetadata(ledger uint32, account string, md *commonpb.Metadata) error {
	b.mu.RLock()
	interceptor := b.OnSaveAccountMetadata
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate, ledger, account, md)
	}
	return b.delegate.SaveAccountMetadata(ledger, account, md)
}

func (b *BatchInterceptor) DeleteAccountMetadata(ledger uint32, account string, keys []string) error {
	b.mu.RLock()
	interceptor := b.OnDeleteAccountMetadata
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate, ledger, account, keys)
	}
	return b.delegate.DeleteAccountMetadata(ledger, account, keys)
}

func (b *BatchInterceptor) StoreTransactionID(ledger uint32, transactionID uint64, sequence uint64) error {
	b.mu.RLock()
	interceptor := b.OnStoreTransactionID
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate, ledger, transactionID, sequence)
	}
	return b.delegate.StoreTransactionID(ledger, transactionID, sequence)
}

func (b *BatchInterceptor) StoreRevertedTransactionID(ledger uint32, transactionID uint64, sequence uint64) error {
	b.mu.RLock()
	interceptor := b.OnStoreRevertedTransactionID
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate, ledger, transactionID, sequence)
	}
	return b.delegate.StoreRevertedTransactionID(ledger, transactionID, sequence)
}

func (b *BatchInterceptor) Cancel() error {
	b.mu.RLock()
	interceptor := b.OnCancel
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate)
	}
	return b.delegate.Cancel()
}

func (b *BatchInterceptor) Commit() error {
	b.mu.RLock()
	interceptor := b.OnCommit
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(b.delegate)
	}
	return b.delegate.Commit()
}

func (b *BatchInterceptor) ClearInterceptors() {
	b.mu.Lock()
	b.OnAppendLogs = nil
	b.OnSaveLedger = nil
	b.OnDeleteLedger = nil
	b.OnAppendBalanceDiff = nil
	b.OnSetBalanceBase = nil
	b.OnSaveAccountMetadata = nil
	b.OnDeleteAccountMetadata = nil
	b.OnStoreTransactionID = nil
	b.OnStoreRevertedTransactionID = nil
	b.OnCancel = nil
	b.OnCommit = nil
	b.mu.Unlock()
}

var _ Batch = (*BatchInterceptor)(nil)

// StoreInterceptor wraps a Store and allows intercepting method calls.
type StoreInterceptor struct {
	delegate Store
	mu       sync.RWMutex

	// LogReader interceptors (global logs by sequence)
	OnGetAllLogs       func(delegate Store, from, to uint64) (Cursor[*commonpb.Log], error)
	OnGetLogBySequence func(delegate Store, sequence uint64) (*commonpb.Log, error)

	// Store interceptors
	OnListLedgers                  func(delegate Store) (Cursor[*commonpb.LedgerInfo], error)
	OnGetBalanceDiffs              func(delegate Store, ledgerID uint32, query BalanceDiffsQuery) (BalanceDiffsResult, error)
	OnGetBalanceBase               func(delegate Store, ledgerID uint32, account, asset string, maxRaftIndex uint64) (*StoredBalanceBase, error)
	OnGetAccountMetadata           func(delegate Store, ledgerID uint32, accounts []string) (map[string]metadata.Metadata, error)
	OnGetAccountVolumes            func(delegate Store, ledgerID uint32, account string) (map[string]*commonpb.VolumesWithBalance, error)
	OnGetSequenceForIdempotencyKey func(delegate Store, idempotencyKey string) (uint64, error)
	OnGetSequenceForTransactionID  func(delegate Store, ledgerID uint32, transactionID uint64) (uint64, error)
	OnIsTransactionReverted        func(delegate Store, ledgerID uint32, transactionID uint64) (bool, error)
	OnNewBatch                     func(delegate Store, lastAppliedIndex uint64) Batch
	OnCreateSnapshot               func(delegate Store) error
	OnGetLastAppliedIndex          func(delegate Store) (uint64, error)
	OnGetLastSequence              func(delegate Store) (uint64, error)
	OnGetLedgerByName              func(delegate Store, name string) (*commonpb.LedgerInfo, error)
	OnClose                        func(delegate Store) error

	// BatchInterceptorFactory allows creating intercepted batches
	BatchInterceptorFactory func(batch Batch) Batch
}

// NewStoreInterceptor creates a new StoreInterceptor wrapping the given Store.
func NewStoreInterceptor(delegate Store) *StoreInterceptor {
	return &StoreInterceptor{
		delegate: delegate,
	}
}

func (s *StoreInterceptor) Delegate() Store {
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

func (s *StoreInterceptor) GetBalanceDiffs(ledgerID uint32, query BalanceDiffsQuery) (BalanceDiffsResult, error) {
	s.mu.RLock()
	interceptor := s.OnGetBalanceDiffs
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerID, query)
	}
	return s.delegate.GetBalanceDiffs(ledgerID, query)
}

func (s *StoreInterceptor) GetBalanceBase(ledgerID uint32, account, asset string, maxRaftIndex uint64) (*StoredBalanceBase, error) {
	s.mu.RLock()
	interceptor := s.OnGetBalanceBase
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerID, account, asset, maxRaftIndex)
	}
	return s.delegate.GetBalanceBase(ledgerID, account, asset, maxRaftIndex)
}

func (s *StoreInterceptor) GetAccountMetadata(ledgerID uint32, accounts []string) (map[string]metadata.Metadata, error) {
	s.mu.RLock()
	interceptor := s.OnGetAccountMetadata
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerID, accounts)
	}
	return s.delegate.GetAccountMetadata(ledgerID, accounts)
}

func (s *StoreInterceptor) GetAccountVolumes(ledgerID uint32, account string) (map[string]*commonpb.VolumesWithBalance, error) {
	s.mu.RLock()
	interceptor := s.OnGetAccountVolumes
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerID, account)
	}
	return s.delegate.GetAccountVolumes(ledgerID, account)
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

func (s *StoreInterceptor) GetSequenceForTransactionID(ledgerID uint32, transactionID uint64) (uint64, error) {
	s.mu.RLock()
	interceptor := s.OnGetSequenceForTransactionID
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerID, transactionID)
	}
	return s.delegate.GetSequenceForTransactionID(ledgerID, transactionID)
}

func (s *StoreInterceptor) IsTransactionReverted(ledgerID uint32, transactionID uint64) (bool, error) {
	s.mu.RLock()
	interceptor := s.OnIsTransactionReverted
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(s.delegate, ledgerID, transactionID)
	}
	return s.delegate.IsTransactionReverted(ledgerID, transactionID)
}

func (s *StoreInterceptor) NewBatch(lastAppliedIndex uint64) Batch {
	s.mu.RLock()
	interceptor := s.OnNewBatch
	batchFactory := s.BatchInterceptorFactory
	s.mu.RUnlock()

	var batch Batch
	if interceptor != nil {
		batch = interceptor(s.delegate, lastAppliedIndex)
	} else {
		batch = s.delegate.NewBatch(lastAppliedIndex)
	}

	// Apply batch interceptor factory if set
	if batchFactory != nil {
		batch = batchFactory(batch)
	}

	return batch
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

func (s *StoreInterceptor) SetGetAllLogsInterceptor(fn func(delegate Store, from, to uint64) (Cursor[*commonpb.Log], error)) {
	s.mu.Lock()
	s.OnGetAllLogs = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetGetLogBySequenceInterceptor(fn func(delegate Store, sequence uint64) (*commonpb.Log, error)) {
	s.mu.Lock()
	s.OnGetLogBySequence = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetListLedgersInterceptor(fn func(delegate Store) (Cursor[*commonpb.LedgerInfo], error)) {
	s.mu.Lock()
	s.OnListLedgers = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetGetBalanceDiffsInterceptor(fn func(delegate Store, ledgerID uint32, query BalanceDiffsQuery) (BalanceDiffsResult, error)) {
	s.mu.Lock()
	s.OnGetBalanceDiffs = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetNewBatchInterceptor(fn func(delegate Store, lastAppliedIndex uint64) Batch) {
	s.mu.Lock()
	s.OnNewBatch = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetBatchInterceptorFactory(fn func(batch Batch) Batch) {
	s.mu.Lock()
	s.BatchInterceptorFactory = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetCreateSnapshotInterceptor(fn func(delegate Store) error) {
	s.mu.Lock()
	s.OnCreateSnapshot = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetGetLastAppliedIndexInterceptor(fn func(delegate Store) (uint64, error)) {
	s.mu.Lock()
	s.OnGetLastAppliedIndex = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetCloseInterceptor(fn func(delegate Store) error) {
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
	s.BatchInterceptorFactory = nil
	s.mu.Unlock()
}

var _ Store = (*StoreInterceptor)(nil)
