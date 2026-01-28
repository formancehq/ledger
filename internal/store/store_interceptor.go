package store

import (
	"context"
	"sync"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// BatchInterceptor wraps a Batch and allows intercepting method calls.
type BatchInterceptor struct {
	delegate Batch
	mu       sync.RWMutex

	OnRegisterLedger             func(ctx context.Context, delegate Batch, info *ledgerpb.LedgerInfo) error
	OnDeleteLedger               func(ctx context.Context, delegate Batch, id uint32) error
	OnAppendLogs                 func(ctx context.Context, delegate Batch, logs []*ledgerpb.Log) error
	OnAppendBalanceDiff          func(ctx context.Context, delegate Batch, ledger uint32, account, asset string, diff *ledgerpb.BigInt, logID uint64) error
	OnSaveAccountMetadata        func(ctx context.Context, delegate Batch, ledger uint32, account string, metadata *ledgerpb.Metadata) error
	OnDeleteAccountMetadata      func(ctx context.Context, delegate Batch, ledger uint32, account string, keys []string) error
	OnStoreTransactionID         func(ctx context.Context, delegate Batch, ledger uint32, transactionID uint64, logID uint64) error
	OnStoreRevertedTransactionID func(ctx context.Context, delegate Batch, ledger uint32, transactionID uint64, logID uint64) error
	OnCancel                     func(ctx context.Context, delegate Batch) error
	OnCommit                     func(ctx context.Context, delegate Batch) error
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

func (b *BatchInterceptor) RegisterLedger(ctx context.Context, info *ledgerpb.LedgerInfo) error {
	b.mu.RLock()
	interceptor := b.OnRegisterLedger
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, b.delegate, info)
	}
	return b.delegate.RegisterLedger(ctx, info)
}

func (b *BatchInterceptor) DeleteLedger(ctx context.Context, id uint32) error {
	b.mu.RLock()
	interceptor := b.OnDeleteLedger
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, b.delegate, id)
	}
	return b.delegate.DeleteLedger(ctx, id)
}

func (b *BatchInterceptor) AppendLogs(ctx context.Context, logs ...*ledgerpb.Log) error {
	b.mu.RLock()
	interceptor := b.OnAppendLogs
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, b.delegate, logs)
	}
	return b.delegate.AppendLogs(ctx, logs...)
}

func (b *BatchInterceptor) AppendBalanceDiff(ctx context.Context, ledger uint32, account, asset string, diff *ledgerpb.BigInt, logID uint64) error {
	b.mu.RLock()
	interceptor := b.OnAppendBalanceDiff
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, b.delegate, ledger, account, asset, diff, logID)
	}
	return b.delegate.AppendBalanceDiff(ctx, ledger, account, asset, diff, logID)
}

func (b *BatchInterceptor) SaveAccountMetadata(ctx context.Context, ledger uint32, account string, md *ledgerpb.Metadata) error {
	b.mu.RLock()
	interceptor := b.OnSaveAccountMetadata
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, b.delegate, ledger, account, md)
	}
	return b.delegate.SaveAccountMetadata(ctx, ledger, account, md)
}

func (b *BatchInterceptor) DeleteAccountMetadata(ctx context.Context, ledger uint32, account string, keys []string) error {
	b.mu.RLock()
	interceptor := b.OnDeleteAccountMetadata
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, b.delegate, ledger, account, keys)
	}
	return b.delegate.DeleteAccountMetadata(ctx, ledger, account, keys)
}

func (b *BatchInterceptor) StoreTransactionID(ctx context.Context, ledger uint32, transactionID uint64, logID uint64) error {
	b.mu.RLock()
	interceptor := b.OnStoreTransactionID
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, b.delegate, ledger, transactionID, logID)
	}
	return b.delegate.StoreTransactionID(ctx, ledger, transactionID, logID)
}

func (b *BatchInterceptor) StoreRevertedTransactionID(ctx context.Context, ledger uint32, transactionID uint64, logID uint64) error {
	b.mu.RLock()
	interceptor := b.OnStoreRevertedTransactionID
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, b.delegate, ledger, transactionID, logID)
	}
	return b.delegate.StoreRevertedTransactionID(ctx, ledger, transactionID, logID)
}

func (b *BatchInterceptor) Cancel(ctx context.Context) error {
	b.mu.RLock()
	interceptor := b.OnCancel
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, b.delegate)
	}
	return b.delegate.Cancel(ctx)
}

func (b *BatchInterceptor) Commit(ctx context.Context) error {
	b.mu.RLock()
	interceptor := b.OnCommit
	b.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, b.delegate)
	}
	return b.delegate.Commit(ctx)
}

func (b *BatchInterceptor) ClearInterceptors() {
	b.mu.Lock()
	b.OnRegisterLedger = nil
	b.OnDeleteLedger = nil
	b.OnAppendLogs = nil
	b.OnAppendBalanceDiff = nil
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

	// LogReader interceptors
	OnGetAllLogs func(ctx context.Context, delegate Store, ledger uint32, from, to uint64) (Cursor[*ledgerpb.Log], error)
	OnGetLogByID func(ctx context.Context, delegate Store, ledger uint32, id uint64) (*ledgerpb.Log, error)

	// Store interceptors
	OnListLedgers               func(ctx context.Context, delegate Store) ([]*ledgerpb.LedgerInfo, error)
	OnGetBalances               func(ctx context.Context, delegate Store, ledgerID uint32, balanceQuery map[string][]string) (ledgerpb.Balances, error)
	OnGetAccountMetadata        func(ctx context.Context, delegate Store, ledgerID uint32, accounts []string) (map[string]metadata.Metadata, error)
	OnGetLogIDForIdempotencyKey func(ctx context.Context, delegate Store, ledgerID uint32, idempotencyKey string) (uint64, error)
	OnGetLogIDForTransactionID  func(ctx context.Context, delegate Store, ledgerID uint32, transactionID uint64) (uint64, error)
	OnIsTransactionReverted     func(ctx context.Context, delegate Store, ledgerID uint32, transactionID uint64) (bool, error)
	OnNewBatch                  func(delegate Store, lastAppliedIndex uint64) Batch
	OnCreateSnapshot            func(ctx context.Context, delegate Store) error
	OnGetLastAppliedIndex       func(delegate Store) (uint64, error)
	OnGetLastLogID              func(ctx context.Context, delegate Store, ledgerID uint32) (uint64, error)
	OnGetLedgerByName           func(ctx context.Context, delegate Store, name string) (*ledgerpb.LedgerInfo, error)
	OnClose                     func(ctx context.Context, delegate Store) error

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

func (s *StoreInterceptor) GetAllLogs(ctx context.Context, ledger uint32, from, to uint64) (Cursor[*ledgerpb.Log], error) {
	s.mu.RLock()
	interceptor := s.OnGetAllLogs
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate, ledger, from, to)
	}
	return s.delegate.GetAllLogs(ctx, ledger, from, to)
}

func (s *StoreInterceptor) GetLogByID(ctx context.Context, ledger uint32, id uint64) (*ledgerpb.Log, error) {
	s.mu.RLock()
	interceptor := s.OnGetLogByID
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate, ledger, id)
	}
	return s.delegate.GetLogByID(ctx, ledger, id)
}

func (s *StoreInterceptor) ListLedgers(ctx context.Context) ([]*ledgerpb.LedgerInfo, error) {
	s.mu.RLock()
	interceptor := s.OnListLedgers
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate)
	}
	return s.delegate.ListLedgers(ctx)
}

func (s *StoreInterceptor) GetBalances(ctx context.Context, ledgerID uint32, balanceQuery map[string][]string) (ledgerpb.Balances, error) {
	s.mu.RLock()
	interceptor := s.OnGetBalances
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate, ledgerID, balanceQuery)
	}
	return s.delegate.GetBalances(ctx, ledgerID, balanceQuery)
}

func (s *StoreInterceptor) GetAccountMetadata(ctx context.Context, ledgerID uint32, accounts []string) (map[string]metadata.Metadata, error) {
	s.mu.RLock()
	interceptor := s.OnGetAccountMetadata
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate, ledgerID, accounts)
	}
	return s.delegate.GetAccountMetadata(ctx, ledgerID, accounts)
}

func (s *StoreInterceptor) GetLogIDForIdempotencyKey(ctx context.Context, ledgerID uint32, idempotencyKey string) (uint64, error) {
	s.mu.RLock()
	interceptor := s.OnGetLogIDForIdempotencyKey
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate, ledgerID, idempotencyKey)
	}
	return s.delegate.GetLogIDForIdempotencyKey(ctx, ledgerID, idempotencyKey)
}

func (s *StoreInterceptor) GetLogIDForTransactionID(ctx context.Context, ledgerID uint32, transactionID uint64) (uint64, error) {
	s.mu.RLock()
	interceptor := s.OnGetLogIDForTransactionID
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate, ledgerID, transactionID)
	}
	return s.delegate.GetLogIDForTransactionID(ctx, ledgerID, transactionID)
}

func (s *StoreInterceptor) IsTransactionReverted(ctx context.Context, ledgerID uint32, transactionID uint64) (bool, error) {
	s.mu.RLock()
	interceptor := s.OnIsTransactionReverted
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate, ledgerID, transactionID)
	}
	return s.delegate.IsTransactionReverted(ctx, ledgerID, transactionID)
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

func (s *StoreInterceptor) CreateSnapshot(ctx context.Context) error {
	s.mu.RLock()
	interceptor := s.OnCreateSnapshot
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate)
	}
	return s.delegate.CreateSnapshot(ctx)
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

func (s *StoreInterceptor) GetLastLogID(ctx context.Context, ledgerID uint32) (uint64, error) {
	s.mu.RLock()
	interceptor := s.OnGetLastLogID
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate, ledgerID)
	}
	return s.delegate.GetLastLogID(ctx, ledgerID)
}

func (s *StoreInterceptor) GetLedgerByName(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	s.mu.RLock()
	interceptor := s.OnGetLedgerByName
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate, name)
	}
	return s.delegate.GetLedgerByName(ctx, name)
}

func (s *StoreInterceptor) Close(ctx context.Context) error {
	s.mu.RLock()
	interceptor := s.OnClose
	s.mu.RUnlock()
	if interceptor != nil {
		return interceptor(ctx, s.delegate)
	}
	return s.delegate.Close(ctx)
}

// Setter methods for Store interceptors

func (s *StoreInterceptor) SetGetAllLogsInterceptor(fn func(ctx context.Context, delegate Store, ledger uint32, from, to uint64) (Cursor[*ledgerpb.Log], error)) {
	s.mu.Lock()
	s.OnGetAllLogs = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetGetLogByIDInterceptor(fn func(ctx context.Context, delegate Store, ledger uint32, id uint64) (*ledgerpb.Log, error)) {
	s.mu.Lock()
	s.OnGetLogByID = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetListLedgersInterceptor(fn func(ctx context.Context, delegate Store) ([]*ledgerpb.LedgerInfo, error)) {
	s.mu.Lock()
	s.OnListLedgers = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetGetBalancesInterceptor(fn func(ctx context.Context, delegate Store, ledgerID uint32, balanceQuery map[string][]string) (ledgerpb.Balances, error)) {
	s.mu.Lock()
	s.OnGetBalances = fn
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

func (s *StoreInterceptor) SetCreateSnapshotInterceptor(fn func(ctx context.Context, delegate Store) error) {
	s.mu.Lock()
	s.OnCreateSnapshot = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetGetLastAppliedIndexInterceptor(fn func(delegate Store) (uint64, error)) {
	s.mu.Lock()
	s.OnGetLastAppliedIndex = fn
	s.mu.Unlock()
}

func (s *StoreInterceptor) SetCloseInterceptor(fn func(ctx context.Context, delegate Store) error) {
	s.mu.Lock()
	s.OnClose = fn
	s.mu.Unlock()
}

// ClearInterceptors removes all interceptors.
func (s *StoreInterceptor) ClearInterceptors() {
	s.mu.Lock()
	s.OnGetAllLogs = nil
	s.OnGetLogByID = nil
	s.OnListLedgers = nil
	s.OnGetBalances = nil
	s.OnGetAccountMetadata = nil
	s.OnGetLogIDForIdempotencyKey = nil
	s.OnGetLogIDForTransactionID = nil
	s.OnIsTransactionReverted = nil
	s.OnNewBatch = nil
	s.OnCreateSnapshot = nil
	s.OnGetLastAppliedIndex = nil
	s.OnGetLastLogID = nil
	s.OnGetLedgerByName = nil
	s.OnClose = nil
	s.BatchInterceptorFactory = nil
	s.mu.Unlock()
}

var _ Store = (*StoreInterceptor)(nil)
