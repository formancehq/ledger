package store

import (
	"sync"

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
