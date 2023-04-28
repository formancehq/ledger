package command

import (
	"context"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/vm"
	storageerrors "github.com/formancehq/ledger/pkg/storage/errors"
)

type Store interface {
	vm.Store
	AppendLog(ctx context.Context, log *core.ActiveLog) (*core.LogPersistenceTracker, error)
	ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.PersistedLog, error)
	ReadLogForCreatedTransactionWithReference(ctx context.Context, reference string) (*core.PersistedLog, error)
	ReadLogForCreatedTransaction(ctx context.Context, txID uint64) (*core.PersistedLog, error)
	ReadLogForRevertedTransaction(ctx context.Context, txID uint64) (*core.PersistedLog, error)
	ReadLogWithIdempotencyKey(ctx context.Context, key string) (*core.PersistedLog, error)
}

type alwaysEmptyStore struct{}

func (e alwaysEmptyStore) GetBalanceFromLogs(ctx context.Context, address, asset string) (*big.Int, error) {
	return new(big.Int), nil
}

func (e alwaysEmptyStore) GetMetadataFromLogs(ctx context.Context, address, key string) (string, error) {
	return "", storageerrors.ErrNotFound
}

func (e alwaysEmptyStore) ReadLogWithIdempotencyKey(ctx context.Context, key string) (*core.PersistedLog, error) {
	return nil, storageerrors.ErrNotFound
}

func (e alwaysEmptyStore) ReadLogForCreatedTransaction(ctx context.Context, txID uint64) (*core.PersistedLog, error) {
	return nil, storageerrors.ErrNotFound
}

func (e alwaysEmptyStore) ReadLogForRevertedTransaction(ctx context.Context, txID uint64) (*core.PersistedLog, error) {
	return nil, storageerrors.ErrNotFound
}

func (e alwaysEmptyStore) AppendLog(ctx context.Context, log *core.ActiveLog) (*core.LogPersistenceTracker, error) {
	return core.NewResolvedLogPersistenceTracker(log, log.ComputePersistentLog(nil)), nil
}

func (e alwaysEmptyStore) ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.PersistedLog, error) {
	return nil, storageerrors.ErrNotFound
}

func (e alwaysEmptyStore) ReadLogForCreatedTransactionWithReference(ctx context.Context, reference string) (*core.PersistedLog, error) {
	return nil, storageerrors.ErrNotFound
}

var _ Store = (*alwaysEmptyStore)(nil)

var AlwaysEmptyStore = &alwaysEmptyStore{}

type cacheEntry[T any] struct {
	sync.Mutex
	value T
	inUse atomic.Int64
	ready chan struct{}
}

func getEntry[T any](i *inMemoryStore, key string, valuer func() (T, error)) *cacheEntry[T] {
	v, loaded := i.entries.LoadOrStore(key, &cacheEntry[T]{
		ready: make(chan struct{}),
	})
	entry := v.(*cacheEntry[T])
	if !loaded {
		var err error
		entry.value, err = valuer()
		if err != nil {
			panic(err)
		}
		close(entry.ready)

		return entry
	}
	<-entry.ready
	return entry
}

type inMemoryStore struct {
	Store
	entries sync.Map
}

func (i *inMemoryStore) getBalanceEntry(ctx context.Context, address, asset string) *cacheEntry[*big.Int] {
	return getEntry(i, "accounts/"+address+"/"+asset, func() (*big.Int, error) {
		return i.Store.GetBalanceFromLogs(ctx, address, asset)
	})
}

func (i *inMemoryStore) getMetadataEntry(ctx context.Context, address, key string) *cacheEntry[string] {
	return getEntry(i, "metadata/"+address+"/"+key, func() (string, error) {
		return i.Store.GetMetadataFromLogs(ctx, address, key)
	})
}

func (i *inMemoryStore) deleteBalanceEntry(address, asset string) {
	i.entries.Delete("accounts/" + address + "/" + asset)
}

func (i *inMemoryStore) deleteMetadataEntry(address, key string) {
	i.entries.Delete("metadata/" + address + "/" + key)
}

func (i *inMemoryStore) AppendLog(ctx context.Context, log *core.ActiveLog) (*core.LogPersistenceTracker, error) {
	switch payload := log.Data.(type) {
	case core.NewTransactionLogPayload:
		for _, posting := range payload.Transaction.Postings {
			entry := i.getBalanceEntry(ctx, posting.Source, posting.Asset)
			entry.Lock()
			entry.inUse.Add(1)
			entry.value.Add(entry.value, new(big.Int).Neg(posting.Amount))
			entry.Unlock()

			entry = i.getBalanceEntry(ctx, posting.Destination, posting.Asset)
			entry.Lock()
			entry.inUse.Add(1)
			entry.value.Add(entry.value, posting.Amount)
			entry.Unlock()
		}
		for address, metadata := range payload.AccountMetadata {
			for key, value := range metadata {
				entry := i.getMetadataEntry(ctx, address, key)
				entry.Lock()
				entry.inUse.Add(1)
				entry.value = value
				entry.Unlock()
			}
		}
	case core.SetMetadataLogPayload:
		if payload.TargetType == core.MetaTargetTypeAccount {
			for key, value := range payload.Metadata {
				entry := i.getMetadataEntry(ctx, payload.TargetID.(string), key)
				entry.Lock()
				entry.inUse.Add(1)
				entry.value = value
				entry.Unlock()
			}
		}
	}
	tracker, err := i.Store.AppendLog(ctx, log)
	if err != nil {
		return nil, err
	}
	go func() {
		<-tracker.Done()
		switch payload := log.Data.(type) {
		case core.NewTransactionLogPayload:
			for _, posting := range payload.Transaction.Postings {
				if i.getBalanceEntry(ctx, posting.Source, posting.Asset).inUse.Add(-1) == 0 {
					i.deleteBalanceEntry(posting.Source, posting.Asset)
				}
				if i.getBalanceEntry(ctx, posting.Destination, posting.Asset).inUse.Add(-1) == 0 {
					i.deleteBalanceEntry(posting.Destination, posting.Asset)
				}
			}
			for address, metadata := range payload.AccountMetadata {
				for key := range metadata {
					if i.getMetadataEntry(ctx, address, key).inUse.Add(-1) == 0 {
						i.deleteMetadataEntry(address, key)
					}
				}
			}
		case core.SetMetadataLogPayload:
			if payload.TargetType == core.MetaTargetTypeAccount {
				for key := range payload.Metadata {
					if i.getMetadataEntry(ctx, payload.TargetID.(string), key).inUse.Add(-1) == 0 {
						i.deleteMetadataEntry(payload.TargetID.(string), key)
					}
				}
			}
		}
	}()
	return tracker, nil
}

func (i *inMemoryStore) GetBalanceFromLogs(ctx context.Context, address, asset string) (*big.Int, error) {
	return i.getBalanceEntry(ctx, address, asset).value, nil
}

func (i *inMemoryStore) GetMetadataFromLogs(ctx context.Context, address, key string) (string, error) {
	return i.getMetadataEntry(ctx, address, key).value, nil
}

var _ Store = (*inMemoryStore)(nil)
