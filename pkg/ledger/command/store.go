package command

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/errors"
)

type Store interface {
	AppendLog(ctx context.Context, log *core.ActiveLog) (*core.LogPersistenceTracker, error)
	ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.PersistedLog, error)
	ReadLogForCreatedTransactionWithReference(ctx context.Context, reference string) (*core.PersistedLog, error)
	ReadLogForCreatedTransaction(ctx context.Context, txID uint64) (*core.PersistedLog, error)
	ReadLogForRevertedTransaction(ctx context.Context, txID uint64) (*core.PersistedLog, error)
	ReadLogWithIdempotencyKey(ctx context.Context, key string) (*core.PersistedLog, error)
}

type alwaysEmptyStore struct{}

func (e alwaysEmptyStore) ReadLogWithIdempotencyKey(ctx context.Context, key string) (*core.PersistedLog, error) {
	return nil, errors.ErrNotFound
}

func (e alwaysEmptyStore) ReadLogForCreatedTransaction(ctx context.Context, txID uint64) (*core.PersistedLog, error) {
	return nil, errors.ErrNotFound
}

func (e alwaysEmptyStore) ReadLogForRevertedTransaction(ctx context.Context, txID uint64) (*core.PersistedLog, error) {
	return nil, errors.ErrNotFound
}

func (e alwaysEmptyStore) AppendLog(ctx context.Context, log *core.ActiveLog) (*core.LogPersistenceTracker, error) {
	ret := core.NewLogPersistenceTracker()
	ret.Resolve(log.ComputePersistentLog(nil))
	return ret, nil
}

func (e alwaysEmptyStore) ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.PersistedLog, error) {
	return nil, errors.ErrNotFound
}

func (e alwaysEmptyStore) ReadLogForCreatedTransactionWithReference(ctx context.Context, reference string) (*core.PersistedLog, error) {
	return nil, errors.ErrNotFound
}

var _ Store = (*alwaysEmptyStore)(nil)

var AlwaysEmptyStore = &alwaysEmptyStore{}
