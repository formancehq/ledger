package command

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
)

type Store interface {
	AppendLog(ctx context.Context, log *core.Log) error
	ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.Log, error)
	ReadLogForCreatedTransactionWithReference(ctx context.Context, reference string) (*core.Log, error)
	ReadLogForCreatedTransaction(ctx context.Context, txID uint64) (*core.Log, error)
	ReadLogForRevertedTransaction(ctx context.Context, txID uint64) (*core.Log, error)
}

type alwaysEmptyStore struct{}

func (e alwaysEmptyStore) ReadLogForCreatedTransaction(ctx context.Context, txID uint64) (*core.Log, error) {
	return nil, storage.ErrNotFound
}

func (e alwaysEmptyStore) ReadLogForRevertedTransaction(ctx context.Context, txID uint64) (*core.Log, error) {
	return nil, storage.ErrNotFound
}

func (e alwaysEmptyStore) AppendLog(ctx context.Context, log *core.Log) error {
	return nil
}

func (e alwaysEmptyStore) ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.Log, error) {
	return nil, storage.ErrNotFound
}

func (e alwaysEmptyStore) ReadLogForCreatedTransactionWithReference(ctx context.Context, reference string) (*core.Log, error) {
	return nil, storage.ErrNotFound
}

var _ Store = (*alwaysEmptyStore)(nil)

var AlwaysEmptyStore = &alwaysEmptyStore{}
