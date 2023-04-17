package command

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
)

type Store interface {
	GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error)
	AppendLog(ctx context.Context, log *core.Log) error
	ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.Log, error)
	ReadLogWithReference(ctx context.Context, reference string) (*core.Log, error)
}

type alwaysEmptyStore struct{}

func (e alwaysEmptyStore) GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
	return nil, storage.ErrNotFound
}

func (e alwaysEmptyStore) AppendLog(ctx context.Context, log *core.Log) error {
	return nil
}

func (e alwaysEmptyStore) ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.Log, error) {
	return nil, storage.ErrNotFound
}

func (e alwaysEmptyStore) ReadLogWithReference(ctx context.Context, reference string) (*core.Log, error) {
	return nil, storage.ErrNotFound
}

var _ Store = (*alwaysEmptyStore)(nil)

var AlwaysEmptyStore = &alwaysEmptyStore{}
