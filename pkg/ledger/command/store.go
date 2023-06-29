package command

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/vm"
)

type Store interface {
	vm.Store
	AppendLog(ctx context.Context, log *core.ActiveLog) (*core.LogPersistenceTracker, error)
	ReadLastLogWithType(ctx context.Context, logType ...core.LogType) (*core.ChainedLog, error)
	ReadLogForCreatedTransactionWithReference(ctx context.Context, reference string) (*core.ChainedLog, error)
	ReadLogForCreatedTransaction(ctx context.Context, txID uint64) (*core.ChainedLog, error)
	ReadLogForRevertedTransaction(ctx context.Context, txID uint64) (*core.ChainedLog, error)
	ReadLogWithIdempotencyKey(ctx context.Context, key string) (*core.ChainedLog, error)
	GetLastLog(ctx context.Context) (*core.ChainedLog, error)
}
