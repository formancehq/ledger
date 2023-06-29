package query

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
)

type Store interface {
	IsInitialized() bool
	GetNextLogID(ctx context.Context) (uint64, error)
	ReadLogsRange(ctx context.Context, idMin, idMax uint64) ([]core.ChainedLog, error)
	GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error)
	GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error)
	UpdateAccountsMetadata(ctx context.Context, update ...core.Account) error
	InsertTransactions(ctx context.Context, insert ...core.Transaction) error
	InsertMoves(ctx context.Context, insert ...*core.Move) error
	UpdateTransactionsMetadata(ctx context.Context, update ...core.TransactionWithMetadata) error
	MarkedLogsAsProjected(ctx context.Context, id uint64) error
}
