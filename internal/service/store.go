package service

import (
	"context"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

type Store interface {
	InsertLogs(ctx context.Context, logs ...ledger.Log) error
	GetBalance(ctx context.Context, balanceQuery map[string][]string) (ledger.Balances, error)
	GetLogWithIdempotencyKey(ctx context.Context, idempotencyKey string) (*ledger.Log, error)
	GetLastLog(ctx context.Context) (*ledger.Log, error)
}
