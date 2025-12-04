package service

import (
	"context"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// LogStore handles log storage and retrieval
type LogStore interface {
	InsertLogs(ctx context.Context, logs ...ledger.Log) error
	GetLogWithIdempotencyKey(ctx context.Context, idempotencyKey string) (*ledger.Log, error)
	GetLastLog(ctx context.Context) (*ledger.Log, error)
	GetAllLogs(ctx context.Context) ([]ledger.Log, error)
}

// VolumesStore handles balance/volume queries
type VolumesStore interface {
	GetBalance(ctx context.Context, balanceQuery map[string][]string) (ledger.Balances, error)
}

// Store embeds both LogStore and VolumesStore
type Store interface {
	LogStore
	VolumesStore
}
