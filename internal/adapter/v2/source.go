package v2

import "context"

// Source fetches v2 log entries from a ledger source.
type Source interface {
	FetchLogs(ctx context.Context, afterID uint64, pageSize int) ([]V2Log, bool, error)
	GetLatestLogID(ctx context.Context) (uint64, error)
	Close() error
}
