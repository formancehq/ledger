package v2

import "context"

// Source fetches v2 log entries from a ledger source.
//
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source source.go -destination source_generated.go -package v2 . Source
type Source interface {
	FetchLogs(ctx context.Context, afterID uint64, pageSize int) ([]V2Log, bool, error)
	GetLatestLogID(ctx context.Context) (uint64, error)
	Close() error
}
