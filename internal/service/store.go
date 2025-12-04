package service

import (
	"context"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// Cursor provides a way to iterate over a stream of items
type Cursor[T any] interface {
	// Next returns the next item in the cursor
	// Returns io.EOF when there are no more items
	Next(ctx context.Context) (T, error)
	// Close closes the cursor and releases any resources
	Close() error
}

// LogWriter handles log writing operations
type LogWriter interface {
	InsertLogs(ctx context.Context, logs ...ledger.Log) error
}

// LogReader handles log reading operations
type LogReader interface {
	GetLogWithIdempotencyKey(ctx context.Context, idempotencyKey string) (*ledger.Log, error)
	GetLastLog(ctx context.Context) (*ledger.Log, error)
	GetAllLogs(ctx context.Context) (*Cursor[ledger.Log], error)
}

// LogStore embeds both LogWriter and LogReader for backward compatibility
type LogStore interface {
	LogWriter
	LogReader
}

// VolumesStore handles balance/volume queries
type VolumesStore interface {
	GetBalance(ctx context.Context, balanceQuery map[string][]string) (ledger.Balances, error)
}

// Store embeds LogWriter, LogReader and VolumesStore
type Store interface {
	LogWriter
	LogReader
	VolumesStore
}
