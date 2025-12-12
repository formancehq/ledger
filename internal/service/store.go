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
	GetLastSequenceID(ctx context.Context) (uint64, error)
}

// LogReader handles log reading operations
type LogReader interface {
	GetAllLogs(ctx context.Context, from uint64) (*Cursor[ledger.Log], error) // from: optional sequence number to start from (0 = from beginning)
}

type LogReaderFn func(ctx context.Context, from uint64) (*Cursor[ledger.Log], error)

func (fn LogReaderFn) GetAllLogs(ctx context.Context, from uint64) (*Cursor[ledger.Log], error) {
	return fn(ctx, from)
}

func NewLogReaderFn(fn LogReaderFn) LogReader {
	return fn
}

// LogStore embeds both LogWriter and LogReader, plus additional methods
type LogStore interface {
	// todo: relax ?
	BalancesStore
	LogWriter
	LogReader
	GetLogWithIdempotencyKey(ctx context.Context, ledgerName string, idempotencyKey string) (*ledger.Log, error)
	GetLastLog(ctx context.Context, ledgerName string) (*ledger.Log, error)
}

// Store embeds LogWriter and LogReader
type Store interface {
	LogWriter
	LogReader
}

// BalancesStore handles balance/volume queries
type BalancesStore interface {
	GetBalances(ctx context.Context, ledgerName string, balanceQuery map[string][]string) (ledger.Balances, error)
}
