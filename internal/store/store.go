package store

import (
	"context"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// LogWriter handles log writing operations
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated.go -package store . LogWriter
type LogWriter interface {
	InsertLogs(ctx context.Context, logs ...*ledgerpb.Log) error
}

// LogReader handles log reading operations
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated.go -package store . LogReader
type LogReader interface {
	GetAllLogs(ctx context.Context, ledger string, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) // from: optional log ID to start from (0 = from beginning), to: optional log ID to stop at (0 = until end, inclusive)
	GetLogByID(ctx context.Context, ledger string, id uint64) (*ledgerpb.Log, error)
}

// LogStore combines LogWriter and LogReader
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated.go -package store . LogStore
type LogStore interface {
	LogWriter
	LogReader
}

// LogReaderFn is a functional type that implements LogReader
type LogReaderFn func(ctx context.Context, ledger string, from uint64, to uint64) (Cursor[*ledgerpb.Log], error)

func (fn LogReaderFn) GetAllLogs(ctx context.Context, ledger string, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) {
	return fn(ctx, ledger, from, to)
}

func (fn LogReaderFn) GetLogByID(ctx context.Context, ledger string, id uint64) (*ledgerpb.Log, error) {
	if id == 0 {
		return nil, nil
	}
	cursor, err := fn(ctx, ledger, id-1, id)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = cursor.Close()
	}()
	return cursor.Next(ctx)
}

// RuntimeUpdate contains all the updates to apply to the runtime store
type RuntimeUpdate struct {
	BalanceDiffs           map[string]map[string]map[string]*big.Int
	AccountMetadata        map[string]map[string]map[string]string
	AccountMetadataDeletes map[string]map[string][]string
	TransactionIDs         map[string]map[uint64]uint64
	RevertedTransactionIDs map[string]map[uint64]bool
}

// Runtime handles runtime queries and provides log access.
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated.go -package store . Runtime
type Store interface {
	LogStore
	GetBalances(ctx context.Context, ledger string, balanceQuery map[string][]string) (ledgerpb.Balances, error)
	GetAccountMetadata(ctx context.Context, ledger string, accounts []string) (map[string]metadata.Metadata, error)
	// GetLogForIdempotencyKey retrieves the idempotency hash and the id of a log for its idempotency key
	GetLogIDForIdempotencyKey(ctx context.Context, ledger string, idempotencyKey string) (uint64, error)
	// GetLogIDForTransactionID retrieves the log ID for a given transaction ID
	GetLogIDForTransactionID(ctx context.Context, ledger string, transactionID uint64) (uint64, error)
	// IsTransactionReverted checks if a transaction has been reverted
	IsTransactionReverted(ctx context.Context, ledger string, transactionID uint64) (bool, error)
	Close(ctx context.Context) error
	CreateSnapshot(ctx context.Context) error
}
