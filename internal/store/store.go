package store

import (
	"context"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

type LogStreamer interface {
	// GetAllLogs returns a cursor over all logs in the given ledger
	// from: optional log ID to start from (0 = from beginning)
	// to: optional log ID to stop at (0 = until end, inclusive)
	GetAllLogs(ctx context.Context, ledger uint32, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) // from: optional log ID to start from (0 = from beginning), to: optional log ID to stop at (0 = until end, inclusive)
}

// LogReader handles log reading operations
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated.go -package store . LogReader
type LogReader interface {
	LogStreamer
	GetLogByID(ctx context.Context, ledger uint32, id uint64) (*ledgerpb.Log, error)
}

// Batch allows atomic operations on the store.
// All operations are buffered until Commit is called.
// Cancel must be called if the batch is not committed to release resources.
type Batch interface {
	// RegisterLedger ledger register a new ledger in the store
	RegisterLedger(ctx context.Context, info *ledgerpb.LedgerInfo) error
	// DeleteLedger deletes all data for a ledger
	DeleteLedger(ctx context.Context, id uint32) error
	// AppendLogs appends logs to the store
	AppendLogs(ctx context.Context, logs ...*ledgerpb.Log) error
	// AppendBalanceDiff appends a balance diff for an account/asset pair
	AppendBalanceDiff(ctx context.Context, ledger uint32, account, asset string, diff *ledgerpb.BigInt, logID uint64) error
	// SaveAccountMetadata saves metadata for an account
	SaveAccountMetadata(ctx context.Context, ledger uint32, account string, metadata *ledgerpb.Metadata) error
	// DeleteAccountMetadata deletes metadata keys for an account
	DeleteAccountMetadata(ctx context.Context, ledger uint32, account string, keys []string) error
	// StoreTransactionID stores the log ID associated to a transaction ID
	StoreTransactionID(ctx context.Context, ledger uint32, transactionID uint64, logID uint64) error
	// StoreRevertedTransactionID stores the log ID associated to a transaction ID that has been reverted
	StoreRevertedTransactionID(ctx context.Context, ledger uint32, transactionID uint64, logID uint64) error
	// Cancel cancels the batch and releases resources
	Cancel(ctx context.Context) error
	// Commit commits all buffered operations atomically
	Commit(ctx context.Context) error
}

// Store handles runtime queries and provides log access.
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated.go -package store . Store
type Store interface {
	LogReader
	// ListLedgers lists all ledgers
	ListLedgers(ctx context.Context) ([]*ledgerpb.LedgerInfo, error)
	GetBalances(ctx context.Context, ledgerID uint32, balanceQuery map[string][]string) (ledgerpb.Balances, error)
	GetAccountMetadata(ctx context.Context, ledgerID uint32, accounts []string) (map[string]metadata.Metadata, error)
	// GetLogForIdempotencyKey retrieves the idempotency hash and the id of a log for its idempotency key
	GetLogIDForIdempotencyKey(ctx context.Context, ledgerID uint32, idempotencyKey string) (uint64, error)
	// GetLogIDForTransactionID retrieves the log ID for a given transaction ID
	GetLogIDForTransactionID(ctx context.Context, ledgerID uint32, transactionID uint64) (uint64, error)
	// IsTransactionReverted checks if a transaction has been reverted
	IsTransactionReverted(ctx context.Context, ledgerID uint32, transactionID uint64) (bool, error)
	// NewBatch creates a new batch for atomic operations.
	// lastAppliedIndex is the raft index that will be stored when the batch is committed.
	NewBatch(lastAppliedIndex uint64) Batch
	CreateSnapshot(ctx context.Context) error
	GetLastAppliedIndex() (uint64, error)
	GetLastLogID(ctx context.Context, ledgerID uint32) (uint64, error)
	GetLedgerByName(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error)
	Close(ctx context.Context) error
}
