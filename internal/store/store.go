package store

import (
	"context"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

type LogStreamer interface {
	// GetAllLogs returns a cursor over all logs (global logs by sequence)
	// from: optional sequence to start from (0 = from beginning)
	// to: optional sequence to stop at (0 = until end, inclusive)
	GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*commonpb.Log], error)
}

// LogReader handles log reading operations (global logs by sequence)
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated.go -package store . LogReader
type LogReader interface {
	LogStreamer
	GetLogBySequence(ctx context.Context, sequence uint64) (*commonpb.Log, error)
}

type LedgerLogStreamer interface {
	// GetAllLedgerLogs returns a cursor over all ledger logs in the given ledger
	// from: optional log ID to start from (0 = from beginning)
	// to: optional log ID to stop at (0 = until end, inclusive)
	GetAllLedgerLogs(ctx context.Context, ledger uint32, from uint64, to uint64) (Cursor[*commonpb.LedgerLog], error)
}

// LedgerLogReader handles ledger log reading operations (for service layer compatibility)
type LedgerLogReader interface {
	LedgerLogStreamer
}

// Batch allows atomic operations on the store.
// All operations are buffered until Commit is called.
// Cancel must be called if the batch is not committed to release resources.
type Batch interface {
	// AppendLogs appends logs to the store
	AppendLogs(ctx context.Context, logs ...*commonpb.Log) error
	// RegisterLedger registers a new ledger in the store
	RegisterLedger(ctx context.Context, info *commonpb.LedgerInfo) error
	// DeleteLedger deletes all data for a ledger
	DeleteLedger(ctx context.Context, id uint32) error
	// AppendBalanceDiff appends a balance diff for an account/asset pair
	AppendBalanceDiff(ctx context.Context, ledger uint32, account, asset string, diff *commonpb.BigInt, sequence uint64) error
	// SaveAccountMetadata saves metadata for an account
	SaveAccountMetadata(ctx context.Context, ledger uint32, account string, metadata *commonpb.Metadata) error
	// DeleteAccountMetadata deletes metadata keys for an account
	DeleteAccountMetadata(ctx context.Context, ledger uint32, account string, keys []string) error
	// StoreTransactionID stores the sequence associated to a transaction ID
	StoreTransactionID(ctx context.Context, ledger uint32, transactionID uint64, sequence uint64) error
	// StoreRevertedTransactionID stores the sequence associated to a transaction ID that has been reverted
	StoreRevertedTransactionID(ctx context.Context, ledger uint32, transactionID uint64, sequence uint64) error
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
	LedgerLogReader
	// ListLedgers lists all ledgers
	ListLedgers(ctx context.Context) ([]*commonpb.LedgerInfo, error)
	GetBalances(ctx context.Context, ledgerID uint32, balanceQuery map[string][]string) (commonpb.Balances, error)
	GetAccountMetadata(ctx context.Context, ledgerID uint32, accounts []string) (map[string]metadata.Metadata, error)
	// GetSequenceForIdempotencyKey retrieves the sequence of a log for its idempotency key (global)
	GetSequenceForIdempotencyKey(ctx context.Context, idempotencyKey string) (uint64, error)
	// GetSequenceForTransactionID retrieves the sequence for a given transaction ID
	GetSequenceForTransactionID(ctx context.Context, ledgerID uint32, transactionID uint64) (uint64, error)
	// IsTransactionReverted checks if a transaction has been reverted
	IsTransactionReverted(ctx context.Context, ledgerID uint32, transactionID uint64) (bool, error)
	// NewBatch creates a new batch for atomic operations.
	// lastAppliedIndex is the raft index that will be stored when the batch is committed.
	NewBatch(lastAppliedIndex uint64) Batch
	CreateSnapshot(ctx context.Context) error
	GetLastAppliedIndex() (uint64, error)
	// GetLastSequence returns the last sequence number for logs
	GetLastSequence(ctx context.Context) (uint64, error)
	GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error)
	Close(ctx context.Context) error
}
