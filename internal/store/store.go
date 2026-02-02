package store

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// MetricsProvider is an optional interface for stores that provide metrics.
type MetricsProvider interface {
	// GetMetrics returns store-specific metrics as proto message.
	GetMetrics() any
}

// BalanceDiff represents a balance change for an account/asset pair (for writing).
type BalanceDiff struct {
	LedgerID  uint32
	Account   string
	Asset     string
	Diff      *commonpb.BigInt
	RaftIndex uint64
}

// StoredBalanceDiff represents a balance change retrieved from storage (for reading).
type StoredBalanceDiff struct {
	RaftIndex uint64
	Diff      *commonpb.BigInt
}

// BalanceBase represents a compacted balance snapshot for an account/asset pair.
// It stores the cumulative balance at a specific Raft index, allowing efficient
// balance computation by summing the base with subsequent diffs.
type BalanceBase struct {
	LedgerID  uint32
	Account   string
	Asset     string
	Balance   *commonpb.BigInt
	RaftIndex uint64
}

// StoredBalanceBase represents a balance base retrieved from storage.
type StoredBalanceBase struct {
	RaftIndex uint64
	Balance   *commonpb.BigInt
}

type LogStreamer interface {
	// GetAllLogs returns a cursor over all logs (global logs by sequence)
	// from: optional sequence to start from (0 = from beginning)
	// to: optional sequence to stop at (0 = until end, inclusive)
	GetAllLogs(from uint64, to uint64) (Cursor[*commonpb.Log], error)
}

// LogReader handles log reading operations (global logs by sequence)
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated.go -package store . LogReader,Batch
type LogReader interface {
	LogStreamer
	GetLogBySequence(sequence uint64) (*commonpb.Log, error)
}

// Batch allows atomic operations on the store.
// All operations are buffered until Commit is called.
// Cancel must be called if the batch is not committed to release resources.
type Batch interface {
	// AppendLogs appends logs to the store
	AppendLogs(logs ...*commonpb.Log) error
	// SaveLedger saves or updates a ledger in the store
	SaveLedger(info *commonpb.LedgerInfo) error
	// DeleteLedger deletes all data for a ledger
	DeleteLedger(id uint32) error
	// AppendBalanceDiff appends a balance diff for an account/asset pair
	AppendBalanceDiff(diff BalanceDiff) error
	// SetBalanceBase stores a balance base (compacted snapshot) for an account/asset pair
	SetBalanceBase(base BalanceBase) error
	// SaveAccountMetadata saves metadata for an account
	SaveAccountMetadata(ledger uint32, account string, metadata *commonpb.Metadata) error
	// DeleteAccountMetadata deletes metadata keys for an account
	DeleteAccountMetadata(ledger uint32, account string, keys []string) error
	// StoreTransactionID stores the sequence associated to a transaction ID
	StoreTransactionID(ledger uint32, transactionID uint64, sequence uint64) error
	// StoreRevertedTransactionID stores the sequence associated to a transaction ID that has been reverted
	StoreRevertedTransactionID(ledger uint32, transactionID uint64, sequence uint64) error
	// Cancel cancels the batch and releases resources
	Cancel() error
	// Commit commits all buffered operations atomically
	Commit() error
}

// BalanceDiffsQuery is a query for balance diffs: map[account][]asset
type BalanceDiffsQuery = map[string][]string

// BalanceDiffsResult is the result of GetBalanceDiffs: map[account][asset][]StoredBalanceDiff
type BalanceDiffsResult = map[string]map[string][]StoredBalanceDiff

