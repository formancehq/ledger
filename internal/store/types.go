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

// MetadataDiff represents a metadata change for an account (for writing).
// Each entry stores a single key-value pair with its associated raft index.
// If Value is nil, it represents a deletion of the key.
type MetadataDiff struct {
	LedgerID  uint32
	Account   string
	Key       string
	Value     *string // nil means deletion
	RaftIndex uint64
}

// StoredMetadataDiff represents a metadata entry retrieved from storage (for reading).
type StoredMetadataDiff struct {
	RaftIndex uint64
	Value     *string // nil means deleted
}

type LogStreamer interface {
	// GetAllLogs returns a cursor over all logs (global logs by sequence)
	// from: optional sequence to start from (0 = from beginning)
	// to: optional sequence to stop at (0 = until end, inclusive)
	GetAllLogs(from uint64, to uint64) (Cursor[*commonpb.Log], error)
}

// LogReader handles log reading operations (global logs by sequence)
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source types.go -destination store_generated.go -package store . LogReader
type LogReader interface {
	LogStreamer
	GetLogBySequence(sequence uint64) (*commonpb.Log, error)
}

// BalanceDiffsQuery is a query for balance diffs: map[account][]asset
type BalanceDiffsQuery = map[string][]string

// BalanceDiffsResult is the result of GetBalanceDiffs: map[account][asset][]StoredBalanceDiff
type BalanceDiffsResult = map[string]map[string][]StoredBalanceDiff
