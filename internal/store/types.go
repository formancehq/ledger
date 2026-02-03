package store

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

type AccountKey struct {
	LedgerName string
	Account    string
}

type TimestampedAccountKey struct {
	AccountKey
	RaftIndex uint64
}

type TimestampedBalanceKey struct {
	TimestampedAccountKey
	Asset string
}

type TimestampedMetadataKey struct {
	TimestampedAccountKey
	Key string
}

// StoredBalanceDiff represents a balance change retrieved from storage (for reading).
type StoredBalanceDiff struct {
	RaftIndex uint64
	Diff      *commonpb.BigInt
}

// StoredBalanceBase represents a balance base retrieved from storage.
type StoredBalanceBase struct {
	RaftIndex uint64
	Balance   *commonpb.BigInt
}

// StoredMetadataDiff represents a metadata entry retrieved from storage (for reading).
type StoredMetadataDiff struct {
	RaftIndex uint64
	Value     *commonpb.MetadataValue // nil means deleted
}

// StoredMetadataBase represents a metadata base retrieved from storage.
type StoredMetadataBase struct {
	RaftIndex uint64
	Value     *commonpb.MetadataValue // nil means deleted
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
