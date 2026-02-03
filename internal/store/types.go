package store

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

type AccountKey struct {
	LedgerID  uint32
	Account   string
	RaftIndex uint64
}

type BalanceKey struct {
	AccountKey
	Asset string
}

type MetadataKey struct {
	AccountKey
	Key string
}

// BalanceDiff represents a balance change for an account/asset pair (for writing).
type BalanceDiff struct {
	Diff *commonpb.BigInt
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
	Balance *commonpb.BigInt
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
	Value *commonpb.MetadataValue // nil means deletion
}

// StoredMetadataDiff represents a metadata entry retrieved from storage (for reading).
type StoredMetadataDiff struct {
	RaftIndex uint64
	Value     *commonpb.MetadataValue // nil means deleted
}

// MetadataBase represents a compacted metadata snapshot for an account/key pair.
// It stores the metadata value at a specific Raft index, allowing efficient
// metadata computation by applying the base and subsequent diffs.
type MetadataBase struct {
	Value *commonpb.MetadataValue // nil means the key was deleted at this base
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
