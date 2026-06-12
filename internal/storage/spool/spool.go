package spool

import (
	"context"

	"go.etcd.io/raft/v3/raftpb"
)

type Position struct {
	SegID  uint64
	Offset int64
}

// PruneStats reports what Prune observed and removed during its scan.
type PruneStats struct {
	// SegmentsRemoved is the number of sealed, fully-applied segments deleted.
	SegmentsRemoved int
	// BytesRemoved is the total on-disk size of the removed segments.
	BytesRemoved int64
	// SegmentsRemaining is the number of segments still on disk after the
	// scan: sealed segments with maxIndex > lastApplied, the trailer-less
	// active segment, and any segment that could not be inspected or removed.
	SegmentsRemaining int
	// SegmentsUnreadable counts segments that could not be opened for trailer
	// inspection (included in SegmentsRemaining); their sealed/applied state
	// is unknown.
	SegmentsUnreadable int
	// SealedFullyAppliedRemaining counts segments known to be sealed
	// (trailer-bearing) and fully applied (maxIndex <= lastApplied) that
	// nevertheless survived the scan because removal failed. Always zero in
	// a healthy run.
	SealedFullyAppliedRemaining int
}

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source spool.go -destination spool_generated_test.go -typed -package spool . Spool
type Spool interface {
	AppendCommittedEntries(ctx context.Context, entries ...raftpb.Entry) error
	End() (*Position, error)
	ReplayUntil(
		ctx context.Context,
		end Position,
		lastApplied uint64,
		applyFn func(raftpb.Entry) error) error
	Prune(lastApplied uint64) (PruneStats, error)
	Reset() error
	Close() error
}
