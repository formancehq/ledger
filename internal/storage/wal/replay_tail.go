package wal

import (
	"errors"
	"fmt"

	"go.etcd.io/raft/v3/raftpb"
)

// errStaleTailIntersectsCommit reports a replayed WAL suffix that is provably
// not part of the live Raft log but starts at or below the durable commit
// index. Dropping it would discard a committed position, so startup fails
// closed instead.
var errStaleTailIntersectsCommit = errors.New("WAL replay produced an invalid entry suffix at or below the durable commit index")

// staleReplayTail is the result of validating the entries returned by etcd's
// wal.ReadAll against the replay snapshot.
type staleReplayTail struct {
	// kept is the prefix of the replayed entries that may belong to the live log.
	kept []*raftpb.Entry
	// firstDiscarded is the first entry removed, or nil when nothing was removed.
	firstDiscarded *raftpb.Entry
	// discarded is the number of entries removed.
	discarded int
}

// discardStaleReplayTail removes a replayed suffix that cannot belong to the
// live Raft log.
//
// etcd's wal.ReadAll truncates its result only for entry records whose index
// is above the replay snapshot. A record at or below the snapshot index — for
// example a conflict overwrite at index t written before a later snapshot at
// S >= t — does not drop entries collected from older records. An uncommitted
// tail above S that a later leader had already truncated in memory is
// therefore resurrected on restart when no newer record above S was written.
// The resurrected tail can report a stale last term to the vote check and let
// a candidate missing committed entries win an election.
//
// A live Raft log never has an entry term below the snapshot term or below the
// term of the entry before it. The first entry violating either rule, and every
// entry after it, is discarded. Discarding is only safe above the durable commit
// index: a violation at or below it returns errStaleTailIntersectsCommit.
//
// This check is a safety net. It does not detect every resurrected tail: a
// stale tail whose terms are not lower than the snapshot term and its
// predecessor passes unchanged.
func discardStaleReplayTail(snapshotTerm, commit uint64, entries []*raftpb.Entry) (staleReplayTail, error) {
	previousTerm := snapshotTerm
	for i, entry := range entries {
		if entry.GetTerm() >= previousTerm {
			previousTerm = entry.GetTerm()

			continue
		}

		if entry.GetIndex() <= commit {
			return staleReplayTail{}, fmt.Errorf(
				"%w: entry index %d term %d follows term %d (snapshot term %d, commit %d)",
				errStaleTailIntersectsCommit,
				entry.GetIndex(), entry.GetTerm(), previousTerm, snapshotTerm, commit,
			)
		}

		return staleReplayTail{
			kept:           entries[:i:i],
			firstDiscarded: entry,
			discarded:      len(entries) - i,
		}, nil
	}

	return staleReplayTail{kept: entries}, nil
}
