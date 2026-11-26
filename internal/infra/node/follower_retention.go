package node

import (
	"fmt"
	"math"
	"slices"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// noRetentionFloor means no follower currently holds back WAL compaction.
const noRetentionFloor = math.MaxUint64

// followerRetention holds back leader WAL compaction for followers that are
// still catching up through log replication (EN-3429).
//
// Raft snapshots carry no data in this system: a follower that receives
// MsgSnap downloads a full Pebble checkpoint out of band, which can take tens
// of minutes, and then replays the Raft log from the checkpoint index. While
// it downloads it keeps appending the leader's entries. If the leader compacts
// past the follower's Match during that window, the follower receives a new
// MsgSnap and the download starts over. CompactionMargin alone sizes the
// retained log in entries, not in time, so under sustained writes it can be
// shorter than one download.
//
// The leader therefore keeps every entry after the lowest Match among
// followers whose Match advanced within idleTimeout. Two bounds keep a slow or
// dead follower from growing the WAL without limit:
//   - a follower whose Match stopped advancing for idleTimeout no longer
//     counts, and a follower in StateSnapshot never counts;
//   - the entries retained beyond the normal compaction target never exceed
//     maxBytes. The WAL keeps retained entries in memory, so this caps memory
//     as well as disk.
//
// The hold is node-local, leader-only policy outside the FSM apply path: it
// only decides how much log the leader keeps, never how an entry applies.
type followerRetention struct {
	maxBytes    uint64
	idleTimeout time.Duration

	// matches is owned by the orchestrate goroutine.
	matches map[uint64]followerMatch

	// floor is published by the orchestrate goroutine and read by
	// maintenance. noRetentionFloor means no hold.
	floor atomic.Uint64
}

type followerMatch struct {
	match      uint64
	advancedAt time.Time
}

// followerProgress is the part of a Raft tracker.Progress the hold needs.
type followerProgress struct {
	id           uint64
	match        uint64
	snapshotting bool
}

// retentionEntries is the WAL surface compactionTarget reads.
type retentionEntries interface {
	FirstIndex() (uint64, error)
	Entries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error)
}

// retentionDecision describes how compactionTarget changed a target.
type retentionDecision struct {
	// held is true when the returned target is below the requested one.
	held bool
	// capped is true when maxBytes stopped the hold above the floor.
	capped        bool
	floor         uint64
	retainedBytes uint64
}

// newFollowerRetention returns nil when maxBytes is zero: the hold is off.
func newFollowerRetention(maxBytes uint64, idleTimeout time.Duration) *followerRetention {
	if maxBytes == 0 {
		return nil
	}

	r := &followerRetention{
		maxBytes:    maxBytes,
		idleTimeout: idleTimeout,
		matches:     make(map[uint64]followerMatch),
	}
	r.floor.Store(noRetentionFloor)

	return r
}

// observe records the followers' progress and publishes the new floor. Call it
// from the orchestrate goroutine only. followers must exclude the local node.
func (r *followerRetention) observe(now time.Time, isLeader bool, followers []followerProgress) {
	if !isLeader {
		clear(r.matches)
		r.floor.Store(noRetentionFloor)

		return
	}

	for id := range r.matches {
		if !slices.ContainsFunc(followers, func(f followerProgress) bool { return f.id == id }) {
			delete(r.matches, id)
		}
	}

	floor := uint64(noRetentionFloor)
	for _, f := range followers {
		// The leader is already sending this follower a snapshot: its Match
		// is older than the leader's log and the hold cannot help it.
		if f.snapshotting {
			delete(r.matches, f.id)

			continue
		}

		prev, ok := r.matches[f.id]
		if !ok || f.match > prev.match {
			prev = followerMatch{match: f.match, advancedAt: now}
			r.matches[f.id] = prev
		}

		if now.Sub(prev.advancedAt) > r.idleTimeout {
			continue
		}

		floor = min(floor, prev.match)
	}

	r.floor.Store(floor)
}

// compactionTarget lowers target to keep the entries the followers still
// need, within maxBytes of entries beyond target.
func (r *followerRetention) compactionTarget(target uint64, walLog retentionEntries) (uint64, retentionDecision, error) {
	floor := r.floor.Load()
	if floor >= target {
		return target, retentionDecision{}, nil
	}

	firstIndex, err := walLog.FirstIndex()
	if err != nil {
		return target, retentionDecision{}, fmt.Errorf("reading WAL first index: %w", err)
	}

	// Entries up to floor are already gone: the follower will receive a
	// snapshot whatever this node keeps.
	if floor+1 < firstIndex {
		return target, retentionDecision{}, nil
	}

	entries, err := walLog.Entries(floor+1, target+1, math.MaxUint64)
	if err != nil {
		return target, retentionDecision{}, fmt.Errorf("reading retained entries [%d, %d]: %w", floor+1, target, err)
	}

	var size uint64
	for i := len(entries) - 1; i >= 0; i-- {
		entrySize := uint64(proto.Size(entries[i]))
		if size+entrySize > r.maxBytes {
			return entries[i].GetIndex(), retentionDecision{
				held:          entries[i].GetIndex() < target,
				capped:        true,
				floor:         floor,
				retainedBytes: size,
			}, nil
		}
		size += entrySize
	}

	return floor, retentionDecision{held: true, floor: floor, retainedBytes: size}, nil
}
