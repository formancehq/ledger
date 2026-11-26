package node

import (
	"fmt"
	"math"
	"slices"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

// followerRetention holds back leader WAL compaction for followers that are
// still catching up through log replication (EN-3429).
//
// Raft snapshots carry no data in this system: a follower that receives
// MsgSnap downloads a full Pebble checkpoint out of band, which can take tens
// of minutes, and then replays the Raft log from the checkpoint index. While
// it downloads it keeps appending the leader's entries. If the leader compacts
// past the follower's position during that window, the follower receives a
// new MsgSnap and the download starts over. CompactionMargin alone sizes the
// retained log in entries, not in time, so under sustained writes it can be
// shorter than one download.
//
// The leader therefore keeps every entry after the lowest position a follower
// still needs, among followers whose position or replication state changed
// within idleTimeout and whose position is still in the leader's log. Two
// bounds keep a slow or dead follower from growing the WAL without limit:
//   - a follower whose progress has not changed for idleTimeout no longer
//     counts;
//   - the encoded entries retained beyond the normal compaction target stay
//     within maxBytes, plus at most the WAL's boundary entry. The WAL keeps
//     retained entries in memory, so this bounds memory as well as disk.
//
// The hold is node-local, leader-only policy outside the FSM apply path: it
// only decides how much log the leader keeps, never how an entry applies.
type followerRetention struct {
	maxBytes    uint64
	idleTimeout time.Duration

	// positions is owned by the orchestrate goroutine.
	positions map[uint64]followerPosition

	// needs is published by the orchestrate goroutine and read by
	// maintenance: the ascending positions of the followers that hold, or
	// nil when none does.
	needs atomic.Pointer[[]uint64]
}

type followerPosition struct {
	state      tracker.StateType
	need       uint64
	advancedAt time.Time
}

// followerProgress is a follower's position as the leader tracks it: the
// index of the last entry the follower is known or about to have, and the
// replication state that index comes from. The leader sends the follower the
// entries after that index.
type followerProgress struct {
	id    uint64
	state tracker.StateType
	need  uint64
}

// followerNeed returns the last entry index the leader expects pr's follower
// to hold:
//   - StateReplicate: Match, the follower acknowledged it.
//   - StateProbe: Next-1, the entry the leader probes from. It can be
//     speculative: a new leader probes every follower from its own log tail.
//     After a snapshot is reported sent, raft probes from the snapshot index
//     while Match is still the follower's pre-snapshot position, which the
//     leader's log no longer holds. A probe the follower rejects lowers Next,
//     and a position below the leader's log no longer holds.
//   - StateSnapshot: the snapshot index being sent; the follower resumes
//     after it.
func followerNeed(pr tracker.Progress) uint64 {
	switch pr.State {
	case tracker.StateSnapshot:
		return max(pr.Match, pr.PendingSnapshot)
	case tracker.StateProbe:
		return max(pr.Match, pr.Next-1)
	default:
		return pr.Match
	}
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
	// capped is true when maxBytes stopped the hold above the follower's
	// position.
	capped bool
	// need is the follower position the hold was sized for.
	need          uint64
	retainedBytes uint64
}

// newFollowerRetention returns nil when maxBytes is zero: the hold is off.
func newFollowerRetention(maxBytes uint64, idleTimeout time.Duration) *followerRetention {
	if maxBytes == 0 {
		return nil
	}

	return &followerRetention{
		maxBytes:    maxBytes,
		idleTimeout: idleTimeout,
		positions:   make(map[uint64]followerPosition),
	}
}

// observe records the followers' positions and publishes the ones that hold.
// Call it from the orchestrate goroutine only. followers must exclude the
// local node.
func (r *followerRetention) observe(now time.Time, isLeader bool, followers []followerProgress) {
	if !isLeader {
		clear(r.positions)
		r.needs.Store(nil)

		return
	}

	for id := range r.positions {
		if !slices.ContainsFunc(followers, func(f followerProgress) bool { return f.id == id }) {
			delete(r.positions, id)
		}
	}

	var needs []uint64
	for _, f := range followers {
		// Any change of position or of replication state is activity: a
		// speculative probe cursor can equal the snapshot index sent after
		// it, and only the state change shows the follower is reachable.
		prev, ok := r.positions[f.id]
		if !ok || f.need != prev.need || f.state != prev.state {
			prev = followerPosition{state: f.state, need: f.need, advancedAt: now}
			r.positions[f.id] = prev
		}

		if now.Sub(prev.advancedAt) > r.idleTimeout {
			continue
		}

		needs = append(needs, prev.need)
	}

	if len(needs) == 0 {
		r.needs.Store(nil)

		return
	}

	slices.Sort(needs)
	r.needs.Store(&needs)
}

// compactionTarget lowers target to keep the entries the followers still
// need, within maxBytes of entries beyond target.
func (r *followerRetention) compactionTarget(target uint64, walLog retentionEntries) (uint64, retentionDecision, error) {
	published := r.needs.Load()
	if published == nil || (*published)[0] >= target {
		return target, retentionDecision{}, nil
	}

	firstIndex, err := walLog.FirstIndex()
	if err != nil {
		return target, retentionDecision{}, fmt.Errorf("reading WAL first index: %w", err)
	}

	// A follower whose position is already compacted away will receive a
	// snapshot whatever this node keeps: it must not hold for the others.
	// The compacted index itself (firstIndex-1) is still a valid append
	// boundary.
	i, _ := slices.BinarySearch(*published, firstIndex-1)
	if i == len(*published) || (*published)[i] >= target {
		return target, retentionDecision{}, nil
	}
	need := (*published)[i]

	// Compacting at C keeps entries [C, last] in memory; the normal target T
	// keeps [T, last]. The hold therefore costs entries [C, T-1]. When C is
	// the current boundary firstIndex-1, raft.Storage cannot read that entry,
	// so it goes uncharged: the hold can exceed maxBytes by that one entry,
	// which the WAL holds already.
	lo := max(need, firstIndex)
	if lo >= target {
		return need, retentionDecision{held: true, need: need}, nil
	}

	entries, err := walLog.Entries(lo, target, math.MaxUint64)
	if err != nil {
		return target, retentionDecision{}, fmt.Errorf("reading retained entries [%d, %d): %w", lo, target, err)
	}

	var size uint64
	for j := len(entries) - 1; j >= 0; j-- {
		entrySize := uint64(proto.Size(entries[j]))
		if size+entrySize > r.maxBytes {
			compactIdx := entries[j].GetIndex() + 1

			return compactIdx, retentionDecision{
				held:          compactIdx < target,
				capped:        true,
				need:          need,
				retainedBytes: size,
			}, nil
		}
		size += entrySize
	}

	return need, retentionDecision{held: true, need: need, retainedBytes: size}, nil
}
