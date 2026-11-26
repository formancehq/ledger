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
// The leader therefore keeps the entries after a position each follower it
// heard from within idleTimeout can resume from. Positions come only from
// facts the leader has, never from raft's speculative probe cursor:
//   - Match, the last entry the follower acknowledged in this term;
//   - the index of a snapshot this leader sent the follower in this term and
//     that Match has not passed yet. The follower resumes after it once it
//     restores the checkpoint; the node reports every sent snapshot at once,
//     which leaves Match at its pre-snapshot value.
//
// Either can be the one the follower resumes from: a snapshot handed to the
// transport can be lost, and a Match the leader compacted past is useless.
// Both are published, and maintenance holds at the lowest one still in the
// leader's log. A follower heard from with neither (typically just after an
// election) has an unknown position: the leader keeps its whole retained log
// until the follower acknowledges.
//
// Two bounds keep a slow or dead follower from growing the WAL without limit:
//   - a follower the leader has not heard from for idleTimeout no longer
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

	// The fields below are owned by the orchestrate goroutine.
	//
	// lastHeard is when a message from each node was last stepped. Liveness
	// does not depend on the term, so it survives elections.
	lastHeard map[uint64]time.Time
	// term is the leader term peers belongs to; 0 when not leader.
	term  uint64
	peers map[uint64]*followerHistory

	// published is read by maintenance.
	published atomic.Pointer[retentionPositions]
}

// followerHistory is what the leader learned about a follower in term.
type followerHistory struct {
	match uint64
	// sentSnapshot is the highest snapshot index sent to the follower that
	// match has not reached yet, or 0.
	sentSnapshot uint64
}

// retentionPositions is what maintenance holds compaction for.
type retentionPositions struct {
	// positions lists the followers' candidate positions in ascending order.
	positions []uint64
	// unknown is true when a follower heard from has no known position.
	unknown bool
}

// followerProgress is the part of a raft tracker.Progress the hold reads.
type followerProgress struct {
	id              uint64
	state           tracker.StateType
	match           uint64
	pendingSnapshot uint64
}

// retentionSizingWindow is how many entries compactionTarget sizes per WAL
// read.
const retentionSizingWindow = 1024

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
		lastHeard:   make(map[uint64]time.Time),
		peers:       make(map[uint64]*followerHistory),
	}
}

// heard records a message from node id stepped at at. Call it from the
// orchestrate goroutine for every message stepped from another node.
func (r *followerRetention) heard(id uint64, at time.Time) {
	r.lastHeard[id] = at
}

// forget drops what is known about node id. Call it from the orchestrate
// goroutine when a configuration change for id applies: a member removed and
// re-added under the same ID, or replaced, is a different follower whose
// progress raft tracks afresh.
func (r *followerRetention) forget(id uint64) {
	delete(r.lastHeard, id)
	delete(r.peers, id)
}

// observe records the followers' progress and publishes the positions to
// hold for. Call it from the orchestrate goroutine only. term is the leader
// term, or 0 when the node is not the leader; followers must exclude the
// local node.
func (r *followerRetention) observe(now time.Time, term uint64, followers []followerProgress) {
	for id, at := range r.lastHeard {
		if now.Sub(at) > r.idleTimeout {
			delete(r.lastHeard, id)
		}
	}

	if term != r.term {
		clear(r.peers)
		r.term = term
	}
	if term == 0 {
		r.published.Store(nil)

		return
	}

	for id := range r.peers {
		if !slices.ContainsFunc(followers, func(f followerProgress) bool { return f.id == id }) {
			delete(r.peers, id)
		}
	}

	var published retentionPositions
	for _, f := range followers {
		h, ok := r.peers[f.id]
		if !ok {
			h = &followerHistory{}
			r.peers[f.id] = h
		}
		h.match = f.match
		if f.state == tracker.StateSnapshot {
			h.sentSnapshot = max(h.sentSnapshot, f.pendingSnapshot)
		}
		if h.match >= h.sentSnapshot {
			h.sentSnapshot = 0
		}

		if _, heard := r.lastHeard[f.id]; !heard {
			continue
		}

		if h.match == 0 && h.sentSnapshot == 0 {
			published.unknown = true

			continue
		}
		for _, position := range []uint64{h.match, h.sentSnapshot} {
			if position > 0 {
				published.positions = append(published.positions, position)
			}
		}
	}

	if len(published.positions) == 0 && !published.unknown {
		r.published.Store(nil)

		return
	}

	slices.Sort(published.positions)
	r.published.Store(&published)
}

// compactionTarget lowers target to keep the entries the followers still
// need, within maxBytes of entries beyond target.
func (r *followerRetention) compactionTarget(target uint64, walLog retentionEntries) (uint64, retentionDecision, error) {
	published := r.published.Load()
	if published == nil || (!published.unknown && published.positions[0] >= target) {
		return target, retentionDecision{}, nil
	}

	firstIndex, err := walLog.FirstIndex()
	if err != nil {
		return target, retentionDecision{}, fmt.Errorf("reading WAL first index: %w", err)
	}

	// A follower of unknown position keeps everything retained. A follower
	// whose position is already compacted away will receive a snapshot
	// whatever this node keeps: it must not hold for the others. The
	// compacted index itself (firstIndex-1) is still a valid append boundary.
	need := firstIndex - 1
	if !published.unknown {
		i, _ := slices.BinarySearch(published.positions, firstIndex-1)
		if i == len(published.positions) {
			return target, retentionDecision{}, nil
		}
		need = published.positions[i]
	}
	if need >= target {
		return target, retentionDecision{}, nil
	}

	// Compacting at C keeps entries [C, last] in memory; the normal target T
	// keeps [T, last]. The hold therefore costs entries [C, T-1]. When C is
	// the current boundary firstIndex-1, raft.Storage cannot read that entry,
	// so it goes uncharged: the hold can exceed maxBytes by that one entry,
	// which the WAL holds already.
	lo := max(need, firstIndex)
	if lo >= target {
		return need, retentionDecision{held: true, need: need}, nil
	}

	// Walk back from the target in bounded windows and stop at the cap: each
	// Entries call sizes its window under the WAL's read lock, which holds
	// off appends, so a single call over a large backlog would stall them.
	var size uint64
	for hi := target; hi > lo; {
		windowLo := lo
		if hi-lo > retentionSizingWindow {
			windowLo = hi - retentionSizingWindow
		}

		entries, err := walLog.Entries(windowLo, hi, math.MaxUint64)
		if err != nil {
			return target, retentionDecision{}, fmt.Errorf("reading retained entries [%d, %d): %w", windowLo, hi, err)
		}

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
		hi = windowLo
	}

	return need, retentionDecision{held: true, need: need, retainedBytes: size}, nil
}
