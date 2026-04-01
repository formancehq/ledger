package node

import "sync/atomic"

// IndexTracker provides an accurate prediction of the next Raft index that will
// be assigned. It is advanced by processReady (to match the last committed
// entry) and by Propose (to account for the local proposal before commit).
// The preloader uses this to determine cache generation boundaries without
// maintaining its own divergent counter.
type IndexTracker struct {
	nextIndex atomic.Uint64
}

// NewIndexTracker creates an IndexTracker starting at the given index.
func NewIndexTracker(initialIndex uint64) *IndexTracker {
	t := &IndexTracker{}
	t.nextIndex.Store(initialIndex)

	return t
}

// Next returns the predicted next Raft index.
func (t *IndexTracker) Next() uint64 {
	return t.nextIndex.Load()
}

// Increment advances the tracker by n indices. Used by Propose to account
// for the local proposal before it is committed.
func (t *IndexTracker) Increment(n uint64) {
	t.nextIndex.Add(n)
}

// Advance sets the tracker to at least minNext. Used by processReady to
// catch up with all committed entries (including proposals from other leaders)
// so that followers have an accurate index prediction if they become leader.
func (t *IndexTracker) Advance(minNext uint64) {
	for {
		cur := t.nextIndex.Load()
		if cur >= minNext {
			return
		}

		if t.nextIndex.CompareAndSwap(cur, minNext) {
			return
		}
	}
}
