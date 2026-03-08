package node

import "sync/atomic"

// IndexTracker provides an accurate prediction of the next Raft index that will
// be assigned. It is incremented by every Raft index consumer: proposals (via
// Node.Propose) and non-proposal committed entries such as no-ops and config
// changes (via Node.processReady). The preloader uses this to determine cache
// generation boundaries without maintaining its own divergent counter.
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

// Increment advances the tracker by n indices. Used by processReady to account
// for non-proposal committed entries (Raft no-ops, config changes).
func (t *IndexTracker) Increment(n uint64) {
	t.nextIndex.Add(n)
}
