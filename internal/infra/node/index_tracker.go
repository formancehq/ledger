package node

import (
	"sync"
	"sync/atomic"
)

// IndexTracker provides an accurate prediction of the next Raft index that will
// be assigned. It is advanced by processReady (to match the last committed
// entry) and by Propose (to account for the local proposal before commit).
// The preloader uses this to determine cache generation boundaries without
// maintaining its own divergent counter.
//
// The mu mutex serializes Decrement (called from readyLoop when a proposal is
// dropped) with the proposal guard held by admission goroutines. Without this,
// a Decrement between AcquireProposalGuard's validation and Node.Propose could
// shift the tracker across a generation boundary, causing a preload/cache
// mismatch in the FSM.
type IndexTracker struct {
	nextIndex atomic.Uint64

	// mu serializes Decrement with the preloader's propose guard.
	// Admission goroutines hold this lock from AcquireProposalGuard through
	// Node.Propose+Release. Decrement acquires it before rolling back the
	// tracker. This prevents the race where a dropped proposal shifts the
	// tracker across a generation boundary while another goroutine is between
	// validation and Propose.
	mu sync.Mutex
}

// NewIndexTracker creates an IndexTracker starting at the given index.
func NewIndexTracker(initialIndex uint64) *IndexTracker {
	t := &IndexTracker{}
	t.nextIndex.Store(initialIndex)

	return t
}

// Lock acquires the tracker's mutex. Used by the preloader's proposal guard
// to serialize with Decrement.
func (t *IndexTracker) Lock() { t.mu.Lock() }

// Unlock releases the tracker's mutex.
func (t *IndexTracker) Unlock() { t.mu.Unlock() }

// Next returns the predicted next Raft index.
func (t *IndexTracker) Next() uint64 {
	return t.nextIndex.Load()
}

// Increment advances the tracker by n indices. Used by Propose to account
// for the local proposal before it is committed.
// Called while the caller holds the tracker's mutex (via the proposal guard).
func (t *IndexTracker) Increment(n uint64) {
	t.nextIndex.Add(n)
}

// RollbackIncrement reverses a previous Increment without acquiring the mutex.
// Used by Propose when the context is cancelled after a pre-increment. The
// caller already holds the tracker's mutex (via the proposal guard), so
// acquiring it again would deadlock.
func (t *IndexTracker) RollbackIncrement(n uint64) {
	t.nextIndex.Add(^(n - 1)) // atomic subtract via two's complement
}

// Decrement rolls back the tracker by n indices. Used when a proposal is
// dropped by Raft (e.g. leadership lost) to compensate for the optimistic
// Increment in Propose.
//
// Acquires mu to serialize with the preloader's proposal guard, preventing
// the race where a Decrement shifts the tracker across a generation boundary
// between AcquireProposalGuard's validation and Node.Propose.
func (t *IndexTracker) Decrement(n uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nextIndex.Add(^(n - 1)) // atomic subtract via two's complement
}

// Advance sets the tracker to at least minNext. Used by finishReady to
// catch up with all committed entries (including proposals from other leaders)
// so that followers have an accurate index prediction if they become leader.
//
// Advance only increases the tracker, so it cannot shift it to a lower
// generation — no mutex needed.
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

// Correct unconditionally sets the tracker to exactly nextIndex.
// Used by finishReady when the node is not the leader to correct tracker
// inflation from proposals that were accepted by rawNode.Propose but never
// committed (truncated after leadership loss).
//
// Safe to call without accounting for buffered proposals: admission is only
// routed to the leader, so a non-leader's proposeCh is always empty.
//
// Acquires mu to serialize with the preloader's proposal guard, preventing
// a correction from shifting the tracker across a generation boundary while
// an admission goroutine is between AcquireProposalGuard and Propose.
func (t *IndexTracker) Correct(nextIndex uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nextIndex.Store(nextIndex)
}
