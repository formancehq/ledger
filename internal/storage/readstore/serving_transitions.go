package readstore

import (
	"fmt"
	"maps"
	"sync"
)

// servingTransitions tracks index promotions committed to a Store but not
// yet flushed to stable storage, and performs that flush. Store exposes the
// operations its writer and readers need (MarkPromotion, UnmarkPromotion,
// FlushPromotions, PromotionInFlight); the state itself stays here.
//
// A serving transition is a write to an index's IndexVersionState that
// changes what queries may serve — the builder promoting a pending version
// to current. The store has no WAL, so a committed transition lives only in
// the memtable until Pebble flushes it: a hard kill in that window reopens
// the store at the pre-promotion state, and a node that had already answered
// queries under the promoted binding answers under the one before it again.
//
// Transitions ordered by the fold cursor do not have this problem — a rewound
// fold re-walks the log that produced them before alignment lets a query
// through. A promotion is not ordered by the fold: the backfill or rewrite
// that completes it runs beside the fold, so after a rewind the fold catches
// up while the promotion is still being redone, and the old binding serves
// at states past the new one. Hence a promotion is made durable before it is
// served: the builder marks the index in flight before committing, readers
// refuse a marked index as still building, and Flush clears the marks once
// the flush has completed.
//
// Marks live in memory only. After a restart nothing is in flight: whatever
// the reopened store holds was flushed by construction.
//
// Single-writer contract: a mark, the commit it covers, and the Flush that
// releases it must be issued by one goroutine in that order (the builder
// loop). Flush releases every count it captured before flushing; a mark whose
// commit could land after the flush's memtable rotation would be released
// without its data on disk. Readers (InFlight) may run from any goroutine.
type servingTransitions struct {
	mu sync.Mutex
	// inFlight counts, per index, the promotions staged or committed since
	// the last completed flush, so withdrawing a later promotion that never
	// committed cannot lift the gate an earlier, committed one still needs.
	inFlight map[servingTransitionKey]int
	// flush makes the owning store's memtable durable. A store built with
	// WithPromotionFlushForTest wraps it, which is how a test drives the
	// flush-failure path: Pebble retries a failed memtable flush internally
	// and returns only on success, so a real store cannot produce one.
	flush func() error
}

type servingTransitionKey struct {
	ledger    string
	canonical string
}

func newServingTransitions(flush func() error) *servingTransitions {
	return &servingTransitions{flush: flush}
}

// Mark records that a serving transition for the index is about to be
// committed. Call from the writer goroutine, before the commit; readers refuse
// the index from this point until Flush clears it.
func (t *servingTransitions) Mark(ledgerName, canonicalID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.inFlight == nil {
		t.inFlight = make(map[servingTransitionKey]int, 1)
	}

	t.inFlight[servingTransitionKey{ledgerName, canonicalID}]++
}

// Unmark withdraws one mark whose transition was never committed (the batch
// carrying it was cancelled). Marks of other transitions on the same index
// stay.
func (t *servingTransitions) Unmark(ledgerName, canonicalID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.release(servingTransitionKey{ledgerName, canonicalID}, 1)
}

// release drops n marks from key. Caller holds mu.
func (t *servingTransitions) release(key servingTransitionKey, n int) {
	if t.inFlight[key] <= n {
		delete(t.inFlight, key)

		return
	}

	t.inFlight[key] -= n
}

// InFlight reports whether the index has a committed but not yet flushed
// serving transition.
func (t *servingTransitions) InFlight(ledgerName, canonicalID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.inFlight[servingTransitionKey{ledgerName, canonicalID}] > 0
}

// Flush makes every marked transition durable and clears its mark. A no-op
// when nothing is marked, so callers can invoke it freely without paying for
// a flush when no promotion is pending. On failure the marks stay, so the
// indexes keep refusing until a later call succeeds.
func (t *servingTransitions) Flush() error {
	t.mu.Lock()
	pending := make(map[servingTransitionKey]int, len(t.inFlight))
	maps.Copy(pending, t.inFlight)
	flush := t.flush
	t.mu.Unlock()

	if len(pending) == 0 {
		return nil
	}

	if err := flush(); err != nil {
		return fmt.Errorf("flushing read store for serving transitions: %w", err)
	}

	// Only the counts captured before flush() are released. Under the
	// single-writer contract every captured mark's commit preceded the
	// flush, so it is on disk; a mark taken meanwhile by the writer covers a
	// later commit and stays.
	t.mu.Lock()
	defer t.mu.Unlock()

	for k, n := range pending {
		t.release(k, n)
	}

	return nil
}
