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
// A serving transition promotes an index version to current. The builder
// marks it before committing and clears the mark after a successful flush.
// This retains the reader admission contract across a failed flush.
// The builder is the single writer for mark, commit and flush ordering;
// readers may inspect marks concurrently.
type servingTransitions struct {
	mu sync.Mutex
	// inFlight counts, per index and version, the promotions staged or
	// committed since the last completed flush, so withdrawing a later
	// promotion that never committed cannot lift the gate an earlier,
	// committed one still needs.
	inFlight map[servingTransitionKey]int
	// flush makes committed promotions durable. Tests may wrap it to
	// exercise the failed-flush path.
	flush func() error
}

type servingTransitionKey struct {
	ledger    string
	canonical string
	version   uint32
}

func newServingTransitions(flush func() error) *servingTransitions {
	return &servingTransitions{flush: flush}
}

// Mark records that version is about to be promoted to current. Call from
// the writer goroutine, before the commit; readers do not serve the version
// from this point until Flush clears it.
func (t *servingTransitions) Mark(ledgerName, canonicalID string, version uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.inFlight == nil {
		t.inFlight = make(map[servingTransitionKey]int, 1)
	}

	t.inFlight[servingTransitionKey{ledgerName, canonicalID, version}]++
}

// Unmark withdraws one mark whose transition was never committed (the batch
// carrying it was cancelled). Marks of other transitions on the same index
// stay.
func (t *servingTransitions) Unmark(ledgerName, canonicalID string, version uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.release(servingTransitionKey{ledgerName, canonicalID, version}, 1)
}

// release drops n marks from key. Caller holds mu.
func (t *servingTransitions) release(key servingTransitionKey, n int) {
	if t.inFlight[key] <= n {
		delete(t.inFlight, key)

		return
	}

	t.inFlight[key] -= n
}

// InFlight reports whether version has a committed but not yet flushed
// promotion.
func (t *servingTransitions) InFlight(ledgerName, canonicalID string, version uint32) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.inFlight[servingTransitionKey{ledgerName, canonicalID, version}] > 0
}

// Flush makes every marked transition durable and clears its mark. A no-op
// when nothing is marked, so callers can invoke it freely without paying for
// a flush when no promotion is pending. On failure the marks stay, so the
// marked versions stay unserved until a later call succeeds.
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
