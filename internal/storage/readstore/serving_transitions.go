package readstore

import "fmt"

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
// refuse a marked index as still building, and FlushServingTransitions
// clears the marks once the flush has completed.
//
// Marks live in memory only. After a restart nothing is in flight: whatever
// the reopened store holds was flushed by construction.
type servingTransitionKey struct {
	ledger    string
	canonical string
}

// MarkServingTransition records that a serving transition for the index is
// about to be committed. Call before the commit; readers refuse the index
// from this point until FlushServingTransitions clears it.
func (s *Store) MarkServingTransition(ledgerName, canonicalID string) {
	s.servingMu.Lock()
	defer s.servingMu.Unlock()

	if s.servingInFlight == nil {
		s.servingInFlight = make(map[servingTransitionKey]struct{}, 1)
	}

	s.servingInFlight[servingTransitionKey{ledgerName, canonicalID}] = struct{}{}
}

// UnmarkServingTransition withdraws a mark whose transition was never
// committed (the batch carrying it was cancelled).
func (s *Store) UnmarkServingTransition(ledgerName, canonicalID string) {
	s.servingMu.Lock()
	defer s.servingMu.Unlock()

	delete(s.servingInFlight, servingTransitionKey{ledgerName, canonicalID})
}

// ServingTransitionInFlight reports whether the index has a committed but not
// yet flushed serving transition.
func (s *Store) ServingTransitionInFlight(ledgerName, canonicalID string) bool {
	s.servingMu.Lock()
	defer s.servingMu.Unlock()

	_, ok := s.servingInFlight[servingTransitionKey{ledgerName, canonicalID}]

	return ok
}

// FlushServingTransitions makes every marked transition durable and clears
// its mark. A no-op when nothing is marked, so callers can invoke it freely
// without paying for a flush when no promotion is pending. On failure the
// marks stay, so the indexes keep refusing until a later call succeeds.
func (s *Store) FlushServingTransitions() error {
	s.servingMu.Lock()
	pending := make([]servingTransitionKey, 0, len(s.servingInFlight))
	for k := range s.servingInFlight {
		pending = append(pending, k)
	}
	flush := s.servingFlush
	s.servingMu.Unlock()

	if len(pending) == 0 {
		return nil
	}

	if flush == nil {
		flush = s.db.Flush
	}

	if err := flush(); err != nil {
		return fmt.Errorf("flushing read store for serving transitions: %w", err)
	}

	// Only the marks captured before the flush are cleared: a transition
	// marked after the memtable rotated is in the new memtable, not on disk.
	s.servingMu.Lock()
	defer s.servingMu.Unlock()

	for _, k := range pending {
		delete(s.servingInFlight, k)
	}

	return nil
}

// OverrideServingFlushForTest replaces the Pebble flush FlushServingTransitions
// performs. Pebble retries a failed memtable flush internally and only returns
// once one succeeds, so a failing flush cannot be produced against a real
// store; tests inject one here. nil restores the real flush.
func (s *Store) OverrideServingFlushForTest(flush func() error) {
	s.servingMu.Lock()
	defer s.servingMu.Unlock()

	s.servingFlush = flush
}
