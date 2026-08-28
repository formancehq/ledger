package main

import (
	"fmt"

	"github.com/formancehq/ledger/v3/tests/oracle"
)

// candidateBases enumerates the distinct committed states the server could be in
// relative to a not-yet-linearized observation (a failure or a read): modelState
// folded with the in-flight/pending bulks in some commit-consistent order. Only
// bulks dispatched no later than maxTicket — the observation's high-water (a
// failure's observeTicket, or the ticket high-water captured when a read
// returned) — are folded; ones dispatched after the observation cannot precede
// it. visit is called for each distinct base; returning true stops the search
// early, once an observation is explained. Caller holds c.mu.
//
// This is the one primitive the whole checker rests on. The in-flight bulks are
// of two kinds, with different freedom:
//
//   - pending: committed successes still buffered in the re-order queue. Their
//     commit order is KNOWN (c.pending is sorted by minSeq), so they may only
//     appear as an ordered prefix — pending[0], then pending[1], … — never
//     reordered or skipped. The committed prefix at any point includes a
//     contiguous prefix of them.
//   - inflight: dispatched bulks whose response hasn't arrived. Their sequence
//     is unknown, so each may be interleaved at any position (any ordered subset).
//
// So branching is driven by the (few) in-flight bulks, not by how many pending
// are buffered — a pending bulk is at most one deterministic step. Dedup collapses
// commutative orderings; success-gating (res.OK) prunes orders in which a bulk
// could not have committed at that point. Dedup is keyed on the state's 128-bit
// fingerprint (see pmap.go — maintained incrementally, so reading it is O(1))
// plus the pending index and the remaining-inflight set: collisions are
// infeasible for the model's non-adversarial inputs, so dedup is exact.
// pendingIndex and remaining-inflight are folded in because a state reachable
// with different continuations (e.g. a duplicate-effect in-flight bulk landing
// on the same state as a pending one) must be explored under each.
//
// A third kind folds alongside them: the chapter transitions the server makes with
// no request behind it — the Sealer's CLOSING -> CLOSED, the Archiver's ARCHIVING ->
// ARCHIVED. They are the same kind of uncertainty as an in-flight bulk (an event
// that may already have happened, that no response announced), so they fold the
// same way. Without them a committed archive buffered in the re-order queue would
// stall the pending prefix — a prefix advances only through Apply succeeding — and
// hide every later pending effect from the search, turning a correct server into a
// business-read or unexplained-failure finding.
//
// Only the chapters an order in play names are folded (addressed, plus the ones the
// buffered and in-flight bulks ask to archive). No other chapter's seal state is
// visible to what this search validates: business reads carry no chapter
// information, and a failure reveals a chapter's status only through a chapter
// order's own reason. ListChapters would see every chapter's status, which is why
// runChapterRead validates against the model directly instead of through here — a
// new read that exposes chapter state must do the same, or this bound stops
// holding.
//
// The set's size is bounded at the source: the driver withholds an archive order
// that would push the chapters in play past maxChaptersInPlay, so this branches
// over a handful at most.
func (c *Checker) candidateBases(maxTicket uint64, addressed []uint64, visit func(oracle.GlobalState) bool) {
	// Only operations dispatched no later than maxTicket (the observation's
	// high-water) can precede it; one dispatched after the observation's response
	// cannot have committed before it, so folding it would invent a state the
	// server was never in and could explain away a real divergence.
	pending := make([]oracle.Bulk, 0, len(c.pending))
	for _, pe := range c.pending {
		// pending is minSeq-ordered, so an entry dispatched after the observation
		// committed after it — and so did every later (higher-minSeq) entry.
		if pe.obs.ticket > maxTicket {
			break
		}
		pending = append(pending, pe.obs.bulk)
	}

	inflight := make([]oracle.Bulk, 0, len(c.inflight))
	for t, b := range c.inflight {
		if t <= maxTicket {
			inflight = append(inflight, b)
		}
	}

	// The remaining-inflight set is a bitmask, so the dedup key stays a small
	// comparable value.
	if len(inflight) > 64 {
		panic(fmt.Sprintf("candidateBases: %d in-flight bulks exceed the 64-bit set", len(inflight)))
	}
	allRem := uint64(1)<<len(inflight) - 1

	// The observation being validated is no longer in the in-flight set by the time
	// a failure validates (handleObservation drops the ticket first), so its own
	// chapter arrives through addressed — which is why folding it back in cannot
	// grow the set past what was in play when it was dispatched.
	sealable := chaptersInPlay(pending, inflight, addressed)

	type dedupKey struct {
		state oracle.Digest
		pIdx  int
		rem   uint64
	}
	seen := map[dedupKey]bool{}

	var rec func(base oracle.GlobalState, pIdx int, rem uint64) bool

	rec = func(base oracle.GlobalState, pIdx int, rem uint64) bool {
		k := dedupKey{state: base.Fingerprint(), pIdx: pIdx, rem: rem}
		if seen[k] {
			return false
		}
		seen[k] = true

		if visit(base) {
			return true
		}

		// Advance the pending prefix by one, in minSeq order.
		if pIdx < len(pending) {
			if res := base.Apply(pending[pIdx]); res.OK {
				if rec(res.State, pIdx+1, rem) {
					return true
				}
			}
		}

		// Fold in any one of the remaining in-flight bulks (unknown position).
		for idx := 0; idx < len(inflight); idx++ {
			if rem&(1<<idx) == 0 {
				continue
			}

			res := base.Apply(inflight[idx])
			if !res.OK {
				// Could not have committed at this point — not a predecessor.
				continue
			}

			if rec(res.State, pIdx, rem&^(1<<idx)) {
				return true
			}
		}

		// Fold one transition the server makes unasked. Each moves a chapter forward
		// along a five-state chain with no cycles, so the recursion still terminates,
		// and the dedup key collapses the orders in which a step and a bulk commute.
		for _, id := range sealable {
			if next, sealed := base.Chapters().WithSealed(id); sealed {
				if rec(base.WithChapters(next), pIdx, rem) {
					return true
				}
			}
		}

		if next, confirmed := base.Chapters().WithConfirmed(); confirmed {
			if rec(base.WithChapters(next), pIdx, rem) {
				return true
			}
		}

		return false
	}

	rec(c.modelState, 0, allRem)
}
