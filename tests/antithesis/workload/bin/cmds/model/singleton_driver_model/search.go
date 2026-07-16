package main

import (
	"crypto/sha256"
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
// could not have committed at that point. Dedup is keyed on a 256-bit hash of
// (state, pendingIndex, remaining-inflight): collisions are infeasible (~2^-128),
// so dedup is exact while the retained key stays 32 bytes rather than a full
// serialized state. pendingIndex and remaining-inflight are folded in because a
// state reachable with different continuations (e.g. a duplicate-effect in-flight
// bulk landing on the same state as a pending one) must be explored under each.
func (c *Checker) candidateBases(maxTicket uint64, visit func(oracle.GlobalState) bool) {
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

	allIdx := make([]int, len(inflight))
	for i := range inflight {
		allIdx[i] = i
	}

	seen := map[[sha256.Size]byte]bool{}
	hasher := sha256.New()
	key := func(base oracle.GlobalState, pIdx int, rem []int) [sha256.Size]byte {
		hasher.Reset()
		base.Hash(hasher)
		fmt.Fprintf(hasher, "#%d#%v", pIdx, rem)

		var k [sha256.Size]byte
		hasher.Sum(k[:0])
		return k
	}

	var rec func(base oracle.GlobalState, pIdx int, rem []int) bool

	rec = func(base oracle.GlobalState, pIdx int, rem []int) bool {
		k := key(base, pIdx, rem)
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
		for i, idx := range rem {
			res := base.Apply(inflight[idx])
			if !res.OK {
				// Could not have committed at this point — not a predecessor.
				continue
			}

			next := make([]int, 0, len(rem)-1)
			next = append(next, rem[:i]...)
			next = append(next, rem[i+1:]...)

			if rec(res.State, pIdx, next) {
				return true
			}
		}

		return false
	}

	rec(c.modelState, 0, allIdx)
}
