package main

import (
	"strings"

	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Candidate enumeration is exponential in independently committable in-flight
// bulks. Workers are capped two below this limit so the separately dispatched
// maintenance recovery and one coalesced ambiguous maintenance enable can both
// be represented without making a read or failure validation effectively
// non-terminating while it holds c.mu.
const maxCandidateInflight = 8

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
func (c *Checker) candidateBases(maxTicket uint64, visit func(oracle.GlobalState) bool) {
	c.walkCandidateStates(maxTicket, visit, nil)
}

// admittedBases is candidateBases for a bulk the server rejected: visit gets
// each base together with the verdicts admission reaches for bulk on that base
// and on every base before it along the path. The leader admits on the state
// it has applied and the FSM applies later, so the verdict can be older than
// the state the bulk is applied on — the model must pair them the same way.
// Caller holds c.mu.
func (c *Checker) admittedBases(maxTicket uint64, bulk oracle.Bulk, visit func(oracle.GlobalState, []oracle.Admission) bool) {
	c.walk(maxTicket, &bulk, visit, nil)
}

// checkpointCreationMatches checks a served predicted checkpoint against the
// state immediately before the candidate create transition that assigned id.
// Caller holds c.mu.
func (c *Checker) checkpointCreationMatches(maxTicket, id uint64, match func(oracle.GlobalState) bool) bool {
	if snapshot, ok := c.checkpoints[id]; ok && match(snapshot.state) {
		return true
	}
	if snapshot, ok := c.deletedCheckpointSnapshots[id]; ok && match(snapshot.state) {
		return true
	}

	matched := false
	c.walkCandidateStates(maxTicket, func(oracle.GlobalState) bool { return matched }, func(before oracle.GlobalState, bulk oracle.Bulk, result oracle.ApplyResult) bool {
		for i, request := range bulk.Requests {
			if request.GetCreateQueryCheckpoint() != nil && i < len(result.Orders) && result.Orders[i].CheckpointID == id {
				matched = match(before)

				return matched
			}
		}

		return false
	})

	return matched
}

// walkCandidateStates enumerates candidate states and optionally observes each
// successful transition. Caller holds c.mu.
func (c *Checker) walkCandidateStates(maxTicket uint64, visit func(oracle.GlobalState) bool, transition func(oracle.GlobalState, oracle.Bulk, oracle.ApplyResult) bool) {
	c.walk(maxTicket, nil, func(base oracle.GlobalState, _ []oracle.Admission) bool { return visit(base) }, transition)
}

// walk is the enumeration behind walkCandidateStates and admittedBases. With
// admitted set, each path accumulates the distinct verdicts admitted gets on
// its states, and a state reached under a different verdict set is a distinct
// node: the verdicts are part of what the server could have done there.
func (c *Checker) walk(maxTicket uint64, admitted *oracle.Bulk, visit func(oracle.GlobalState, []oracle.Admission) bool, transition func(oracle.GlobalState, oracle.Bulk, oracle.ApplyResult) bool) {
	// Only operations dispatched no later than maxTicket (the observation's
	// high-water) can precede it; one dispatched after the observation's response
	// cannot have committed before it, so folding it would invent a state the
	// server was never in and could explain away a real divergence.
	type candidateBulk struct {
		ticket uint64
		bulk   oracle.Bulk
	}

	pending := make([]candidateBulk, 0, len(c.pending))
	for _, pe := range c.pending {
		// pending is minSeq-ordered, so an entry dispatched after the observation
		// committed after it — and so did every later (higher-minSeq) entry.
		if pe.obs.ticket > maxTicket {
			break
		}
		pending = append(pending, candidateBulk{ticket: pe.obs.ticket, bulk: pe.obs.bulk})
	}

	inflight := make([]candidateBulk, 0, len(c.inflight))
	for t, b := range c.inflight {
		if t <= maxTicket {
			inflight = append(inflight, candidateBulk{ticket: t, bulk: b})
		}
	}
	if len(inflight) > maxCandidateInflight {
		panic("candidate search exceeded its bounded in-flight set")
	}

	// Keep the remaining-set representation dynamic so the bound is independent
	// of the machine word size and explicit above.
	allRem := make([]byte, (len(inflight)+7)/8)
	for idx := range inflight {
		allRem[idx/8] |= 1 << (idx % 8)
	}

	type dedupKey struct {
		state    oracle.Digest
		pIdx     int
		rem      string
		verdicts string
	}
	seen := map[dedupKey]bool{}

	var rec func(base oracle.GlobalState, pIdx int, rem []byte, verdicts []oracle.Admission) bool

	rec = func(base oracle.GlobalState, pIdx int, rem []byte, verdicts []oracle.Admission) bool {
		var verdictKeys []string
		if admitted != nil {
			verdicts = withVerdict(verdicts, base.Admit(*admitted))
			for _, v := range verdicts {
				verdictKeys = append(verdictKeys, v.Key())
			}
		}

		k := dedupKey{state: base.Fingerprint(), pIdx: pIdx, rem: string(rem), verdicts: strings.Join(verdictKeys, "\x00")}
		if seen[k] {
			return false
		}
		seen[k] = true

		if visit(base, verdicts) {
			return true
		}

		// Advance the pending prefix by one, in minSeq order.
		if pIdx < len(pending) {
			candidate := pending[pIdx]
			if res := base.Apply(candidate.bulk); res.OK {
				if transition != nil && transition(base, candidate.bulk, res) {
					return true
				}
				if rec(res.State, pIdx+1, rem, verdicts) {
					return true
				}
			}
		}

		// Fold in any one of the remaining in-flight bulks (unknown position).
		for idx := range inflight {
			byteIdx, mask := idx/8, byte(1<<(idx%8))
			if rem[byteIdx]&mask == 0 {
				continue
			}

			candidate := inflight[idx]

			res := base.Apply(candidate.bulk)
			if !res.OK {
				// Could not have committed at this point — not a predecessor.
				continue
			}
			if transition != nil && transition(base, candidate.bulk, res) {
				return true
			}

			rem[byteIdx] &^= mask
			matched := rec(res.State, pIdx, rem, verdicts)
			rem[byteIdx] |= mask
			if matched {
				return true
			}
		}

		return false
	}

	rec(c.modelState, 0, allRem, nil)
}

// withVerdict appends v to the path's verdicts unless an equal one is there.
// The result never aliases verdicts: sibling branches extend the same prefix.
func withVerdict(verdicts []oracle.Admission, v oracle.Admission) []oracle.Admission {
	key := v.Key()
	for _, have := range verdicts {
		if have.Key() == key {
			return verdicts
		}
	}

	return append(verdicts[:len(verdicts):len(verdicts)], v)
}
