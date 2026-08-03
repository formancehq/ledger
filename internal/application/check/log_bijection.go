package check

import (
	"sort"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// logRange is a contiguous, inclusive log-sequence interval.
type logRange struct {
	min uint64
	max uint64
}

// logRangeSet is a set of log sequences held as sorted, merged, inclusive
// intervals.
//
// Interval representation rather than a per-sequence map is deliberate.
// AuditSuccess.[min,max]_log_sequence is a contiguous inclusive range
// (misc/proto/audit.proto), and log_sequence never advances on a failed
// proposal, so successive success ranges are adjacent: a healthy store
// collapses to a SINGLE interval. Memory is O(number of anomalies) rather
// than O(number of logs), so unlike the replay accumulators in
// replay_store.go this needs no Pebble-backed scratch.
//
// The zero value is ready to use.
type logRangeSet struct {
	ranges     []logRange
	normalized bool
}

// add records [minSeq, maxSeq] as a member interval. A zero bound means the
// range carries no logs (a proposal in which every order was an idempotent
// replay) and contributes nothing; an inverted range is likewise ignored —
// the caller that produced it reports the malformed entry separately.
// Intervals may be added in any order.
func (s *logRangeSet) add(minSeq, maxSeq uint64) {
	if minSeq == 0 || maxSeq == 0 || minSeq > maxSeq {
		return
	}

	s.ranges = append(s.ranges, logRange{min: minSeq, max: maxSeq})
	s.normalized = false
}

// normalize sorts the intervals ascending and coalesces overlapping or
// adjacent ones. Idempotent.
func (s *logRangeSet) normalize() {
	if s.normalized {
		return
	}

	sort.Slice(s.ranges, func(i, j int) bool { return s.ranges[i].min < s.ranges[j].min })

	// In-place coalesce: writes only ever land at an index at or below the
	// current read index, so aliasing the backing array is safe.
	merged := s.ranges[:0]

	for _, r := range s.ranges {
		if n := len(merged); n > 0 && r.min <= merged[n-1].max+1 {
			if r.max > merged[n-1].max {
				merged[n-1].max = r.max
			}

			continue
		}

		merged = append(merged, r)
	}

	s.ranges = merged
	s.normalized = true
}

// intervals returns the normalized intervals. The result aliases internal
// state and must not be mutated.
func (s *logRangeSet) intervals() []logRange {
	s.normalize()

	return s.ranges
}

// contains reports whether seq is a member of the set.
func (s *logRangeSet) contains(seq uint64) bool {
	s.normalize()

	i := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].max >= seq })

	return i < len(s.ranges) && seq >= s.ranges[i].min
}

// total returns the number of distinct sequences in the set.
func (s *logRangeSet) total() uint64 {
	s.normalize()

	var n uint64

	for _, r := range s.ranges {
		n += r.max - r.min + 1
	}

	return n
}

// empty reports whether the set holds no sequences.
func (s *logRangeSet) empty() bool {
	s.normalize()

	return len(s.ranges) == 0
}

// chainVerification bundles everything verifyAuditHashChain derives from the
// hash-verified audit range. A struct rather than extra out-parameters: the
// function already takes 7 arguments, and its five direct test call sites
// discard this return with `_`, so widening the type costs no test churn.
type chainVerification struct {
	// skippable is the per-log-sequence skippable_reasons whitelist consumed
	// by verifySkippedOrder during log replay.
	skippable map[uint64]*expectedSkippableOrder
	// authenticated holds the log sequences the chain proves must exist in
	// the store. Fed by every verified AuditSuccess range.
	authenticated logRangeSet
	// unverifiable holds log spans the checker can prove nothing about,
	// because the entry covering them is structurally malformed. compareLogs
	// skips these rather than reporting false gaps across them.
	unverifiable logRangeSet
}

// newChainVerification returns a chainVerification with its maps ready.
func newChainVerification() *chainVerification {
	return &chainVerification{
		skippable: make(map[uint64]*expectedSkippableOrder),
	}
}

// unverifiableEvent builds a CheckStoreUnverifiableRange event. It is NOT an
// error: it must never affect the CLI exit code or the JSON `valid` flag. It
// exists so that a range the checker could not authenticate is declared rather
// than silently passed over — the absence of a finding in an unauthenticated
// range is not proof.
//
// Every occurrence marks a defect; a healthy cluster, archived or not, emits
// none. reason is typed so operators route on it rather than string-matching
// message, since the two causes need different runbooks.
//
// rangeStart/rangeEnd are inclusive. Log sequences are 1-based, so pass 0/0
// when the bounds genuinely could not be determined — consumers render that as
// unknown, never as a literal "0-0".
func unverifiableEvent(
	reason servicepb.CheckStoreUnverifiableReason,
	message string,
	rangeStart, rangeEnd uint64,
) *servicepb.CheckStoreEvent {
	return &servicepb.CheckStoreEvent{
		Type: &servicepb.CheckStoreEvent_UnverifiableRange{
			UnverifiableRange: &servicepb.CheckStoreUnverifiableRange{
				Reason:     reason,
				Message:    message,
				RangeStart: rangeStart,
				RangeEnd:   rangeEnd,
			},
		},
	}
}
