package check

import "sort"

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
