package check

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestLogRangeSetAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		adds  [][2]uint64
		want  []logRange
		total uint64
	}{
		{
			name:  "single range",
			adds:  [][2]uint64{{1, 3}},
			want:  []logRange{{min: 1, max: 3}},
			total: 3,
		},
		{
			name:  "adjacent ranges coalesce",
			adds:  [][2]uint64{{1, 3}, {4, 6}},
			want:  []logRange{{min: 1, max: 6}},
			total: 6,
		},
		{
			name:  "overlapping ranges coalesce",
			adds:  [][2]uint64{{1, 5}, {3, 8}},
			want:  []logRange{{min: 1, max: 8}},
			total: 8,
		},
		{
			name:  "disjoint ranges stay separate",
			adds:  [][2]uint64{{1, 3}, {7, 9}},
			want:  []logRange{{min: 1, max: 3}, {min: 7, max: 9}},
			total: 6,
		},
		{
			name:  "out of order adds are sorted",
			adds:  [][2]uint64{{7, 9}, {1, 3}},
			want:  []logRange{{min: 1, max: 3}, {min: 7, max: 9}},
			total: 6,
		},
		{
			name:  "single sequence range",
			adds:  [][2]uint64{{5, 5}},
			want:  []logRange{{min: 5, max: 5}},
			total: 1,
		},
		{
			name:  "zero range is ignored (proposal produced no logs)",
			adds:  [][2]uint64{{0, 0}},
			want:  nil,
			total: 0,
		},
		{
			name:  "inverted range is ignored",
			adds:  [][2]uint64{{9, 4}},
			want:  nil,
			total: 0,
		},
		{
			name:  "nested range does not shrink the parent",
			adds:  [][2]uint64{{1, 10}, {3, 4}},
			want:  []logRange{{min: 1, max: 10}},
			total: 10,
		},
		{
			name:  "empty set",
			adds:  nil,
			want:  nil,
			total: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var s logRangeSet
			for _, a := range tc.adds {
				s.add(a[0], a[1])
			}

			require.Equal(t, tc.want, s.intervals())
			require.Equal(t, tc.total, s.total())
		})
	}
}

func TestLogRangeSetContains(t *testing.T) {
	t.Parallel()

	var s logRangeSet
	s.add(10, 12)
	s.add(20, 20)

	require.False(t, s.contains(9))
	require.True(t, s.contains(10))
	require.True(t, s.contains(11))
	require.True(t, s.contains(12))
	require.False(t, s.contains(13))
	require.True(t, s.contains(20))
	require.False(t, s.contains(21))
}

func TestLogRangeSetContainsEmpty(t *testing.T) {
	t.Parallel()

	var s logRangeSet

	require.False(t, s.contains(0))
	require.False(t, s.contains(1))
	require.True(t, s.empty())
}

// TestLogRangeSetAddAfterNormalize pins the cache-invalidation contract: a read
// normalizes and sets the flag, so a subsequent add MUST clear it or every
// later read returns state frozen at the first read.
func TestLogRangeSetAddAfterNormalize(t *testing.T) {
	t.Parallel()

	var s logRangeSet
	s.add(1, 3)

	// Force normalization via a read.
	require.True(t, s.contains(2))
	require.Equal(t, uint64(3), s.total())

	// Add after that read must be visible to every subsequent read.
	s.add(10, 12)

	require.True(t, s.contains(11), "an add after a read must be visible to contains")
	require.Equal(t, uint64(6), s.total(), "an add after a read must be counted by total")
	require.Equal(t, []logRange{{min: 1, max: 3}, {min: 10, max: 12}}, s.intervals())

	// An add that extends an existing interval must also invalidate.
	s.add(4, 5)

	require.True(t, s.contains(4))
	require.Equal(t, []logRange{{min: 1, max: 5}, {min: 10, max: 12}}, s.intervals())
}

// TestLogRangeSetChainedCoalesce covers three or more ranges collapsing
// transitively — the healthy-store shape, where every successful proposal's
// range is adjacent to the previous one and the whole set must become a single
// interval.
func TestLogRangeSetChainedCoalesce(t *testing.T) {
	t.Parallel()

	var s logRangeSet
	s.add(1, 2)
	s.add(3, 4)
	s.add(5, 6)
	s.add(7, 7)

	require.Equal(t, []logRange{{min: 1, max: 7}}, s.intervals(),
		"adjacent ranges must collapse transitively, not just pairwise")
	require.Equal(t, uint64(7), s.total())
}

// TestLogRangeSetNormalizeIdempotent verifies repeated reads do not corrupt the
// set. normalize() coalesces in place over its own backing array, so a second
// pass over already-merged state must be a no-op rather than re-processing it.
func TestLogRangeSetNormalizeIdempotent(t *testing.T) {
	t.Parallel()

	var s logRangeSet
	s.add(5, 6)
	s.add(1, 2)
	s.add(3, 4)

	first := s.intervals()
	require.Equal(t, []logRange{{min: 1, max: 6}}, first)

	// Repeated reads must return identical state.
	for range 3 {
		require.Equal(t, []logRange{{min: 1, max: 6}}, s.intervals())
		require.Equal(t, uint64(6), s.total())
		require.False(t, s.empty())
	}
}

// TestLogRangeSetAscendingAddsStayCompact pins the memory contract the type doc
// asserts: callers feed ascending ranges over the whole history (one per stored
// log in compareLogs, one per audit entry in verifyAuditStructure), so the
// backing slice must stay O(anomalies) as they arrive — not grow to O(history)
// and only collapse when a read triggers normalize().
func TestLogRangeSetAscendingAddsStayCompact(t *testing.T) {
	t.Parallel()

	var contiguous logRangeSet
	for seq := uint64(1); seq <= 1000; seq++ {
		contiguous.add(seq, seq)
	}

	require.Len(t, contiguous.ranges, 1, "a contiguous ascending run must coalesce as it is added")
	require.Equal(t, []logRange{{min: 1, max: 1000}}, contiguous.intervals())
	require.Equal(t, uint64(1000), contiguous.total())

	// One gap must cost exactly one extra interval, no more.
	var withGap logRangeSet
	for seq := uint64(1); seq <= 1000; seq++ {
		if seq == 500 {
			continue
		}

		withGap.add(seq, seq)
	}

	require.Len(t, withGap.ranges, 2)
	require.Equal(t, []logRange{{min: 1, max: 499}, {min: 501, max: 1000}}, withGap.intervals())
	require.False(t, withGap.contains(500))
}

func TestBuildPurgedLogSet(t *testing.T) {
	t.Parallel()

	// closeSeq, not close: `close` is a builtin.
	chapter := func(id, start, closeSeq uint64, status commonpb.ChapterStatus) *commonpb.Chapter {
		return &commonpb.Chapter{Id: id, StartSequence: start, CloseSequence: closeSeq, Status: status}
	}

	t.Run("only archived chapters contribute", func(t *testing.T) {
		t.Parallel()

		purged := buildPurgedLogSet([]*commonpb.Chapter{
			chapter(1, 1, 10, commonpb.ChapterStatus_CHAPTER_ARCHIVED),
			chapter(2, 11, 20, commonpb.ChapterStatus_CHAPTER_ARCHIVING),
		}, func(*servicepb.CheckStoreEvent) {
			t.Fatal("no unverifiable event expected")
		})

		require.True(t, purged.contains(10))
		require.False(t, purged.contains(11), "an ARCHIVING chapter's logs are still live")
	})

	t.Run("out of order archival is handled", func(t *testing.T) {
		t.Parallel()

		purged := buildPurgedLogSet([]*commonpb.Chapter{
			chapter(3, 21, 30, commonpb.ChapterStatus_CHAPTER_ARCHIVED),
			chapter(2, 11, 20, commonpb.ChapterStatus_CHAPTER_ARCHIVING),
			chapter(1, 1, 10, commonpb.ChapterStatus_CHAPTER_ARCHIVED),
		}, func(*servicepb.CheckStoreEvent) {
			t.Fatal("no unverifiable event expected")
		})

		require.True(t, purged.contains(5))
		require.False(t, purged.contains(15), "chapter 2 is not archived, so its logs are live")
		require.True(t, purged.contains(25))
	})

	t.Run("unusable bounds are declared with undetermined range", func(t *testing.T) {
		t.Parallel()

		cases := map[string]*commonpb.Chapter{
			"inverted":   chapter(1, 30, 10, commonpb.ChapterStatus_CHAPTER_ARCHIVED),
			"zero close": chapter(2, 5, 0, commonpb.ChapterStatus_CHAPTER_ARCHIVED),
			"zero start": chapter(3, 0, 5, commonpb.ChapterStatus_CHAPTER_ARCHIVED),
		}

		for name, ch := range cases {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				var events []*servicepb.CheckStoreEvent

				purged := buildPurgedLogSet([]*commonpb.Chapter{ch}, func(e *servicepb.CheckStoreEvent) {
					events = append(events, e)
				})

				require.True(t, purged.empty(), "an unusable chapter must contribute no purged range")
				require.Len(t, events, 1)

				u := events[0].GetUnverifiableRange()
				require.NotNil(t, u)
				require.Equal(t,
					servicepb.CheckStoreUnverifiableReason_CHECK_STORE_UNVERIFIABLE_REASON_ARCHIVED_CHAPTER_BOUNDS_UNUSABLE,
					u.GetReason())

				// The bounds must be reported as undetermined, never echoed
				// back — the rejected values are garbage by construction and a
				// client would render them as a literal range.
				require.Zero(t, u.GetRangeStart())
				require.Zero(t, u.GetRangeEnd())
			})
		}
	})
}

func TestVerifyAuditStructure(t *testing.T) {
	t.Parallel()

	success := func(minLog, maxLog uint64, orderCount uint32) *auditpb.AuditEntry {
		return &auditpb.AuditEntry{
			Sequence:   7,
			OrderCount: orderCount,
			Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{MinLogSequence: minLog, MaxLogSequence: maxLog},
			},
		}
	}

	item := func(idx uint32, logSeq uint64) *auditpb.AuditItem {
		return &auditpb.AuditItem{OrderIndex: idx, LogSequence: logSeq}
	}

	tests := []struct {
		name              string
		entry             *auditpb.AuditEntry
		items             []*auditpb.AuditItem
		wantOK            bool
		wantAuthenticated uint64
		wantUnverifiable  uint64
	}{
		{
			name:              "well-formed success",
			entry:             success(10, 11, 2),
			items:             []*auditpb.AuditItem{item(0, 10), item(1, 11)},
			wantOK:            true,
			wantAuthenticated: 2,
		},
		{
			name:              "success with an idempotent reference item",
			entry:             success(10, 10, 2),
			items:             []*auditpb.AuditItem{item(0, 10), item(1, 0)},
			wantOK:            true,
			wantAuthenticated: 1,
		},
		{
			name:              "legacy replay item below the range is tolerated",
			entry:             success(10, 10, 2),
			items:             []*auditpb.AuditItem{item(0, 10), item(1, 3)},
			wantOK:            true,
			wantAuthenticated: 1,
		},
		{
			name:             "order_count disagrees with item count",
			entry:            success(10, 11, 3),
			items:            []*auditpb.AuditItem{item(0, 10), item(1, 11)},
			wantOK:           false,
			wantUnverifiable: 2,
		},
		{
			name:             "order_index out of range",
			entry:            success(10, 11, 2),
			items:            []*auditpb.AuditItem{item(0, 10), item(5, 11)},
			wantOK:           false,
			wantUnverifiable: 2,
		},
		{
			name:             "duplicate order_index",
			entry:            success(10, 11, 2),
			items:            []*auditpb.AuditItem{item(0, 10), item(0, 11)},
			wantOK:           false,
			wantUnverifiable: 2,
		},
		{
			name:             "two items claim one log sequence",
			entry:            success(10, 11, 2),
			items:            []*auditpb.AuditItem{item(0, 10), item(1, 10)},
			wantOK:           false,
			wantUnverifiable: 2,
		},
		{
			name:   "inverted success range",
			entry:  success(11, 10, 0),
			items:  nil,
			wantOK: false,
		},
		{
			name: "failure with one zero-log item per order is legitimate",
			entry: &auditpb.AuditEntry{
				Sequence:   7,
				OrderCount: 3,
				Outcome: &auditpb.AuditEntry_Failure{
					Failure: &auditpb.AuditFailure{Reason: commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND},
				},
			},
			items:  []*auditpb.AuditItem{item(0, 0), item(1, 0), item(2, 0)},
			wantOK: true,
		},
		{
			name: "failure whose item claims a log sequence",
			entry: &auditpb.AuditEntry{
				Sequence:   7,
				OrderCount: 2,
				Outcome: &auditpb.AuditEntry_Failure{
					Failure: &auditpb.AuditFailure{Reason: commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND},
				},
			},
			items:  []*auditpb.AuditItem{item(0, 0), item(1, 42)},
			wantOK: false,
		},
		{
			name: "failure with fewer items than order_count",
			entry: &auditpb.AuditEntry{
				Sequence:   7,
				OrderCount: 3,
				Outcome: &auditpb.AuditEntry_Failure{
					Failure: &auditpb.AuditFailure{Reason: commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND},
				},
			},
			items:  []*auditpb.AuditItem{item(0, 0)},
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := newChainVerification()

			var (
				errs     []*servicepb.CheckStoreError
				unproven []*servicepb.CheckStoreUnverifiableRange
			)

			// The switch variable is named `ev`, not `t`: `t` is the *testing.T
			// of the enclosing subtest and shadowing it here would be a trap.
			ok := verifyAuditStructure(tc.entry, tc.items, v, func(e *servicepb.CheckStoreEvent) {
				switch ev := e.GetType().(type) {
				case *servicepb.CheckStoreEvent_Error:
					errs = append(errs, ev.Error)
				case *servicepb.CheckStoreEvent_UnverifiableRange:
					unproven = append(unproven, ev.UnverifiableRange)
				}
			})

			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.wantAuthenticated, v.authenticated.total())
			require.Equal(t, tc.wantUnverifiable, v.unverifiable.total())

			if tc.wantOK {
				require.Empty(t, errs)
				require.Empty(t, unproven)

				return
			}

			// Every rejection reports exactly one structural error.
			require.Len(t, errs, 1)
			require.Equal(t,
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_AUDIT_STRUCTURE_INVALID,
				errs[0].GetErrorType())

			// A rejection that blanks out a log span must also declare that
			// span unverifiable, so compareLogs skipping it is visible.
			if tc.wantUnverifiable > 0 {
				require.Len(t, unproven, 1)
			} else {
				require.Empty(t, unproven)
			}
		})
	}
}

// TestLogRangeSetGapsIn covers the interval subtraction compareLogs uses to
// derive missing log ranges. The last case is the load-bearing one: an
// enormous uncovered range must come back as ONE logRange, since the whole
// point is that the reverse pass never iterates a store-supplied width.
func TestLogRangeSetGapsIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		covered [][2]uint64
		r       logRange
		want    []logRange
	}{
		{
			name: "empty covered set yields the whole range",
			r:    logRange{min: 5, max: 9},
			want: []logRange{{min: 5, max: 9}},
		},
		{
			name:    "full overlap yields nothing",
			covered: [][2]uint64{{1, 20}},
			r:       logRange{min: 5, max: 9},
			want:    nil,
		},
		{
			name:    "exact cover yields nothing",
			covered: [][2]uint64{{5, 9}},
			r:       logRange{min: 5, max: 9},
			want:    nil,
		},
		{
			name:    "covered set entirely above the range",
			covered: [][2]uint64{{100, 200}},
			r:       logRange{min: 5, max: 9},
			want:    []logRange{{min: 5, max: 9}},
		},
		{
			name:    "covered set entirely below the range",
			covered: [][2]uint64{{1, 4}},
			r:       logRange{min: 5, max: 9},
			want:    []logRange{{min: 5, max: 9}},
		},
		{
			name:    "covered prefix",
			covered: [][2]uint64{{5, 6}},
			r:       logRange{min: 5, max: 9},
			want:    []logRange{{min: 7, max: 9}},
		},
		{
			name:    "covered prefix overlapping the lower edge",
			covered: [][2]uint64{{1, 6}},
			r:       logRange{min: 5, max: 9},
			want:    []logRange{{min: 7, max: 9}},
		},
		{
			name:    "covered suffix",
			covered: [][2]uint64{{8, 9}},
			r:       logRange{min: 5, max: 9},
			want:    []logRange{{min: 5, max: 7}},
		},
		{
			name:    "covered suffix overlapping the upper edge",
			covered: [][2]uint64{{8, 20}},
			r:       logRange{min: 5, max: 9},
			want:    []logRange{{min: 5, max: 7}},
		},
		{
			name:    "covered middle produces two gaps",
			covered: [][2]uint64{{7, 7}},
			r:       logRange{min: 5, max: 9},
			want:    []logRange{{min: 5, max: 6}, {min: 8, max: 9}},
		},
		{
			name:    "multiple covered intervals inside one range",
			covered: [][2]uint64{{2, 3}, {6, 6}, {9, 10}},
			r:       logRange{min: 1, max: 12},
			want:    []logRange{{min: 1, max: 1}, {min: 4, max: 5}, {min: 7, max: 8}, {min: 11, max: 12}},
		},
		{
			name:    "single sequence range uncovered",
			covered: [][2]uint64{{1, 4}, {6, 9}},
			r:       logRange{min: 5, max: 5},
			want:    []logRange{{min: 5, max: 5}},
		},
		{
			name:    "single sequence range covered",
			covered: [][2]uint64{{1, 9}},
			r:       logRange{min: 5, max: 5},
			want:    nil,
		},
		{
			name:    "inverted range yields nothing",
			covered: nil,
			r:       logRange{min: 9, max: 5},
			want:    nil,
		},
		{
			name:    "covered range ending at the maximum uint64 does not overflow",
			covered: [][2]uint64{{10, math.MaxUint64}},
			r:       logRange{min: 5, max: math.MaxUint64},
			want:    []logRange{{min: 5, max: 9}},
		},
		{
			name:    "an enormous uncovered range is a single gap",
			covered: nil,
			r:       logRange{min: 1, max: 1 << 62},
			want:    []logRange{{min: 1, max: 1 << 62}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var s logRangeSet
			for _, c := range tc.covered {
				s.add(c[0], c[1])
			}

			require.Equal(t, tc.want, s.gapsIn(tc.r))
		})
	}
}

// TestReportMissingLogs pins the emission contract: individual events up to the
// cap, then exactly one summary event naming the residual range. A range whose
// width comes from a forged AuditSuccess.max_log_sequence must not be
// enumerated.
func TestReportMissingLogs(t *testing.T) {
	t.Parallel()

	collect := func(gap logRange) []*servicepb.CheckStoreError {
		var errs []*servicepb.CheckStoreError

		reportMissingLogs(gap, func(e *servicepb.CheckStoreEvent) {
			require.NotNil(t, e.GetError())
			require.Equal(t,
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP,
				e.GetError().GetErrorType(),
				"a collapsed report is still a divergence report and must keep the SEQUENCE_GAP type")

			errs = append(errs, e.GetError())
		})

		return errs
	}

	t.Run("a small gap is fully enumerated", func(t *testing.T) {
		t.Parallel()

		errs := collect(logRange{min: 1, max: 3})

		require.Len(t, errs, 3)
		require.Equal(t, []uint64{1, 2, 3}, []uint64{
			errs[0].GetLogSequence(), errs[1].GetLogSequence(), errs[2].GetLogSequence(),
		})

		for _, e := range errs {
			require.NotContains(t, e.GetMessage(), "individual reporting stopped")
		}
	})

	t.Run("a gap exactly at the cap emits no summary", func(t *testing.T) {
		t.Parallel()

		errs := collect(logRange{min: 1, max: maxEnumeratedGapSequences})

		require.Len(t, errs, maxEnumeratedGapSequences)
		require.Equal(t, uint64(maxEnumeratedGapSequences), errs[len(errs)-1].GetLogSequence())
		require.NotContains(t, errs[len(errs)-1].GetMessage(), "individual reporting stopped")
	})

	t.Run("one sequence past the cap is collapsed into a summary", func(t *testing.T) {
		t.Parallel()

		errs := collect(logRange{min: 1, max: maxEnumeratedGapSequences + 1})

		require.Len(t, errs, maxEnumeratedGapSequences+1)

		summary := errs[len(errs)-1]
		require.Equal(t, uint64(maxEnumeratedGapSequences+1), summary.GetLogSequence(),
			"the summary must point at the first un-enumerated sequence")
		require.Contains(t, summary.GetMessage(), "logs 1025-1025 (1 sequences)")
	})

	t.Run("an enormous gap terminates with a bounded event count", func(t *testing.T) {
		t.Parallel()

		errs := collect(logRange{min: 1, max: 1 << 62})

		require.Len(t, errs, maxEnumeratedGapSequences+1,
			"a forged max_log_sequence must not be enumerated")

		summary := errs[len(errs)-1]
		require.Equal(t, uint64(maxEnumeratedGapSequences+1), summary.GetLogSequence())
		require.Contains(t, summary.GetMessage(), "4611686018427387904")
		require.Contains(t, summary.GetMessage(), "sequences) are authenticated by the audit chain but absent")
	})

	t.Run("a gap ending at the maximum uint64 does not overflow", func(t *testing.T) {
		t.Parallel()

		errs := collect(logRange{min: math.MaxUint64 - 1, max: math.MaxUint64})

		require.Len(t, errs, 2)
		require.Equal(t, uint64(math.MaxUint64-1), errs[0].GetLogSequence())
		require.Equal(t, uint64(math.MaxUint64), errs[1].GetLogSequence())
	})

	t.Run("a single missing sequence emits one event", func(t *testing.T) {
		t.Parallel()

		errs := collect(logRange{min: math.MaxUint64, max: math.MaxUint64})

		require.Len(t, errs, 1)
		require.Equal(t, uint64(math.MaxUint64), errs[0].GetLogSequence())
	})
}

// TestCompareLogsBoundsForgedAuthenticatedRange is the regression test for the
// hang: authenticated is fed a range as wide as a tampered
// AuditSuccess.max_log_sequence would make it, and compareLogs must still
// return with a bounded number of events. Against the pre-fix per-sequence
// reverse pass this test never terminates.
func TestCompareLogsBoundsForgedAuthenticatedRange(t *testing.T) {
	t.Parallel()

	// compareLogs reads only the SubColdLog prefix, so an empty store is the
	// worst case: nothing is covered and the whole forged range is missing.
	store := createTestStore(t)

	reader, err := store.NewReadHandle()
	require.NoError(t, err)

	t.Cleanup(func() { _ = reader.Close() })

	c := &Checker{}

	t.Run("one forged interval emits at most the cap plus one summary", func(t *testing.T) {
		t.Parallel()

		var (
			authenticated logRangeSet
			purged        logRangeSet
			unverifiable  logRangeSet
			count         int
			summaries     int
		)

		authenticated.add(1, 1<<62)

		require.NoError(t, c.compareLogs(reader, &authenticated, &purged, &unverifiable,
			func(e *servicepb.CheckStoreEvent) {
				require.Equal(t,
					servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP,
					e.GetError().GetErrorType())

				count++

				if strings.Contains(e.GetError().GetMessage(), "individual reporting stopped") {
					summaries++
				}
			}))

		require.Equal(t, maxEnumeratedGapSequences+1, count)
		require.Equal(t, 1, summaries)
	})

	t.Run("an unverifiable span splits the forged interval into two bounded gaps", func(t *testing.T) {
		t.Parallel()

		var (
			authenticated logRangeSet
			purged        logRangeSet
			unverifiable  logRangeSet
			count         int
			summaries     int
		)

		authenticated.add(1, 1<<62)
		unverifiable.add(1<<40, 1<<41)

		require.NoError(t, c.compareLogs(reader, &authenticated, &purged, &unverifiable,
			func(e *servicepb.CheckStoreEvent) {
				count++

				if strings.Contains(e.GetError().GetMessage(), "individual reporting stopped") {
					summaries++
				}
			}))

		require.Equal(t, 2*(maxEnumeratedGapSequences+1), count,
			"each of the two gaps is bounded independently")
		require.Equal(t, 2, summaries)
	})

	t.Run("a fully covered authenticated range reports nothing", func(t *testing.T) {
		t.Parallel()

		var (
			authenticated logRangeSet
			purged        logRangeSet
			unverifiable  logRangeSet
		)

		authenticated.add(1, 1<<62)
		unverifiable.add(1, 1<<62)

		require.NoError(t, c.compareLogs(reader, &authenticated, &purged, &unverifiable,
			func(*servicepb.CheckStoreEvent) {
				t.Fatal("no event expected when every authenticated sequence is accounted for")
			}))
	})
}
