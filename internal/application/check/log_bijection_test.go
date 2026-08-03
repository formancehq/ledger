package check

import (
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
			name: "failure with order_count set and no items is legitimate",
			entry: &auditpb.AuditEntry{
				Sequence:   7,
				OrderCount: 3,
				Outcome: &auditpb.AuditEntry_Failure{
					Failure: &auditpb.AuditFailure{Reason: commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND},
				},
			},
			items:  nil,
			wantOK: true,
		},
		{
			name: "failure carrying an item",
			entry: &auditpb.AuditEntry{
				Sequence:   7,
				OrderCount: 1,
				Outcome: &auditpb.AuditEntry_Failure{
					Failure: &auditpb.AuditFailure{Reason: commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND},
				},
			},
			items:  []*auditpb.AuditItem{item(0, 42)},
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
