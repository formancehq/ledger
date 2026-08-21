package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestCheckFindingsClassifyCoverageGaps pins the split `store check` was missing:
// a pass the checker could not complete is not a divergence, and the verdict this
// command reports must say which of the two it found.
//
// The regression it guards is a misreport, not a crash. `store check` counted
// every event as an integrity error, so an archived store with no baseline
// checkpoint -- a node restored from a backup before it closes a chapter of its
// own, or one whose non-fatal createBaselineSnapshot failed -- was announced as
// `store validation failed: 1 integrity error(s)` with `"valid": false,
// "errorCount": 1`. Nothing there is known to be wrong. `restore validate` and
// `store bootstrap --validate` already classified; this is the third consumer of
// the same checker and the only one with a machine-readable contract, so the
// conflation was worst here.
//
// Asserted through checkFindings rather than the command: the classification and
// the payload are the contract, and reaching them through runCheck would need a
// gRPC stream and a terminal for no added coverage.
func TestCheckFindingsClassifyCoverageGaps(t *testing.T) {
	t.Parallel()

	const (
		// The routine gap on a baseline-less archived store: the projection
		// comparisons are skipped wholesale.
		archivedGap = servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_ARCHIVED_STATE_VERIFICATION_INCOMPLETE
		// A real divergence: the store contradicts its own audit chain.
		divergence = servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH
	)

	for _, tc := range []struct {
		name       string
		errorTypes []servicepb.CheckStoreErrorType
		want       checkResult
	}{
		{
			name: "nothing found is clean and valid",
			want: checkResult{Valid: true, Outcome: "clean"},
		},
		{
			// The load-bearing case. Before the split this produced
			// {valid: false, errorCount: 1} and a failure message naming an
			// integrity error that no pass ever found.
			name:       "a coverage gap alone is incomplete, not an error",
			errorTypes: []servicepb.CheckStoreErrorType{archivedGap},
			want: checkResult{
				Valid:            false,
				Outcome:          "incomplete",
				ErrorCount:       0,
				CoverageGapCount: 1,
			},
		},
		{
			name:       "a divergence fails",
			errorTypes: []servicepb.CheckStoreErrorType{divergence},
			want: checkResult{
				Valid:      false,
				Outcome:    "failed",
				ErrorCount: 1,
			},
		},
		{
			// A divergence outranks any number of gaps: the store is demonstrably
			// wrong, and incomplete coverage must not soften that into a warning.
			name:       "a divergence outranks a coverage gap",
			errorTypes: []servicepb.CheckStoreErrorType{archivedGap, divergence, archivedGap},
			want: checkResult{
				Valid:            false,
				Outcome:          "failed",
				ErrorCount:       1,
				CoverageGapCount: 2,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var findings checkFindings

			for _, errorType := range tc.errorTypes {
				finding := &servicepb.CheckStoreError{ErrorType: errorType}

				require.Equal(t, errorType == archivedGap, findings.add(finding),
					"add must report the gap/divergence split back to the caller, "+
						"which is what decides whether the printed line says WARNING "+
						"or ERROR")
			}

			got := findings.result()

			// Compared field by field rather than as a whole struct: the two slices
			// carry the findings themselves and are asserted by their counts, which
			// is what a consumer switches on.
			require.Equal(t, tc.want.Valid, got.Valid,
				"valid is reserved for the clean outcome: every pass ran and none "+
					"found a divergence")
			require.Equal(t, tc.want.Outcome, got.Outcome)
			require.Equal(t, tc.want.ErrorCount, got.ErrorCount,
				"errorCount counts divergences only")
			require.Equal(t, tc.want.CoverageGapCount, got.CoverageGapCount)
			require.Len(t, got.Errors, tc.want.ErrorCount)
			require.Len(t, got.CoverageGaps, tc.want.CoverageGapCount)
		})
	}
}
