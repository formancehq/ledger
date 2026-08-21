package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegrityResult exercises the verdict mapping directly: any integrity
// error count must produce a non-nil error so the CLI exits non-zero and a
// chain such as `restore validate && restore finalize` stops before finalizing.
func TestIntegrityResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		subject    string
		errorCount int
		wantErr    string
	}{
		{name: "no errors is valid", subject: "backup validation", errorCount: 0, wantErr: ""},
		{name: "one error fails", subject: "backup validation", errorCount: 1, wantErr: "backup validation failed: 1 integrity error(s)"},
		{name: "many errors fail", subject: "store validation", errorCount: 5, wantErr: "store validation failed: 5 integrity error(s)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := IntegrityResult(tt.subject, tt.errorCount)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestReportIntegrityVerdict pins the three-outcome mapping, and in particular
// that a coverage gap is neither a failure nor a clean bill of health.
//
// Both halves are regressions that have actually happened on this code:
// counting gaps as errors failed `store bootstrap --validate` on healthy
// archived backups, and printing the clean message over them let
// `restore validate` report a backup valid while the projection comparisons had
// been skipped wholesale. The outcome and the exit status are therefore asserted
// as two independent properties: the outcome decides which line is printed, and
// only IntegrityOutcomeFailed may exit non-zero.
func TestReportIntegrityVerdict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		errors       int
		coverageGaps int
		wantOutcome  IntegrityOutcome
		wantErr      string
	}{
		{
			name:        "nothing found is clean",
			wantOutcome: IntegrityOutcomeClean,
		},
		{
			// The load-bearing case: no divergence, but part of the store was never
			// compared. Must NOT fail — the gap is permanent on a restore path, so
			// failing here dead-ends every archived cluster.
			name:         "coverage gaps alone do not fail",
			coverageGaps: 2,
			wantOutcome:  IntegrityOutcomeIncomplete,
		},
		{
			name:        "a divergence fails",
			errors:      1,
			wantOutcome: IntegrityOutcomeFailed,
			wantErr:     "backup validation failed: 1 integrity error(s)",
		},
		{
			// A divergence outranks the gaps: the store is demonstrably wrong, and
			// the incomplete-coverage line must not soften that into a warning.
			name:         "a divergence outranks coverage gaps",
			errors:       3,
			coverageGaps: 7,
			wantOutcome:  IntegrityOutcomeFailed,
			wantErr:      "backup validation failed: 3 integrity error(s)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.wantOutcome,
				ClassifyIntegrity(tt.errors, tt.coverageGaps),
				"the outcome decides which verdict line the operator sees")

			err := ReportIntegrityVerdict(IntegrityVerdictInput{
				Subject:      "backup validation",
				CleanMessage: "Backup is valid - no integrity errors found",
				Errors:       tt.errors,
				CoverageGaps: tt.coverageGaps,
			})

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
