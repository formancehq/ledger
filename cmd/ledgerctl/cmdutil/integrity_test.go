package cmdutil

import (
	"errors"
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

// TestReportIntegrityVerdict pins the three-outcome mapping and, separately,
// which of those outcomes may exit zero.
//
// Two independent properties, because two different regressions live here. The
// outcome decides which line the operator reads: counting gaps as errors failed
// `store bootstrap --validate` on healthy archived backups, and printing the
// clean message over them let `restore validate` report a backup valid while the
// projection comparisons had been skipped wholesale.
//
// The exit status is the automation gate, and it fails closed. An incomplete run
// established nothing about the projections, so returning nil for it let
// `restore validate && restore finalize` act on a store nothing compared —
// changing the printed verdict informs a human and protects no script. Only an
// explicit AllowIncomplete may exit zero on that outcome, and it must never
// touch a divergence.
//
// Deliberately not parallel, at either level. ReportIntegrityVerdict prints
// through pterm's package-level printers, and PrefixPrinter.Printfln takes a
// pointer receiver and moves LineNumberOffset around the write, so two
// goroutines calling it mutate the shared pterm.Warning (EN-1781 is the same
// class of problem). Go holds parallel tests until the sequential phase of their
// parent finishes, so dropping t.Parallel() here also keeps this test from
// overlapping the parallel tests elsewhere in the package. ClassifyIntegrity is
// split out of ReportIntegrityVerdict precisely so the mapping can be asserted
// without any of that, and it is asserted separately below.
func TestReportIntegrityVerdict(t *testing.T) {
	tests := []struct {
		name            string
		errors          int
		coverageGaps    int
		allowIncomplete bool
		wantOutcome     IntegrityOutcome
		wantErr         string
		// wantIncomplete asserts the error is the incomplete sentinel rather
		// than a divergence, so the two stay distinguishable by type and not
		// only by their message.
		wantIncomplete bool
	}{
		{
			name:        "nothing found is clean",
			wantOutcome: IntegrityOutcomeClean,
		},
		{
			// The load-bearing case: no divergence, but part of the store was never
			// compared. Must fail — this return value is the automation gate, and an
			// unverified store must not pass it silently.
			name:           "coverage gaps alone fail closed",
			coverageGaps:   2,
			wantOutcome:    IntegrityOutcomeIncomplete,
			wantIncomplete: true,
			wantErr: "backup validation incomplete: 2 pass(es) could not be completed, " +
				"so the projections were NOT verified; " +
				"pass --allow-incomplete to accept an unverified store: verification incomplete",
		},
		{
			// The escape hatch. The gap is permanent on a restore path, so without
			// this an archived cluster could never validate a backup at all.
			name:            "acknowledged coverage gaps pass",
			coverageGaps:    2,
			allowIncomplete: true,
			wantOutcome:     IntegrityOutcomeIncomplete,
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
		{
			// The acknowledgement is scoped to what was not checked. It says "an
			// unverified store is acceptable", never "a wrong one is".
			name:            "allow-incomplete never accepts a divergence",
			errors:          1,
			coverageGaps:    4,
			allowIncomplete: true,
			wantOutcome:     IntegrityOutcomeFailed,
			wantErr:         "backup validation failed: 1 integrity error(s)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantOutcome,
				ClassifyIntegrity(tt.errors, tt.coverageGaps),
				"the outcome decides which verdict line the operator sees, and is "+
					"independent of whether the run is accepted")

			err := ReportIntegrityVerdict(IntegrityVerdictInput{
				Subject:         "backup validation",
				CleanMessage:    "Backup is valid - no integrity errors found",
				Errors:          tt.errors,
				CoverageGaps:    tt.coverageGaps,
				AllowIncomplete: tt.allowIncomplete,
			})

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				require.Equal(t, tt.wantIncomplete,
					errors.Is(err, ErrIncompleteVerification),
					"an unverified store and a divergent one must be told apart by "+
						"type: only the former is ever acceptable")

				return
			}

			require.NoError(t, err)
		})
	}
}
