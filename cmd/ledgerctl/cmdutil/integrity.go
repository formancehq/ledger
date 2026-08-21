package cmdutil

import (
	"fmt"

	"github.com/pterm/pterm"
)

// IntegrityResult maps the number of integrity errors found by a check onto a
// command result. Any non-zero count is a hard failure so the CLI exits
// non-zero and callers chaining commands (e.g. `restore validate &&
// restore finalize`, or `store bootstrap` proceeding to finalize) stop before
// acting on a corrupt store or backup.
//
// subject names what was checked and is used to build the error message, e.g.
// IntegrityResult("backup validation", 3) yields
// "backup validation failed: 3 integrity error(s)".
//
// Divergences only. A pass the checker could not complete is not a divergence
// and must not be counted here — see ReportIntegrityVerdict.
func IntegrityResult(subject string, errorCount int) error {
	if errorCount > 0 {
		return fmt.Errorf("%s failed: %d integrity error(s)", subject, errorCount)
	}

	return nil
}

// IntegrityVerdictInput carries the findings of one completed check.
type IntegrityVerdictInput struct {
	// Subject names what was checked, for the failure message. See
	// IntegrityResult.
	Subject string
	// CleanMessage is printed verbatim when everything was verified and nothing
	// diverged. It is the only case that may claim validity.
	CleanMessage string
	// Errors counts divergences: the store contradicts its own audit chain.
	Errors int
	// CoverageGaps counts passes that could not be completed, so part of the
	// store was never compared. Excluded from the exit status.
	CoverageGaps int
}

// IntegrityOutcome is the verdict of one completed check.
type IntegrityOutcome int

const (
	// IntegrityOutcomeClean means every pass ran and none found a divergence.
	// The only outcome entitled to claim validity.
	IntegrityOutcomeClean IntegrityOutcome = iota
	// IntegrityOutcomeIncomplete means nothing diverged, but at least one pass
	// could not be completed, so part of the store was never compared.
	IntegrityOutcomeIncomplete
	// IntegrityOutcomeFailed means at least one divergence was found.
	IntegrityOutcomeFailed
)

// ClassifyIntegrity maps a check's findings onto one of three outcomes.
//
// Three, not two. Divergences fail. Everything verified with nothing wrong is
// clean. In between sits a run that found nothing wrong but could not complete
// every pass: on an archived store the checker has no baseline checkpoint to
// compare the primary-store projections against (the baseline is never part of a
// backup), so volumes, metadata, transactions, boundaries and the rest are
// skipped wholesale. Announcing that as "valid" claims far more than the run
// established.
//
// A divergence outranks any number of gaps — the store is demonstrably wrong,
// and incomplete coverage must not soften that into a warning.
//
// Coverage gaps deliberately do NOT fail. The gap is permanent for that shape —
// there is no cold reader or baseline to attach on a restore path — so failing
// on it would leave every archived cluster unable to validate a backup at all.
// Whether an unverified-projections backup should instead block automation is a
// separate operator-contract decision, tracked apart from this.
//
// Split out from ReportIntegrityVerdict so the mapping is testable without
// capturing terminal output: asserting the printed line would mean swapping a
// pterm printer's global Writer, which races under t.Parallel().
func ClassifyIntegrity(errorCount, coverageGaps int) IntegrityOutcome {
	switch {
	case errorCount > 0:
		return IntegrityOutcomeFailed
	case coverageGaps > 0:
		return IntegrityOutcomeIncomplete
	default:
		return IntegrityOutcomeClean
	}
}

// ReportIntegrityVerdict prints the verdict for a completed check and returns a
// non-nil error when the command must exit non-zero.
func ReportIntegrityVerdict(in IntegrityVerdictInput) error {
	switch ClassifyIntegrity(in.Errors, in.CoverageGaps) {
	case IntegrityOutcomeFailed:
		return IntegrityResult(in.Subject, in.Errors)
	case IntegrityOutcomeIncomplete:
		pterm.Warning.Printfln(
			"Audit chain verified; %d pass(es) could not be completed "+
				"(projections NOT verified - see WARNING lines above)",
			in.CoverageGaps)
	case IntegrityOutcomeClean:
		pterm.Success.Println(in.CleanMessage)
	}

	return nil
}
