package cmdutil

import (
	"errors"
	"fmt"

	"github.com/pterm/pterm"
)

// ErrIncompleteVerification reports that a check found no divergence but could
// not complete every pass, so part of the store was never compared. It is
// distinct from a divergence: the store is not known to be wrong, it is only
// unverified. Callers match it with errors.Is rather than on the message.
var ErrIncompleteVerification = errors.New("verification incomplete")

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
	// store was never compared. Non-zero fails the command unless
	// AllowIncomplete is set.
	CoverageGaps int
	// AllowIncomplete is the operator acknowledging that an unverified store is
	// acceptable for this invocation. It downgrades coverage gaps from a failure
	// to a warning. It never softens a divergence.
	AllowIncomplete bool
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

// String names the outcome for a machine-readable payload.
//
// Kept next to the constants so the JSON vocabulary a consumer switches on
// cannot drift away from the Go one, the way a name spelled out at the call site
// would.
func (o IntegrityOutcome) String() string {
	switch o {
	case IntegrityOutcomeClean:
		return "clean"
	case IntegrityOutcomeIncomplete:
		return "incomplete"
	case IntegrityOutcomeFailed:
		return "failed"
	default:
		// Unreachable: ClassifyIntegrity is the only producer and it returns one
		// of the three above. Named rather than left blank so a value that did
		// escape reads as a defect instead of an empty verdict.
		return "unknown"
	}
}

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
// This states what the run established, not what the command does about it: an
// incomplete run is not a divergence, and the two must stay distinguishable so
// the acknowledgement flag can accept one without ever accepting the other.
// ReportIntegrityVerdict owns the exit-status policy.
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
//
// Incomplete coverage fails closed. Both `restore validate` and
// `store bootstrap --validate` use this return value as their automation gate,
// so an unverified store that exits zero lets `restore validate &&
// restore finalize` — or a bootstrap — act on projections nothing compared.
// Printing "incomplete" instead of "valid" informs a human reading the output
// and protects no caller.
//
// The gap is permanent on those paths: there is no cold reader and no baseline
// to attach, so a healthy backup of an archived cluster always reports at least
// one. AllowIncomplete is the way through — the operator states that an
// unverified store is acceptable for this invocation, which keeps archived
// clusters able to validate while making the acknowledgement explicit and
// auditable in the command line rather than implicit in the exit code.
func ReportIntegrityVerdict(in IntegrityVerdictInput) error {
	switch ClassifyIntegrity(in.Errors, in.CoverageGaps) {
	case IntegrityOutcomeFailed:
		return IntegrityResult(in.Subject, in.Errors)
	case IntegrityOutcomeIncomplete:
		pterm.Warning.Printfln(
			"Audit chain verified; %d pass(es) could not be completed "+
				"(projections NOT verified - see WARNING lines above)",
			in.CoverageGaps)

		if !in.AllowIncomplete {
			return fmt.Errorf(
				"%s incomplete: %d pass(es) could not be completed, "+
					"so the projections were NOT verified; "+
					"pass --allow-incomplete to accept an unverified store: %w",
				in.Subject, in.CoverageGaps, ErrIncompleteVerification)
		}

		pterm.Info.Println("Accepted as incomplete (--allow-incomplete)")
	case IntegrityOutcomeClean:
		pterm.Success.Println(in.CleanMessage)
	}

	return nil
}
