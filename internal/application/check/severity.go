package check

import "github.com/formancehq/ledger/v3/internal/proto/servicepb"

// IsCoverageGap reports whether an error type means "this could not be
// verified" rather than "this is wrong".
//
// CheckStoreEvent has only Error and Progress variants, so a pass that cannot
// build a trustworthy expectation has to ride the error channel to say so. Two
// passes do exactly that, and both are reachable on entirely healthy stores:
//
//   - SIGNING_VERIFICATION_INCOMPLETE, whenever archived chapters exist and no
//     cold reader is attached. Both restore-side callers pass none by
//     construction, and the baseline checkpoint is never backed up, so a healthy
//     backup of an archived cluster produces this finding every time.
//   - LOG_VERIFICATION_INCOMPLETE, when the chain walk stopped short and the
//     audited log maximum is only a prefix maximum.
//
// Neither is a divergence. Suppressing them was considered and rejected — the
// statement is true and a checker that silently skips a pass is worse than one
// that admits it — so consumers that turn findings into a pass/fail verdict
// classify them instead. Callers that merely display findings need not.
//
// Kept here, next to the passes that emit these types, so the classification
// cannot drift away from them: a new *_VERIFICATION_INCOMPLETE class belongs in
// this list at the point it is introduced.
func IsCoverageGap(errorType servicepb.CheckStoreErrorType) bool {
	switch errorType {
	case servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_VERIFICATION_INCOMPLETE,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE:
		return true
	default:
		return false
	}
}
