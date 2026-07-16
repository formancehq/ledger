package numscript

import (
	"context"
	"errors"
	"fmt"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// convertNumscriptError translates known numscript library errors into domain
// errors so that the gRPC error mapper can return proper status codes. Library
// errors that have no specific mapping are wrapped as ErrNumscriptRuntime
// (KindInternal) — the script ran and produced an unhandled failure mode,
// which is a server bug the user cannot fix.
func convertNumscriptError(err error) domain.Describable {
	if err == nil {
		return nil
	}

	var missingFunds numscriptlib.MissingFundsErr
	if errors.As(err, &missingFunds) {
		// Interpreter limitation: numscriptlib.MissingFundsErr carries only
		// {Asset, Needed, Available, parser.Range} — it exposes neither the
		// failing account nor the color of the bucket that ran short (see the
		// pinned numscript's interpreter_error.go / interpreter.go, where the
		// error is built with s.CurrentAsset only). A single asset can be
		// sourced from several (account, color) buckets in one script and the
		// Range points into source text, not a resolved bucket, so the color
		// cannot be recovered reliably from the error alone. We therefore leave
		// Color (and Account) empty rather than inventing a value that is not
		// reliably known — an empty Color here means "unknown", NOT the
		// uncolored bucket. ColorKnown is left false so ErrInsufficientFunds
		// omits the color key from its wire metadata, keeping this "unknown"
		// distinct from a definite hit on the uncolored bucket (color: "").
		// Surfacing the true color would require the interpreter to attach the
		// resolved (account, color) to MissingFundsErr upstream, at which point
		// this path sets the real Color with ColorKnown: true.
		return &domain.ErrInsufficientFunds{
			Asset:   missingFunds.Asset,
			Amount:  missingFunds.Needed.String(),
			Balance: missingFunds.Available.String(),
			// ColorKnown intentionally false: color is unresolved on this path.
		}
	}

	// errors.As walks the chain in case a caller has already wrapped the
	// numscript-library error in a Describable. This also unwraps
	// QueryBalanceError / QueryMetadataError, whose WrappedError is the Store
	// error — so a rejected colored read (domain.ErrColoredBalanceUnsupported)
	// surfaces here as the validation sentinel it already is.
	var d domain.Describable
	if errors.As(err, &d) {
		return d
	}

	// Every other library error becomes ErrNumscriptRuntime (KindInternal).
	//
	// Note: this deliberately does NOT reclassify deterministic client-side
	// resolver errors (undeclared/mistyped variable, bad portion, …) to
	// KindValidation, even though that would let admission surface them
	// definitively instead of forwarding them as a retryable PRELOAD_UNAVAILABLE
	// under an idempotency key. The reason is that the upstream library reports
	// script-deterministic errors and *state-dependent* ones (e.g. MetadataNotFound
	// when a meta()-referenced account was deleted after an earlier success) with
	// the same leaf InterpreterError shape, and the concrete types live in an
	// internal package we cannot import to tell them apart. The state-dependent
	// case MUST stay forwardable so the FSM can replay a frozen success (EN-1406
	// idempotent-replay), so we keep the conservative KindInternal classification
	// for all of them; reclassifying by "leaf error" would break that replay. A
	// precise split needs upstream to expose an error category. See the tracking
	// ticket for the deterministic-error UX gap.
	return &domain.ErrNumscriptRuntime{Detail: err.Error()}
}

// panicError marks a Describable that originated from a recovered panic inside
// the numscript library (as opposed to a normal library-returned error). It
// behaves exactly like the ErrNumscriptRuntime it wraps for every external
// concern — same Error/Reason/Metadata, so the gRPC/HTTP mapping is unchanged —
// but IsPanic can recognise it so a caller that would otherwise soften a normal
// resolution error (e.g. the FSM apply path funnelling resolve errors to
// ErrStaleInputsResolution) can instead surface the panic loudly. A recovered
// panic is a "should not happen" (invariant #7): masking it as a retryable
// stale-inputs error would hide a real defect.
type panicError struct {
	*domain.ErrNumscriptRuntime
}

// Unwrap exposes the embedded ErrNumscriptRuntime so errors.As(err,
// **domain.ErrNumscriptRuntime) succeeds: a recovered panic must present
// externally as an ordinary ErrNumscriptRuntime (same Reason/Kind/mapping),
// while IsPanic keeps it internally distinguishable.
func (e panicError) Unwrap() error { return e.ErrNumscriptRuntime }

// IsPanic reports whether err was produced by a numscript-library panic that a
// Safe* wrapper recovered, rather than by a normal library-returned error.
func IsPanic(err error) bool {
	var pe panicError

	return errors.As(err, &pe)
}

// numscriptPanicToDescribable maps a value recovered from a panic inside the
// numscript library into a Describable panicError (which behaves as an
// ErrNumscriptRuntime). It returns nil when recovered is nil (no panic in
// flight). Callers invoke recover() themselves — directly inside their own
// deferred closure, as the Go runtime requires — and pass the result here so
// the panic→Describable conversion lives in one place (DRY) across every Safe*
// wrapper.
func numscriptPanicToDescribable(recovered any) domain.Describable {
	if recovered == nil {
		return nil
	}

	return panicError{&domain.ErrNumscriptRuntime{Detail: fmt.Sprintf("numscript panic: %v", recovered)}}
}

// SafeRun wraps ParseResult.Run with a deferred recover to catch panics from the
// numscript library and convert them into regular errors.
func SafeRun(parsed numscriptlib.ParseResult, ctx context.Context, vars numscriptlib.VariablesMap, store numscriptlib.Store) (result numscriptlib.ExecutionResult, err domain.Describable) {
	defer func() {
		if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
			result = numscriptlib.ExecutionResult{}
			err = panicErr
		}
	}()

	result, runErr := parsed.Run(ctx, vars, store)
	err = convertNumscriptError(runErr)

	return
}

// SafeResolveDependencies wraps ParseResult.ResolveDependencies with the same
// deferred recover + error-conversion contract as SafeRun. ResolveDependencies
// runs untrusted script analysis and can panic on adversarial input; both call
// sites (admission dependency discovery and — critically — the FSM apply-path
// stale-inputs re-resolution) must never let that panic escape, so the panic is
// converted into a Describable ErrNumscriptRuntime rather than crashing the
// request goroutine or the Raft apply loop. Library errors are mapped through
// the shared convertNumscriptError, identically to SafeRun.
func SafeResolveDependencies(parsed numscriptlib.ParseResult, ctx context.Context, vars numscriptlib.VariablesMap, store numscriptlib.Store) (resolved numscriptlib.ResolvedDependencies, err domain.Describable) {
	defer func() {
		if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
			resolved = numscriptlib.ResolvedDependencies{}
			err = panicErr
		}
	}()

	resolved, resolveErr := parsed.ResolveDependencies(ctx, vars, store)
	err = convertNumscriptError(resolveErr)

	return
}
