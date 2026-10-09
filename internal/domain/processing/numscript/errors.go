package numscript

import (
	"context"
	"errors"
	"fmt"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// convertNumscriptError translates numscript library errors raised by
// dependency resolution (its only caller, SafeResolveDependencies) into domain
// errors so that the gRPC error mapper can return proper status codes.
// Resolution does no balance arithmetic, so a missing-funds failure cannot
// originate here; execution errors go through the VM's convertVMError. A
// failure caused by the script, its vars, or the balances and metadata it read
// becomes ErrNumscriptExecution; library defects (InternalError,
// UnhandledError) and anything unmapped stay ErrNumscriptRuntime.
func convertNumscriptError(err error) domain.SerializableError {
	if err == nil {
		return nil
	}

	// Asset scaling (`… with scaling through …`) is unsupported by dependency
	// resolution regardless of any balance or metadata, so no retry can satisfy
	// it. As a validation failure it terminates at admission even after a
	// balance()/meta() origin read set MutableReadAttempted (EN-1557).
	if errors.Is(err, numscriptlib.ErrScalingNotSupported) {
		return domain.ErrNumscriptScalingUnsupported
	}

	// errors.As walks the chain in case a caller has already wrapped the
	// numscript-library error in a typed domain failure. This also unwraps
	// QueryBalanceError / QueryMetadataError, whose WrappedError is the Store
	// error — so a rejected scoped read (domain.ErrScopedBalanceUnsupported)
	// surfaces here as the validation sentinel it already is.
	if d, ok := errors.AsType[domain.SerializableError](err); ok {
		return d
	}

	if isScriptResolutionError(err) {
		return &domain.ErrNumscriptExecution{Detail: err.Error()}
	}

	return &domain.ErrNumscriptRuntime{Detail: err.Error()}
}

// isScriptResolutionError reports whether err is an interpreter failure caused
// by the script, its vars, or the balances and metadata it read. Values can
// come from balance() or meta(), so any of them may depend on state; admission
// keeps such a failure forwardable when resolution read mutable state (see
// classifyResolutionFailure).
func isScriptResolutionError(err error) bool {
	return isErrorType[numscriptlib.NegativeAmountErr](err) ||
		isErrorType[numscriptlib.MissingVariableErr](err) ||
		isErrorType[numscriptlib.InvalidAccountName](err) ||
		isErrorType[numscriptlib.InvalidAsset](err) ||
		isErrorType[numscriptlib.InvalidColor](err) ||
		isErrorType[numscriptlib.InvalidScope](err) ||
		isErrorType[numscriptlib.InvalidMonetaryLiteral](err) ||
		isErrorType[numscriptlib.InvalidNumberLiteral](err) ||
		isErrorType[numscriptlib.BadPortionParsingErr](err) ||
		isErrorType[numscriptlib.MismatchedCurrencyError](err) ||
		isErrorType[numscriptlib.DivideByZero](err) ||
		isErrorType[numscriptlib.TypeError](err) ||
		isErrorType[numscriptlib.MetadataNotFound](err) ||
		isErrorType[numscriptlib.NegativeBalanceError](err) ||
		isErrorType[numscriptlib.InvalidAllotmentSum](err) ||
		isErrorType[numscriptlib.NegativePortion](err) ||
		isErrorType[numscriptlib.InvalidRemainingAllotment](err) ||
		isErrorType[numscriptlib.InvalidAllotmentInSendAll](err) ||
		isErrorType[numscriptlib.InvalidUnboundedInSendAll](err) ||
		isErrorType[numscriptlib.InvalidUnboundedAddressInScalingAddress](err) ||
		isErrorType[numscriptlib.InvalidNestedMeta](err) ||
		isErrorType[numscriptlib.CannotCastToString](err) ||
		isErrorType[numscriptlib.CannotCastScopedAccountToString](err) ||
		isErrorType[numscriptlib.CannotStoreScopedAccountInMeta](err) ||
		isErrorType[numscriptlib.UnboundVariableErr](err) ||
		isErrorType[numscriptlib.UnboundFunctionErr](err) ||
		isErrorType[numscriptlib.BadArityErr](err) ||
		isErrorType[numscriptlib.InvalidTypeErr](err) ||
		isErrorType[numscriptlib.ExperimentalFeature](err) ||
		isErrorType[numscriptlib.InvalidFeature](err)
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
func numscriptPanicToDescribable(recovered any) domain.SerializableError {
	if recovered == nil {
		return nil
	}

	return panicError{&domain.ErrNumscriptRuntime{Detail: fmt.Sprintf("numscript panic: %v", recovered)}}
}

// SafeResolveDependencies wraps ParseResult.ResolveDependencies with a
// deferred recover and the shared error conversion. ResolveDependencies
// runs untrusted script analysis and can panic on adversarial input; both call
// sites (admission dependency discovery and — critically — the FSM apply-path
// stale-inputs re-resolution) must never let that panic escape, so the panic is
// converted into a Describable ErrNumscriptRuntime rather than crashing the
// request goroutine or the Raft apply loop. Library errors are mapped through
// the shared convertNumscriptError.
func SafeResolveDependencies(parsed numscriptlib.ParseResult, ctx context.Context, vars numscriptlib.VariablesMap, store numscriptlib.Store) (resolved numscriptlib.ResolvedDependencies, err domain.SerializableError) {
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
