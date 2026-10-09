package numscript

import (
	"context"
	"errors"
	"fmt"
	"strconv"

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
	if mapped := mapInterpreterError(err); mapped != nil {
		return mapped
	}

	return &domain.ErrNumscriptRuntime{Detail: err.Error()}
}

// mapInterpreterError uses only public, typed Numscript errors. Never classify
// by Error() text: a wording change must not change an audited outcome.
func mapInterpreterError(err error) domain.SerializableError {
	makeError := func(code string, facts map[string]string) domain.SerializableError {
		return &domain.ErrNumscriptExecution{Detail: err.Error(), Code: code, Facts: facts}
	}
	if e, ok := errors.AsType[numscriptlib.MissingFundsErr](err); ok {
		return makeError("NUMSCRIPT_MISSING_FUNDS", map[string]string{"asset": e.Asset, "needed": e.Needed.String(), "available": e.Available.String()})
	}
	if e, ok := errors.AsType[numscriptlib.NegativeAmountErr](err); ok {
		return makeError("NUMSCRIPT_NEGATIVE_AMOUNT", map[string]string{"amount": e.Amount.String()})
	}
	if e, ok := errors.AsType[numscriptlib.MissingVariableErr](err); ok {
		return makeError("NUMSCRIPT_MISSING_VARIABLE", map[string]string{"name": e.Name})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidAccountName](err); ok {
		return makeError("NUMSCRIPT_INVALID_ACCOUNT_NAME", map[string]string{"name": e.Name})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidAsset](err); ok {
		return makeError("NUMSCRIPT_INVALID_ASSET", map[string]string{"name": e.Name})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidColor](err); ok {
		return makeError("NUMSCRIPT_INVALID_COLOR", map[string]string{"color": e.Color})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidScope](err); ok {
		return makeError("NUMSCRIPT_INVALID_SCOPE", map[string]string{"scope": e.Scope})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidMonetaryLiteral](err); ok {
		return makeError("NUMSCRIPT_INVALID_MONETARY_LITERAL", map[string]string{"source": e.Source})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidNumberLiteral](err); ok {
		return makeError("NUMSCRIPT_INVALID_NUMBER_LITERAL", map[string]string{"source": e.Source})
	}
	if e, ok := errors.AsType[numscriptlib.BadPortionParsingErr](err); ok {
		return makeError("NUMSCRIPT_BAD_PORTION", map[string]string{"source": e.Source})
	}
	if e, ok := errors.AsType[numscriptlib.MismatchedCurrencyError](err); ok {
		return makeError("NUMSCRIPT_CURRENCY_MISMATCH", map[string]string{"expected": e.Expected, "got": e.Got})
	}
	if e, ok := errors.AsType[numscriptlib.DivideByZero](err); ok {
		facts := map[string]string{}
		if e.Numerator != nil {
			facts["numerator"] = e.Numerator.String()
		}
		return makeError("NUMSCRIPT_DIVIDE_BY_ZERO", facts)
	}
	if e, ok := errors.AsType[numscriptlib.TypeError](err); ok {
		return makeError("NUMSCRIPT_TYPE_ERROR", map[string]string{"expected": e.Expected})
	}
	if e, ok := errors.AsType[numscriptlib.MetadataNotFound](err); ok {
		return makeError("NUMSCRIPT_METADATA_NOT_FOUND", map[string]string{"account": e.Account, "scope": e.Scope, "key": e.Key})
	}
	if e, ok := errors.AsType[numscriptlib.NegativeBalanceError](err); ok {
		return makeError("NUMSCRIPT_NEGATIVE_BALANCE", map[string]string{"account": e.Account, "scope": e.Scope, "asset": e.Asset, "amount": e.Amount.String()})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidAllotmentSum](err); ok {
		return makeError("NUMSCRIPT_INVALID_ALLOTMENT_SUM", map[string]string{"actualSum": e.ActualSum.String()})
	}
	if e, ok := errors.AsType[numscriptlib.NegativePortion](err); ok {
		return makeError("NUMSCRIPT_NEGATIVE_PORTION", map[string]string{"portion": e.Portion.String()})
	}
	if _, ok := errors.AsType[numscriptlib.InvalidRemainingAllotment](err); ok {
		return makeError("NUMSCRIPT_INVALID_REMAINING_ALLOTMENT", nil)
	}
	if _, ok := errors.AsType[numscriptlib.InvalidAllotmentInSendAll](err); ok {
		return makeError("NUMSCRIPT_INVALID_ALLOTMENT_IN_SEND_ALL", nil)
	}
	if e, ok := errors.AsType[numscriptlib.InvalidUnboundedInSendAll](err); ok {
		return makeError("NUMSCRIPT_INVALID_UNBOUNDED_SEND_ALL", map[string]string{"name": e.Name, "scope": e.Scope})
	}
	if _, ok := errors.AsType[numscriptlib.InvalidUnboundedAddressInScalingAddress](err); ok {
		return makeError("NUMSCRIPT_INVALID_UNBOUNDED_SCALING_ADDRESS", nil)
	}
	if _, ok := errors.AsType[numscriptlib.InvalidNestedMeta](err); ok {
		return makeError("NUMSCRIPT_INVALID_NESTED_META", nil)
	}
	if _, ok := errors.AsType[numscriptlib.CannotCastToString](err); ok {
		return makeError("NUMSCRIPT_CANNOT_CAST_TO_STRING", nil)
	}
	if e, ok := errors.AsType[numscriptlib.CannotCastScopedAccountToString](err); ok {
		return makeError("NUMSCRIPT_CANNOT_CAST_SCOPED_ACCOUNT", map[string]string{"account": e.Account, "scope": e.Scope})
	}
	if e, ok := errors.AsType[numscriptlib.CannotStoreScopedAccountInMeta](err); ok {
		return makeError("NUMSCRIPT_CANNOT_STORE_SCOPED_ACCOUNT", map[string]string{"account": e.Account, "scope": e.Scope})
	}
	if e, ok := errors.AsType[numscriptlib.UnboundVariableErr](err); ok {
		return makeError("NUMSCRIPT_UNBOUND_VARIABLE", map[string]string{"name": e.Name})
	}
	if e, ok := errors.AsType[numscriptlib.UnboundFunctionErr](err); ok {
		return makeError("NUMSCRIPT_UNBOUND_FUNCTION", map[string]string{"name": e.Name})
	}
	if e, ok := errors.AsType[numscriptlib.BadArityErr](err); ok {
		return makeError("NUMSCRIPT_BAD_ARITY", map[string]string{"expectedArity": strconv.Itoa(e.ExpectedArity), "givenArguments": strconv.Itoa(e.GivenArguments)})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidTypeErr](err); ok {
		return makeError("NUMSCRIPT_INVALID_TYPE", map[string]string{"name": e.Name})
	}
	if e, ok := errors.AsType[numscriptlib.ExperimentalFeature](err); ok {
		return makeError("NUMSCRIPT_EXPERIMENTAL_FEATURE", map[string]string{"flagName": e.FlagName})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidFeature](err); ok {
		return makeError("NUMSCRIPT_INVALID_FEATURE", map[string]string{"feature": e.Feature})
	}
	return nil
}

// mapCompilerError records compiler classifications without depending on
// diagnostic wording. Unknown compiler defects retain the generic reason.
func mapCompilerError(err error) *domain.ErrNumscriptCompile {
	result := &domain.ErrNumscriptCompile{Detail: err.Error()}
	set := func(code string, facts map[string]string) *domain.ErrNumscriptCompile {
		result.Code, result.Facts = code, facts
		return result
	}
	if e, ok := errors.AsType[numscriptlib.CompilerTypeError](err); ok {
		switch kind := e.Kind.(type) {
		case numscriptlib.TypecheckTypeMismatch:
			return set("NUMSCRIPT_COMPILE_TYPE_MISMATCH", map[string]string{"expected": kind.Expected, "got": kind.Got})
		case numscriptlib.TypecheckUnboundVariable:
			return set("NUMSCRIPT_COMPILE_UNBOUND_VARIABLE", map[string]string{"name": kind.Name, "type": kind.Type})
		case numscriptlib.TypecheckInvalidType:
			return set("NUMSCRIPT_COMPILE_INVALID_TYPE", map[string]string{"name": kind.Name})
		case numscriptlib.TypecheckBadArity:
			return set("NUMSCRIPT_COMPILE_BAD_ARITY", map[string]string{"expected": strconv.Itoa(kind.Expected), "actual": strconv.Itoa(kind.Actual)})
		case numscriptlib.TypecheckUnknownFunction:
			return set("NUMSCRIPT_COMPILE_UNKNOWN_FUNCTION", map[string]string{"name": kind.Name, "wrongContext": kind.WrongContext})
		case numscriptlib.TypecheckDuplicateVariable:
			return set("NUMSCRIPT_COMPILE_DUPLICATE_VARIABLE", map[string]string{"name": kind.Name})
		}
	}
	if _, ok := errors.AsType[numscriptlib.CompilerInvalidUncappedSource](err); ok {
		return set("NUMSCRIPT_COMPILE_INVALID_UNCAPPED_SOURCE", nil)
	}
	if _, ok := errors.AsType[numscriptlib.CompilerDuplicateRemaining](err); ok {
		return set("NUMSCRIPT_COMPILE_DUPLICATE_REMAINING", nil)
	}
	if _, ok := errors.AsType[numscriptlib.CompilerInvalidMetaPosition](err); ok {
		return set("NUMSCRIPT_COMPILE_INVALID_META_POSITION", nil)
	}
	if e, ok := errors.AsType[numscriptlib.CompilerCannotCastToString](err); ok {
		return set("NUMSCRIPT_COMPILE_CANNOT_CAST_TO_STRING", map[string]string{"type": e.Type})
	}
	if _, ok := errors.AsType[numscriptlib.CompilerCannotStoreScopedAccountInMeta](err); ok {
		return set("NUMSCRIPT_COMPILE_CANNOT_STORE_SCOPED_ACCOUNT", nil)
	}
	if e, ok := errors.AsType[numscriptlib.CompilerExperimentalFeature](err); ok {
		return set("NUMSCRIPT_COMPILE_EXPERIMENTAL_FEATURE", map[string]string{"flagName": string(e.FlagName)})
	}
	if e, ok := errors.AsType[numscriptlib.CompilerInvalidFeature](err); ok {
		return set("NUMSCRIPT_COMPILE_INVALID_FEATURE", map[string]string{"feature": e.Feature})
	}
	if e, ok := errors.AsType[numscriptlib.CompilerMissingVariable](err); ok {
		return set("NUMSCRIPT_COMPILE_MISSING_VARIABLE", map[string]string{"name": e.Name})
	}
	if e, ok := errors.AsType[numscriptlib.CompilerInvalidVariableValue](err); ok {
		return set("NUMSCRIPT_COMPILE_INVALID_VARIABLE_VALUE", map[string]string{"name": e.Name, "type": e.Type})
	}
	return result
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
