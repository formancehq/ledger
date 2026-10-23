package numscript

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
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
	makeError := func(details *ledgerpb.FailureDetails) domain.SerializableError {
		return domain.NewNumscriptExecution(err.Error(), details)
	}
	if e, ok := errors.AsType[numscriptlib.MissingFundsErr](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptMissingFunds{NumscriptMissingFunds: &ledgerpb.NumscriptMissingFundsDetails{Asset: new(e.Asset), Needed: new(e.Needed.String()), Available: new(e.Available.String())}}})
	}
	if e, ok := errors.AsType[numscriptlib.NegativeAmountErr](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptNegativeAmount{NumscriptNegativeAmount: &ledgerpb.NumscriptNegativeAmountDetails{Amount: new(e.Amount.String())}}})
	}
	if e, ok := errors.AsType[numscriptlib.MissingVariableErr](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptMissingVariable{NumscriptMissingVariable: &ledgerpb.NumscriptMissingVariableDetails{Name: new(e.Name)}}})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidAccountName](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidAccountName{NumscriptInvalidAccountName: &ledgerpb.NumscriptInvalidAccountNameDetails{Name: new(e.Name)}}})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidAsset](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidAsset{NumscriptInvalidAsset: &ledgerpb.NumscriptInvalidAssetDetails{Name: new(e.Name)}}})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidColor](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidColor{NumscriptInvalidColor: &ledgerpb.NumscriptInvalidColorDetails{Color: new(e.Color)}}})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidScope](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidScope{NumscriptInvalidScope: &ledgerpb.NumscriptInvalidScopeDetails{Scope: new(e.Scope)}}})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidMonetaryLiteral](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidMonetaryLiteral{NumscriptInvalidMonetaryLiteral: &ledgerpb.NumscriptInvalidMonetaryLiteralDetails{Source: new(e.Source)}}})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidNumberLiteral](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidNumberLiteral{NumscriptInvalidNumberLiteral: &ledgerpb.NumscriptInvalidNumberLiteralDetails{Source: new(e.Source)}}})
	}
	if e, ok := errors.AsType[numscriptlib.BadPortionParsingErr](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptBadPortion{NumscriptBadPortion: &ledgerpb.NumscriptBadPortionDetails{Source: new(e.Source)}}})
	}
	if e, ok := errors.AsType[numscriptlib.MismatchedCurrencyError](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCurrencyMismatch{NumscriptCurrencyMismatch: &ledgerpb.NumscriptCurrencyMismatchDetails{Expected: new(e.Expected), Got: new(e.Got)}}})
	}
	if e, ok := errors.AsType[numscriptlib.DivideByZero](err); ok {
		if e.Numerator != nil {
			return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptDivideByZeroWithNumerator{
				NumscriptDivideByZeroWithNumerator: &ledgerpb.NumscriptDivideByZeroWithNumeratorDetails{Numerator: new(e.Numerator.String())},
			}})
		}

		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptDivideByZero{
			NumscriptDivideByZero: &ledgerpb.NumscriptDivideByZeroDetails{},
		}})
	}
	if e, ok := errors.AsType[numscriptlib.TypeError](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptTypeError{NumscriptTypeError: &ledgerpb.NumscriptTypeErrorDetails{Expected: new(e.Expected)}}})
	}
	if e, ok := errors.AsType[numscriptlib.MetadataNotFound](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptScopedMetadataNotFound{NumscriptScopedMetadataNotFound: &ledgerpb.NumscriptScopedMetadataNotFoundDetails{Account: new(e.Account), Scope: new(e.Scope), Key: new(e.Key)}}})
	}
	if e, ok := errors.AsType[numscriptlib.NegativeBalanceError](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptScopedNegativeBalance{NumscriptScopedNegativeBalance: &ledgerpb.NumscriptScopedNegativeBalanceDetails{Account: new(e.Account), Scope: new(e.Scope), Asset: new(e.Asset), Amount: new(e.Amount.String())}}})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidAllotmentSum](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidAllotmentSum{NumscriptInvalidAllotmentSum: &ledgerpb.NumscriptInvalidAllotmentSumDetails{ActualSum: new(e.ActualSum.String())}}})
	}
	if e, ok := errors.AsType[numscriptlib.NegativePortion](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptNegativePortion{NumscriptNegativePortion: &ledgerpb.NumscriptNegativePortionDetails{Portion: new(e.Portion.String())}}})
	}
	if _, ok := errors.AsType[numscriptlib.InvalidRemainingAllotment](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidRemainingAllotment{NumscriptInvalidRemainingAllotment: &ledgerpb.NumscriptInvalidRemainingAllotmentDetails{}}})
	}
	if _, ok := errors.AsType[numscriptlib.InvalidAllotmentInSendAll](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidAllotmentInSendAll{NumscriptInvalidAllotmentInSendAll: &ledgerpb.NumscriptInvalidAllotmentInSendAllDetails{}}})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidUnboundedInSendAll](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidUnboundedSendAll{NumscriptInvalidUnboundedSendAll: &ledgerpb.NumscriptInvalidUnboundedSendAllDetails{Name: new(e.Name), Scope: new(e.Scope)}}})
	}
	if _, ok := errors.AsType[numscriptlib.InvalidUnboundedAddressInScalingAddress](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidUnboundedScalingAddress{NumscriptInvalidUnboundedScalingAddress: &ledgerpb.NumscriptInvalidUnboundedScalingAddressDetails{}}})
	}
	if _, ok := errors.AsType[numscriptlib.InvalidNestedMeta](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidNestedMeta{NumscriptInvalidNestedMeta: &ledgerpb.NumscriptInvalidNestedMetaDetails{}}})
	}
	if _, ok := errors.AsType[numscriptlib.CannotCastToString](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCannotCastToString{NumscriptCannotCastToString: &ledgerpb.NumscriptCannotCastToStringDetails{}}})
	}
	if e, ok := errors.AsType[numscriptlib.CannotCastScopedAccountToString](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCannotCastScopedAccount{NumscriptCannotCastScopedAccount: &ledgerpb.NumscriptCannotCastScopedAccountDetails{Account: new(e.Account), Scope: new(e.Scope)}}})
	}
	if e, ok := errors.AsType[numscriptlib.CannotStoreScopedAccountInMeta](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCannotStoreScopedAccount{NumscriptCannotStoreScopedAccount: &ledgerpb.NumscriptCannotStoreScopedAccountDetails{Account: new(e.Account), Scope: new(e.Scope)}}})
	}
	if e, ok := errors.AsType[numscriptlib.UnboundVariableErr](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptUnboundVariable{NumscriptUnboundVariable: &ledgerpb.NumscriptUnboundVariableDetails{Name: new(e.Name)}}})
	}
	if e, ok := errors.AsType[numscriptlib.UnboundFunctionErr](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptUnboundFunction{NumscriptUnboundFunction: &ledgerpb.NumscriptUnboundFunctionDetails{Name: new(e.Name)}}})
	}
	if e, ok := errors.AsType[numscriptlib.BadArityErr](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptBadArity{NumscriptBadArity: &ledgerpb.NumscriptBadArityDetails{ExpectedArity: new(strconv.Itoa(e.ExpectedArity)), GivenArguments: new(strconv.Itoa(e.GivenArguments))}}})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidTypeErr](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidType{NumscriptInvalidType: &ledgerpb.NumscriptInvalidTypeDetails{Name: new(e.Name)}}})
	}
	if e, ok := errors.AsType[numscriptlib.ExperimentalFeature](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptExperimentalFeature{NumscriptExperimentalFeature: &ledgerpb.NumscriptExperimentalFeatureDetails{FlagName: new(e.FlagName)}}})
	}
	if e, ok := errors.AsType[numscriptlib.InvalidFeature](err); ok {
		return makeError(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidFeature{NumscriptInvalidFeature: &ledgerpb.NumscriptInvalidFeatureDetails{Feature: new(e.Feature)}}})
	}

	return nil
}

// mapCompilerError records compiler classifications without depending on
// diagnostic wording. Unknown compiler defects retain the public reason but
// are not frozen as audited script rejections.
func mapCompilerError(err error) domain.SerializableError {
	result := &domain.ErrNumscriptCompile{Detail: err.Error()}
	set := func(details *ledgerpb.FailureDetails) *domain.ErrNumscriptCompile {
		result.Details = details

		return result
	}
	if e, ok := errors.AsType[numscriptlib.CompilerTypeError](err); ok {
		switch kind := e.Kind.(type) {
		case numscriptlib.TypecheckTypeMismatch:
			return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileTypeMismatch{NumscriptCompileTypeMismatch: &ledgerpb.NumscriptCompileTypeMismatchDetails{Expected: new(kind.Expected), Got: new(kind.Got)}}})
		case numscriptlib.TypecheckUnboundVariable:
			return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileUnboundVariable{NumscriptCompileUnboundVariable: &ledgerpb.NumscriptCompileUnboundVariableDetails{Name: new(kind.Name), Type: new(kind.Type)}}})
		case numscriptlib.TypecheckInvalidType:
			return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileInvalidType{NumscriptCompileInvalidType: &ledgerpb.NumscriptCompileInvalidTypeDetails{Name: new(kind.Name)}}})
		case numscriptlib.TypecheckBadArity:
			return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileBadArity{NumscriptCompileBadArity: &ledgerpb.NumscriptCompileBadArityDetails{Expected: new(uint64(kind.Expected)), Actual: new(uint64(kind.Actual))}}})
		case numscriptlib.TypecheckUnknownFunction:
			return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileUnknownFunction{NumscriptCompileUnknownFunction: &ledgerpb.NumscriptCompileUnknownFunctionDetails{Name: new(kind.Name), WrongContext: new(kind.WrongContext)}}})
		case numscriptlib.TypecheckDuplicateVariable:
			return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileDuplicateVariable{NumscriptCompileDuplicateVariable: &ledgerpb.NumscriptCompileDuplicateVariableDetails{Name: new(kind.Name)}}})
		}
	}
	if _, ok := errors.AsType[numscriptlib.CompilerInvalidUncappedSource](err); ok {
		return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileInvalidUncappedSource{NumscriptCompileInvalidUncappedSource: &ledgerpb.NumscriptCompileInvalidUncappedSourceDetails{}}})
	}
	if _, ok := errors.AsType[numscriptlib.CompilerDuplicateRemaining](err); ok {
		return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileDuplicateRemaining{NumscriptCompileDuplicateRemaining: &ledgerpb.NumscriptCompileDuplicateRemainingDetails{}}})
	}
	if _, ok := errors.AsType[numscriptlib.CompilerInvalidMetaPosition](err); ok {
		return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileInvalidMetaPosition{NumscriptCompileInvalidMetaPosition: &ledgerpb.NumscriptCompileInvalidMetaPositionDetails{}}})
	}
	if e, ok := errors.AsType[numscriptlib.CompilerCannotCastToString](err); ok {
		return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileCannotCastToString{NumscriptCompileCannotCastToString: &ledgerpb.NumscriptCompileCannotCastToStringDetails{Type: new(e.Type)}}})
	}
	if _, ok := errors.AsType[numscriptlib.CompilerCannotStoreScopedAccountInMeta](err); ok {
		return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileCannotStoreScopedAccount{NumscriptCompileCannotStoreScopedAccount: &ledgerpb.NumscriptCompileCannotStoreScopedAccountDetails{}}})
	}
	if e, ok := errors.AsType[numscriptlib.CompilerExperimentalFeature](err); ok {
		return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileExperimentalFeature{NumscriptCompileExperimentalFeature: &ledgerpb.NumscriptCompileExperimentalFeatureDetails{FlagName: new(e.FlagName)}}})
	}
	if e, ok := errors.AsType[numscriptlib.CompilerInvalidFeature](err); ok {
		return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileInvalidFeature{NumscriptCompileInvalidFeature: &ledgerpb.NumscriptCompileInvalidFeatureDetails{Feature: new(e.Feature)}}})
	}
	if e, ok := errors.AsType[numscriptlib.CompilerMissingVariable](err); ok {
		return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileMissingVariable{NumscriptCompileMissingVariable: &ledgerpb.NumscriptCompileMissingVariableDetails{Name: new(e.Name)}}})
	}
	if e, ok := errors.AsType[numscriptlib.CompilerInvalidVariableValue](err); ok {
		return set(&ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptCompileInvalidVariableValue{NumscriptCompileInvalidVariableValue: &ledgerpb.NumscriptCompileInvalidVariableValueDetails{Name: new(e.Name), Type: new(e.Type)}}})
	}

	return domain.NewUnclassifiedNumscriptCompile(err.Error())
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
