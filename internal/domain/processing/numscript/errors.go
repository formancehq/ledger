package numscript

import (
	"context"
	"errors"
	"fmt"

	numscriptlib "github.com/formancehq/numscript"
	"github.com/formancehq/numscript/accounts"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// ErrNonDeterministicScript is returned when a Numscript script calls
// GetBalances more than once during discovery.
// Deterministic scripts must declare all their reads in a single batch.
type ErrNonDeterministicScript struct {
	Method string // "GetBalances"
}

func (e *ErrNonDeterministicScript) Error() string {
	return fmt.Sprintf("non-deterministic script: %s called more than once", e.Method)
}
func (*ErrNonDeterministicScript) Reason() string { return domain.ErrReasonNonDeterministicScript }
func (e *ErrNonDeterministicScript) Metadata() map[string]string {
	return map[string]string{"method": e.Method}
}

// ErrMetaNotSupported is returned when a Numscript script uses meta() to
// resolve variables dynamically. meta() prevents the admission layer from
// statically discovering all accounts needed for preloading.
type errMetaNotSupported struct{}

func (errMetaNotSupported) Error() string {
	return "meta() is not supported: scripts must use static account references"
}
func (errMetaNotSupported) Reason() string              { return domain.ErrReasonValidation }
func (errMetaNotSupported) Metadata() map[string]string { return nil }

var ErrMetaNotSupported domain.Describable = errMetaNotSupported{}

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
		return &domain.ErrInsufficientFunds{
			Asset:   missingFunds.Asset,
			Amount:  missingFunds.Needed.String(),
			Balance: missingFunds.Available.String(),
		}
	}

	// errors.As walks the chain in case a caller has already wrapped the
	// numscript-library error in a Describable.
	var d domain.Describable
	if errors.As(err, &d) {
		return d
	}

	return &domain.ErrNumscriptRuntime{Detail: err.Error()}
}

// SafeRun wraps ParseResult.Run with a deferred recover to catch panics from the
// numscript library and convert them into regular errors.
func SafeRun(parsed numscriptlib.ParseResult, ctx context.Context, vars numscriptlib.VariablesMap, store numscriptlib.Store) (result numscriptlib.ExecutionResult, err domain.Describable) {
	defer func() {
		if r := recover(); r != nil {
			result = numscriptlib.ExecutionResult{}
			err = &domain.ErrNumscriptRuntime{Detail: fmt.Sprintf("numscript panic: %v", r)}
		}
	}()

	result, runErr := parsed.Run(ctx, vars, store)
	err = convertNumscriptError(runErr)

	return
}

// safeGetInvolvedAccounts wraps ParseResult.GetInvolvedAccounts with a deferred
// recover. The numscript interpreter can still panic on open issues (see e.g.
// formancehq/numscript#153); admission callers treat both an error and a nil
// slice as "keep the emulation-only result", so we surface a panic through the
// same path rather than crashing the request. InvolvedMeta is intentionally
// dropped — the ledger admission layer only consumes InvolvedAccount.
func safeGetInvolvedAccounts(parsed numscriptlib.ParseResult, vars numscriptlib.VariablesMap) (involved []accounts.InvolvedAccount, err error) {
	defer func() {
		if r := recover(); r != nil {
			involved = nil
			err = fmt.Errorf("numscript panic: %v", r)
		}
	}()

	involved, _, ierr := parsed.GetInvolvedAccounts(vars)
	if ierr != nil {
		return nil, ierr
	}

	return involved, nil
}
