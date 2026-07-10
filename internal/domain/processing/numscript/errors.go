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
