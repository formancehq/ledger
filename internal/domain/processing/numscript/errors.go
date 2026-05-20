package numscript

import (
	"context"
	"errors"
	"fmt"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
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

// ErrMetaNotSupported is returned when a Numscript script uses meta() to
// resolve variables dynamically. meta() prevents the admission layer from
// statically discovering all accounts needed for preloading.
var ErrMetaNotSupported = errors.New("meta() is not supported: scripts must use static account references")

// convertNumscriptError translates known numscript library errors into domain
// errors so that the gRPC error mapper can return proper status codes.
func convertNumscriptError(err error) error {
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

	return err
}

// SafeRun wraps ParseResult.Run with a deferred recover to catch panics from the
// numscript library and convert them into regular errors.
func SafeRun(parsed numscriptlib.ParseResult, ctx context.Context, vars numscriptlib.VariablesMap, store numscriptlib.Store) (result numscriptlib.ExecutionResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = numscriptlib.ExecutionResult{}
			err = fmt.Errorf("numscript panic: %v", r)
		}
	}()

	result, err = parsed.Run(ctx, vars, store)
	err = convertNumscriptError(err)

	return
}
