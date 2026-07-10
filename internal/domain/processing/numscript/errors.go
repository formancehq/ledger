package numscript

import (
	"context"
	"errors"
	"fmt"

	numscriptlib "github.com/formancehq/numscript"

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

// ErrCatchAllAssetNotSupported is returned when a Numscript script triggers
// the runtime's catch-all asset query (`BASE/*`) — used internally by the
// numscript interpreter to enumerate every precision flavor of an asset
// on an account when the script references the bare base. The ledger
// adapter cannot today expand the catch-all because the in-memory store
// exposes only point lookups, not iteration; until that capability lands
// we fail explicitly rather than letting the script see a phantom
// ErrBalanceNotPreloaded for `BASE/*`.
type errCatchAllAssetNotSupported struct{}

func (errCatchAllAssetNotSupported) Error() string {
	return "asset catch-all queries (BASE/*) are not yet supported: use the explicit precision (e.g. `send [USD/2 N]` instead of `send [USD N]`)"
}
func (errCatchAllAssetNotSupported) Kind() domain.ErrorKind      { return domain.KindValidation }
func (errCatchAllAssetNotSupported) Reason() string              { return domain.ErrReasonValidation }
func (errCatchAllAssetNotSupported) Metadata() map[string]string { return nil }

var ErrCatchAllAssetNotSupported domain.Describable = errCatchAllAssetNotSupported{}

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
		// uncolored bucket. Surfacing the true color would require the
		// interpreter to attach the resolved (account, color) to
		// MissingFundsErr upstream.
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
