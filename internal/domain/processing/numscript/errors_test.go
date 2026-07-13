package numscript

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// describableErrorSource is a ValueSource whose Balance read fails with an
// arbitrary domain.Describable. It lets TestSafeResolveDependencies_*Survives*
// prove that a typed Describable (its concrete type AND its Reason) survives
// round-tripping through the numscript library's error path — the library wraps
// store errors in QueryBalanceError, which implements Unwrap, and
// convertNumscriptError returns the underlying Describable as-is.
//
// A real *state.ErrCoverageMiss cannot be constructed here (importing
// internal/infra/state forms a cycle: state → processing → processing/numscript);
// TestCoverageMissSurvivesNumscriptLibrary in internal/infra/state proves the
// same round-trip with the concrete *state.ErrCoverageMiss.
type describableErrorSource struct {
	err domain.Describable
}

func (s describableErrorSource) Balance(string, string) (*big.Int, error) {
	return nil, s.err
}

func (describableErrorSource) Metadata(string, string) (string, bool, error) {
	return "", false, nil
}

// TestSafeResolveDependencies_DescribableSurvivesLibrary verifies that a typed
// domain.Describable returned by the store is NOT stringified/lost passing
// through numscriptlib.ResolveDependencies: errors.As reaches the concrete type
// and the Reason survives. This is what lets the FSM apply path recognise a
// coverage-contract violation and surface it loudly (invariant #7) instead of
// masking it as retryable stale.
func TestSafeResolveDependencies_DescribableSurvivesLibrary(t *testing.T) {
	t.Parallel()

	// balance() in a var origin forces a store read during resolution.
	parsed := numscriptlib.Parse(`
		vars { monetary $amt = balance(@wallet, USD/2) }
		send $amt (source = @wallet destination = @out)
	`)
	require.Empty(t, parsed.GetParsingErrors())

	// *domain.ErrInvalidExecutionPlan is a coverage-contract Describable owned by
	// the domain package (no import cycle), standing in for the store-returned
	// typed error.
	sentinel := &domain.ErrInvalidExecutionPlan{Reason_: "undeclared key"}
	store := NewStore(describableErrorSource{err: sentinel}, false)

	_, err := SafeResolveDependencies(parsed, context.Background(), numscriptlib.VariablesMap{}, store)
	require.NotNil(t, err)

	var invalid *domain.ErrInvalidExecutionPlan
	require.ErrorAs(t, err, &invalid,
		"a typed Describable must survive the numscript library error path (errors.As)")
	require.False(t, IsPanic(err))
	require.Equal(t, domain.ErrReasonInvalidExecutionPlan, err.Reason())
}

// panickingStore is a minimal numscriptlib.Store whose reads panic. A generated
// mock is used elsewhere for ValueSource, but the numscript-library Store type
// has no mockgen directive in this repo, and a panic-path test only needs a
// store that panics on the first read — a hand-rolled double is the simplest
// faithful way to drive that path.
type panickingStore struct{}

func (panickingStore) GetBalances(context.Context, numscriptlib.BalanceQuery) (numscriptlib.Balances, error) {
	panic("boom: numscript library store panic")
}

func (panickingStore) GetAccountsMetadata(context.Context, numscriptlib.MetadataQuery) (numscriptlib.AccountsMetadata, error) {
	panic("boom: numscript library store panic")
}

// TestSafeResolveDependencies_RecoversPanic proves the finding-#1 fix: a panic
// raised while resolving dependencies (here from the store, reachable on the FSM
// apply path and at admission) is recovered and returned as a domain.Describable
// ErrNumscriptRuntime, never escaping the wrapper. An escaped panic on the Raft
// apply loop would crash the node / diverge the cluster (invariant #7).
func TestSafeResolveDependencies_RecoversPanic(t *testing.T) {
	t.Parallel()

	// balance() in a var origin is evaluated during dependency resolution, so
	// the store's GetBalances is consulted (and panics).
	parsed := numscriptlib.Parse(`
		vars { monetary $amt = balance(@wallet, USD/2) }
		send $amt (source = @wallet destination = @out)
	`)
	require.Empty(t, parsed.GetParsingErrors())

	var recovered domain.Describable
	require.NotPanics(t, func() {
		_, recovered = SafeResolveDependencies(parsed, context.Background(), numscriptlib.VariablesMap{}, panickingStore{})
	})

	require.NotNil(t, recovered)

	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, recovered, &runtimeErr)
	require.Contains(t, runtimeErr.Detail, "numscript panic:")

	require.True(t, IsPanic(recovered),
		"a recovered panic must be identifiable so the FSM apply path surfaces it loudly instead of masking it as stale")
}

// TestSafeResolveDependencies_NormalErrorIsNotPanic guards the discriminator the
// FSM apply path relies on: a genuine (non-panic) resolution error must NOT be
// flagged as a panic, so it can still be softened to ErrStaleInputsResolution.
func TestSafeResolveDependencies_NormalErrorIsNotPanic(t *testing.T) {
	t.Parallel()

	// An undefined variable used as an account origin fails resolution with a
	// normal library error (no panic, no store read).
	parsed := numscriptlib.Parse(`
		vars { account $missing }
		send [USD/2 100] (source = $missing destination = @out)
	`)
	require.Empty(t, parsed.GetParsingErrors())

	_, err := SafeResolveDependencies(parsed, context.Background(), numscriptlib.VariablesMap{}, numscriptlib.StaticStore{})
	require.NotNil(t, err)
	require.False(t, IsPanic(err),
		"a normal resolution error must not be flagged as a panic — the apply path still maps it to stale")
}

func TestErrNumscriptParse_Error(t *testing.T) {
	t.Parallel()

	err := &domain.ErrNumscriptParse{Details: "unexpected token"}
	require.Contains(t, err.Error(), "numscript parse error")
	require.Contains(t, err.Error(), "unexpected token")
}

func TestConvertNumscriptError_MissingFunds(t *testing.T) {
	t.Parallel()

	numscriptErr := numscriptlib.MissingFundsErr{
		Asset:     "USD/2",
		Needed:    *big.NewInt(283334),
		Available: *big.NewInt(0),
	}
	converted := convertNumscriptError(numscriptErr)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, converted, &insufficientFunds)
	require.Equal(t, "USD/2", insufficientFunds.Asset)
	require.Equal(t, "283334", insufficientFunds.Amount)
	require.Equal(t, "0", insufficientFunds.Balance)
}

func TestConvertNumscriptError_MissingFunds_WrappedPreservesErrorsAs(t *testing.T) {
	t.Parallel()

	numscriptErr := numscriptlib.MissingFundsErr{
		Asset:     "EUR",
		Needed:    *big.NewInt(1000),
		Available: *big.NewInt(500),
	}
	converted := convertNumscriptError(numscriptErr)
	wrapped := fmt.Errorf("numscript execution error: %w", converted)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.True(t, errors.As(wrapped, &insufficientFunds))
	require.Equal(t, "EUR", insufficientFunds.Asset)
	require.Equal(t, "1000", insufficientFunds.Amount)
	require.Equal(t, "500", insufficientFunds.Balance)
}

func TestConvertNumscriptError_OtherError(t *testing.T) {
	t.Parallel()

	// Unmapped library errors now type as ErrNumscriptRuntime
	// (KindInternal) so they flow through the BusinessError pipeline
	// without falling through the FSM's boundary cast (#431).
	other := errors.New("some other error")
	got := convertNumscriptError(other)

	require.IsType(t, &domain.ErrNumscriptRuntime{}, got)
	require.Equal(t, "numscript runtime error: some other error", got.Error())
}

func TestConvertNumscriptError_Nil(t *testing.T) {
	t.Parallel()

	require.Nil(t, convertNumscriptError(nil))
}

func TestErrBalanceNotPreloaded_Error(t *testing.T) {
	t.Parallel()

	err := &domain.ErrBalanceNotPreloaded{Account: "users:alice", Asset: "USD/2"}
	require.Contains(t, err.Error(), "balance not preloaded")
	require.Contains(t, err.Error(), "users:alice")
	require.Contains(t, err.Error(), "USD/2")
}
