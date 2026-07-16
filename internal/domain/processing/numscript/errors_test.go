package numscript

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// balanceReadingScript reads a balance DURING dependency resolution via
// balance() in a var origin, so the store's GetBalances is consulted (a plain
// bounded source only records the dependency; its balance is not read at
// resolution time).
const balanceReadingScript = `
	vars { monetary $amt = balance(@wallet, USD/2) }
	send $amt (source = @wallet destination = @out)
`

// TestSafeResolveDependencies_DescribableSurvivesLibrary verifies that a typed
// domain.Describable returned by the store is NOT stringified/lost passing
// through numscriptlib.ResolveDependencies: errors.As reaches the concrete type
// and the Reason survives. This is what lets the FSM apply path recognise a
// coverage-contract violation and surface it loudly (invariant #7) instead of
// masking it as retryable stale.
//
// A real *state.ErrCoverageMiss cannot be constructed here (importing
// internal/infra/state forms a cycle: state → processing → processing/numscript);
// TestCoverageMissSurvivesNumscriptLibrary in internal/infra/state proves the
// same round-trip with the concrete *state.ErrCoverageMiss.
func TestSafeResolveDependencies_DescribableSurvivesLibrary(t *testing.T) {
	t.Parallel()

	parsed := numscriptlib.Parse(balanceReadingScript)
	require.Empty(t, parsed.GetParsingErrors())

	// *domain.ErrInvalidExecutionPlan is a coverage-contract Describable owned by
	// the domain package (no import cycle), standing in for the store-returned
	// typed error. The generated MockValueSource returns it from Balance.
	sentinel := &domain.ErrInvalidExecutionPlan{Reason_: "undeclared key"}

	ctrl := gomock.NewController(t)
	source := NewMockValueSource(ctrl)
	source.EXPECT().Balance(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, sentinel)
	source.EXPECT().Metadata(gomock.Any(), gomock.Any()).AnyTimes().Return("", false, nil)

	store := NewStore(source, false)

	_, err := SafeResolveDependencies(parsed, context.Background(), numscriptlib.VariablesMap{}, store)
	require.NotNil(t, err)

	var invalid *domain.ErrInvalidExecutionPlan
	require.ErrorAs(t, err, &invalid,
		"a typed Describable must survive the numscript library error path (errors.As)")
	require.False(t, IsPanic(err))
	require.Equal(t, domain.ErrReasonInvalidExecutionPlan, err.Reason())
}

// TestSafeResolveDependencies_RecoversPanic proves the finding-#1 fix: a panic
// raised while resolving dependencies (here injected at the ValueSource layer,
// reachable on the FSM apply path and at admission) is recovered and returned as
// a domain.Describable ErrNumscriptRuntime, never escaping the wrapper. An
// escaped panic on the Raft apply loop would crash the node / diverge the
// cluster (invariant #7).
//
// The panic is injected through the generated MockValueSource (its Balance
// DoAndReturn panics), fed through numscript.NewStore, rather than hand-rolling
// a numscriptlib.Store double — this exercises the same recover path while
// honouring the repo's "no hand-rolled fakes" convention.
func TestSafeResolveDependencies_RecoversPanic(t *testing.T) {
	t.Parallel()

	parsed := numscriptlib.Parse(balanceReadingScript)
	require.Empty(t, parsed.GetParsingErrors())

	ctrl := gomock.NewController(t)
	source := NewMockValueSource(ctrl)
	source.EXPECT().Balance(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(string, string, string) (*big.Int, error) {
			panic("boom: numscript store panic")
		})
	source.EXPECT().Metadata(gomock.Any(), gomock.Any()).AnyTimes().Return("", false, nil)

	store := NewStore(source, false)

	var recovered domain.Describable
	require.NotPanics(t, func() {
		_, recovered = SafeResolveDependencies(parsed, context.Background(), numscriptlib.VariablesMap{}, store)
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

// limitedColorStore serves a single, capped colored balance so a colored
// `send` overruns it and the interpreter raises MissingFundsErr. It is a
// minimal numscriptlib.Store used to exercise the real interpreter path.
type limitedColorStore struct {
	account string
	asset   string
	color   string
	amount  *big.Int
}

func (s limitedColorStore) GetBalances(_ context.Context, q numscriptlib.BalanceQuery) (numscriptlib.Balances, error) {
	out := make(numscriptlib.Balances, 0, len(q))
	for _, item := range q {
		amount := new(big.Int)
		if item.Account == s.account && item.Asset == s.asset && item.Color == s.color {
			amount.Set(s.amount)
		}
		out = append(out, numscriptlib.BalanceRow{
			Account: item.Account,
			Asset:   item.Asset,
			Color:   item.Color,
			Amount:  amount,
		})
	}

	return out, nil
}

func (limitedColorStore) GetAccountsMetadata(context.Context, numscriptlib.MetadataQuery) (numscriptlib.AccountsMetadata, error) {
	return numscriptlib.AccountsMetadata{}, nil
}

// TestSafeRun_ColoredInsufficientFunds runs a real colored `send` that overruns
// a capped RED bucket and asserts the surfaced ErrInsufficientFunds. It pins the
// interpreter limitation: numscriptlib.MissingFundsErr does not carry the color
// (nor the account), so the converted error's Color is empty even though the
// failing bucket is COIN/RED. An empty Color here means "unknown", NOT the
// uncolored bucket — see convertNumscriptError. If a future numscript bump
// attaches the resolved (account, color) to MissingFundsErr, this test should be
// tightened to assert Color == "RED".
func TestSafeRun_ColoredInsufficientFunds(t *testing.T) {
	t.Parallel()

	script := `#![feature("experimental-asset-colors")]
send [COIN 100] (
	source = @alice \ "RED"
	destination = @bob
)`

	parsed := numscriptlib.Parse(script)
	require.Empty(t, parsed.GetParsingErrors())

	store := limitedColorStore{
		account: "alice",
		asset:   "COIN",
		color:   "RED",
		amount:  big.NewInt(40),
	}

	_, runErr := SafeRun(parsed, context.Background(), numscriptlib.VariablesMap{}, store)
	require.NotNil(t, runErr)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, runErr, &insufficientFunds)
	require.Equal(t, "COIN", insufficientFunds.Asset)
	require.Equal(t, "100", insufficientFunds.Amount)
	require.Equal(t, "40", insufficientFunds.Balance)
	// Interpreter limitation: color is not recoverable from MissingFundsErr.
	require.Empty(t, insufficientFunds.Color)
	// ColorKnown must be false so the empty color is surfaced as "unknown", not
	// as a definite hit on the uncolored bucket. The wire metadata therefore
	// omits the color key entirely.
	require.False(t, insufficientFunds.ColorKnown)
	_, colorPresent := insufficientFunds.Metadata()["color"]
	require.False(t, colorPresent, "colored Numscript failure must not publish an empty color as the uncolored bucket")
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
