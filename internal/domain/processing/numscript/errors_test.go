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

func TestErrNumscriptParse_Error(t *testing.T) {
	t.Parallel()

	err := &domain.ErrNumscriptParse{Details: "unexpected token"}
	require.Contains(t, err.Error(), "numscript parse error")
	require.Contains(t, err.Error(), "unexpected token")
}

func TestErrNonDeterministicScript_Error(t *testing.T) {
	t.Parallel()

	err := &ErrNonDeterministicScript{Method: "GetBalances"}
	require.Contains(t, err.Error(), "non-deterministic script")
	require.Contains(t, err.Error(), "GetBalances")
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
