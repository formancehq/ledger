package numscript

import (
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
