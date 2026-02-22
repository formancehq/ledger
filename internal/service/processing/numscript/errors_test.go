package numscript

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrNumscriptParse_Error(t *testing.T) {
	t.Parallel()

	err := &ErrNumscriptParse{Details: "unexpected token"}
	require.Contains(t, err.Error(), "numscript parse error")
	require.Contains(t, err.Error(), "unexpected token")
}

func TestErrNonDeterministicScript_Error(t *testing.T) {
	t.Parallel()

	err := &ErrNonDeterministicScript{Method: "GetBalances"}
	require.Contains(t, err.Error(), "non-deterministic script")
	require.Contains(t, err.Error(), "GetBalances")
}

func TestErrBalanceNotPreloaded_Error(t *testing.T) {
	t.Parallel()

	err := &ErrBalanceNotPreloaded{Account: "users:alice", Asset: "USD/2"}
	require.Contains(t, err.Error(), "balance not preloaded")
	require.Contains(t, err.Error(), "users:alice")
	require.Contains(t, err.Error(), "USD/2")
}
