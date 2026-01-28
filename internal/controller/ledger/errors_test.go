package ledger_test

import (
	"errors"
	"math/big"
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/numscript"
	"github.com/stretchr/testify/require"
)

func TestMissingFundsUnwrap(t *testing.T) {
	// This part of golang's behaviour can be a little fragile
	// so we make sure we don't break that in the future

	err := ledgercontroller.ErrRuntime{
		Source: "",
		InterpreterError: numscript.MissingFundsErr{
			Asset:     "EUR/2",
			Needed:    *big.NewInt(100),
			Available: *big.NewInt(0),
		},
	}

	require.True(t, errors.Is(err, numscript.MissingFundsErr{}))
}
