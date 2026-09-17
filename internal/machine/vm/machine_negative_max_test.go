package vm

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/machine"
)

// A `max` clause with a negative amount is clamped to zero: the clause caps at
// nothing rather than aborting the script. `negExpr` ([COIN -90]) and `run` come
// from machine_negative_amount_test.go.

// A negative source `max` contributes nothing, and the next source funds the send.
func TestNegativeSourceMaxIsClamped(t *testing.T) {
	postings, err := run(t, `send [COIN 100] (
		source = {
			max `+negExpr+` from @alice
			@charlie
		}
		destination = @bob
	)`, map[string]int64{"alice": 1000, "charlie": 1000})
	require.NoError(t, err)
	require.Equal(t, []Posting{{
		Source:      "charlie",
		Destination: "bob",
		Asset:       "COIN",
		Amount:      machine.NewMonetaryInt(100),
	}}, postings)
}

// A negative source `max` behaves exactly like `max [COIN 0]`.
func TestNegativeSourceMaxMatchesZeroMax(t *testing.T) {
	src := func(max string) string {
		return `send [COIN 100] (
			source = {
				max ` + max + ` from @alice
				@charlie
			}
			destination = @bob
		)`
	}
	balances := map[string]int64{"alice": 1000, "charlie": 1000}

	negPostings, err := run(t, src(negExpr), balances)
	require.NoError(t, err)
	zeroPostings, err := run(t, src(`[COIN 0]`), balances)
	require.NoError(t, err)
	require.Equal(t, zeroPostings, negPostings)
}

// Clamping does not invent funds: with no other source the send is unfunded,
// and the error is insufficient funds rather than a negative-amount rejection.
func TestNegativeSourceMaxAloneIsUnfunded(t *testing.T) {
	_, err := run(t, `send [COIN 100] (
		source = max `+negExpr+` from @alice
		destination = @bob
	)`, map[string]int64{"alice": 1000})
	require.ErrorIs(t, err, &machine.ErrInsufficientFund{})
}

// A negative destination `max` receives nothing, and `remaining` takes it all.
// TakeMax of zero yields an empty funding, so no zero-amount posting is emitted
// for  at all -- unlike OP_TAKE, which does attach a zero part.
func TestNegativeDestinationMaxIsClamped(t *testing.T) {
	postings, err := run(t, `send [COIN 100] (
		source = @alice
		destination = {
			max `+negExpr+` to @bob
			remaining to @charlie
		}
	)`, map[string]int64{"alice": 1000})
	require.NoError(t, err)
	require.Equal(t, []Posting{{
		Source:      "alice",
		Destination: "charlie",
		Asset:       "COIN",
		Amount:      machine.NewMonetaryInt(100),
	}}, postings)
}

// A non-negative `max` is untouched by the clamp.
func TestPositiveMaxIsUnaffected(t *testing.T) {
	postings, err := run(t, `send [COIN 100] (
		source = {
			max [COIN 30] from @alice
			@charlie
		}
		destination = @bob
	)`, map[string]int64{"alice": 1000, "charlie": 1000})
	require.NoError(t, err)
	require.Equal(t, []Posting{
		{
			Source:      "alice",
			Destination: "bob",
			Asset:       "COIN",
			Amount:      machine.NewMonetaryInt(30),
		},
		{
			Source:      "charlie",
			Destination: "bob",
			Asset:       "COIN",
			Amount:      machine.NewMonetaryInt(70),
		},
	}, postings)
}

// The clamp is scoped to `max` clauses: a negative *send amount* still reaches
// the OP_TAKE_MAX guard and is rejected.
func TestNegativeSendAmountIsStillRejected(t *testing.T) {
	_, err := run(t, `send `+negExpr+` (
		source = @world
		destination = @bob
	)`, nil)
	require.ErrorContains(t, err, "cannot send a monetary with a negative amount: [COIN -90]")
}
