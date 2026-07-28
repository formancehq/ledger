package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestModelKnowsAccount(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	// world -> t-0:5 gives t-0:5 a volume cell; world keeps one too.
	c.modelState = c.modelState.Apply(bulkOf(oracletest.TxReq("world", "t-0:5", "USD/2", 10))).State
	ls := c.modelState.Ledger("L")

	require.True(t, modelKnowsAccount(ls, "t-0:5"))
	require.True(t, modelKnowsAccount(ls, "world"))
	require.False(t, modelKnowsAccount(ls, "t-0:6"))
	require.False(t, modelKnowsAccount(ls, "t-99:1"))
}

// pickAbsentAccount must always hand back an address the model has no state for,
// carrying a workload asset, on a known ledger — the negative-space target runRead
// probes.
func TestPickAbsentAccount(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.modelState = c.modelState.Apply(bulkOf(oracletest.TxReq("world", "t-0:5", "USD/2", 10))).State
	ls := c.modelState.Ledger("L")

	for i := 0; i < 100; i++ {
		ledger, addr, asset, ok := pickAbsentAccount(c.modelState, []string{"L"})
		require.True(t, ok)
		require.Equal(t, "L", ledger)
		require.False(t, modelKnowsAccount(ls, addr), "returned a known account: %s", addr)
		require.Contains(t, assets, asset)
	}

	// No ledgers -> nothing to probe.
	_, _, _, ok := pickAbsentAccount(c.modelState, nil)
	require.False(t, ok)
}

// absentLedgerName must never collide with the fleet, and pickLedgerReadTarget's
// absent branch must hand back exactly those absent names.
func TestPickLedgerReadTarget(t *testing.T) {
	t.Parallel()

	fleet := []string{"L", "L2", "L3"}
	known := map[string]bool{"L": true, "L2": true, "L3": true}

	for i := 0; i < 200; i++ {
		require.False(t, known[absentLedgerName(fleet)], "absent name collided with fleet")

		ledger, absent := pickLedgerReadTarget(fleet)
		if absent {
			require.False(t, known[ledger], "absent target is a fleet ledger: %s", ledger)
		} else {
			require.True(t, known[ledger], "known target is not a fleet ledger: %s", ledger)
		}
	}
}
