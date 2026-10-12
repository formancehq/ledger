package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLedgerWindow_OrdersSkipsAndTruncates(t *testing.T) {
	t.Parallel()

	fleet := []string{"l-c", "l-a", "l-d", "l-b"}

	window, more := ledgerWindow(fleet, "", 10, false)
	require.Equal(t, []string{"l-a", "l-b", "l-c", "l-d"}, window, "the listing is name-ordered whatever order the fleet was created in")
	require.Equal(t, cursorForbidden, more)

	window, more = ledgerWindow(fleet, "", 10, true)
	require.Equal(t, []string{"l-d", "l-c", "l-b", "l-a"}, window)
	require.Equal(t, cursorForbidden, more)

	// Resume is exclusive and runs in the iteration's own direction.
	window, _ = ledgerWindow(fleet, "l-b", 10, false)
	require.Equal(t, []string{"l-c", "l-d"}, window)

	window, _ = ledgerWindow(fleet, "l-b", 10, true)
	require.Equal(t, []string{"l-a"}, window)

	// A name the fleet does not hold still bounds the scan.
	window, _ = ledgerWindow(fleet, "l-bb", 10, false)
	require.Equal(t, []string{"l-c", "l-d"}, window)

	window, more = ledgerWindow(fleet, "", 2, false)
	require.Equal(t, []string{"l-a", "l-b"}, window)
	require.Equal(t, cursorRequired, more, "two ledgers are still waiting")

	window, more = ledgerWindow(fleet, "l-b", 2, false)
	require.Equal(t, []string{"l-c", "l-d"}, window)
	require.Equal(t, cursorForbidden, more, "the page ended on the last ledger")

	window, more = ledgerWindow(fleet, "l-d", 2, false)
	require.Empty(t, window)
	require.Equal(t, cursorForbidden, more)
}

func TestLastLedgerKey_IsTheLastNameServed(t *testing.T) {
	t.Parallel()

	require.Equal(t, "l-b", lastLedgerKey([]string{"l-a", "l-b"}))
	require.Empty(t, lastLedgerKey(nil))
}
