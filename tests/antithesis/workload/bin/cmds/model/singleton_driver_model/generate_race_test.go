package main

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// Generation runs lock-free on a published GlobalState while the processor
// keeps folding forward from the same snapshot. This test reproduces that
// sharing pattern under the race detector: any in-place mutation of a
// published state — in Apply, in the collections, or in the compiled-chart
// memo — would be flagged.
func TestGenerateBulkConcurrentWithApply(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L", "L2", "L3", "L4"}, nil)

	seed := bulkOf(
		oracletest.AddTypeReq("t-0"),
		oracletest.TxReq("world", "t-0:5", "USD/2", 100),
		oracletest.TxReqRefL("L", "seed-ref", "world", "t-1:9", "EUR/2", 50),
	)
	state := c.modelState.Apply(seed).State

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 200 {
				generateBulk(state, c.ledgerNames, c.nextLedgerName(), c.liveTarget)
			}
		})
	}

	wg.Go(func() {
		s := state
		for i := range 200 {
			s = s.Apply(bulkOf(oracletest.TxReq("world", fmt.Sprintf("t-0:%d", i%100), "USD/2", 1))).State
		}
	})

	wg.Wait()
}

func TestActiveLedgersExcludesMirrorsUntilPromotion(t *testing.T) {
	t.Parallel()

	state := oracle.NewGlobalState()
	create := func(name string, mode ledgerpb.LedgerMode) {
		result := state.Apply(oracle.Bulk{Requests: []*ledgerpb.Request{{Type: &ledgerpb.Request_CreateLedger{
			CreateLedger: &ledgerpb.CreateLedgerRequest{Name: name, Mode: mode},
		}}}})
		require.True(t, result.OK)
		state = result.State
	}
	create("normal", ledgerpb.LedgerMode_LEDGER_MODE_NORMAL)
	create("mirror", ledgerpb.LedgerMode_LEDGER_MODE_MIRROR)

	require.Equal(t, []string{"normal"}, activeLedgers(state, []string{"normal", "mirror"}))
	promoted := state.Apply(oracle.Bulk{Requests: []*ledgerpb.Request{{Type: &ledgerpb.Request_PromoteLedger{
		PromoteLedger: &ledgerpb.PromoteLedgerRequest{Ledger: "mirror"},
	}}}})
	require.True(t, promoted.OK)
	require.Equal(t, []string{"normal", "mirror"}, activeLedgers(promoted.State, []string{"normal", "mirror"}))
}
