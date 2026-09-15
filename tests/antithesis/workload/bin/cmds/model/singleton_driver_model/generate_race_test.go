package main

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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

	c := NewChecker([]string{"L", "L2"}, nil)

	seed := bulkOf(
		oracletest.AddTypeReq("t-0"),
		oracletest.TxReq("world", "t-0:5", "USD/2", 100),
		oracletest.TxReqRefL("L", "seed-ref", "world", "t-1:9", "EUR/2", 50),
	)
	state := c.modelState.Apply(seed).State

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				generateBulk(state, c.ledgerNames, c.nextLedgerName())
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s := state
		for i := 0; i < 200; i++ {
			s = s.Apply(bulkOf(oracletest.TxReq("world", fmt.Sprintf("t-0:%d", i%100), "USD/2", 1))).State
		}
	}()

	wg.Wait()
}

func TestActiveLedgersExcludesMirrorsUntilPromotion(t *testing.T) {
	t.Parallel()

	state := oracle.NewGlobalState()
	create := func(name string, mode commonpb.LedgerMode) {
		result := state.Apply(oracle.Bulk{Requests: []*servicepb.Request{{Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{Name: name, Mode: mode},
		}}}})
		require.True(t, result.OK)
		state = result.State
	}
	create("normal", commonpb.LedgerMode_LEDGER_MODE_NORMAL)
	create("mirror", commonpb.LedgerMode_LEDGER_MODE_MIRROR)

	require.Equal(t, []string{"normal"}, activeLedgers(state, []string{"normal", "mirror"}))
	promoted := state.Apply(oracle.Bulk{Requests: []*servicepb.Request{{Type: &servicepb.Request_PromoteLedger{
		PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: "mirror"},
	}}}})
	require.True(t, promoted.OK)
	require.Equal(t, []string{"normal", "mirror"}, activeLedgers(promoted.State, []string{"normal", "mirror"}))
}
