package main

import (
	"fmt"
	"sync"
	"testing"

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
				generateBulk(state, c.ledgerNames)
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
