package testserver

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/stack/ledger/client/models/operations"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"math/big"
)

type HaveCoherentStateMatcher struct{}

func (h HaveCoherentStateMatcher) Match(actual interface{}) (success bool, err error) {
	srv, ok := actual.(*Server)
	if !ok {
		return false, fmt.Errorf("expect type %T", new(Server))
	}
	ctx := context.Background()

	ledgers, err := ListLedgers(ctx, srv, operations.V2ListLedgersRequest{
		PageSize: pointer.For(int64(100)),
	})
	if err != nil {
		return false, err
	}

	for _, ledger := range ledgers.Data {
		aggregatedBalances, err := GetAggregatedBalances(ctx, srv, operations.V2GetBalancesAggregatedRequest{
			Ledger:           ledger.Name,
			UseInsertionDate: pointer.For(true),
		})
		Expect(err).To(BeNil())
		if len(aggregatedBalances) == 0 { // it's random, a ledger could not have been targeted
			// just in case, check if the ledger has transactions
			txs, err := ListTransactions(ctx, srv, operations.V2ListTransactionsRequest{
				Ledger: ledger.Name,
			})
			Expect(err).To(BeNil())
			Expect(txs.Data).To(HaveLen(0))
		} else {
			Expect(aggregatedBalances).To(HaveLen(1))
			Expect(aggregatedBalances["USD"]).To(Equal(big.NewInt(0)))
		}
	}

	return true, nil
}

func (h HaveCoherentStateMatcher) FailureMessage(_ interface{}) (message string) {
	return "server should has coherent state"
}

func (h HaveCoherentStateMatcher) NegatedFailureMessage(_ interface{}) (message string) {
	return "server should not has coherent state but has"
}

var _ types.GomegaMatcher = (*HaveCoherentStateMatcher)(nil)

func HaveCoherentState() *HaveCoherentStateMatcher {
	return &HaveCoherentStateMatcher{}
}
