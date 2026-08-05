//go:build e2e

package business

import (
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Cross-store snapshot alignment (EN-1748): a transaction visible in the
// main store whose index rows have not folded yet must never surface through
// an index complement. not(ts[_,_]) over a READY TIMESTAMP index is the
// sharpest probe — any transaction it returns claims "committed but absent
// from the timestamp index", a state no aligned snapshot pair can produce.
// The index builder is kept busy with create/drop backfill churn to stretch
// the fold lag that made the unaligned snapshots observable.
var _ = Describe("Cross-store snapshot alignment", Ordered, func() {
	const (
		ledgerName = "cross-store-alignment-ledger"
		seedTxs    = 1500
	)

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)))
		Expect(err).To(Succeed())

		for i := 0; i < seedTxs; i++ {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", fmt.Sprintf("acc:%d", i%64), big.NewInt(1), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		}

		Expect(actions.WaitForBuiltinIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)).To(Succeed())
	})

	It("never surfaces a committed tx as missing from a READY index", func() {
		stopChurn := make(chan struct{})

		var churn sync.WaitGroup

		// Backfill churn: create/drop cycles over a second builtin index force
		// the builder to rescan the seeded history over and over, delaying its
		// live fold — the condition that widens the cross-store window.
		churn.Add(1)
		go func() {
			defer GinkgoRecover()
			defer churn.Done()

			for {
				select {
				case <-stopChurn:
					return
				default:
				}

				_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT)))
				Expect(err).To(Succeed())
				_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.DropBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT)))
				Expect(err).To(Succeed())
			}
		}()

		notTs := actions.NotFilter(actions.BuiltinUintRangeFilter(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, 0, ^uint64(0)))
		deadline := time.Now().Add(30 * time.Second)

		i := 0
		for time.Now().Before(deadline) {
			i++

			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", fmt.Sprintf("live:%d", i%64), big.NewInt(1), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, notTs)
			if err != nil {
				// A stalled builder legitimately rejects with the
				// not-caught-up precondition; only phantom ROWS are the bug.
				continue
			}

			Expect(txs).To(BeEmpty(), "iteration %d: a committed tx surfaced as missing from the READY timestamp index", i)
		}

		close(stopChurn)
		churn.Wait()
	})
})
