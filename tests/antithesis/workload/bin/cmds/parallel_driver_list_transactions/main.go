package main

import (
	"context"
	"io"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_list_transactions", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		// 1. Create a transaction so there is at least one.
		resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Data: &servicepb.LedgerApplyRequest_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: internal.RandomPostings(),
								Force:    true,
							},
						},
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to create tx for list test", internal.Details{"ledger": ledger, "error": err})
		if err != nil {
			return
		}

		createdTx := internal.ExtractCreatedTransaction(resp)
		if createdTx == nil {
			return
		}

		txID := createdTx.Transaction.Id
		details := internal.Details{"ledger": ledger, "txId": txID}

		// 2. List transactions (forward order, small page).
		stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
			Ledger:   ledger,
			PageSize: 50,
		})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			assert.Unreachable("ListTransactions should not fail", details.With(internal.Details{"error": err}))

			return
		}

		var count int
		found := false

		for {
			tx, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				break
			}

			count++

			if tx.GetId() == txID {
				found = true
			}
		}

		assert.AlwaysOrUnreachable(count > 0, "ListTransactions should return at least one transaction", details)
		assert.AlwaysOrUnreachable(found, "ListTransactions should contain the just-created transaction", details)

		// 3. List transactions in reverse order.
		reverseStream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
			Ledger:   ledger,
			PageSize: 10,
			Reverse:  true,
		})
		if err != nil {
			return
		}

		var reverseCount int

		for {
			_, err := reverseStream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				break
			}

			reverseCount++
		}

		assert.AlwaysOrUnreachable(reverseCount > 0, "reverse ListTransactions should return results", details)
	})
}
