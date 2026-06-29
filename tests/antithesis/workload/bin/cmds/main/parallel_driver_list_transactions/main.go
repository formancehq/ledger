package main

import (
	"context"
	"io"
	"strconv"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_list_transactions", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		// 1. Create a transaction so there is at least one.
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: internal.RandomPostings(),
							Force:    true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to create tx for list test", internal.Details{"ledger": ledger, "error": err})
		if err != nil {
			return
		}

		createdTx := internal.ExtractCreatedTransaction(resp)
		if createdTx == nil {
			return
		}

		txID := createdTx.Transaction.Id
		details := internal.Details{"ledger": ledger, "txId": txID}

		// Extract the log sequence from the Apply response to guarantee
		// read-after-write consistency on subsequent queries.
		var minLogSeq uint64
		if logs := resp.GetLogs(); len(logs) > 0 {
			minLogSeq = logs[len(logs)-1].GetSequence()
		}

		// 2. Paginate through all transactions until we find the one we created.
		var (
			totalCount int
			found      bool
			afterTxID  uint64
			streamErr  bool
		)

		for !found {
			var cursor string
			if afterTxID > 0 {
				cursor = strconv.FormatUint(afterTxID, 10)
			}
			stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
				Ledger: ledger,
				Options: &commonpb.ListOptions{
					PageSize: 50,
					Cursor:   cursor,
					Read:     &commonpb.ReadOptions{MinLogSequence: minLogSeq},
				},
			})
			if err != nil {
				if internal.IsTransient(err) {
					return
				}

				assert.Unreachable("ListTransactions should not fail", details.With(internal.Details{"error": err}))

				return
			}

			var pageCount int

			for {
				tx, err := stream.Recv()
				if err == io.EOF {
					break
				}

				if err != nil {
					streamErr = true
					if !internal.IsTolerated(err) {
						assert.Unreachable("ListTransactions stream returned unexpected error",
							details.With(internal.Details{"error": err}))
					}

					break
				}

				pageCount++
				totalCount++
				afterTxID = tx.GetId()

				if tx.GetId() == txID {
					found = true
				}
			}

			if streamErr || pageCount == 0 {
				break
			}
		}

		if !streamErr {
			assert.AlwaysOrUnreachable(totalCount > 0, "ListTransactions should return at least one transaction", details)
			assert.AlwaysOrUnreachable(found, "ListTransactions should contain the just-created transaction", details)
		}

		// 3. List transactions in reverse order.
		reverseStream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
			Ledger: ledger,
			Options: &commonpb.ListOptions{
				PageSize: 10,
				Reverse:  true,
			},
		})
		if err != nil {
			internal.LogCleanupError("reverse list transactions", err)
			return
		}

		var reverseCount int
		streamErr = false

		for {
			_, err := reverseStream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				streamErr = true
				if !internal.IsTolerated(err) {
					assert.Unreachable("reverse ListTransactions stream returned unexpected error",
						details.With(internal.Details{"error": err}))
				}

				break
			}

			reverseCount++
		}

		if !streamErr {
			assert.AlwaysOrUnreachable(reverseCount > 0, "reverse ListTransactions should return results", details)
		}
	})
}
