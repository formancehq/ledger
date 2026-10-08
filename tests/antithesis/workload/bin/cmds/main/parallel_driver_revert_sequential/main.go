package main

import (
	"context"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_revert_sequential", func(ctx context.Context, client ledgerpb.BucketServiceClient, ledger string) {
		resp, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_Apply{
				Apply: &ledgerpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
						CreateTransaction: &ledgerpb.CreateTransactionPayload{
							Postings: internal.RandomPostings(),
							Force:    true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to create a transaction for revert", internal.Details{"ledger": ledger, "error": err})
		if err != nil {
			return
		}

		createdTx := internal.ExtractCreatedTransaction(resp)
		if createdTx == nil {
			return
		}

		txID := createdTx.GetTransaction().GetId()
		details := internal.Details{"ledger": ledger, "txId": txID}

		revertResp, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_Apply{
				Apply: &ledgerpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_RevertTransaction{
						RevertTransaction: &ledgerpb.RevertTransactionPayload{
							TransactionId: txID,
							Force:         true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to revert a transaction", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		if revertResp != nil && len(revertResp.GetLogs()) > 0 {
			applyLog := revertResp.GetLogs()[0].GetPayload().GetApply()
			if applyLog != nil {
				if revertedTx := applyLog.GetLog().GetData().GetRevertedTransaction(); revertedTx != nil {
					internal.CheckPostCommitVolumes(revertedTx.GetRevertTransaction().GetPostCommitVolumes(), details)
				}
			}
		}

		getTx, err := client.GetTransaction(ctx, &ledgerpb.GetTransactionRequest{
			Ledger:        ledger,
			TransactionId: txID,
		})
		if err != nil {
			internal.LogCleanupError("get transaction after revert", err)

			return
		}

		assert.AlwaysOrUnreachable(getTx.GetTransaction().GetReverted(),
			"reverted transaction should be marked as reverted", details)
	})
}
