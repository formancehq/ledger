package main

import (
	"context"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_reverts", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings:      internal.RandomPostings(),
							Force:         true,
							ExpandVolumes: true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to create a transaction for revert", internal.Details{"ledger": ledger, "error": err})
		if err != nil {
			return
		}

		createdTx := internal.ExtractCreatedTransaction(resp)
		if createdTx == nil {
			return
		}

		txID := createdTx.Transaction.Id
		details := internal.Details{"ledger": ledger, "txId": txID}

		revertResp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{
						RevertTransaction: &servicepb.RevertTransactionPayload{
							TransactionId: txID,
							Force:         true,
							ExpandVolumes: true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to revert a transaction", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		if revertResp != nil && len(revertResp.Logs) > 0 {
			applyLog := revertResp.Logs[0].Payload.GetApply()
			if applyLog != nil {
				if revertedTx := applyLog.Log.Data.GetRevertedTransaction(); revertedTx != nil {
					internal.CheckPostCommitVolumes(revertedTx.PostCommitVolumes, details)
				}
			}
		}

		getTx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
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
