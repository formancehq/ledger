package main

import (
	"context"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_double_revert", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		// 1. Create a transaction.
		resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
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
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"should be able to create a transaction for double revert",
			internal.Details{"ledger": ledger, "error": err})
		if err != nil {
			return
		}

		createdTx := internal.ExtractCreatedTransaction(resp)
		if createdTx == nil {
			return
		}

		txID := createdTx.Transaction.Id
		details := internal.Details{"ledger": ledger, "txId": txID}

		// 2. First revert — should succeed.
		revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
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
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"first revert should succeed", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// Verify volumes are consistent after first revert.
		if revertResp != nil && len(revertResp.Logs) > 0 {
			applyLog := revertResp.Logs[0].Payload.GetApply()
			if applyLog != nil {
				if revertedTx := applyLog.Log.Data.GetRevertedTransaction(); revertedTx != nil {
					internal.CheckPostCommitVolumes(revertedTx.PostCommitVolumes, details)
				}
			}
		}

		// Verify the transaction is marked as reverted.
		getTx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
			Ledger:        ledger,
			TransactionId: txID,
		})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			assert.Unreachable("GetTransaction should not fail after revert",
				details.With(internal.Details{"error": err}))

			return
		}

		assert.AlwaysOrUnreachable(getTx.GetTransaction().GetReverted(),
			"transaction should be marked as reverted after first revert", details)

		// 3. Second revert — must fail with TRANSACTION_ALREADY_REVERTED.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{
							RevertTransaction: &servicepb.RevertTransactionPayload{
								TransactionId: txID,
								Force:         true,
							},
						}},
					},
				},
			}},
		})

		if err == nil {
			assert.Unreachable("double revert should fail", details)

			return
		}

		if internal.IsTransient(err) {
			return
		}

		isAlreadyReverted := internal.HasErrorReason(err, domain.ErrReasonTransactionAlreadyReverted) ||
			status.Code(err) == codes.FailedPrecondition
		assert.AlwaysOrUnreachable(isAlreadyReverted,
			"double revert should return TRANSACTION_ALREADY_REVERTED",
			details.With(internal.Details{"error": err}))

		assert.Reachable("double revert path exercised", details)
	})
}
