package main

import (
	"context"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_concurrent_revert", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		// 1. Create a transaction to revert.
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
			"should be able to create a transaction for concurrent revert",
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

		// 2. Fire two reverts concurrently.
		revertReq := &servicepb.ApplyRequest{
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
		}

		var (
			wg   sync.WaitGroup
			err1 error
			err2 error
		)

		wg.Add(2)
		go func() {
			defer wg.Done()

			_, err1 = client.Apply(ctx, revertReq)
		}()
		go func() {
			defer wg.Done()

			_, err2 = client.Apply(ctx, revertReq)
		}()
		wg.Wait()

		// If both are transient, nothing to check.
		if internal.IsTransient(err1) && internal.IsTransient(err2) {
			return
		}

		// 3. Exactly one should succeed and the other should fail with
		// TRANSACTION_ALREADY_REVERTED (FailedPrecondition).
		// Under chaos, both may fail transiently, which is acceptable.
		successes := 0
		alreadyReverted := 0

		for _, e := range []error{err1, err2} {
			switch {
			case e == nil:
				successes++
			case internal.IsTransient(e):
				// transient: doesn't count
			case internal.HasErrorReason(e, domain.ErrReasonTransactionAlreadyReverted):
				alreadyReverted++
			case status.Code(e) == codes.FailedPrecondition:
				alreadyReverted++
			default:
				assert.Unreachable("concurrent revert returned unexpected error",
					details.With(internal.Details{"error": e}))
			}
		}

		// If we got clear results (non-transient), at most one should succeed.
		if successes+alreadyReverted > 0 {
			assert.AlwaysOrUnreachable(successes <= 1,
				"at most one concurrent revert should succeed",
				details.With(internal.Details{
					"successes":      successes,
					"alreadyReverted": alreadyReverted,
					"err1":           err1,
					"err2":           err2,
				}))
		}

		assert.Reachable("concurrent revert path exercised", details)
	})
}
