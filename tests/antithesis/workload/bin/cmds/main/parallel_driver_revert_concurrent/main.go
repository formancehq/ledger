package main

import (
	"context"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/internal/domain"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_revert_concurrent", func(ctx context.Context, client ledgerpb.BucketServiceClient, ledger string) {
		// 1. Create a transaction to revert.
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

		assert.Sometimes(internal.IsTolerated(err),
			"should be able to create a transaction for concurrent revert",
			internal.Details{"ledger": ledger, "error": err})
		if err != nil {
			return
		}

		details := internal.Details{"ledger": ledger}

		createdTx := internal.CheckCreatedTransaction(resp, details)
		if createdTx == nil {
			return
		}

		txID := createdTx.GetTransaction().GetId()
		details["txId"] = txID

		// 2. Fire two reverts concurrently.
		revertReq := ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
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
		})

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
					"successes":       successes,
					"alreadyReverted": alreadyReverted,
					"err1":            err1,
					"err2":            err2,
				}))
		}

		assert.Reachable("concurrent revert path exercised", details)
	})
}
