package main

import (
	"context"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func main() {
	internal.RunGeneratedDriver("parallel_driver_revert_concurrent", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string, src *generation.Source) {
		// 1. Create a transaction to revert.
		postings := internal.GeneratedPostings(src)
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: postings,
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
		revertReq := servicepb.UnsignedApplyRequest("", &servicepb.Request{
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
		})

		var (
			wg           sync.WaitGroup
			err1         error
			err2         error
			resp1, resp2 *servicepb.ApplyResponse
		)

		wg.Add(2)
		go func() {
			defer wg.Done()

			resp1, err1 = client.Apply(ctx, revertReq)
		}()
		go func() {
			defer wg.Done()

			resp2, err2 = client.Apply(ctx, revertReq)
		}()
		wg.Wait()

		// Inspect only acknowledged successes. Unkeyed interceptor retries can
		// return already-reverted after losing a committed success response;
		// neither that outcome nor a transient error proves absence of an effect.
		for i, result := range []*servicepb.ApplyResponse{resp1, resp2} {
			if []error{err1, err2}[i] != nil {
				continue
			}
			checkErr := validateReversal(result, ledger, txID, postings)
			assert.AlwaysOrUnreachable(checkErr == nil,
				"successful concurrent revert contains exactly the expected reversal",
				details.With(internal.Details{"worker": i, "error": checkErr}))
		}

		// If both are transient, nothing conclusive to check.
		if internal.IsTransient(err1) && internal.IsTransient(err2) {
			return
		}

		// 3. At most one call may report success. An unkeyed retry after a
		// lost success can leave both calls reporting already-reverted; both
		// may also fail transiently. Neither implies that no revert committed.
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
