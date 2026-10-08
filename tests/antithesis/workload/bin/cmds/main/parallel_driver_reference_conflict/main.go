package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/internal/domain"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_reference_conflict", func(ctx context.Context, client ledgerpb.BucketServiceClient, ledger string) {
		ref := fmt.Sprintf("ref-%d", internal.Rand().Uint64())
		details := internal.Details{"ledger": ledger, "reference": ref}

		// 1. Create a transaction with a unique reference.
		resp, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_Apply{
				Apply: &ledgerpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
						CreateTransaction: &ledgerpb.CreateTransactionPayload{
							Postings:  internal.RandomPostings(),
							Reference: ref,
							Force:     true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err),
			"should be able to create a transaction with reference",
			details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		createdTx := internal.ExtractCreatedTransaction(resp)
		if createdTx == nil {
			return
		}

		details["firstTxId"] = createdTx.GetTransaction().GetId()

		// 2. Create a different transaction with the same reference — must fail.
		_, err = client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_Apply{
				Apply: &ledgerpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
						CreateTransaction: &ledgerpb.CreateTransactionPayload{
							Postings:  internal.RandomPostings(),
							Reference: ref,
							Force:     true,
						},
					}},
				},
			},
		}))

		if err == nil {
			assert.Unreachable("duplicate reference should be rejected", details)

			return
		}

		if internal.IsTransient(err) {
			return
		}

		isRefConflict := internal.HasErrorReason(err, domain.ErrReasonTransactionReferenceConflict)
		assert.AlwaysOrUnreachable(isRefConflict,
			"duplicate reference should return TRANSACTION_REFERENCE_CONFLICT",
			details.With(internal.Details{"error": err}))

		assert.Reachable("reference conflict path exercised", details)
	})
}
