package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_idempotency_conflict", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		idemKey := fmt.Sprintf("idem-conflict-%d", internal.Rand().Uint64())
		details := internal.Details{"ledger": ledger, "idempotencyKey": idemKey}

		// 1. Create a transaction with an idempotency key.
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(idemKey, &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{{
								Source:      "world",
								Destination: "users:0",
								Amount:      commonpb.NewUint256FromUint64(100),
								Asset:       "USD/2",
							}},
							Force: true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err),
			"should be able to create idempotent transaction",
			details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		createdTx := internal.ExtractCreatedTransaction(resp)
		if createdTx == nil {
			return
		}

		details["firstTxId"] = createdTx.Transaction.Id

		// 2. Reuse the same idempotency key with a DIFFERENT payload.
		//    This must return IDEMPOTENCY_KEY_CONFLICT.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest(idemKey, &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{{
								Source:      "world",
								Destination: "users:1",
								Amount:      commonpb.NewUint256FromUint64(999),
								Asset:       "EUR/2",
							}},
							Force: true,
						},
					}},
				},
			},
		}))

		if err == nil {
			assert.Unreachable("reusing idempotency key with different payload should fail", details)

			return
		}

		if internal.IsTransient(err) {
			return
		}

		isConflict := internal.HasErrorReason(err, domain.ErrReasonIdempotencyKeyConflict)
		assert.AlwaysOrUnreachable(isConflict,
			"mismatched idempotency key should return IDEMPOTENCY_KEY_CONFLICT",
			details.With(internal.Details{"error": err}))

		assert.Reachable("idempotency conflict path exercised", details)
	})
}
