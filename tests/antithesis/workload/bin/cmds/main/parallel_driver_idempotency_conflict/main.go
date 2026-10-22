package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func main() {
	internal.RunGeneratedDriver("parallel_driver_idempotency_conflict", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string, src *generation.Source) {
		idemKey := fmt.Sprintf("idem-conflict-%d", internal.Rand().Uint64())
		firstAmount, conflictingAmount := uint64(100), uint64(999)
		if src.Intn(2) != 0 {
			firstAmount, conflictingAmount = src.Uint64(), src.Uint64()
		}
		// Keep the original multi-field conflict and also isolate amount hashing.
		amountOnly := src.Bool()
		runConflict(ctx, client, ledger, idemKey, firstAmount, conflictingAmount, amountOnly)
	})
}

func runConflict(ctx context.Context, client servicepb.BucketServiceClient, ledger, idemKey string, firstAmount, conflictingAmount uint64, amountOnly bool) {
	conflict := conflictingPosting(firstAmount, conflictingAmount, amountOnly)
	details := internal.Details{"ledger": ledger, "idempotencyKey": idemKey, "amountOnly": amountOnly}

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
							Amount:      commonpb.NewUint256FromUint64(firstAmount),
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

	details["firstTxId"] = createdTx.GetTransaction().GetId()

	// 2. Reuse the same idempotency key with a DIFFERENT payload.
	//    This must return IDEMPOTENCY_KEY_CONFLICT.
	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest(idemKey, &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{conflict},
						Force:    true,
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
}

// conflictingPosting preserves the original destination/asset conflict unless
// amountOnly is selected. Equal Hegel draws must still produce a conflict;
// toggling one bit works at both zero and MaxUint64 without overflow.
func conflictingPosting(firstAmount, amount uint64, amountOnly bool) *commonpb.Posting {
	posting := &commonpb.Posting{
		Source: "world", Destination: "users:1", Asset: "EUR/2",
		Amount: commonpb.NewUint256FromUint64(amount),
	}
	if amountOnly {
		if amount == firstAmount {
			amount ^= 1
		}
		posting.Destination = "users:0"
		posting.Asset = "USD/2"
		posting.Amount = commonpb.NewUint256FromUint64(amount)
	}

	return posting
}
