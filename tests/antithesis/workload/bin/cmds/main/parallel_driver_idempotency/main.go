package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_idempotency", func(ctx context.Context, client ledgerpb.BucketServiceClient, ledger string) {
		postings := internal.RandomPostings()
		idemKey := fmt.Sprintf("idem-%d", internal.Rand().Uint64())

		req := ledgerpb.UnsignedApplyRequest(idemKey, &ledgerpb.Request{
			Type: &ledgerpb.Request_Apply{
				Apply: &ledgerpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
						CreateTransaction: &ledgerpb.CreateTransactionPayload{
							Postings: postings,
							Force:    true,
						},
					}},
				},
			},
		})

		details := internal.Details{"ledger": ledger, "idempotencyKey": idemKey}

		resp1, err := client.Apply(ctx, req)
		assert.Sometimes(internal.IsTolerated(err), "should be able to create idempotent transaction (first)", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		tx1 := internal.ExtractCreatedTransaction(resp1)
		if tx1 == nil {
			return
		}

		resp2, err := client.Apply(ctx, req)
		assert.Sometimes(internal.IsTolerated(err), "should be able to create idempotent transaction (second)", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		tx2 := internal.ExtractCreatedTransaction(resp2)
		if tx2 == nil {
			return
		}

		assert.Always(tx1.GetTransaction().GetId() == tx2.GetTransaction().GetId(),
			"idempotent transactions should return the same ID", details.With(internal.Details{
				"firstTxId":  tx1.GetTransaction().GetId(),
				"secondTxId": tx2.GetTransaction().GetId(),
			}))
	})
}
