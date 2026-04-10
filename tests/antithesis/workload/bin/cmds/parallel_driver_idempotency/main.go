package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_idempotency", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		postings := internal.RandomPostings()
		idemKey := fmt.Sprintf("idem-%d", internal.Rand().Uint64())

		req := &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				IdempotencyKey: idemKey,
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Data: &servicepb.LedgerApplyRequest_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings:      postings,
								Force:         true,
								ExpandVolumes: true,
							},
						},
					},
				},
			}},
		}

		details := internal.Details{"ledger": ledger, "idempotencyKey": idemKey}

		resp1, err := client.Apply(ctx, req)
		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to create idempotent transaction (first)", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		tx1 := internal.ExtractCreatedTransaction(resp1)
		if tx1 == nil {
			return
		}

		resp2, err := client.Apply(ctx, req)
		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to create idempotent transaction (second)", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		tx2 := internal.ExtractCreatedTransaction(resp2)
		if tx2 == nil {
			return
		}

		assert.Always(tx1.Transaction.Id == tx2.Transaction.Id,
			"idempotent transactions should return the same ID", details.With(internal.Details{
				"firstTxId":  tx1.Transaction.Id,
				"secondTxId": tx2.Transaction.Id,
			}))
	})
}
