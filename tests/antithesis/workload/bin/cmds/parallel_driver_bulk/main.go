package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_bulk", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		addr1 := internal.GetRandomAddress()
		addr2 := internal.GetRandomAddress()
		metaKey := fmt.Sprintf("bulk-meta-%d", r.Uint64())
		metaValue := fmt.Sprintf("v-%d", r.Uint64())

		details := internal.Details{"ledger": ledger, "addr1": addr1, "addr2": addr2}

		// Send a batch of operations in a single Apply call:
		// - Two transactions
		// - One metadata save
		resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				&servicepb.Request{
					Type: &servicepb.Request_Apply{
						Apply: &servicepb.LedgerApplyRequest{
							Ledger: ledger,
							Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
								CreateTransaction: &servicepb.CreateTransactionPayload{
									Postings: []*commonpb.Posting{{
										Source:      "world",
										Destination: addr1,
										Amount:      commonpb.NewUint256FromUint64(r.Uint64()%1000 + 1),
										Asset:       "USD/2",
									}},
									Force: true,
								},
							}},
						},
					},
				},
				&servicepb.Request{
					Type: &servicepb.Request_Apply{
						Apply: &servicepb.LedgerApplyRequest{
							Ledger: ledger,
							Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
								CreateTransaction: &servicepb.CreateTransactionPayload{
									Postings: []*commonpb.Posting{{
										Source:      "world",
										Destination: addr2,
										Amount:      commonpb.NewUint256FromUint64(r.Uint64()%1000 + 1),
										Asset:       "EUR/2",
									}},
									Force: true,
								},
							}},
						},
					},
				},
				&servicepb.Request{
					Type: &servicepb.Request_Apply{
						Apply: &servicepb.LedgerApplyRequest{
							Ledger: ledger,
							Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
								AddMetadata: &commonpb.SaveMetadataCommand{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Account{
											Account: &commonpb.TargetAccount{Addr: addr1},
										},
									},
									Metadata: commonpb.MetadataFromGoMap(map[string]string{metaKey: metaValue}),
								},
							}},
						},
					},
				},
			),
		})

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"bulk Apply should succeed", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// Verify the bulk produced logs for all three operations.
		assert.AlwaysOrUnreachable(len(resp.GetLogs()) >= 3,
			"bulk Apply should produce at least 3 logs",
			details.With(internal.Details{"logCount": len(resp.GetLogs())}))

		// Verify read-after-write for the metadata.
		acct, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
			Ledger:  ledger,
			Address: addr1,
		})
		if err != nil {
			internal.LogCleanupError("read account after bulk apply", err)
			return
		}

		found := false

		if v, ok := acct.GetMetadata()[metaKey]; ok && v.GetStringValue() == metaValue {
			found = true
		}

		assert.AlwaysOrUnreachable(found,
			"bulk metadata should be readable after write",
			details.With(internal.Details{"key": metaKey}))
	})
}
