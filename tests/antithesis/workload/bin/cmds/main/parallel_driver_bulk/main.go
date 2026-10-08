package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/internal/protohelpers"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_bulk", func(ctx context.Context, client ledgerpb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		addr1 := internal.GetRandomAddress()
		addr2 := internal.GetRandomAddress()
		metaKey := fmt.Sprintf("bulk-meta-%d", r.Uint64())
		metaValue := fmt.Sprintf("v-%d", r.Uint64())

		details := internal.Details{"ledger": ledger, "addr1": addr1, "addr2": addr2}

		// Send a batch of operations in a single Apply call:
		// - Two transactions
		// - One metadata save
		resp, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("",
			&ledgerpb.Request{
				Type: &ledgerpb.Request_Apply{
					Apply: &ledgerpb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
							CreateTransaction: &ledgerpb.CreateTransactionPayload{
								Postings: []*ledgerpb.Posting{{
									Source:      "world",
									Destination: addr1,
									Amount:      ledgerpb.NewUint256FromUint64(r.Uint64()%1000 + 1),
									Asset:       "USD/2",
								}},
								Force: true,
							},
						}},
					},
				},
			},
			&ledgerpb.Request{
				Type: &ledgerpb.Request_Apply{
					Apply: &ledgerpb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
							CreateTransaction: &ledgerpb.CreateTransactionPayload{
								Postings: []*ledgerpb.Posting{{
									Source:      "world",
									Destination: addr2,
									Amount:      ledgerpb.NewUint256FromUint64(r.Uint64()%1000 + 1),
									Asset:       "EUR/2",
								}},
								Force: true,
							},
						}},
					},
				},
			},
			&ledgerpb.Request{
				Type: &ledgerpb.Request_Apply{
					Apply: &ledgerpb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddMetadata{
							AddMetadata: &ledgerpb.SaveMetadataCommand{
								Target: &ledgerpb.Target{
									Target: &ledgerpb.Target_Account{
										Account: &ledgerpb.TargetAccount{Addr: addr1},
									},
								},
								Metadata: protohelpers.MetadataFromGoMap(map[string]string{metaKey: metaValue}),
							},
						}},
					},
				},
			},
		))

		assert.Sometimes(internal.IsTolerated(err),
			"bulk Apply should succeed", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// Verify the bulk produced logs for all three operations.
		assert.AlwaysOrUnreachable(len(resp.GetLogs()) >= 3,
			"bulk Apply should produce at least 3 logs",
			details.With(internal.Details{"logCount": len(resp.GetLogs())}))

		// Sanity-check the first transaction's post-commit volumes.
		internal.CheckCreatedTransaction(resp, details)

		// Verify read-after-write for the metadata.
		acct, err := client.GetAccount(ctx, &ledgerpb.GetAccountRequest{
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
