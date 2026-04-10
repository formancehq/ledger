package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_metadata", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		address := internal.GetRandomAddress()
		key := fmt.Sprintf("meta-%d", internal.Rand().Uint64()%100)
		value := fmt.Sprintf("val-%d", internal.Rand().Uint64()%1000)

		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Data: &servicepb.LedgerApplyRequest_AddMetadata{
							AddMetadata: &commonpb.SaveMetadataCommand{
								Target: &commonpb.Target{
									Target: &commonpb.Target_Account{
										Account: &commonpb.TargetAccount{Addr: address},
									},
								},
								Metadata: commonpb.MetadataSetFromMap(map[string]string{key: value}),
							},
						},
					},
				},
			}},
		})

		details := internal.Details{
			"ledger":  ledger,
			"account": address,
			"key":     key,
		}

		assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to save account metadata", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		acct, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
			Ledger:  ledger,
			Address: address,
		})
		if err != nil {
			return
		}

		found := false
		for _, m := range acct.GetMetadata().GetMetadata() {
			if m.GetKey() == key {
				found = true
				actual := m.GetValue().GetStringValue()
				assert.AlwaysOrUnreachable(actual == value, "metadata read-after-write should return saved value", details.With(internal.Details{
					"expected": value,
					"actual":   actual,
				}))

				break
			}
		}

		assert.AlwaysOrUnreachable(found, "saved metadata key should be present in account", details)
	})
}
