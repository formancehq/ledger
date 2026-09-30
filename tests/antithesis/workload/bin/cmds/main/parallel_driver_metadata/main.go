package main

import (
	"context"
	"fmt"
	"github.com/formancehq/ledger/v3/internal/protohelpers"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_metadata", func(ctx context.Context, client commonpb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		address := internal.GetRandomAddress()
		key := fmt.Sprintf("meta-%d", r.Uint64())
		value := fmt.Sprintf("val-%d", r.Uint64())

		details := internal.Details{
			"ledger":  ledger,
			"account": address,
			"key":     key,
		}

		// Save metadata.
		_, err := client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
			Type: &commonpb.Request_Apply{
				Apply: &commonpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{Addr: address},
								},
							},
							Metadata: protohelpers.MetadataFromGoMap(map[string]string{key: value}),
						},
					}},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to save account metadata", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// Read-after-write: verify the key is present.
		acct, err := client.GetAccount(ctx, &commonpb.GetAccountRequest{
			Ledger:  ledger,
			Address: address,
		})
		if err != nil {
			internal.LogCleanupError("read account after metadata write", err)

			return
		}

		// GetAccount reads the main store under the default linearizable
		// consistency (ReadIndex + WaitForApplied), so even when round-robin
		// lands on a follower it waits to catch up to the committed write.
		assert.Always(findMetadata(acct, key) == value, "metadata read-after-write should return saved value", details.With(internal.Details{
			"expected": value,
			"actual":   findMetadata(acct, key),
		}))

		// Delete the metadata key.
		_, err = client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
			Type: &commonpb.Request_Apply{
				Apply: &commonpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_DeleteMetadata{
						DeleteMetadata: &commonpb.DeleteMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{Addr: address},
								},
							},
							Key: key,
						},
					}},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to delete account metadata", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// Read-after-delete: verify the key is gone.
		acct, err = client.GetAccount(ctx, &commonpb.GetAccountRequest{
			Ledger:  ledger,
			Address: address,
		})
		if err != nil {
			internal.LogCleanupError("read account after metadata delete", err)

			return
		}

		// Same as the read-after-write above: the linearizable GetAccount waits
		// for the serving node to apply the committed delete before returning.
		assert.Always(findMetadata(acct, key) == "", "deleted metadata key should be absent", details.With(internal.Details{
			"actual": findMetadata(acct, key),
		}))
	})
}

func findMetadata(acct *commonpb.Account, key string) string {
	if v, ok := acct.GetMetadata()[key]; ok {
		return v.GetStringValue()
	}

	return ""
}
