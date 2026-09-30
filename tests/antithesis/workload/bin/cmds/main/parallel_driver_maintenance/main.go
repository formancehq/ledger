package main

import (
	"context"
	"github.com/formancehq/ledger/v3/internal/protohelpers"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_maintenance", func(ctx context.Context, client commonpb.BucketServiceClient, _ string) {
		// Create a dedicated ledger so we don't depend on a shared ledger
		// that may be deleted by another concurrent driver.
		ledger := internal.PrefixMaintenance.New()
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		// Enable maintenance mode.
		_, err := client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
			Type: &commonpb.Request_SetMaintenanceMode{
				SetMaintenanceMode: &commonpb.SetMaintenanceModeRequest{
					Enabled: true,
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to enable maintenance mode", internal.Details{"error": err})
		if err != nil {
			return
		}

		// Writes should be rejected while in maintenance mode.
		_, err = client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
			Type: &commonpb.Request_Apply{
				Apply: &commonpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
						CreateTransaction: &commonpb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								protohelpers.NewPosting("world", "users:0", "USD/2", internal.RandomBigInt()),
							},
							Force: true,
						},
					}},
				},
			},
		}))

		if err != nil {
			st, _ := status.FromError(err)
			assert.AlwaysOrUnreachable(st.Code() == codes.Unavailable, "write during maintenance should be rejected as Unavailable", internal.Details{
				"code":  st.Code().String(),
				"error": err,
			})
		}

		// Disable maintenance mode.
		_, err = client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
			Type: &commonpb.Request_SetMaintenanceMode{
				SetMaintenanceMode: &commonpb.SetMaintenanceModeRequest{
					Enabled: false,
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to disable maintenance mode", internal.Details{"error": err})
		if err != nil {
			return
		}

		// Writes should work again.
		_, err = client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
			Type: &commonpb.Request_Apply{
				Apply: &commonpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
						CreateTransaction: &commonpb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								protohelpers.NewPosting("world", "users:0", "USD/2", internal.RandomBigInt()),
							},
							Force: true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "write after disabling maintenance should succeed", internal.Details{"error": err})

		log.Println("maintenance mode cycle completed")
	})
}
