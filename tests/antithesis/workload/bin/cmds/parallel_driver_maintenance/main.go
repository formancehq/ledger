package main

import (
	"context"
	"fmt"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_maintenance", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		// Create a dedicated ledger so we don't depend on a shared ledger
		// that may be deleted by another concurrent driver.
		ledger := fmt.Sprintf("maint-%d", internal.Rand().Uint64()%1_000_000)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		// Enable maintenance mode.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_SetMaintenanceMode{
					SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{
						Enabled: true,
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to enable maintenance mode", internal.Details{"error": err})
		if err != nil {
			return
		}

		// Writes should be rejected while in maintenance mode.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Data: &servicepb.LedgerApplyRequest_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{
									commonpb.NewPosting("world", "users:0", "USD/2", internal.RandomBigInt()),
								},
								Force: true,
							},
						},
					},
				},
			}},
		})

		if err != nil {
			st, _ := status.FromError(err)
			assert.AlwaysOrUnreachable(st.Code() == codes.Unavailable, "write during maintenance should be rejected as Unavailable", internal.Details{
				"code":  st.Code().String(),
				"error": err,
			})
		}

		// Disable maintenance mode.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_SetMaintenanceMode{
					SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{
						Enabled: false,
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to disable maintenance mode", internal.Details{"error": err})
		if err != nil {
			return
		}

		// Writes should work again.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Data: &servicepb.LedgerApplyRequest_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{
									commonpb.NewPosting("world", "users:0", "USD/2", internal.RandomBigInt()),
								},
								Force: true,
							},
						},
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsUnavailable(err), "write after disabling maintenance should succeed", internal.Details{"error": err})

		log.Println("maintenance mode cycle completed")
	})
}
