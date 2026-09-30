package main

import (
	"context"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_maintenance", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		// Create a dedicated ledger so we don't depend on a shared ledger
		// that may be deleted by another concurrent driver.
		ledger := internal.PrefixMaintenance.New()
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		// On normal returns, restore writes even after an ambiguous enable or
		// an interrupted operation. The eventual oracle also resets the gate
		// because process termination does not run Go defers.
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := client.Apply(cleanupCtx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
				Type: &servicepb.Request_SetMaintenanceMode{
					SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: false},
				},
			})); err != nil {
				log.Printf("disable maintenance during shutdown: %v", err)
			}
		}()

		// Enable maintenance mode.
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_SetMaintenanceMode{
				SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{
					Enabled: true,
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to enable maintenance mode", internal.Details{"error": err})
		if err != nil {
			return
		}

		// Writes should be rejected while in maintenance mode.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewPosting("world", "users:0", "USD/2", internal.RandomBigInt()),
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
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_SetMaintenanceMode{
				SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{
					Enabled: false,
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err), "should be able to disable maintenance mode", internal.Details{"error": err})
		if err != nil {
			return
		}

		// Writes should work again.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewPosting("world", "users:0", "USD/2", internal.RandomBigInt()),
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
