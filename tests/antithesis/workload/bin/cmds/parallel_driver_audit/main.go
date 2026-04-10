package main

import (
	"context"
	"io"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_audit", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		// Enable audit logging.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_SetAuditConfig{
					SetAuditConfig: &servicepb.SetAuditConfigRequest{
						Enabled: true,
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to enable audit logging", internal.Details{"error": err})
		if err != nil {
			return
		}

		// Create a transaction so the audit trail has something.
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
			return
		}

		// List audit entries and verify at least one exists.
		stream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{
			PageSize: 10,
		})
		if err != nil {
			if !internal.IsTransient(err) {
				assert.Unreachable("ListAuditEntries returned unexpected error", internal.Details{"error": err})
			}

			return
		}

		count := 0
		streamFailed := false

		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				if internal.IsTransient(err) {
					streamFailed = true
				}

				break
			}

			count++
		}

		// If the stream failed due to a leadership change, we cannot draw
		// any conclusion about the audit trail contents — just bail out.
		if streamFailed {
			return
		}

		assert.AlwaysOrUnreachable(count > 0, "audit trail should contain entries after enabling audit", internal.Details{
			"count": count,
		})

		// Disable audit logging.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_SetAuditConfig{
					SetAuditConfig: &servicepb.SetAuditConfigRequest{
						Enabled: false,
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to disable audit logging", internal.Details{"error": err})

		log.Printf("audit cycle completed: %d entries found", count)
	})
}
