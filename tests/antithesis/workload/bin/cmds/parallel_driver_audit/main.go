package main

import (
	"context"
	"io"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_audit", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		// Create a transaction so the audit trail has something.
		resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
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
			}),
		})
		assert.Sometimes(err == nil || internal.IsTransient(err),
			"should be able to create tx for audit trail", internal.Details{"ledger": ledger, "error": err})
		if err != nil {
			return
		}
		// A successful Apply of a committed transaction always returns its log
		// (failures come back as err, not a short response), so an empty slice
		// is a server invariant violation, not natural skew.
		if len(resp.GetLogs()) == 0 {
			assert.Unreachable("Apply succeeded but returned no committed log", internal.Details{"ledger": ledger})

			return
		}

		// Gate on the Apply's log sequence: the audit index is built async from
		// the log, so wait for it to process our entry before asserting.
		minLogSeq := resp.GetLogs()[len(resp.GetLogs())-1].GetSequence()

		// List audit entries and verify at least one exists.
		stream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{
			Options: &commonpb.ListOptions{
				PageSize: 10,
				Read:     &commonpb.ReadOptions{MinLogSequence: minLogSeq},
			},
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

		assert.AlwaysOrUnreachable(count > 0, "audit trail should contain entries", internal.Details{
			"count": count,
		})

		log.Printf("audit cycle completed: %d entries found", count)
	})
}
