package main

import (
	"context"
	"errors"
	"io"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_audit", runAuditCycle)
}

func runAuditCycle(ctx context.Context, client commonpb.BucketServiceClient, ledger string) {
	// Create a transaction so the audit trail has something.
	resp, err := client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
					CreateTransaction: &commonpb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							commonpb.NewPosting("world", "users:0", "USD/2", internal.RandomBigInt()),
						},
						Force: true,
					},
				}},
			},
		},
	}))
	assert.Sometimes(internal.IsTolerated(err),
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

	// List audit entries; the default read aligns any required projection.
	stream, err := client.ListAuditEntries(ctx, &commonpb.ListAuditEntriesRequest{
		Options: &commonpb.ListOptions{
			PageSize: 10,
		},
	})
	if err != nil {
		if !internal.IsTransient(err) {
			assert.Unreachable("ListAuditEntries returned unexpected error", internal.Details{"error": err})
		}

		return
	}

	count := 0

	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Local teardown explains Canceled, not a concurrent server error.
			locallyCanceled := status.Code(err) == codes.Canceled && ctx.Err() != nil
			if !internal.IsTransient(err) && !locallyCanceled {
				assert.Unreachable("ListAuditEntries stream returned unexpected error", internal.Details{
					"ledger": ledger,
					"count":  count,
					"code":   status.Code(err).String(),
					"error":  err.Error(),
				})
			}

			// Even a nonempty prefix is inconclusive until a clean EOF.
			return
		}

		count++
	}

	assert.AlwaysOrUnreachable(count > 0, "audit trail should contain entries", internal.Details{
		"count": count,
	})

	log.Printf("audit cycle completed: %d entries found", count)
}
