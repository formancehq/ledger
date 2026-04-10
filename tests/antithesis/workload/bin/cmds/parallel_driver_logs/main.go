package main

import (
	"context"
	"io"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_logs", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		// 1. Create a transaction to generate a log entry.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Data: &servicepb.LedgerApplyRequest_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: internal.RandomPostings(),
								Force:    true,
							},
						},
					},
				},
			}},
		})
		if err != nil {
			return
		}

		details := internal.Details{"ledger": ledger}

		// 2. List logs (global).
		stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
			PageSize: 20,
		})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			assert.Unreachable("ListLogs should not fail", details.With(internal.Details{"error": err}))

			return
		}

		var (
			count    int
			firstSeq uint64
		)

		for {
			logEntry, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				break
			}

			count++

			if firstSeq == 0 {
				firstSeq = logEntry.GetSequence()
			}
		}

		assert.AlwaysOrUnreachable(count > 0, "ListLogs should return at least one entry", details)

		if firstSeq == 0 {
			return
		}

		// 3. GetLog for the first sequence we found.
		logEntry, err := client.GetLog(ctx, &servicepb.GetLogRequest{
			Sequence: firstSeq,
		})
		if err != nil {
			return
		}

		assert.AlwaysOrUnreachable(logEntry.GetSequence() == firstSeq,
			"GetLog should return the requested log entry",
			details.With(internal.Details{"sequence": firstSeq}))
	})
}
