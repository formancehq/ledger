package main

import (
	"context"
	"io"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_logs", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		// 1. Create a transaction to generate a log entry.
		resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: internal.RandomPostings(),
								Force:    true,
							},
						}},
					},
				},
			}},
		})
		assert.Sometimes(err == nil || internal.IsTransient(err),
			"should be able to create tx for logs test", internal.Details{"ledger": ledger, "error": err})
		if err != nil {
			return
		}

		details := internal.Details{"ledger": ledger}

		// A successful Apply of a committed transaction always returns its log
		// (failures come back as err, not a short response), so an empty slice
		// is a server invariant violation, not natural skew.
		if len(resp.GetLogs()) == 0 {
			assert.Unreachable("Apply succeeded but returned no committed log", details)

			return
		}

		// Gate the read on the Apply's log sequence: ListLogs reads the async
		// query index, which lags the FSM commit. Waiting for the index to
		// reach this sequence turns the read-after-write into a guarantee.
		minLogSeq := resp.GetLogs()[len(resp.GetLogs())-1].GetSequence()

		// 2. List logs for the ledger.
		stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
			Ledger:         ledger,
			PageSize:       20,
			MinLogSequence: minLogSeq,
		})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			assert.Unreachable("ListLogs should not fail", details.With(internal.Details{"error": err}))

			return
		}

		var (
			count     int
			firstSeq  uint64
			streamErr bool
		)

		for {
			logEntry, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				streamErr = true

				break
			}

			count++

			if firstSeq == 0 {
				firstSeq = logEntry.GetSequence()
			}
		}

		if streamErr {
			return
		}

		// MinLogSequence guaranteed the index caught up to our write, so the
		// log we just committed must be present.
		assert.Always(count > 0, "ListLogs should return at least one entry after a confirmed write", details)

		if firstSeq == 0 {
			return
		}

		// 3. GetLog for the first sequence we found.
		logEntry, err := client.GetLog(ctx, &servicepb.GetLogRequest{
			Sequence: firstSeq,
		})
		if err != nil {
			internal.LogCleanupError("get log by sequence", err)
			return
		}

		assert.AlwaysOrUnreachable(logEntry.GetSequence() == firstSeq,
			"GetLog should return the requested log entry",
			details.With(internal.Details{"sequence": firstSeq}))
	})
}
