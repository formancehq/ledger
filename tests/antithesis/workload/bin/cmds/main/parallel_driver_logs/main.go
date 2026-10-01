package main

import (
	"context"
	"errors"
	"io"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_logs", func(ctx context.Context, client commonpb.BucketServiceClient, ledger string) {
		// 1. Create a transaction to generate a log entry.
		resp, err := client.Apply(ctx, commonpb.UnsignedApplyRequest("", &commonpb.Request{
			Type: &commonpb.Request_Apply{
				Apply: &commonpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
						CreateTransaction: &commonpb.CreateTransactionPayload{
							Postings: internal.RandomPostings(),
							Force:    true,
						},
					}},
				},
			},
		}))
		assert.Sometimes(internal.IsTolerated(err),
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

		// 2. List logs; the default read aligns the asynchronous index.
		stream, err := client.ListLogs(ctx, &commonpb.ListLogsRequest{
			Ledger: ledger,
			Options: &commonpb.ListOptions{
				PageSize: 20,
			},
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
			if errors.Is(err, io.EOF) {
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

		// The default ReadIndex plus projection alignment covers our write, so the
		// log we just committed must be present.
		assert.Always(count > 0, "ListLogs should return at least one entry after a confirmed write", details)

		if firstSeq == 0 {
			return
		}

		// 3. GetLog for the first sequence we found.
		logEntry, err := client.GetLog(ctx, &commonpb.GetLogRequest{
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
