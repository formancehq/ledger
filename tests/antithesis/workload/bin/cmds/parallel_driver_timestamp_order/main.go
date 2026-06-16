// Driver for the "log-sequence-timestamps-strictly-increase" property, in its
// review-corrected form: effective (HLC) timestamps are NON-DECREASING with
// log sequence. One proposal calls Machine.hlcTimestamp exactly once
// (machine.go:1150) and shares the resulting effectiveDate across all of its
// orders, so logs from a single proposal carry EQUAL timestamps; strictness
// only holds across proposal boundaries. Proposal boundaries are not cheaply
// attributable from the read side (a driver cannot tell which logs shared a
// proposal), so the implemented invariant is the non-decreasing form —
// asserting strict increase would false-positive on every bulk apply.
//
// IMPORTANT — what feeds the HLC: the proposal date is the SERVER's
// time.Now() at admission (command.go:32 via admission.go:341); user-supplied
// transaction timestamps do NOT feed the HLC and are stored on the
// transaction VERBATIM (processor_transaction.go:187-190 falls back to the
// HLC effective date only when the timestamp is nil). The assertions below
// therefore read the LOG date (LedgerLog.Date — the HLC effective date),
// never the transaction's user-supplied timestamp.
//
// Soundness against benign interleavings:
//   - Owned "tsorder-" ledger (restricted prefix): no foreign writes, no
//     concurrent ledger deletion.
//   - Unique references make UNAVAILABLE-retry double commits impossible.
//   - Only Apply-payload logs reach the per-ledger log index
//     (process_logs.go:133-144), so every ListLogs entry carries a LedgerLog
//     with an HLC date — no payload-type exclusions needed.
//   - Errors during writes or pagination → inconclusive, skip. Only a clean
//     pagination participates in the Always.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	antirandom "github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// backdatedEpochMicros (1µs after epoch) is harmless input variety for the
// user-timestamp field: it is stored verbatim on the transaction and never
// touches the HLC or the asserted log dates. The driver never sends FUTURE
// dates: pure politeness toward date-sensitive consumers — the field has no
// HLC effect either way.
const backdatedEpochMicros = 1

func main() {
	internal.RunDriver("parallel_driver_timestamp_order", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		run := r.Uint64()
		ledger := fmt.Sprintf("tsorder-%d", run%1_000_000)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		// Menu axis: write-set size.
		txCount := antirandom.RandomChoice([]int{3, 8, 20})

		var (
			ackedWrites int
			maxSeq      uint64
		)

		for i := range txCount {
			payload := &servicepb.CreateTransactionPayload{
				Postings: []*commonpb.Posting{{
					Source:      "world",
					Destination: "tsorder-dst:probe",
					Amount:      commonpb.NewUint256FromUint64(1),
					Asset:       "USD/2",
				}},
				Reference: fmt.Sprintf("tsorder-%d-%d", run, i),
				Force:     true,
			}

			// Every third write carries a backdated user timestamp — input
			// variety only: stored verbatim, no effect on HLC log dates.
			if i%3 == 1 {
				payload.Timestamp = &commonpb.Timestamp{Data: backdatedEpochMicros}
			}

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
					Type: &servicepb.Request_Apply{
						Apply: &servicepb.LedgerApplyRequest{
							Ledger: ledger,
							Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
								CreateTransaction: payload,
							}},
						},
					},
				}),
			})
			if err != nil {
				// Ambiguous: may have committed; harmless for ordering, skip.
				continue
			}

			ackedWrites++

			logs := resp.GetLogs()
			if len(logs) > 0 {
				if seq := logs[len(logs)-1].GetSequence(); seq > maxSeq {
					maxSeq = seq
				}
			}
		}

		details := internal.Details{"ledger": ledger, "ackedWrites": ackedWrites, "maxSeq": maxSeq}

		assert.Sometimes(ackedWrites > 0, "timestamp order write set recorded", details)
		if ackedWrites == 0 {
			return
		}

		// Menu axis: page size — small pages span many stream boundaries.
		pageSize := antirandom.RandomChoice([]uint32{1, 2, 5})

		var (
			afterLocalID uint64
			lastSeq      uint64
			lastTs       uint64
			haveLast     bool
			seqOrdered   = true
			tsOrdered    = true
			clean        = false
			seenLogs     int
			badSeq       uint64
		)

		maxPages := txCount*4 + 16

		for range maxPages {
			var cursor string
			if afterLocalID > 0 {
				cursor = strconv.FormatUint(afterLocalID, 10) // ledger-local log ID, exclusive
			}
			stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
				Ledger: ledger,
				Options: &commonpb.ListOptions{
					PageSize: pageSize,
					Cursor:   cursor,
					Read:     &commonpb.ReadOptions{MinLogSequence: maxSeq},
				},
			})
			if err != nil {
				return
			}

			var pageCount int

			for {
				entry, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}

				if err != nil {
					// Aborted stream: explicit error is the correct outcome
					// for an incomplete stream — inconclusive, never asserted.
					return
				}

				pageCount++
				seenLogs++

				applyLog := entry.GetPayload().GetApply()
				if applyLog == nil || applyLog.GetLog() == nil {
					// Defensive: only Apply logs are indexed per-ledger, so
					// this should not happen; without a ledger-local ID the
					// cursor cannot advance — stop, inconclusive.
					return
				}

				seq := entry.GetSequence()
				ts := applyLog.GetLog().GetDate().GetData()

				if haveLast {
					if seq <= lastSeq {
						seqOrdered = false
						badSeq = seq
					}

					if ts < lastTs {
						tsOrdered = false
						badSeq = seq
					}
				}

				lastSeq = seq
				lastTs = ts
				haveLast = true
				afterLocalID = applyLog.GetLog().GetId()
			}

			if pageCount == 0 {
				clean = true

				break
			}
		}

		if !clean {
			return
		}

		resultDetails := details.With(internal.Details{
			"pageSize": pageSize,
			"seenLogs": seenLogs,
			"badSeq":   badSeq,
			"lastSeq":  lastSeq,
			"lastTs":   lastTs,
		})

		assert.Always(seqOrdered,
			"ListLogs pagination yields strictly increasing log sequences",
			resultDetails)
		assert.Always(tsOrdered,
			"effective timestamps are non-decreasing with log sequence",
			resultDetails)
	})
}
