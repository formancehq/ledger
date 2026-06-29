// Driver for the "successful-list-streams-are-complete" property: a list
// stream that terminates cleanly (EOF) must contain every transaction whose
// commit was acknowledged before the pagination started — exactly once, in
// strictly increasing ID order. The regression family is #367 (storage
// iteration errors surfacing as truncated-but-OK results) plus the
// Canceled→io.EOF rewrite on the forwarded-read path (cursor.go:22-24), which
// can make a mid-stream teardown look like clean completion.
//
// Soundness against benign interleavings:
//   - The ledger is driver-owned (unique "listcomp-" name, restricted prefix),
//     so no other driver writes to it or deletes it.
//   - Every write carries a unique reference, so a transparent UNAVAILABLE
//     retry that re-proposes an already-committed write is rejected with
//     AlreadyExists — each logical write commits at most once.
//   - An Apply that returns an error is AMBIGUOUS (it may still have
//     committed): such writes are excluded from the acked set, and the driver
//     never asserts on the total count — only that acked items are present.
//   - Errors anywhere in the pagination (stream creation or Recv) make the run
//     inconclusive: only CLEAN termination participates in the assertions.
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

func main() {
	internal.RunDriver("parallel_driver_list_completeness", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		run := r.Uint64()
		ledger := internal.PrefixListCompleteness.WithSeed(run)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		// Menu axis: size of the confirmed write set.
		txCount := antirandom.RandomChoice([]int{3, 8, 20})

		var (
			acked  = map[uint64]uint64{} // acked tx ID → log sequence from the Apply ack
			maxSeq uint64
		)

		for i := range txCount {
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: fmt.Sprintf("listcomp-dst:%d", i%4),
									Amount:      commonpb.NewUint256FromUint64(1),
									Asset:       "USD/2",
								}},
								Reference: fmt.Sprintf("listcomp-%d-%d", run, i),
								Force:     true,
							},
						}},
					},
				},
			}))
			if err != nil {
				// Ambiguous outcome: may have committed. Not acked, and the
				// completeness check below tolerates extra (non-acked) IDs.
				continue
			}

			created := internal.ExtractCreatedTransaction(resp)
			logs := resp.GetLogs()
			if created == nil || created.GetTransaction() == nil || len(logs) == 0 {
				continue
			}

			seq := logs[len(logs)-1].GetSequence()
			acked[created.GetTransaction().GetId()] = seq
			if seq > maxSeq {
				maxSeq = seq
			}
		}

		details := internal.Details{"ledger": ledger, "ackedCount": len(acked), "maxSeq": maxSeq}

		assert.Sometimes(len(acked) > 0, "list completeness write set recorded", details)
		if len(acked) == 0 {
			return
		}

		// Menu axis: page size — small on purpose so a pagination spans many
		// request/stream boundaries, maximizing mid-list fault windows.
		pageSize := antirandom.RandomChoice([]uint32{1, 2, 5})

		var (
			afterTxID uint64
			lastID    uint64
			haveLast  bool
			ordered   = true
			seen      = map[uint64]int{}
			clean     = false
		)

		// Bounded exit: the owned ledger holds at most txCount acked writes
		// plus a handful of ambiguous-but-committed ones; cap pages generously.
		maxPages := txCount*4 + 16

		for range maxPages {
			var cursor string
			if afterTxID > 0 {
				cursor = strconv.FormatUint(afterTxID, 10)
			}
			stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
				Ledger: ledger,
				Options: &commonpb.ListOptions{
					PageSize: pageSize,
					Cursor:   cursor,
					Reverse:  true,                                          // oldest-first, so IDs must increase across pages
					Read:     &commonpb.ReadOptions{MinLogSequence: maxSeq}, // freshness barrier: index must cover every acked write
				},
			})
			if err != nil {
				// Errored termination (transient or otherwise): inconclusive.
				return
			}

			var pageCount int

			for {
				tx, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}

				if err != nil {
					// Aborted stream: an explicit error is the CORRECT way to
					// end an incomplete stream — never a violation here.
					return
				}

				pageCount++

				id := tx.GetId()
				if haveLast && id <= lastID {
					ordered = false
				}

				seen[id]++
				lastID = id
				haveLast = true
				afterTxID = id
			}

			if pageCount == 0 {
				// Clean EOF on an empty page: pagination is complete.
				clean = true

				break
			}
		}

		if !clean {
			// Page budget exhausted without a clean end — inconclusive.
			return
		}

		var (
			missing    []uint64
			duplicates []uint64
		)

		for id := range acked {
			if seen[id] == 0 {
				missing = append(missing, id)
			}
		}

		for id, n := range seen {
			if n > 1 {
				duplicates = append(duplicates, id)
			}
		}

		resultDetails := details.With(internal.Details{
			"pageSize":   pageSize,
			"seenCount":  len(seen),
			"missing":    fmt.Sprintf("%v", missing),
			"duplicates": fmt.Sprintf("%v", duplicates),
		})

		assert.Always(len(missing) == 0,
			"clean-EOF list pagination contains every acked transaction",
			resultDetails)
		assert.Always(len(duplicates) == 0,
			"clean-EOF list pagination yields no duplicate transaction IDs",
			resultDetails)
		assert.Always(ordered,
			"clean-EOF list pagination yields strictly increasing transaction IDs",
			resultDetails)
	})
}
