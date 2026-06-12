// Driver for the "transaction-reference-unique-after-rotation" property: at
// most one committed transaction per (ledger, reference) — including when
// duplicates race each other concurrently through admission (both miss in
// cache, both preload "absent") and when a duplicate is issued after the
// original aged out of the cache window (the Pebble→preload→bloom path,
// where a bloom false negative directly admits a duplicate).
//
// Soundness against benign interleavings:
//   - The ledger is driver-owned (unique "refrace-" name, restricted prefix),
//     so no foreign driver can take a reference or write to it.
//   - Ack counting only treats explicit OK responses as wins. A transparent
//     UNAVAILABLE retry whose first attempt actually committed surfaces as a
//     reference conflict, REDUCING the OK count — never inflating it. Two OK
//     acks for the same reference therefore always means two committed
//     transactions.
//   - The list-based oracle (count of distinct transaction IDs per reference)
//     is retry-proof: a re-proposed duplicate either conflicts (one ID) or
//     commits a second transaction (two IDs — the genuine violation).
//   - Ambiguous outcomes (Unavailable after retries, Internal, ledger races)
//     never participate in conclusive-outcome assertions.
//   - Reads use a MinLogSequence floor from a post-hoc marker write, so a
//     stale read store cannot hide a committed duplicate (#398 class).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	antirandom "github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// createWithReference attempts a single CreateTransaction carrying ref.
func createWithReference(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	ledger, ref, destination string,
) (*servicepb.ApplyResponse, error) {
	return client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{{
								Source:      "world",
								Destination: destination,
								Amount:      commonpb.NewUint256FromUint64(1),
								Asset:       "USD/2",
							}},
							Reference: ref,
							Force:     true,
						},
					}},
				},
			},
		}},
	})
}

// writeMarker commits a reference-less marker transaction and returns its log
// sequence, usable as a MinLogSequence floor: the marker is proposed after
// every assertion-relevant response was received, so a read floored at the
// marker covers any write those responses could correspond to. Returns
// (0, false) when the barrier could not be established (inconclusive).
func writeMarker(ctx context.Context, client servicepb.BucketServiceClient, ledger string) (uint64, bool) {
	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{{
								Source:      "world",
								Destination: "refrace-marker",
								Amount:      commonpb.NewUint256FromUint64(1),
								Asset:       "USD/2",
							}},
							Force: true,
						},
					}},
				},
			},
		}},
	})
	if err != nil {
		return 0, false
	}

	logs := resp.GetLogs()
	if len(logs) == 0 {
		return 0, false
	}

	return logs[len(logs)-1].GetSequence(), true
}

// countTransactionsWithReference lists transactions matching ref at a
// MinLogSequence floor and returns (distinctTxIDs, conclusive). Any read
// error makes the result inconclusive — never a violation.
func countTransactionsWithReference(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	ledger, ref string,
	minLogSeq uint64,
) ([]uint64, bool) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:         ledger,
		PageSize:       10,
		Filter:         actions.ReferenceFilter(ref),
		MinLogSequence: minLogSeq,
	})
	if err != nil {
		return nil, false
	}

	var ids []uint64

	for {
		tx, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, false
		}

		ids = append(ids, tx.GetId())
	}

	return ids, true
}

// assertReferenceUnique is the shared end-state oracle for both halves of the
// driver: after a marker barrier, a reference must map to at most one
// committed transaction. Single call site per assertion message.
func assertReferenceUnique(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	ledger, ref string,
	details internal.Details,
) {
	floor, ok := writeMarker(ctx, client, ledger)
	if !ok {
		return
	}

	ids, conclusive := countTransactionsWithReference(ctx, client, ledger, ref, floor)
	if !conclusive {
		return
	}

	assert.Always(len(ids) <= 1,
		"a reference never maps to more than one committed transaction",
		details.With(internal.Details{"reference": ref, "txIds": fmt.Sprintf("%v", ids)}))
}

func main() {
	internal.RunDriver("parallel_driver_reference_race", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		run := r.Uint64()
		ledger := fmt.Sprintf("refrace-%d", run%1_000_000)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		details := internal.Details{"ledger": ledger}

		// ---- Half 1: concurrent same-reference creates ----------------------

		// Menu axis: degree of concurrency on the shared reference.
		goroutines := antirandom.RandomChoice([]int{2, 4, 8})
		raceRef := fmt.Sprintf("refrace-%d-race", run)

		var (
			wg            sync.WaitGroup
			mu            sync.Mutex
			okCount       int
			conflictCount int
		)

		wg.Add(goroutines)

		for g := range goroutines {
			go func() {
				defer wg.Done()

				_, err := createWithReference(ctx, client, ledger, raceRef, fmt.Sprintf("refrace-dst:%d", g))

				mu.Lock()
				defer mu.Unlock()

				switch {
				case err == nil:
					okCount++
				case internal.HasErrorReason(err, domain.ErrReasonTransactionReferenceConflict):
					conflictCount++
				default:
					// Transient or ambiguous: may or may not have committed.
					// The list-based oracle below covers the committed case.
				}
			}()
		}

		wg.Wait()

		raceDetails := details.With(internal.Details{
			"reference":     raceRef,
			"goroutines":    goroutines,
			"okCount":       okCount,
			"conflictCount": conflictCount,
		})

		// Two explicit OK acks = two committed transactions with one
		// reference (retries can only demote an OK into a conflict).
		assert.Always(okCount <= 1,
			"at most one concurrent same-reference create is acked",
			raceDetails)

		// Coverage: prove the race actually produced a loser at least once
		// across the run, so the Always above is not vacuously green.
		assert.Sometimes(okCount == 1 && conflictCount > 0,
			"concurrent same-reference race produced a winner and a conflict loser",
			raceDetails)

		assertReferenceUnique(ctx, client, ledger, raceRef, raceDetails)

		// ---- Half 2: aged-reference reuse ------------------------------------

		agedRef := fmt.Sprintf("refrace-%d-aged", run)

		if _, err := createWithReference(ctx, client, ledger, agedRef, "refrace-aged-dst"); err != nil {
			// Ambiguous originals would make the conflict expectation unsound
			// (the reference may not be taken) — only an acked original arms
			// the aged-reuse check.
			return
		}

		// Menu axis: filler volume. The harness runs with
		// CACHE_ROTATION_THRESHOLD=50, so ~100+ subsequent entries push the
		// original reference out of gen0+gen1 and onto the Pebble→preload→
		// bloom path; 60 stays within the window as a control point.
		fillerCount := antirandom.RandomChoice([]int{60, 120, 250})

		written := 0

		for i := range fillerCount {
			if ctx.Err() != nil {
				return
			}

			// Unique references so transparent retries cannot double-commit.
			if _, err := createWithReference(ctx, client, ledger,
				fmt.Sprintf("refrace-%d-fill-%d", run, i),
				fmt.Sprintf("refrace-fill:%d", i%8)); err == nil {
				written++
			}
		}

		agedDetails := details.With(internal.Details{
			"reference":   agedRef,
			"fillerCount": fillerCount,
			"fillerAcked": written,
		})

		// Retry the aged reference: a conclusive OK is a duplicate.
		_, err := createWithReference(ctx, client, ledger, agedRef, "refrace-aged-dst")

		switch {
		case err == nil:
			assert.Unreachable("aged reference reuse was acked as a new transaction", agedDetails)
		case internal.IsTransient(err):
			// Ambiguous: the list oracle below decides.
		default:
			assert.AlwaysOrUnreachable(
				internal.HasErrorReason(err, domain.ErrReasonTransactionReferenceConflict),
				"aged reference reuse is rejected with TRANSACTION_REFERENCE_CONFLICT",
				agedDetails.With(internal.Details{"error": err.Error()}))
		}

		assertReferenceUnique(ctx, client, ledger, agedRef, agedDetails)
	})
}
