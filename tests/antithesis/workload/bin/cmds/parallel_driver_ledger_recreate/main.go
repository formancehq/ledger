// Driver for the "deleted-ledger-data-isolation-and-eventual-purge" property
// (workload half): after DeleteLedger commits, a same-name recreated ledger
// behaves as a brand-new ledger — none of the predecessor's transactions or
// account activity is visible through it, and the predecessor's references
// are reusable (reference uniqueness is keyed by the monotonic LedgerID, not
// the name). The deferred-Pebble-cleanup half is anchored SUT-side by the
// Reachable at executePurge (internal/infra/state/write_set.go); the
// in-flight-delete outcome disjunction is exercised by
// parallel_driver_concurrent_ledger_delete and the final-state oracle.
//
// Soundness against benign interleavings:
//   - The ledger is driver-owned ("lrecreate-" restricted prefix): no foreign
//     driver writes to it, deletes it, or takes its references.
//   - Pre-delete writes carry unique run-scoped references, recorded only on
//     explicit ack; the isolation check runs BEFORE the reference-reuse
//     write, so an old reference found through the new incarnation can only
//     be predecessor data.
//   - All isolation reads go through ListTransactions with a MinLogSequence
//     floor taken from a marker write in the NEW incarnation. The floor
//     guarantees the serving store has applied past the delete+recreate, so
//     name→ID resolution yields the new LedgerID — without it, a legitimately
//     stale (prefix-consistent) read could resolve the name to the OLD ID and
//     "see" old data, a false positive. GetAccount has no freshness floor and
//     is deliberately not used.
//   - An ambiguous delete followed by a successful recreate is safe: the
//     recreate only succeeds if the delete committed (AlreadyExists
//     otherwise, which bails out as inconclusive).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	antirandom "github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// createTx attempts a CreateTransaction and returns the Apply response.
func createTx(
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
								Amount:      commonpb.NewUint256FromUint64(100),
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

// listMatches lists transactions matching the filter at the MinLogSequence
// floor and returns (foundIDs, conclusive). Read errors are inconclusive.
func listMatches(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	ledger string,
	filter *commonpb.QueryFilter,
	minLogSeq uint64,
) ([]uint64, bool) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: 10,
			Filter:   filter,
			Read:     &commonpb.ReadOptions{MinLogSequence: minLogSeq},
		},
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

func main() {
	internal.RunDriver("parallel_driver_ledger_recreate", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		run := r.Uint64()
		ledger := fmt.Sprintf("lrecreate-%d", run%1_000_000)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		details := internal.Details{"ledger": ledger}

		// 1. Populate the first incarnation. Menu axis: predecessor data size.
		txCount := antirandom.RandomChoice([]int{3, 5})

		var (
			ackedRefs     []string
			ackedAccounts []string
		)

		for i := range txCount {
			ref := fmt.Sprintf("lrec-%d-%d", run, i)
			account := fmt.Sprintf("lrec-old:%d:%d", run%1_000_000, i)

			if _, err := createTx(ctx, client, ledger, ref, account); err != nil {
				// Ambiguous outcomes still belong to the OLD incarnation, but
				// only acked writes participate in the oracles.
				continue
			}

			ackedRefs = append(ackedRefs, ref)
			ackedAccounts = append(ackedAccounts, account)
		}

		assert.Sometimes(len(ackedRefs) > 0,
			"ledger recreate predecessor write set recorded", details)

		if len(ackedRefs) == 0 {
			return
		}

		// 2. Delete the first incarnation.
		if _, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_DeleteLedger{
					DeleteLedger: &servicepb.DeleteLedgerRequest{Name: ledger},
				},
			}},
		}); err != nil && !internal.IsTransient(err) {
			// Ambiguous deletes are resolved by step 3 (recreate succeeds only
			// if the delete committed); definitive failures are inconclusive
			// for this property.
			return
		}

		// 3. Recreate the SAME name. AlreadyExists ⇒ the delete did not
		// commit ⇒ inconclusive.
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		// 4. Marker write in the NEW incarnation: its global log sequence is
		// the freshness floor for every isolation read below.
		markerResp, err := createTx(ctx, client, ledger, "", "lrec-marker")
		if err != nil || len(markerResp.GetLogs()) == 0 {
			return
		}

		minLogSeq := markerResp.GetLogs()[len(markerResp.GetLogs())-1].GetSequence()
		details["minLogSeq"] = minLogSeq

		// 5. Isolation: no predecessor reference or account activity may be
		// visible through the recreated ledger. Runs BEFORE the reuse write.
		for i, ref := range ackedRefs {
			ids, conclusive := listMatches(ctx, client, ledger, actions.ReferenceFilter(ref), minLogSeq)
			if conclusive {
				assert.Always(len(ids) == 0,
					"recreated ledger never exposes predecessor transactions",
					details.With(internal.Details{"reference": ref, "txIds": fmt.Sprintf("%v", ids)}))
			}

			ids, conclusive = listMatches(ctx, client, ledger, actions.AddressExactFilter(ackedAccounts[i]), minLogSeq)
			if conclusive {
				assert.Always(len(ids) == 0,
					"recreated ledger never exposes predecessor account activity",
					details.With(internal.Details{"account": ackedAccounts[i], "txIds": fmt.Sprintf("%v", ids)}))
			}
		}

		// 6. Reference reuse: the new incarnation has a fresh LedgerID, so a
		// predecessor reference must be accepted. A definitive
		// TRANSACTION_REFERENCE_CONFLICT means the old incarnation's
		// reference attribute leaked into the new namespace.
		reuseRef := ackedRefs[0]
		_, err = createTx(ctx, client, ledger, reuseRef, "lrec-new:0")

		reuseDetails := details.With(internal.Details{"reference": reuseRef, "error": fmt.Sprintf("%v", err)})

		if err != nil && internal.IsTransient(err) {
			return
		}

		assert.Always(!internal.HasErrorReason(err, domain.ErrReasonTransactionReferenceConflict),
			"predecessor references are reusable after ledger recreate",
			reuseDetails)

		assert.Sometimes(err == nil,
			"predecessor reference accepted by recreated ledger",
			reuseDetails)
	})
}
