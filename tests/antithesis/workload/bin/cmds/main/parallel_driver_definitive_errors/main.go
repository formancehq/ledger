// Driver for the "definitive-write-error-no-commit" property: a write
// rejected with a definitive gRPC code (InvalidArgument, FailedPrecondition,
// NotFound) must never appear in the ledger. Only ambiguous codes
// (Unavailable, Internal, Unknown, DeadlineExceeded) may correspond to a
// committed write — those attempts do not participate in the absence check.
//
// AlreadyExists is deliberately excluded from the definitive set even though
// it is definitive for the rejected attempt: a reference conflict means a
// transaction with that reference already exists (written by someone else, or
// by an earlier ambiguous-but-committed attempt of the same payload via the
// client's transparent UNAVAILABLE retry), so asserting its absence would
// false-positive by construction.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

const attemptCount = 8

// rejection records an attempt that failed with a definitive gRPC code.
type rejection struct {
	reference string
	code      codes.Code
	errMsg    string
}

// isDefinitiveCode reports whether the gRPC code promises the write did not
// and will never take effect. Maintenance-mode and stale-proposal errors map
// to Unavailable server-side, so they never reach this set.
func isDefinitiveCode(c codes.Code) bool {
	switch c {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound:
		return true
	default:
		return false
	}
}

// referenceFilterCheck lists transactions matching the reference and returns
// (found, foundTxID, conclusive). conclusive is false when the read failed
// (transient error, store lag, mid-stream error) — the ref is then skipped
// rather than asserted on.
func referenceFilterCheck(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	ledger, ref string,
	minLogSeq uint64,
) (bool, uint64, bool) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: 10,
			Filter:   actions.ReferenceFilter(ref),
			Read:     &commonpb.ReadOptions{MinLogSequence: minLogSeq},
		},
	})
	if err != nil {
		return false, 0, false
	}

	var (
		found   bool
		foundID uint64
	)

	for {
		tx, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			// Mid-stream failure: the read is inconclusive, never a violation.
			return false, 0, false
		}

		found = true
		foundID = tx.GetId()
	}

	return found, foundID, true
}

func main() {
	internal.RunDriver("parallel_driver_definitive_errors", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		// Dedicated ledger + dedicated account prefix: nothing ever funds
		// "deferr-src:*", so an overdraft from it is deterministically
		// rejected and a committed one is a genuine violation (no benign
		// interleaving where another driver credits the source).
		run := r.Uint64()
		ledger := internal.PrefixDefinitiveErrors.WithSeed(run)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		// Menu axis (issue #321 overflow class): boundary amounts 1, 2^63,
		// and 2^256-1 — all strictly above the source's zero balance.
		amounts := []*commonpb.Uint256{
			commonpb.NewUint256FromUint64(1),
			commonpb.NewUint256FromUint64(1 << 63),
			{V0: math.MaxUint64, V1: math.MaxUint64, V2: math.MaxUint64, V3: math.MaxUint64},
		}

		var rejected []rejection

		for i := range attemptCount {
			ref := fmt.Sprintf("deferr-%d-%d", run, i)
			source := fmt.Sprintf("deferr-src:%d", r.Uint64()%internal.UserAccountCount)

			// Two engineered rejection classes: (a) overdraft without Force
			// from a never-funded account → FailedPrecondition-class;
			// (b) malformed asset → InvalidArgument-class (the source is
			// still empty, so even lax asset validation cannot commit it).
			asset := "USD/2"
			if i%4 == 3 {
				asset = "!!not a valid asset!!"
			}

			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      source,
									Destination: "deferr-sink",
									Amount:      amounts[i%len(amounts)],
									Asset:       asset,
								}},
								Reference: ref,
							},
						}},
					},
				},
			}))
			if err == nil {
				// Unexpected success: not this property's concern (the
				// insufficient-funds driver owns "overdraft must fail").
				// The ref simply does not participate.
				continue
			}

			// Ambiguous outcomes (Unavailable after retries, ledger races,
			// Internal/Unknown/DeadlineExceeded) may have committed — skip.
			if internal.IsTransient(err) || ctx.Err() != nil {
				continue
			}

			st, ok := status.FromError(err)
			if !ok || !isDefinitiveCode(st.Code()) {
				continue
			}

			rejected = append(rejected, rejection{reference: ref, code: st.Code(), errMsg: st.Message()})
		}

		details := internal.Details{"ledger": ledger, "rejectedCount": len(rejected)}

		assert.Sometimes(len(rejected) > 0,
			"definitive write rejection recorded for absence check",
			details)

		if len(rejected) == 0 {
			return
		}

		// Consistency barrier with a usable read floor: a successful marker
		// write is sequenced after any hypothetically-committed rejected
		// write (its rejection response was received before the marker was
		// proposed), so MinLogSequence = marker sequence forces the read
		// store past the window where a committed-but-rejected write could
		// hide. Transparent UNAVAILABLE retries of the marker are harmless
		// (no reference, idempotent for this purpose).
		markerResp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{{
								Source:      "world",
								Destination: "deferr-marker",
								Amount:      commonpb.NewUint256FromUint64(1),
								Asset:       "USD/2",
							}},
							Force: true,
						},
					}},
				},
			},
		}))
		if err != nil {
			// Without the barrier the absence check would race read-staleness
			// (#398 class) — bail out, inconclusive.
			return
		}

		logs := markerResp.GetLogs()
		if len(logs) == 0 {
			return
		}

		minLogSeq := logs[len(logs)-1].GetSequence()

		for _, rej := range rejected {
			found, foundID, conclusive := referenceFilterCheck(ctx, client, ledger, rej.reference, minLogSeq)
			if !conclusive {
				continue
			}

			assert.Always(!found,
				"definitively rejected write never appears in the ledger",
				details.With(internal.Details{
					"reference": rej.reference,
					"code":      rej.code.String(),
					"error":     rej.errMsg,
					"foundTxId": foundID,
				}))
		}
	})
}
