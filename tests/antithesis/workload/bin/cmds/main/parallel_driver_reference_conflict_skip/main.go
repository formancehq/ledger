// parallel_driver_reference_conflict_skip is the skip-tolerant sibling of
// parallel_driver_reference_conflict: it exercises the per-order
// skippable_reasons opt-in that converts a TRANSACTION_REFERENCE_CONFLICT
// failure into a well-formed OrderSkipped log at HTTP 200. Under chaos the
// interesting properties are:
//
//   - the audit chain re-derives the whitelist per order, so the checker's
//     verifySkippedOrder pass validates every landed skip against the
//     chain-bound Order.skippable_reasons;
//
//   - dispatchElisionCheck runs at every skip-expected seq, so a projection
//     tampered to a non-Apply payload / nil Data at the skip's seq is
//     caught (Antithesis's SUT does not tamper on disk, but the elision
//     guard's forward pass — INVALID_SKIP on a fabricated skip — is
//     exercised here whenever a partition rewrites a seq under contention).
//
// The driver also covers the "different reference" path (skippable_reasons
// set but skip NOT fired) so a bug that always fires the skip converts a
// legitimate first-claim into an OrderSkipped and this driver flags it.
package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_reference_conflict_skip", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		ref := fmt.Sprintf("skipref-%d", internal.Rand().Uint64())
		details := internal.Details{"ledger": ledger, "reference": ref}

		// 1. Prime the reference with a first successful transaction.
		firstReq := servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings:  internal.RandomPostings(),
							Reference: ref,
							Force:     true,
						},
					}},
				},
			},
		})

		firstResp, err := client.Apply(ctx, firstReq)
		assert.Sometimes(internal.IsTolerated(err),
			"skip-tolerant first-claim should be able to create a transaction with a reference",
			details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		firstTx := internal.CheckCreatedTransaction(firstResp, details)
		if firstTx == nil {
			return
		}

		details["firstTxId"] = firstTx.Transaction.Id

		// 2. Replay with the SAME reference AND skippable_reasons opt-in — the
		// FSM must convert the reference-conflict failure into an
		// OrderSkipped log carrying the first tx id in its context.
		skipReq := servicepb.UnsignedApplyRequest("", actions.WithSkippableReasons(
			&servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings:  internal.RandomPostings(),
								Reference: ref,
								Force:     true,
							},
						}},
					},
				},
			},
			commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		))

		skipResp, err := client.Apply(ctx, skipReq)
		if !internal.IsTolerated(err) {
			// A business error here means the skip conversion failed to fire
			// even though skippable_reasons opted in and the reference IS
			// already claimed — that is the exact bug this driver guards.
			assert.Unreachable("skip-tolerant duplicate reference must not return a business error",
				details.With(internal.Details{"error": err}))

			return
		}
		if err != nil {
			return
		}

		if len(skipResp.GetLogs()) == 0 {
			assert.Unreachable("skip-tolerant duplicate reference must return exactly one log",
				details.With(internal.Details{"logs": len(skipResp.GetLogs())}))

			return
		}

		skipLog := skipResp.Logs[0].GetPayload().GetApply().GetLog()
		if skipLog == nil {
			assert.Unreachable("skip-tolerant duplicate reference must return an Apply log",
				details)

			return
		}

		skipped := skipLog.GetData().GetOrderSkipped()
		assert.AlwaysOrUnreachable(skipped != nil,
			"skip-tolerant duplicate reference must land as an OrderSkipped log",
			details.With(internal.Details{"payload_type": fmt.Sprintf("%T", skipLog.GetData().GetPayload())}))
		if skipped == nil {
			return
		}

		reason := skipped.GetReason()
		assert.AlwaysOrUnreachable(reason == commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
			"OrderSkipped reason must be TRANSACTION_REFERENCE_CONFLICT",
			details.With(internal.Details{"reason": reason.String()}))

		gotRef := skipped.GetContext()["reference"]
		assert.AlwaysOrUnreachable(gotRef == ref,
			"OrderSkipped.context.reference must match the requested reference",
			details.With(internal.Details{"context_reference": gotRef}))

		gotLedger := skipped.GetContext()["ledger"]
		assert.AlwaysOrUnreachable(gotLedger == ledger,
			"OrderSkipped.context.ledger must match the target ledger",
			details.With(internal.Details{"context_ledger": gotLedger}))

		gotExistingID := skipped.GetContext()["existingTransactionId"]
		expectedExistingID := strconv.FormatUint(firstTx.Transaction.Id, 10)
		assert.AlwaysOrUnreachable(gotExistingID == expectedExistingID,
			"OrderSkipped.context.existingTransactionId must point to the first tx",
			details.With(internal.Details{"context_existingTransactionId": gotExistingID, "expected": expectedExistingID}))

		assert.Reachable("reference-conflict skip path exercised", details)

		// 3. Same skippable_reasons opt-in on a FRESH reference must NOT fire
		// the skip — the FSM has to produce a normal CreatedTransaction. A
		// bug that fires the skip unconditionally would otherwise mask the
		// first-claim path here.
		freshRef := fmt.Sprintf("skipref-fresh-%d", internal.Rand().Uint64())
		freshDetails := internal.Details{"ledger": ledger, "reference": freshRef}

		freshReq := servicepb.UnsignedApplyRequest("", actions.WithSkippableReasons(
			&servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings:  internal.RandomPostings(),
								Reference: freshRef,
								Force:     true,
							},
						}},
					},
				},
			},
			commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		))

		freshResp, err := client.Apply(ctx, freshReq)
		assert.Sometimes(internal.IsTolerated(err),
			"skip-tolerant first-claim on a fresh reference should be able to create a transaction",
			freshDetails.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		if freshTx := internal.CheckCreatedTransaction(freshResp, freshDetails); freshTx != nil {
			assert.Reachable("skip-tolerant first-claim on a fresh reference landed as CreatedTransaction", freshDetails)

			return
		}

		// Fresh reference produced neither a CreatedTransaction nor a
		// tolerated error — inspect whether the FSM incorrectly fired the
		// skip. Only flag if we can conclusively identify an OrderSkipped;
		// an ambiguous transient path leaves the response empty and stays
		// silent.
		if len(freshResp.GetLogs()) > 0 {
			if freshResp.Logs[0].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped() != nil {
				assert.Unreachable("skip-tolerant first-claim on a fresh reference must NOT fire the skip",
					freshDetails)
			}
		}
	})
}
