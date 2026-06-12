// Driver for the "bulk-mid-failure-no-partial-effects" property: a
// multi-request Apply (one proposal carrying multiple orders,
// raft_cmd.proto:362-365 "List of orders to execute atomically") whose LAST
// order deterministically fails must leave zero business effects from the
// earlier orders — no transactions, no account activity — plus only the
// deliberate Failure audit entry (one per failed proposal, never a Success
// entry). This is the untested failure half of the atomicity contract
// (parallel_driver_bulk only checks the success half; issue #343 alleges
// state can escape the WriteSet overlay).
//
// Soundness against benign interleavings:
//   - The ledgers are driver-owned ("bulkatom-" restricted prefix): nothing
//     can fund the engineered-overdraft source or write foreign entries into
//     the per-ledger audit stream.
//   - The failure is deterministic (overdraft from a never-funded source,
//     no Force), so a transparent UNAVAILABLE retry re-proposing the whole
//     bulk fails identically — retries cannot commit partial effects, they
//     can only add more Failure audit entries (hence "exactly one Failure
//     entry" is asserted per PROPOSAL via ProposalId dedup, not as a global
//     count).
//   - Effects checks run only after a definitive (FailedPrecondition-class)
//     rejection; ambiguous outcomes are skipped.
//   - Reads use a MinLogSequence floor from a marker write in a SEPARATE
//     owned helper ledger: the floor defeats read staleness (#398 class)
//     without polluting the primary ledger's audit stream with marker
//     Success entries.
//   - Audit "at least one Failure entry" is only a Sometimes: period
//     archival confirmation purges audit ranges, so an Always would
//     false-positive when the purge wins the race. The Success-entry bound
//     (<= 1, the CreateLedger entry) is purge-proof: purges only remove
//     entries, never add them.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	antirandom "github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// isDefinitiveBulkRejection reports whether the error conclusively means the
// proposal did not and will never take effect.
func isDefinitiveBulkRejection(err error) bool {
	if err == nil || internal.IsTransient(err) {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound:
		return true
	default:
		return false
	}
}

// listIsEmpty returns (empty, foundIDs, conclusive) for transactions matching
// the filter at the MinLogSequence floor. Read errors are inconclusive.
func listIsEmpty(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	ledger string,
	filter *commonpb.QueryFilter,
	minLogSeq uint64,
) (bool, []uint64, bool) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:         ledger,
		PageSize:       10,
		Filter:         filter,
		MinLogSequence: minLogSeq,
	})
	if err != nil {
		return false, nil, false
	}

	var ids []uint64

	for {
		tx, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return false, nil, false
		}

		ids = append(ids, tx.GetId())
	}

	return len(ids) == 0, ids, true
}

func main() {
	internal.RunDriver("parallel_driver_bulk_atomicity", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		run := r.Uint64()
		ledger := fmt.Sprintf("bulkatom-%d", run%1_000_000)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		// Menu axis: total order count in the proposal; the last order is the
		// engineered failure, all earlier ones are valid.
		bulkSize := antirandom.RandomChoice([]int{2, 5, 10})

		var (
			requests []*servicepb.Request
			refs     []string
			accounts []string
		)

		for i := range bulkSize - 1 {
			ref := fmt.Sprintf("bulkatom-%d-%d", run, i)
			account := fmt.Sprintf("bulkatom-acc:%d:%d", run%1_000_000, i)
			refs = append(refs, ref)
			accounts = append(accounts, account)

			requests = append(requests, &servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: account,
									Amount:      commonpb.NewUint256FromUint64(100),
									Asset:       "USD/2",
								}},
								Reference: ref,
								Force:     true,
							},
						}},
					},
				},
			})
		}

		// Last order: overdraft from a never-funded, driver-owned source
		// without Force — deterministically rejected with INSUFFICIENT_FUNDS.
		requests = append(requests, &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{{
								Source:      fmt.Sprintf("bulkatom-void:%d", run%1_000_000),
								Destination: "bulkatom-sink",
								Amount:      commonpb.NewUint256FromUint64(1),
								Asset:       "USD/2",
							}},
						},
					}},
				},
			},
		})

		_, err := client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})

		details := internal.Details{"ledger": ledger, "bulkSize": bulkSize}

		assert.Sometimes(isDefinitiveBulkRejection(err),
			"mid-failing atomic bulk definitively rejected",
			details.With(internal.Details{"error": fmt.Sprintf("%v", err)}))

		if !isDefinitiveBulkRejection(err) {
			// nil (the overdraft committed — owned by the insufficient-funds
			// driver, not an atomicity question) or ambiguous: inconclusive.
			return
		}

		// Consistency barrier: marker write in a separate owned helper ledger.
		// Log sequences are global, so the marker's sequence is a valid floor
		// for reads on the primary ledger; the marker's Success audit entry
		// lands in the helper ledger's stream, keeping the primary stream
		// limited to {CreateLedger success, bulk failures}.
		helper := fmt.Sprintf("bulkatom-%d-m", run%1_000_000)
		if err := internal.CreateLedger(ctx, client, helper); err != nil {
			return
		}

		markerResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: helper,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: "bulkatom-marker",
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
		if err != nil || len(markerResp.GetLogs()) == 0 {
			return
		}

		minLogSeq := markerResp.GetLogs()[len(markerResp.GetLogs())-1].GetSequence()
		details["minLogSeq"] = minLogSeq

		// Business effects: none of the earlier orders' references or account
		// activity may be visible — the whole proposal failed.
		for i, ref := range refs {
			empty, ids, conclusive := listIsEmpty(ctx, client, ledger, actions.ReferenceFilter(ref), minLogSeq)
			if conclusive {
				assert.Always(empty,
					"failed atomic bulk leaves no partial transaction effects",
					details.With(internal.Details{"reference": ref, "txIds": fmt.Sprintf("%v", ids)}))
			}

			empty, ids, conclusive = listIsEmpty(ctx, client, ledger, actions.AddressExactFilter(accounts[i]), minLogSeq)
			if conclusive {
				assert.Always(empty,
					"failed atomic bulk leaves no partial account activity",
					details.With(internal.Details{"account": accounts[i], "txIds": fmt.Sprintf("%v", ids)}))
			}
		}

		// Audit-side contract: the owned ledger's audit stream may contain at
		// most one Success entry (the CreateLedger proposal — a retried
		// create fails AlreadyExists and cannot add a second), and Failure
		// entries must be unique per ProposalId (a failed proposal writes
		// exactly one Failure entry by design, machine.go:1163-1204).
		auditStream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{
			Ledger:         ledger,
			MinLogSequence: minLogSeq,
		})
		if err != nil {
			return
		}

		var (
			successCount  int
			failureCount  int
			proposalSeen  = map[uint64]uint64{} // failure ProposalId → first audit sequence
			duplicateProp uint64
			hasDuplicate  bool
			readOK        = true
		)

		for {
			entry, err := auditStream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				readOK = false

				break
			}

			switch {
			case entry.GetSuccess() != nil:
				successCount++
			case entry.GetFailure() != nil:
				failureCount++

				if _, dup := proposalSeen[entry.GetProposalId()]; dup {
					hasDuplicate = true
					duplicateProp = entry.GetProposalId()
				}

				proposalSeen[entry.GetProposalId()] = entry.GetSequence()
			}
		}

		if !readOK {
			// Aborted stream: inconclusive, never a violation.
			return
		}

		auditDetails := details.With(internal.Details{
			"successCount":        successCount,
			"failureCount":        failureCount,
			"duplicateProposalId": duplicateProp,
		})

		assert.Always(successCount <= 1,
			"failed atomic bulk yields no success audit entry beyond ledger creation",
			auditDetails)

		assert.Always(!hasDuplicate,
			"audit failure entries are unique per proposal",
			auditDetails)

		// Coverage only: audit purge (period archival confirmation) can
		// legitimately remove the failure entry before this read.
		assert.Sometimes(failureCount >= 1,
			"failure audit entry observed for failed atomic bulk",
			auditDetails)
	})
}
