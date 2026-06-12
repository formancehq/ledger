// Driver for the "min-log-sequence-honored-or-refused" property on the
// prepared-query path (the list RPCs are already covered by the #398-era
// drivers): a successful ExecutePreparedQuery with MinLogSequence = S must
// reflect index state ≥ S — the acked write is present — and an explicit
// refusal must be a self-consistent FailedPrecondition with reason
// READ_INDEX_NOT_CAUGHT_UP carrying metadata current < requested
// (executor.go:84-96 → server.go:575-593).
//
// Two probes:
//
//	(a) write a tx (ack carries sequence S) then execute the prepared query
//	    with MinLogSequence=S: success ⇒ the destination account is in the
//	    result (fail-fast check + atomic progress/data commit make this exact);
//	    refusal ⇒ metadata must satisfy current < requested.
//	(b) fail-fast reachability: MinLogSequence = S + 1e6 (a sequence that does
//	    not exist yet) should be refused; the refusal's metadata is checked the
//	    same way. If the cluster genuinely advanced 1e6 sequences in between,
//	    success is not attributable as a violation — skipped.
//
// Soundness against benign interleavings:
//   - Owned "minseq-" ledger (restricted prefix) + per-run unique query name:
//     no foreign writes, deletes, or query-name collisions.
//   - The probe account is created by the very write whose ack supplies S, so
//     index coverage of S implies the account row exists.
//   - FailedPrecondition is not retried by any client retry layer (only
//     UNAVAILABLE is), so a refusal observed here is the server's own verdict.
//   - Transient errors (Unavailable, NotFound from lagging metadata) →
//     inconclusive, skip.
package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// futureSeqDelta is added to the acked sequence to build a freshness demand
// the index cannot have satisfied yet, forcing the fail-fast refusal branch.
const futureSeqDelta = 1_000_000

const probeAccount = "minseq-probe:main"

// readIndexRefusal extracts the READ_INDEX_NOT_CAUGHT_UP ErrorInfo metadata
// from a FailedPrecondition status. ok is false for any other error shape.
func readIndexRefusal(err error) (requested, current uint64, ok bool) {
	st, stOK := status.FromError(err)
	if !stOK || st.Code() != codes.FailedPrecondition {
		return 0, 0, false
	}

	for _, detail := range st.Details() {
		info, isInfo := detail.(*errdetails.ErrorInfo)
		if !isInfo || info.GetReason() != "READ_INDEX_NOT_CAUGHT_UP" {
			continue
		}

		req, reqErr := strconv.ParseUint(info.GetMetadata()["requested"], 10, 64)
		cur, curErr := strconv.ParseUint(info.GetMetadata()["current"], 10, 64)
		if reqErr != nil || curErr != nil {
			// Malformed metadata is itself a refusal-fidelity defect.
			assert.Unreachable("READ_INDEX_NOT_CAUGHT_UP refusal carries unparsable metadata",
				internal.Details{"metadata": fmt.Sprintf("%v", info.GetMetadata())})

			return 0, 0, false
		}

		return req, cur, true
	}

	return 0, 0, false
}

// checkRefusal asserts self-consistency of a freshness refusal. Called from
// both probes so the assertion registers as a single site.
func checkRefusal(requested, current uint64, details internal.Details) {
	assert.Always(current < requested,
		"freshness refusal carries current below requested",
		details.With(internal.Details{"requested": requested, "current": current}))
}

func main() {
	internal.RunDriver("parallel_driver_minlogseq", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		run := r.Uint64()
		ledger := fmt.Sprintf("minseq-%d", run%1_000_000)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		queryName := fmt.Sprintf("minseq-q-%d", run)
		details := internal.Details{"ledger": ledger, "queryName": queryName}

		// Prepared query matching the probe account by address prefix.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_CreatePreparedQuery{
					CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
						Query: &commonpb.PreparedQuery{
							Name:   queryName,
							Ledger: ledger,
							Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
							Filter: &commonpb.QueryFilter{
								Filter: &commonpb.QueryFilter_Address{
									Address: &commonpb.AddressMatch{
										Match: &commonpb.AddressMatch_HardcodedPrefix{
											HardcodedPrefix: "minseq-probe:",
										},
									},
								},
							},
						},
					},
				},
			}},
		})
		if err != nil {
			// Ambiguous or transient: without a confirmed query, every later
			// outcome is unattributable — inconclusive.
			return
		}

		// Write the probe transaction; its ack carries the log sequence S.
		resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: probeAccount,
									Amount:      commonpb.NewUint256FromUint64(1),
									Asset:       "USD/2",
								}},
								Reference: fmt.Sprintf("minseq-%d", run),
								Force:     true,
							},
						}},
					},
				},
			}},
		})
		if err != nil {
			return
		}

		logs := resp.GetLogs()
		if len(logs) == 0 {
			return
		}

		ackedSeq := logs[len(logs)-1].GetSequence()
		details = details.With(internal.Details{"ackedSeq": ackedSeq})

		// Probe (a): MinLogSequence = S — honored on success, or refused
		// self-consistently.
		execResp, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
			Ledger:         ledger,
			QueryName:      queryName,
			PageSize:       100,
			MinLogSequence: ackedSeq,
		})

		if err == nil {
			assert.Reachable("prepared query honoring MinLogSequence succeeded", details)

			cursor := execResp.GetCursor()
			if cursor == nil {
				// Accounts-target cursor query must return a cursor result.
				assert.Unreachable("prepared query returned no cursor result", details)

				return
			}

			found := false
			for _, account := range cursor.GetAccountData() {
				if account.GetAddress() == probeAccount {
					found = true

					break
				}
			}

			assert.Always(found,
				"read honoring MinLogSequence includes the acknowledged write",
				details.With(internal.Details{"returned": len(cursor.GetAccountData())}))
		} else if requested, current, ok := readIndexRefusal(err); ok {
			// An explicit freshness refusal of probe (a) is acceptable —
			// provided it is self-consistent. Any other error (Unavailable
			// after retries, etc.) is inconclusive; probe (b) still runs.
			checkRefusal(requested, current, details)
		}

		// Probe (b): demand a sequence far beyond anything committed — the
		// fail-fast branch should refuse. A success here means the cluster
		// really advanced futureSeqDelta sequences meanwhile (possible in a
		// long, busy run) — not attributable as a violation, skipped.
		_, err = client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
			Ledger:         ledger,
			QueryName:      queryName,
			PageSize:       100,
			MinLogSequence: ackedSeq + futureSeqDelta,
		})
		if err == nil {
			return
		}

		if requested, current, ok := readIndexRefusal(err); ok {
			assert.Reachable("prepared query refused with READ_INDEX_NOT_CAUGHT_UP", details)
			checkRefusal(requested, current, details)
		}
	})
}
