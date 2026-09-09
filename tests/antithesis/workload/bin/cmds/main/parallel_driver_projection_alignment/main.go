// Driver for the prepared-query projection-alignment property. After a write
// has been acknowledged, a successful default (linearizable) read must include
// it: ReadIndex fixes a Raft horizon and the query waits only for the read
// projection to certify that same horizon.
//
// Soundness against benign interleavings:
//   - Owned "projection-" ledger (restricted prefix) + per-run unique query name:
//     no foreign writes, deletes, or query-name collisions.
//   - The probe account is created by the very write whose ack supplies S, so
//     index coverage of S implies the account row exists.
//   - Retry-safe infrastructure errors and cancellation are inconclusive;
//     permanent prepared-query errors are reported as unreachable.
package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

const probeAccount = "minseq-probe:main"

func isInconclusiveProjectionRead(err error) bool {
	// The workload client load-balances across replicas. The readiness poll can
	// observe one replica after its local switch while execution lands on
	// another whose projection is still building. That is setup lag, not an
	// alignment violation; a successful read must still satisfy the assertion
	// below. INDEX_NOT_FOUND and every other business error remain findings.
	return internal.IsTolerated(err) || internal.HasErrorReason(err, domain.ErrReasonIndexBuilding)
}

func main() {
	internal.RunDriver("parallel_driver_projection_alignment", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		run := r.Uint64()
		ledger := internal.PrefixProjectionAlign.WithSeed(run)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		queryName := fmt.Sprintf("projection-q-%d", run)
		indexIdempotencyKey := fmt.Sprintf("projection-index-%d", run)
		queryIdempotencyKey := fmt.Sprintf("projection-query-%d", run)
		probeIdempotencyKey := fmt.Sprintf("projection-probe-%d", run)
		details := internal.Details{"ledger": ledger, "queryName": queryName}

		// AccountHasAsset requires the account asset-presence index. Create it
		// with a stable idempotency key so a timed-out setup call can be retried
		// safely by a later driver run, then wait for this replica to switch the
		// freshly built keyspace before exercising projection alignment.
		if _, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(
			indexIdempotencyKey,
			actions.CreateAccountAssetIndexAction(ledger),
		)); err != nil {
			if !internal.IsTolerated(err) {
				assert.Unreachable("projection asset-index creation returned unexpected error",
					details.With(internal.Details{"error": err}))
			}

			return
		}
		if err := actions.WaitForAccountAssetIndexReady(ctx, client, ledger); err != nil {
			if !internal.IsTolerated(err) {
				assert.Unreachable("projection asset index did not become ready",
					details.With(internal.Details{"error": err}))
			}

			return
		}

		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(queryIdempotencyKey, &servicepb.Request{
			Type: &servicepb.Request_CreatePreparedQuery{
				CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
					Ledger: ledger,
					Query: &commonpb.PreparedQuery{
						Name:   queryName,
						Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
						Filter: &commonpb.QueryFilter{
							// AccountHasAsset is served by the asynchronous read
							// projection. The acknowledged transaction below creates
							// exactly this USD/2 membership, so a successful query can
							// only include the probe after projection certification.
							Filter: &commonpb.QueryFilter_AccountHasAsset{
								AccountHasAsset: &commonpb.AccountHasAssetCondition{
									AssetBase: "USD",
									Precision: 2,
								},
							},
						},
					},
				},
			},
		}))
		if err != nil {
			if !internal.IsTolerated(err) {
				assert.Unreachable("projection prepared-query creation returned unexpected error",
					details.With(internal.Details{"error": err}))
			}

			return
		}

		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(probeIdempotencyKey, &servicepb.Request{
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
							Reference: fmt.Sprintf("projection-%d", run),
							Force:     true,
						},
					}},
				},
			},
		}))
		if err != nil {
			if !internal.IsTolerated(err) {
				assert.Unreachable("projection probe apply returned unexpected error",
					details.With(internal.Details{"error": err}))
			}

			return
		}
		if len(resp.GetLogs()) == 0 {
			assert.Unreachable("projection probe apply succeeded without an audit log", details)

			return
		}
		details = details.With(internal.Details{"ackedSeq": resp.GetLogs()[len(resp.GetLogs())-1].GetSequence()})

		execResp, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
			Ledger:    ledger,
			QueryName: queryName,
			PageSize:  100,
		})
		if err != nil {
			if !isInconclusiveProjectionRead(err) {
				assert.Unreachable("projection-aligned prepared query returned unexpected error",
					details.With(internal.Details{"error": err}))
			}

			return
		}
		assert.Reachable("projection-aligned prepared query succeeded", details)

		cursor := execResp.GetCursor()
		if cursor == nil {
			assert.Unreachable("projection-aligned prepared query returned no cursor result", details)

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
			"projection-aligned read includes the acknowledged write",
			details.With(internal.Details{"returned": len(cursor.GetAccountData())}))
	})
}
