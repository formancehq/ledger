package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_concurrent_ledger_delete", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		// 1. Create a dedicated ephemeral ledger.
		ledger := fmt.Sprintf("deltest-%d", r.Uint64()%1_000_000)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		details := internal.Details{"ledger": ledger}

		// 2. Concurrently: one goroutine writes transactions, the other deletes the ledger.
		var wg sync.WaitGroup

		// Track write results.
		var (
			writeErr       error
			deletedSeen    bool
			deleteOK       bool
			writeAttempts  int
		)

		wg.Add(2)

		// Writer: send a few transactions.
		go func() {
			defer wg.Done()

			for range 5 {
				writeAttempts++

				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{{
						Type: &servicepb.Request_Apply{
							Apply: &servicepb.LedgerApplyRequest{
								Ledger: ledger,
								Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
									CreateTransaction: &servicepb.CreateTransactionPayload{
										Postings: []*commonpb.Posting{{
											Source:      "world",
											Destination: fmt.Sprintf("users:%d", r.Uint64()%internal.UserAccountCount),
											Amount:      commonpb.NewUint256FromUint64(100),
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
					if internal.IsLedgerDeleted(err) || internal.IsLedgerNotFound(err) {
						deletedSeen = true

						return
					}

					if internal.IsTransient(err) {
						writeErr = err

						return
					}

					writeErr = err

					return
				}
			}
		}()

		// Deleter: delete the ledger.
		go func() {
			defer wg.Done()

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_DeleteLedger{
						DeleteLedger: &servicepb.DeleteLedgerRequest{Name: ledger},
					},
				}},
			})
			if err == nil {
				deleteOK = true

				return
			}

			if !internal.IsTransient(err) {
				assert.Unreachable("delete ledger should not fail unexpectedly",
					details.With(internal.Details{"error": err}))
			}
		}()

		wg.Wait()

		// If the delete failed (e.g. transient error during leadership change),
		// the ledger still exists — nothing to assert about post-delete writes.
		if !deleteOK {
			assert.Reachable("concurrent ledger delete path exercised", details)

			return
		}

		// 3. After deletion, any write should fail with LEDGER_DELETED or LEDGER_NOT_FOUND.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: "users:0",
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

		if err == nil {
			assert.Unreachable("write after delete should fail",
				details.With(internal.Details{
					"deletedSeen":   deletedSeen,
					"writeAttempts": writeAttempts,
				}))

			return
		}

		if internal.IsTransient(err) {
			assert.Reachable("concurrent ledger delete path exercised", details)

			return
		}

		isDeletedOrNotFound := internal.HasErrorReason(err, domain.ErrReasonLedgerDeleted) ||
			internal.HasErrorReason(err, domain.ErrReasonLedgerNotFound) ||
			internal.IsLedgerNotFound(err)
		assert.AlwaysOrUnreachable(isDeletedOrNotFound,
			"write to deleted ledger should return LEDGER_DELETED or LEDGER_NOT_FOUND",
			details.With(internal.Details{
				"error":         err,
				"deletedSeen":   deletedSeen,
				"writeErr":      writeErr,
				"writeAttempts": writeAttempts,
			}))

		assert.Reachable("concurrent ledger delete path exercised", details)
	})
}
