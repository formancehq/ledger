package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_transient_accounts", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		// Use a dedicated ledger with a transient account type.
		ledger := fmt.Sprintf("transient-%d", r.Uint64()%1_000_000)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		typeName := fmt.Sprintf("clearing-%d", r.Uint64()%10000)
		pattern := fmt.Sprintf("%s:{id}", typeName)
		details := internal.Details{"ledger": ledger, "typeName": typeName, "pattern": pattern}

		// 1. Add an account type with TRANSIENT persistence.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_AddAccountType{
					AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
						Ledger: ledger,
						AccountType: &commonpb.AccountType{
							Name:        typeName,
							Pattern:     pattern,
							Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
						},
					},
				},
			}},
		})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			assert.Unreachable("should be able to add transient account type",
				details.With(internal.Details{"error": err}))

			return
		}

		clearingAddr := fmt.Sprintf("%s:%d", typeName, r.Uint64()%1000)
		amount := uint64(r.Uint64()%1000 + 100)
		details["clearingAddr"] = clearingAddr
		details["amount"] = amount

		// 2. Balanced batch: fund clearing account, then drain it in the same Apply.
		//    The transient account must end at zero — should succeed.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				{
					Type: &servicepb.Request_Apply{
						Apply: &servicepb.LedgerApplyRequest{
							Ledger: ledger,
							Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
								CreateTransaction: &servicepb.CreateTransactionPayload{
									Postings: []*commonpb.Posting{{
										Source:      "world",
										Destination: clearingAddr,
										Amount:      commonpb.NewUint256FromUint64(amount),
										Asset:       "USD/2",
									}},
									Force: true,
								},
							}},
						},
					},
				},
				{
					Type: &servicepb.Request_Apply{
						Apply: &servicepb.LedgerApplyRequest{
							Ledger: ledger,
							Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
								CreateTransaction: &servicepb.CreateTransactionPayload{
									Postings: []*commonpb.Posting{{
										Source:      clearingAddr,
										Destination: "world",
										Amount:      commonpb.NewUint256FromUint64(amount),
										Asset:       "USD/2",
									}},
									Force: true,
								},
							}},
						},
					},
				},
			},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"balanced transient batch should succeed",
			details.With(internal.Details{"error": err}))

		if err == nil {
			assert.Reachable("balanced transient batch succeeded", details)
		}

		// 3. Unbalanced batch: fund the clearing account without draining it.
		//    The transient account ends with non-zero balance — should fail.
		clearingAddr2 := fmt.Sprintf("%s:%d", typeName, r.Uint64()%1000+1000)
		details["clearingAddr2"] = clearingAddr2

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: clearingAddr2,
									Amount:      commonpb.NewUint256FromUint64(amount),
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
			assert.Unreachable("unbalanced transient batch should fail", details)

			return
		}

		if internal.IsTransient(err) {
			return
		}

		isNonZero := internal.HasErrorReason(err, domain.ErrReasonTransientAccountNonZero)
		assert.AlwaysOrUnreachable(isNonZero,
			"unbalanced transient batch should return TRANSIENT_ACCOUNT_NON_ZERO",
			details.With(internal.Details{"error": err}))

		assert.Reachable("transient account non-zero path exercised", details)
	})
}
