package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/internal/domain"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_transient_accounts", func(ctx context.Context, client ledgerpb.BucketServiceClient, _ string) {
		r := internal.Rand()

		// Use a dedicated ledger with a transient account type.
		ledger := internal.PrefixTransientAccounts.New()
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		typeName := fmt.Sprintf("clearing-%d", r.Uint64()%10000)
		pattern := typeName + ":{id}"
		details := internal.Details{"ledger": ledger, "typeName": typeName, "pattern": pattern}

		// 1. Add an account type with TRANSIENT persistence.
		_, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_AddAccountType{
				AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{
					Ledger: ledger,
					AccountType: &ledgerpb.AccountType{
						Name:        typeName,
						Pattern:     pattern,
						Persistence: ledgerpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
					},
				},
			},
		}))
		if err != nil {
			if internal.IsTransient(err) || internal.IsAlreadyExists(err) {
				return
			}

			assert.Unreachable("should be able to add transient account type",
				details.With(internal.Details{"error": err}))

			return
		}

		clearingAddr := fmt.Sprintf("%s:%d", typeName, r.Uint64()%1000)
		amount := r.Uint64()%1000 + 100
		details["clearingAddr"] = clearingAddr
		details["amount"] = amount

		// 2. Balanced batch: fund clearing account, then drain it in the same Apply.
		//    The transient account must end at zero — should succeed.
		_, err = client.Apply(ctx, ledgerpb.UnsignedApplyRequest("",
			&ledgerpb.Request{
				Type: &ledgerpb.Request_Apply{
					Apply: &ledgerpb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
							CreateTransaction: &ledgerpb.CreateTransactionPayload{
								Postings: []*ledgerpb.Posting{{
									Source:      "world",
									Destination: clearingAddr,
									Amount:      ledgerpb.NewUint256FromUint64(amount),
									Asset:       "USD/2",
								}},
								Force: true,
							},
						}},
					},
				},
			},
			&ledgerpb.Request{
				Type: &ledgerpb.Request_Apply{
					Apply: &ledgerpb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
							CreateTransaction: &ledgerpb.CreateTransactionPayload{
								Postings: []*ledgerpb.Posting{{
									Source:      clearingAddr,
									Destination: "world",
									Amount:      ledgerpb.NewUint256FromUint64(amount),
									Asset:       "USD/2",
								}},
								Force: true,
							},
						}},
					},
				},
			},
		))

		assert.Sometimes(internal.IsTolerated(err),
			"balanced transient batch should succeed",
			details.With(internal.Details{"error": err}))

		if err == nil {
			assert.Reachable("balanced transient batch succeeded", details)
		}

		// 3. Unbalanced batch: fund the clearing account without draining it.
		//    The transient account ends with non-zero balance — should fail.
		clearingAddr2 := fmt.Sprintf("%s:%d", typeName, r.Uint64()%1000+1000)
		details["clearingAddr2"] = clearingAddr2

		_, err = client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_Apply{
				Apply: &ledgerpb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
						CreateTransaction: &ledgerpb.CreateTransactionPayload{
							Postings: []*ledgerpb.Posting{{
								Source:      "world",
								Destination: clearingAddr2,
								Amount:      ledgerpb.NewUint256FromUint64(amount),
								Asset:       "USD/2",
							}},
							Force: true,
						},
					}},
				},
			},
		}))

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
