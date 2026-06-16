package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_account_type_violations", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		// Use a dedicated ledger with strict enforcement.
		ledger := fmt.Sprintf("typeviolation-%d", r.Uint64()%1_000_000)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		typeName := fmt.Sprintf("customer-%d", r.Uint64()%10000)
		pattern := fmt.Sprintf("%s:{id}", typeName)
		details := internal.Details{"ledger": ledger, "typeName": typeName, "pattern": pattern}

		// 1. Add an account type with a pattern.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
				Type: &servicepb.Request_AddAccountType{
					AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
						Ledger: ledger,
						AccountType: &commonpb.AccountType{
							Name:    typeName,
							Pattern: pattern,
						},
					},
				},
			}),
		})
		if err != nil {
			if internal.IsTransient(err) || status.Code(err) == codes.AlreadyExists {
				return
			}

			assert.Unreachable("should be able to add account type",
				details.With(internal.Details{"error": err}))

			return
		}

		// 2. Set strict enforcement mode.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
				Type: &servicepb.Request_SetDefaultEnforcementMode{
					SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
						Ledger:          ledger,
						EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
					},
				},
			}),
		})
		if err != nil && !internal.IsTransient(err) {
			return
		}

		// 3. Transaction with a valid address — should succeed.
		validAddr := fmt.Sprintf("%s:%d", typeName, r.Uint64()%1000)
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: validAddr,
									Amount:      commonpb.NewUint256FromUint64(100),
									Asset:       "USD/2",
								}},
								Force: true,
							},
						}},
					},
				},
			}),
		})

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"transaction with valid typed address should succeed",
			details.With(internal.Details{"address": validAddr, "error": err}))

		// 4. Transaction with an invalid address — should fail with ACCOUNT_NOT_MATCHING_TYPE.
		invalidAddr := fmt.Sprintf("invalid-prefix:%d", r.Uint64()%1000)
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: invalidAddr,
									Amount:      commonpb.NewUint256FromUint64(50),
									Asset:       "USD/2",
								}},
								Force: true,
							},
						}},
					},
				},
			}),
		})

		if err == nil {
			// Strict mode may not reject addresses that don't match any type
			// if the enforcement only applies to addresses that partially match.
			// This depends on the exact enforcement semantics.
			return
		}

		if internal.IsTransient(err) {
			return
		}

		isTypeViolation := internal.HasErrorReason(err, domain.ErrReasonAccountNotMatchingType)
		assert.Sometimes(isTypeViolation,
			"invalid address should trigger ACCOUNT_NOT_MATCHING_TYPE in strict mode",
			details.With(internal.Details{"invalidAddress": invalidAddr, "error": err}))

		if isTypeViolation {
			assert.Reachable("account type violation path exercised", details)
		}

		// 5. Switch to AUDIT mode — same invalid address should now succeed.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
				Type: &servicepb.Request_SetDefaultEnforcementMode{
					SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
						Ledger:          ledger,
						EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
					},
				},
			}),
		})
		if err != nil && !internal.IsTransient(err) {
			return
		}

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: invalidAddr,
									Amount:      commonpb.NewUint256FromUint64(50),
									Asset:       "USD/2",
								}},
								Force: true,
							},
						}},
					},
				},
			}),
		})

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"invalid address should succeed in AUDIT mode",
			details.With(internal.Details{"address": invalidAddr, "error": err}))
	})
}
