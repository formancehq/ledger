package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_account_types", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		// Create a dedicated ledger so account type patterns don't interfere
		// with other drivers using the shared default ledger.
		ledger := internal.PrefixAccountTypes.New()
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		typeName := fmt.Sprintf("type-%d", r.Uint64())
		pattern := fmt.Sprintf("%s:{id}", typeName)

		details := internal.Details{"ledger": ledger, "typeName": typeName, "pattern": pattern}

		// 1. Add an account type with a pattern.
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_AddAccountType{
				AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
					Ledger: ledger,
					AccountType: &commonpb.AccountType{
						Name:    typeName,
						Pattern: pattern,
					},
				},
			},
		}))
		assert.Sometimes(internal.IsTolerated(err) || status.Code(err) == codes.AlreadyExists,
			"should be able to add account type", details.With(internal.Details{"error": err}))
		if err != nil && !internal.IsTransient(err) {
			st, _ := status.FromError(err)
			if st.Code() != codes.AlreadyExists {
				return
			}
		}

		// Skip verification if the add was not committed (transient error).
		if err != nil && internal.IsTransient(err) {
			return
		}

		// 2. Verify the account type appears in the ledger info.
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		if err != nil {
			internal.LogCleanupError("get ledger after account type add", err)
			return
		}

		_, found := info.GetAccountTypes()[typeName]
		assert.AlwaysOrUnreachable(found, "added account type should appear in ledger info", details)

		// 3. Create a transaction using an address matching the pattern.
		matchingAddr := fmt.Sprintf("%s:%d", typeName, r.Uint64()%1000)
		typedTxResp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{{
								Source:      "world",
								Destination: matchingAddr,
								Amount:      commonpb.NewUint256FromUint64(100),
								Asset:       "USD/2",
							}},
							Force: true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err),
			"should be able to create tx with typed account address",
			details.With(internal.Details{"address": matchingAddr, "error": err}))
		if err == nil {
			internal.CheckCreatedTransaction(typedTxResp, details.With(internal.Details{"address": matchingAddr}))
		}

		// 4. Set enforcement mode to AUDIT (permissive logging).
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_SetDefaultEnforcementMode{
				SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
					Ledger:          ledger,
					EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err),
			"should be able to set enforcement mode", details.With(internal.Details{"error": err}))

		// 5. Remove the account type.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_RemoveAccountType{
				RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{
					Ledger: ledger,
					Name:   typeName,
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err),
			"should be able to remove account type", details.With(internal.Details{"error": err}))

		// 6. Reset enforcement mode to STRICT. This ledger is selectable by other
		// drivers, so a silent failure leaves it in AUDIT mode for them.
		if _, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_SetDefaultEnforcementMode{
				SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
					Ledger:          ledger,
					EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
				},
			},
		})); err != nil {
			internal.LogCleanupError("reset enforcement mode to STRICT", err)
		}
	})
}
