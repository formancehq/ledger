package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func main() {
	internal.RunGeneratedDriver("parallel_driver_insufficient_funds", func(ctx context.Context, client servicepb.BucketServiceClient, _ string, src *generation.Source) {
		ledger := internal.PrefixInsufficientFunds.New()
		account := fmt.Sprintf("insuf-users:%d", src.Intn(int(internal.UserAccountCount)))
		fundAmount := src.Uint64Range(0, 9999) + 100
		runInsufficientFunds(ctx, client, ledger, account, fundAmount)
	})
}


func runInsufficientFunds(ctx context.Context, client servicepb.BucketServiceClient, ledger, account string, fundAmount uint64) {
	// The driver owns this ledger and account, so no other workload writes can
	// change the balance between the precondition read and the probe.
	if err := internal.CreateLedger(ctx, client, ledger); err != nil {
		return
	}
	asset := "USD/2"
	details := internal.Details{"ledger": ledger, "account": account, "asset": asset, "fundAmount": fundAmount}
	key := func(step string) string { return fmt.Sprintf("insufficient-funds-%s-%s", ledger, step) }

	// 1. Fund the account from world with a known amount.
	resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(key("fund"), &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{{
							Source:      "world",
							Destination: account,
							Amount:      commonpb.NewUint256FromUint64(fundAmount),
							Asset:       asset,
						}},
						Force: true,
					},
				}},
			},
		},
	}))
	if err != nil {
		if internal.IsTransient(err) {
			return
		}

		assert.Unreachable("funding should not fail", details.With(internal.Details{"error": err}))

		return
	}

	internal.CheckCreatedTransaction(resp, details)

	// 2. Attempt exact balance transfer (should succeed without Force).
	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest(key("drain"), &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{{
							Source:      account,
							Destination: "world",
							Amount:      commonpb.NewUint256FromUint64(fundAmount),
							Asset:       asset,
						}},
					},
				}},
			},
		},
	}))

	if err != nil {
		if !internal.IsTransient(err) {
			assert.Unreachable("exact balance transfer should succeed", details.With(internal.Details{"error": err}))
		}

		return
	}

	// A successful default read is linearizable; name the consistency level
	// explicitly so a changed client default cannot invalidate the oracle.
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	balance, err := client.GetAccount(readCtx, &servicepb.GetAccountRequest{Ledger: ledger, Address: account})
	if err != nil {
		if !internal.IsTransient(err) {
			assert.Unreachable("empty-account precondition read failed", details.With(internal.Details{"error": err}))
		}

		return
	}
	volume := balance.FindVolume(asset, "")
	if volume == nil || volume.GetBalance() == nil {
		assert.Unreachable("drained account has no readable balance", details)

		return
	}
	amount, err := volume.GetBalance().ToBigInt()
	if err != nil || amount.Cmp(big.NewInt(0)) != 0 {
		assert.Unreachable("drain did not leave an empty account", details.With(internal.Details{"balance": amount, "error": err}))

		return
	}

	// 3. The observed balance is 0. Sending 1 without Force must fail.
	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest(key("probe"), &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{{
							Source:      account,
							Destination: "world",
							Amount:      commonpb.NewUint256FromUint64(1),
							Asset:       asset,
						}},
					},
				}},
			},
		},
	}))

	if err == nil {
		assert.Unreachable("overdraft without Force should fail", details)

		return
	}

	if internal.IsTransient(err) {
		return
	}

	isInsufficient := internal.HasErrorReason(err, domain.ErrReasonInsufficientFunds)
	assert.AlwaysOrUnreachable(isInsufficient,
		"overdraft should return INSUFFICIENT_FUNDS",
		details.With(internal.Details{"error": err}))
	if !isInsufficient {
		return
	}

	assert.Reachable("insufficient funds path exercised", details)

	// The same one-unit posting is allowed when Force is explicit. Its key
	// differs from the rejected probe because the payload differs.
	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest(key("force"), &servicepb.Request{
		Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
			Ledger: ledger,
			Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{
					Postings: []*commonpb.Posting{{Source: account, Destination: "world", Amount: commonpb.NewUint256FromUint64(1), Asset: asset}},
					Force:    true,
				},
			}},
		}},
	}))
	if err != nil && !internal.IsTransient(err) {
		assert.Unreachable("overdraft with Force should succeed", details.With(internal.Details{"error": err}))
	}
}
