package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_insufficient_funds", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		// Use a dedicated ledger to control account balances precisely.
		ledger := fmt.Sprintf("insuf-%d", r.Uint64()%1_000_000)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		// Use a dedicated account prefix so other drivers (e.g. audit) that post
		// to "users:N" on random ledgers cannot interfere with balance assumptions.
		account := fmt.Sprintf("insuf-users:%d", r.Uint64()%internal.UserAccountCount)
		asset := "USD/2"
		fundAmount := r.Uint64()%10000 + 100
		details := internal.Details{"ledger": ledger, "account": account, "asset": asset, "fundAmount": fundAmount}

		// 1. Fund the account from world with a known amount.
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
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
							Force:         true,
							ExpandVolumes: true,
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

		if ct := internal.ExtractCreatedTransaction(resp); ct != nil {
			internal.CheckPostCommitVolumes(ct.PostCommitVolumes, details)
		}

		// 2. Attempt exact balance transfer (should succeed without Force).
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
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
							ExpandVolumes: true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"exact balance transfer should succeed",
			details.With(internal.Details{"error": err}))

		// EN-1410: a bloom miss on a key still present in Pebble — caused by a
		// rotation that wiped 0xFF while !IsReady, without persisting bloom
		// blocks — surfaces here as a spurious INSUFFICIENT_FUNDS on a transfer
		// whose source was funded earlier in this driver. The exact-balance
		// transfer must NEVER be rejected with INSUFFICIENT_FUNDS: that would
		// mean the bloom diverged from Pebble.
		isSpuriousInsuf := internal.HasErrorReason(err, domain.ErrReasonInsufficientFunds)
		assert.AlwaysOrUnreachable(!isSpuriousInsuf,
			"exact balance transfer must not be rejected as INSUFFICIENT_FUNDS — bloom must reflect persisted Pebble state",
			details.With(internal.Details{"error": err}))

		if err != nil {
			return
		}

		// 3. Now the balance is 0. Attempt to send 1 unit — must get INSUFFICIENT_FUNDS.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
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

		assert.Reachable("insufficient funds path exercised", details)
	})
}
