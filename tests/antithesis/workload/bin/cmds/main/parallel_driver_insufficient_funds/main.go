// parallel_driver_insufficient_funds is the EN-1410 detector: it stresses
// the exact-balance transfer path on a long-lived pool of accounts, so an
// account funded by a past iteration can be picked up by a future iteration
// after restarts, snapshots, and cache rotations have had time to fire.
//
// The bug it targets (EN-1410 bloom boot-ordering): a cache rotation that
// runs while the bloom rebuild is still !IsReady wipes Pebble 0xFF for the
// outgoing generation but skips PersistDirtyBlocks. A later crash loses the
// in-memory bloom; the next boot's rebuild reads only persisted blocks +
// cache 0xFF, both of which are now missing the key. MayContain returns
// false, the FSM injects a zero VolumePair, and an exact-balance transfer
// is rejected with INSUFFICIENT_FUNDS even though the account's volume cell
// still sits in Pebble 0x01.
//
// The asymmetry the driver exploits: the read-side index (GetAccount) and
// the FSM cache (apply-time CheckBalance) are independent. The read-side
// sees the funded balance; the FSM bloom-gated cache may not. A spurious
// INSUFFICIENT_FUNDS on an account whose read-side balance is non-zero is
// the bug's signature.
//
// Iteration:
//  1. Create the shared ledger idempotently (every iteration races; only
//     one CreateLedger succeeds, the rest see AlreadyExists).
//  2. Pick a random account from a fixed pool of accountPoolSize cells.
//  3. Read the account's balance via the read-side. If unknown / empty,
//     fund it (Force=true) and exit — the next iteration to pick this
//     cell will see balance > 0 and attempt the transfer.
//  4. If balance > 0, attempt an exact-balance transfer back to world
//     without Force. The two assertions:
//       - Sometimes: campaign coverage — at least once the transfer must
//         succeed (proves the path is reachable in steady state).
//       - AlwaysOrUnreachable: per-iteration safety — INSUFFICIENT_FUNDS
//         is unreachable; firing it is the EN-1410 signature.
//
// The pool is shared across every instance of the driver: accounts funded
// in past iterations survive restarts and snapshots, so a balance > 0
// observed today may have been written hours ago — that's the time window
// during which a bloom-vs-Pebble desync would have formed.
package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

const (
	// sharedLedger is shared across every instance of the driver so accounts
	// outlive any single iteration; without this the bug cannot surface —
	// fund and transfer happen too close together in time for a !IsReady
	// rotation to fall between them.
	sharedLedger = "insuf-shared"

	// accountPoolSize bounds the address space the driver cycles through.
	// Small enough that repeat picks happen across many iterations (so a
	// post-restart pick lands on a previously funded cell), large enough
	// that consecutive iterations rarely collide on the same cell.
	accountPoolSize = 50

	// fundAmount is the fixed credit applied to an empty pool account. The
	// driver always transfers the exact read-side balance back, so the
	// precise amount does not need to be remembered between iterations.
	fundAmount = 1000

	asset = "USD/2"
)

func main() {
	internal.RunDriver("parallel_driver_insufficient_funds", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		// Race-create the shared ledger. AlreadyExists is the steady state
		// once any prior iteration has won the race.
		if err := internal.CreateLedger(ctx, client, sharedLedger); err != nil &&
			!internal.IsAlreadyExists(err) && !internal.IsTransient(err) {
			return
		}

		account := fmt.Sprintf("insuf-users:%d", r.Uint64()%accountPoolSize)
		details := internal.Details{
			"ledger":  sharedLedger,
			"account": account,
			"asset":   asset,
		}

		balance, err := readBalance(ctx, client, sharedLedger, account)
		if err != nil {
			// Read-side temporarily unavailable or account not yet visible
			// to the read-side index; nothing to assert this round.
			return
		}

		if balance.Sign() == 0 {
			fund(ctx, client, account, details)
			return
		}

		transferAll(ctx, client, account, balance, details)
	})
}

// readBalance returns the (input - output) balance for (account, asset) via
// the read-side. NotFound is mapped to zero so brand-new pool cells are
// distinguishable from the "account exists with zero balance" steady state
// only by the next write — both legitimately route to the fund branch.
func readBalance(ctx context.Context, client servicepb.BucketServiceClient, ledger, account string) (*big.Int, error) {
	resp, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledger,
		Address: account,
	})
	if err != nil {
		if internal.IsNotFound(err) {
			return new(big.Int), nil
		}
		return nil, err
	}

	vol, ok := resp.GetVolumes()[asset]
	if !ok {
		return new(big.Int), nil
	}

	bal, ok := new(big.Int).SetString(vol.GetBalance(), 10)
	if !ok {
		return new(big.Int), nil
	}
	return bal, nil
}

func fund(ctx context.Context, client servicepb.BucketServiceClient, account string, details internal.Details) {
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: sharedLedger,
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
	if err != nil && !internal.IsTransient(err) {
		assert.Unreachable("funding should not fail", details.With(internal.Details{"error": err}))
	}
}

func transferAll(ctx context.Context, client servicepb.BucketServiceClient, account string, balance *big.Int, details internal.Details) {
	amount, overflow := uint256.FromBig(balance)
	if overflow {
		// Pool balances never exceed fundAmount per cell so overflow is
		// impossible in normal operation; an overflow here would mean
		// something other than this driver wrote into the pool address
		// space — skip rather than mis-assert.
		return
	}

	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: sharedLedger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{{
							Source:      account,
							Destination: "world",
							Amount:      commonpb.NewUint256(amount),
							Asset:       asset,
						}},
						ExpandVolumes: true,
					},
				}},
			},
		},
	}))

	withErr := details.With(internal.Details{"error": err, "amount": balance.String()})

	assert.Sometimes(err == nil || internal.IsTransient(err),
		"exact balance transfer should succeed", withErr)

	// EN-1410 detection. The naive "INSUFFICIENT_FUNDS implies bug" check is
	// racy because parallel_driver_ spawns many concurrent instances that
	// may pick the same pool cell between their GetAccount and their Apply.
	// One wins; the others observe a legitimate INSUFFICIENT_FUNDS even
	// though the bloom is healthy. Disambiguate by re-reading the balance
	// after the failure: the bug's signature is "FSM apply rejected on
	// available=0 while the read-side still shows balance > 0", because
	// the only path that produces available=0 on a cell with non-zero
	// volumes in Pebble is the FSM-side cache injecting a zero VolumePair
	// — i.e. the bloom dropped a key whose volume Pebble still holds.
	//
	// The re-read itself is racy too (the read-side can lag, a concurrent
	// fund can re-credit the cell, etc.). We accept those: a confirmed
	// bug requires the conjunction (apply rejected available=0 AND
	// read-side balance > 0 after the fact). A miss in either direction
	// is the safe answer.
	confirmed := false
	if internal.HasErrorReason(err, domain.ErrReasonInsufficientFunds) {
		if confirm, rerr := readBalance(ctx, client, sharedLedger, account); rerr == nil && confirm.Sign() > 0 {
			confirmed = true
			withErr = withErr.With(internal.Details{"reReadBalance": confirm.String()})
		}
	}
	assert.AlwaysOrUnreachable(!confirmed,
		"exact balance transfer must not be rejected as INSUFFICIENT_FUNDS — bloom must reflect persisted Pebble state",
		withErr)
}
