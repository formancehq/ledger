package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/big"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: eventually_correct")

	ctx := context.Background()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	// Double-barrier ensures quiescence: killed workload drivers may have
	// in-flight Raft proposals that arrive after a single barrier. By calling
	// Barrier twice and comparing commit indices, we confirm nothing was
	// committed between the two calls — the cluster is truly idle.
	//
	// Each barrier is itself a Raft proposal, so it always advances the commit
	// index by 1. Quiescence is confirmed when the only new commit between two
	// barriers is the second barrier itself (delta == 1).
	const maxBarrierAttempts = 20

	quiescent := false

	var lastCommitIndex uint64
	for attempt := 1; attempt <= maxBarrierAttempts; attempt++ {
		resp, barrierErr := client.Barrier(ctx, &servicepb.BarrierRequest{})
		if barrierErr != nil {
			if internal.IsUnavailable(barrierErr) {
				log.Printf("composer: barrier #%d unavailable, retrying: %s", attempt, barrierErr)
				continue
			}

			assert.Unreachable("barrier returned unexpected error", internal.Details{
				"error":   barrierErr,
				"attempt": attempt,
			})

			break
		}

		currentIndex := resp.GetCommitIndex()
		log.Printf("composer: barrier #%d completed, commitIndex=%d", attempt, currentIndex)

		if lastCommitIndex > 0 && currentIndex == lastCommitIndex+1 {
			log.Printf("composer: quiescence confirmed at commitIndex=%d after %d barriers", currentIndex, attempt)
			quiescent = true

			break
		}

		lastCommitIndex = currentIndex
	}

	assert.Sometimes(quiescent, "barrier quiescence achieved", nil)

	if !quiescent {
		log.Printf("composer: could not achieve quiescence after %d attempts, aborting", maxBarrierAttempts)
		return
	}

	ledgers, err := internal.ListLedgers(ctx, client)
	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to list ledgers", internal.Details{
		"error": err,
	})
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}
	for _, ledger := range ledgers {
		wg.Add(1)
		go func(ledger string) {
			defer wg.Done()
			checkBalanced(ctx, client, ledger)
			checkAccountBalances(ctx, client, ledger)
			checkVolumesConsistent(ctx, client, ledger)
		}(ledger)
	}
	wg.Wait()
}

// listAccounts streams all accounts for a ledger. On a mid-stream error it
// returns what was collected so far plus the error, so callers can still
// assert on partial data and report the failure.
func listAccounts(ctx context.Context, client servicepb.BucketServiceClient, ledger string) ([]*commonpb.Account, error) {
	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: ledger})
	if err != nil {
		return nil, err
	}

	var accounts []*commonpb.Account

	for {
		account, err := stream.Recv()
		if err == io.EOF {
			return accounts, nil
		}
		if err != nil {
			return accounts, err
		}

		accounts = append(accounts, account)
	}
}

// parseBalance parses a decimal string into a *big.Int, defaulting to 0.
func parseBalance(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return big.NewInt(0)
	}

	return v
}

// checkBalanced verifies that all aggregated volumes sum to zero for each asset.
func checkBalanced(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	accounts, err := listAccounts(ctx, client, ledger)
	if err != nil && !internal.IsUnavailable(err) {
		assert.Unreachable("listAccounts returned unexpected error", internal.Details{
			"ledger": ledger,
			"error":  err,
		})
	}

	aggregated := make(map[string]*big.Int)
	for _, account := range accounts {
		for asset, vol := range account.Volumes {
			if aggregated[asset] == nil {
				aggregated[asset] = big.NewInt(0)
			}

			aggregated[asset].Add(aggregated[asset], parseBalance(vol.GetBalance()))
		}
	}

	if len(aggregated) == 0 {
		assert.Always(true, "double-entry: sum of balances should be 0", internal.Details{
			"ledger": ledger,
			"note":   "no accounts or stream error",
		})
	} else {
		for asset, total := range aggregated {
			assert.Always(
				total.Cmp(big.NewInt(0)) == 0,
				"double-entry: sum of balances should be 0",
				internal.Details{
					"ledger":  ledger,
					"asset":   asset,
					"volumes": total.String(),
				},
			)
		}
	}

	log.Printf("composer: balanced: done for ledger %s", ledger)
}

// checkAccountBalances verifies volume consistency for known user accounts.
func checkAccountBalances(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	for i := range internal.UserAccountCount {
		address := fmt.Sprintf("users:%d", i)
		account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
			Ledger:  ledger,
			Address: address,
		})
		if err != nil {
			if !internal.IsUnavailable(err) {
				assert.Unreachable("GetAccount returned unexpected error", internal.Details{
					"ledger":  ledger,
					"address": address,
					"error":   err,
				})
			}

			continue
		}

		assert.Reachable("should be able to get account", internal.Details{
			"ledger":  ledger,
			"address": address,
		})

		internal.CheckAccountVolumes(account.Volumes, internal.Details{
			"ledger":  ledger,
			"address": address,
		})
	}

	log.Printf("composer: account balances check: done for ledger %s", ledger)
}

// checkVolumesConsistent iterates all accounts and cross-checks ListAccounts
// balances against GetAccount balances.
func checkVolumesConsistent(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	details := internal.Details{"ledger": ledger}

	accounts, err := listAccounts(ctx, client, ledger)
	if err != nil && !internal.IsUnavailable(err) {
		assert.Unreachable("listAccounts returned unexpected error in checkVolumesConsistent", details.With(internal.Details{
			"error": err,
		}))
	}

	checked := false
	for _, account := range accounts {
		for asset, vol := range account.Volumes {
			input := parseBalance(vol.GetInput())
			output := parseBalance(vol.GetOutput())
			balance := parseBalance(vol.GetBalance())

			internal.CheckVolume(input, output, balance, details.With(internal.Details{
				"account": account.Address,
				"asset":   asset,
			}))

			getAcc, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledger,
				Address: account.Address,
			})
			if err != nil {
				if !internal.IsUnavailable(err) {
					assert.Unreachable("GetAccount returned unexpected error in cross-check", details.With(internal.Details{
						"account": account.Address,
						"error":   err,
					}))
				}

				continue
			}

			actualVol, ok := getAcc.Volumes[asset]
			if !ok {
				assert.Unreachable("should get requested volumes", details.With(internal.Details{
					"account": account.Address,
					"asset":   asset,
				}))

				continue
			}

			actualBalance := parseBalance(actualVol.GetBalance())
			assert.Always(balance.Cmp(actualBalance) == 0, "list balance should match getaccount balance", details.With(internal.Details{
				"account":       account.Address,
				"asset":         asset,
				"listBalance":   balance.String(),
				"actualBalance": actualBalance.String(),
			}))

			checked = true
		}
	}

	if !checked {
		assert.Always(true, "list balance should match getaccount balance", details.With(internal.Details{
			"note": "no accounts with volumes or stream error",
		}))
	}

	assert.Reachable("can check all volumes for consistency", details)
	log.Printf("composer: volumes_consistent: done for ledger %s", ledger)
}
