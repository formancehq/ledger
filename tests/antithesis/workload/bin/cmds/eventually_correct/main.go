package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/big"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"

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
	var lastCommitIndex uint64
	for attempt := 1; ; attempt++ {
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

			return
		}

		currentIndex := resp.GetCommitIndex()
		log.Printf("composer: barrier #%d completed, commitIndex=%d", attempt, currentIndex)

		if currentIndex == lastCommitIndex {
			log.Printf("composer: quiescence confirmed at commitIndex=%d after %d barriers", currentIndex, attempt)
			break
		}

		lastCommitIndex = currentIndex
	}

	assert.Reachable("barrier quiescence achieved", nil)

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

// checkBalanced verifies that all aggregated volumes sum to zero for each asset.
func checkBalanced(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	// Aggregate all account balances per asset
	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: ledger})
	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to list accounts", internal.Details{
		"ledger": ledger,
		"error":  err,
	})
	if err != nil {
		return
	}

	aggregated := make(map[string]*big.Int) // asset -> sum of balances
	for {
		account, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}
		for asset, vol := range account.Volumes {
			balance, _ := new(big.Int).SetString(vol.GetBalance(), 10)
			if balance == nil {
				balance = big.NewInt(0)
			}
			if aggregated[asset] == nil {
				aggregated[asset] = big.NewInt(0)
			}
			aggregated[asset].Add(aggregated[asset], balance)
		}
	}

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
		assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to get account", internal.Details{
			"ledger":  ledger,
			"address": address,
			"error":   err,
		})
		if err != nil {
			continue
		}
		internal.CheckAccountVolumes(account.Volumes, internal.Details{
			"ledger":  ledger,
			"address": address,
		})
	}

	log.Printf("composer: account balances check: done for ledger %s", ledger)
}

// checkVolumesConsistent iterates all accounts and verifies volume consistency.
func checkVolumesConsistent(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	details := internal.Details{"ledger": ledger}

	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: ledger})
	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to list accounts", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	for {
		account, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}

		for asset, vol := range account.Volumes {
			input, _ := new(big.Int).SetString(vol.GetInput(), 10)
			output, _ := new(big.Int).SetString(vol.GetOutput(), 10)
			balance, _ := new(big.Int).SetString(vol.GetBalance(), 10)
			if input == nil {
				input = big.NewInt(0)
			}
			if output == nil {
				output = big.NewInt(0)
			}
			if balance == nil {
				balance = big.NewInt(0)
			}
			internal.CheckVolume(input, output, balance, details.With(internal.Details{
				"account": account.Address,
				"asset":   asset,
			}))

			// Cross-check with GetAccount
			getAcc, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledger,
				Address: account.Address,
			})
			assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to get account", details.With(internal.Details{
				"account": account.Address,
				"error":   err,
			}))
			if err != nil {
				continue
			}

			if actualVol, ok := getAcc.Volumes[asset]; ok {
				actualBalance, _ := new(big.Int).SetString(actualVol.GetBalance(), 10)
				if actualBalance == nil {
					actualBalance = big.NewInt(0)
				}
				assert.Always(balance.Cmp(actualBalance) == 0, "list balance should match getaccount balance", details.With(internal.Details{
					"account":       account.Address,
					"asset":         asset,
					"listBalance":   balance.String(),
					"actualBalance": actualBalance.String(),
				}))
			} else {
				assert.Unreachable("should get requested volumes", details.With(internal.Details{
					"account": account.Address,
					"asset":   asset,
				}))
			}
		}
	}

	assert.Reachable("can check all volumes for consistency", details)
	log.Printf("composer: volumes_consistent: done for ledger %s", ledger)
}
