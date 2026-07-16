package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/big"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// waitForQuiescence calls Barrier repeatedly until two consecutive barriers
// return commit indices that differ by exactly 1 (the barrier itself).
// Returns the confirmed commit index, or 0 if quiescence could not be achieved.
func waitForQuiescence(ctx context.Context, client servicepb.BucketServiceClient) uint64 {
	const maxAttempts = 20

	var lastCommitIndex uint64
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
		if err != nil {
			if internal.IsTransient(err) {
				log.Printf("composer: barrier #%d transient, retrying: %s", attempt, err)
				continue
			}

			assert.Unreachable("barrier returned unexpected error", internal.Details{
				"error":   err,
				"attempt": attempt,
			})

			return 0
		}

		currentIndex := resp.GetCommitIndex()
		log.Printf("composer: barrier #%d completed, commitIndex=%d", attempt, currentIndex)

		if lastCommitIndex > 0 && currentIndex == lastCommitIndex+1 {
			log.Printf("composer: quiescence confirmed at commitIndex=%d after %d barriers", currentIndex, attempt)
			return currentIndex
		}

		lastCommitIndex = currentIndex
	}

	return 0
}

func main() {
	log.Println("composer: eventually_correct")

	ctx, cancel := internal.SingletonContext()
	defer cancel()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	commitIndex := waitForQuiescence(ctx, client)
	assert.Sometimes(commitIndex > 0, "barrier quiescence achieved", nil)

	if commitIndex == 0 {
		log.Printf("composer: could not achieve quiescence, aborting")
		return
	}

	ledgers, err := internal.ListLedgers(ctx, client)
	assert.Sometimes(internal.IsTolerated(err), "should be able to list ledgers", internal.Details{
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
			checkVolumesConsistent(ctx, client, ledger, commitIndex)
		}(ledger)
	}
	wg.Wait()
}

// listAccounts streams all accounts for a ledger. On a mid-stream error it
// returns what was collected so far plus the error.
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
	if err != nil && !internal.IsTransient(err) {
		assert.Unreachable("listAccounts returned unexpected error", internal.Details{
			"ledger": ledger,
			"error":  err,
		})
	}

	// Double-entry holds per (asset, color) bucket.
	type aggKey struct{ asset, color string }
	aggregated := make(map[aggKey]*big.Int)
	for _, account := range accounts {
		for _, entry := range account.GetVolumes() {
			k := aggKey{asset: entry.GetAsset(), color: entry.GetColor()}
			if aggregated[k] == nil {
				aggregated[k] = big.NewInt(0)
			}

			aggregated[k].Add(aggregated[k], parseBalance(entry.GetVolumes().GetBalance()))
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
			if !internal.IsTransient(err) {
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
// balances against GetAccount balances. If a mismatch is detected, it re-checks
// quiescence: if the commit index has advanced (late proposals from killed
// drivers), the mismatch is expected and ignored.
func checkVolumesConsistent(ctx context.Context, client servicepb.BucketServiceClient, ledger string, quiescentCommitIndex uint64) {
	details := internal.Details{"ledger": ledger}

	accounts, err := listAccounts(ctx, client, ledger)
	if err != nil && !internal.IsTransient(err) {
		assert.Unreachable("listAccounts returned unexpected error in checkVolumesConsistent", details.With(internal.Details{
			"error": err,
		}))
	}

	checked := false
	for _, account := range accounts {
		for _, entry := range account.GetVolumes() {
			asset := entry.GetAsset()
			color := entry.GetColor()
			vol := entry.GetVolumes()
			input := parseBalance(vol.GetInput())
			output := parseBalance(vol.GetOutput())
			balance := parseBalance(vol.GetBalance())

			internal.CheckVolume(input, output, balance, details.With(internal.Details{
				"account": account.Address,
				"asset":   asset,
				"color":   color,
			}))

			getAcc, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledger,
				Address: account.Address,
			})
			if err != nil {
				if !internal.IsTransient(err) {
					assert.Unreachable("GetAccount returned unexpected error in cross-check", details.With(internal.Details{
						"account": account.Address,
						"error":   err,
					}))
				}

				continue
			}

			// Cross-check the same (asset, color) bucket as the list result.
			actualVol := getAcc.FindVolume(asset, color)
			if actualVol == nil {
				assert.Unreachable("should get requested volumes", details.With(internal.Details{
					"account": account.Address,
					"asset":   asset,
					"color":   color,
				}))

				continue
			}

			actualBalance := parseBalance(actualVol.GetBalance())
			if balance.Cmp(actualBalance) != 0 {
				// Mismatch detected — check if the commit index has advanced
				// (late proposals from killed drivers arrived after quiescence).
				newCommitIndex := waitForQuiescence(ctx, client)
				if newCommitIndex > quiescentCommitIndex+1 {
					log.Printf("composer: balance mismatch on %s/%s (list=%s, get=%s) but commit index advanced %d→%d, late proposals detected — retrying",
						account.Address, asset, balance.String(), actualBalance.String(), quiescentCommitIndex, newCommitIndex)
					// Restart the entire check with the new quiescent state.
					checkVolumesConsistent(ctx, client, ledger, newCommitIndex)

					return
				}

				// Commit index didn't advance — this is a real consistency bug.
				assert.Always(false, "list/get balance divergence persisted past quiescence", details.With(internal.Details{
					"account":       account.Address,
					"asset":         asset,
					"listBalance":   balance.String(),
					"actualBalance": actualBalance.String(),
				}))

				checked = true

				continue
			}

			assert.Reachable("list/get balance pair verified matching", details.With(internal.Details{
				"account":       account.Address,
				"asset":         asset,
				"listBalance":   balance.String(),
				"actualBalance": actualBalance.String(),
			}))

			checked = true
		}

		// Cross-check metadata: ListAccounts metadata should match GetAccount metadata.
		// Only check if we successfully got the account above (getAcc from the last asset iteration).
		if len(account.Volumes) > 0 {
			getAcc, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledger,
				Address: account.Address,
			})
			if err == nil {
				crossCheckMetadata(account, getAcc, details)
			}
		}
	}

	if !checked {
		assert.Reachable("eventually_correct found no list/get balance pairs to compare", details.With(internal.Details{
			"note": "no accounts with volumes or stream error",
		}))
	}

	assert.Reachable("can check all volumes for consistency", details)
	log.Printf("composer: volumes_consistent: done for ledger %s", ledger)
}

// crossCheckMetadata verifies that metadata from ListAccounts matches GetAccount.
func crossCheckMetadata(listAccount, getAccount *commonpb.Account, details internal.Details) {
	listMeta := metadataToMap(listAccount.GetMetadata())
	getMeta := metadataToMap(getAccount.GetMetadata())

	for key, listVal := range listMeta {
		getVal, ok := getMeta[key]
		assert.AlwaysOrUnreachable(ok, "list metadata key should exist in getaccount", details.With(internal.Details{
			"account": listAccount.Address,
			"key":     key,
		}))

		if ok {
			assert.AlwaysOrUnreachable(listVal == getVal, "list metadata value should match getaccount", details.With(internal.Details{
				"account": listAccount.Address,
				"key":     key,
				"listVal": listVal,
				"getVal":  getVal,
			}))
		}
	}

	assert.AlwaysOrUnreachable(len(listMeta) == len(getMeta), "metadata key count should match between list and get", details.With(internal.Details{
		"account":  listAccount.Address,
		"listKeys": len(listMeta),
		"getKeys":  len(getMeta),
	}))
}

func metadataToMap(ms map[string]*commonpb.MetadataValue) map[string]string {
	result := make(map[string]string)
	for k, v := range ms {
		result[k] = v.GetStringValue()
	}

	return result
}
