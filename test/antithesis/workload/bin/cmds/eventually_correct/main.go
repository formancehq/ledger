package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/test/antithesis/internal"
)

func main() {
	log.Println("composer: eventually_correct")
	ctx := context.Background()
	client := internal.NewClient()

	ledgers, err := client.Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{})

	if err != nil {
		log.Printf("error listing ledgers: %s", err)
		os.Exit(1)
	}

	wg := sync.WaitGroup{}
	for _, ledger := range ledgers.V2LedgerListResponse.Cursor.Data {
		wg.Add(1)
		go func(ledger string) {
			defer wg.Done()
			checkBalanced(ctx, client, ledger)
			checkAccountBalances(ctx, client, ledger)
			checkSequentialTxIDs(ctx, client, ledger)
		}(ledger.Name)
	}
	wg.Wait()
}

func checkBalanced(ctx context.Context, client *client.Formance, ledger string) {
	aggregated, err := client.Ledger.V2.GetBalancesAggregated(ctx, operations.V2GetBalancesAggregatedRequest{
		Ledger: ledger,
	})
	if err != nil {
		if internal.IsServerError(aggregated.GetHTTPMeta()) {
			assert.Always(
				false,
				fmt.Sprintf("error getting aggregated balances for ledger %s: %s", ledger, err),
				internal.Details{
					"error": err,
				},
			)
		} else {
			log.Fatalf("error getting aggregated balances for ledger %s: %s", ledger, err)
		}
	}

	for asset, volumes := range aggregated.V2AggregateBalancesResponse.Data {
		assert.Always(
			volumes.Cmp(new(big.Int)) == 0,
			fmt.Sprintf("aggregated volumes for asset %s should be 0",
				asset,
			), internal.Details{
				"error": err,
			})
	}

	log.Printf("composer: balanced: done for ledger %s", ledger)
}

func checkAccountBalances(ctx context.Context, client *client.Formance, ledger string) {
	for i := range internal.USER_ACCOUNT_COUNT {
		address := fmt.Sprintf("users:%d", i)
		account, err := client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
			Ledger: ledger,
			Address: address,
		})
		assert.Sometimes(err != nil, "Client can aggregate account balances", internal.Details{
			"error": err,
		})
		if err != nil {
			continue
		}
		zero := big.NewInt(0)
		for asset, volume := range account.V2AccountResponse.Data.Volumes {
			balance := new(big.Int).Set(volume.Input)
			balance.Sub(balance, volume.Output)
			assert.Always(balance.Cmp(volume.Balance) == 0, "Reported balance and volumes should be consistent", internal.Details{
				"address": address,
				"asset": asset,
				"volume": volume,
			})
			assert.Always(volume.Balance.Cmp(zero) != -1, "Balance should stay positive when no overdraft is allowed", internal.Details{
				"address": address,
				"asset": asset,
				"volume": volume,
			})
		}
		
	}

	log.Printf("composer: account balances check: done for ledger %s", ledger)
}

func checkSequentialTxIDs(ctx context.Context, client *client.Formance, ledger string) {
	var expectedTxId *big.Int
	for {
		transactions, err := client.Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
			Ledger: ledger,
		})
		assert.Sometimes(err != nil, "Client can list transactions", internal.Details{
			"error": err,
		})
		if err != nil {
			return
		}
		if len(transactions.V2TransactionsCursorResponse.Cursor.Data) == 0 {
			return
		}
		if expectedTxId == nil {
			expectedTxId = transactions.V2TransactionsCursorResponse.Cursor.Data[0].ID
			expectedTxId.Add(expectedTxId, big.NewInt(1))
		}
		for _, tx := range transactions.V2TransactionsCursorResponse.Cursor.Data {
			expectedTxId.Sub(expectedTxId, big.NewInt(1))
			assert.Always(tx.ID.Cmp(expectedTxId) == 0, "txId should be sequential", internal.Details{
				"expected": expectedTxId,
				"actual": tx.ID,
			})
		}
		if !transactions.V2TransactionsCursorResponse.Cursor.HasMore {
			break
		}
	}
	log.Printf("composer: sequential transaction id check: done for ledger %s", ledger)
}

func IsServerError(httpMeta components.HTTPMetadata) bool {
	return httpMeta.Response.StatusCode >= 400 && httpMeta.Response.StatusCode < 600
}
