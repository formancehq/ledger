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
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/test/antithesis/internal"
)

func main() {
	log.Println("composer: parallel_driver_volumes")
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
			checkVolumes(ctx, client, ledger)
		}(ledger.Name)
	}
	wg.Wait()
}

func checkVolumes(ctx context.Context, client *client.Formance, ledger string) {
	aggregated, err := client.Ledger.V2.GetBalancesAggregated(ctx, operations.V2GetBalancesAggregatedRequest{
		Ledger: ledger,
	})
	if err != nil {
		log.Fatalf("error getting aggregated balances for ledger %s: %s", ledger, err)
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

	log.Printf("composer: parallel_driver_volumes: done for ledger %s", ledger)
}
