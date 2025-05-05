package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/test/antithesis/internal"
)

func main() {
	ctx := context.Background()
	client := internal.NewClient()

	ledgers, err := client.Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{})
	if err != nil {
		assert.Always(err == nil, "error listing ledgers", internal.Details{
			"error": err,
		})
		return
	}

	for _, ledger := range ledgers.V2LedgerListResponse.Cursor.Data {
		go checkVolumes(ctx, client, ledger.Name)
	}
}

func checkVolumes(ctx context.Context, client *client.Formance, ledger string) {
	aggregated, err := client.Ledger.V2.GetBalancesAggregated(ctx, operations.V2GetBalancesAggregatedRequest{
		Ledger: ledger,
	})
	if err != nil {
		assert.Always(
			err == nil,
			fmt.Sprintf("error getting aggregated balances for ledger %s", ledger),
			internal.Details{
				"error": err,
			})
		return
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
}
