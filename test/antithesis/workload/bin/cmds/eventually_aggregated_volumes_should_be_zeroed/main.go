package main

import (
	"context"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/test/antithesis/internal"
	"math/big"
)

func main() {
	ctx := context.Background()
	client := internal.NewClient()

	aggregated, err := client.Ledger.V2.GetBalancesAggregated(ctx, operations.V2GetBalancesAggregatedRequest{
		Ledger: "default",
	})
	if err != nil {
		assert.Always(err == nil, "error getting aggregated balances", internal.Details{
			"error": err,
		})
		return
	}

	for asset, volumes := range aggregated.V2AggregateBalancesResponse.Data {
		assert.Always(volumes.Cmp(new(big.Int)) == 0, "aggregated volumes for asset "+asset+" should be 0", internal.Details{
			"error": err,
		})
	}
}
