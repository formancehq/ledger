package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/test/antithesis/internal"
)

func main() {
	ctx := context.Background()
	client := internal.NewClient()
	id := big.NewInt(0).Abs(internal.RandomBigInt()).Int64()
	ledger := fmt.Sprintf("ledger-%d", id)

	err := internal.CreateLedger(
		ctx,
		client,
		ledger,
	)
	if err != nil {
		assert.Always(err == nil, "ledger should have been created properly", internal.Details{
			"error": err,
		})
		return
	}
}
