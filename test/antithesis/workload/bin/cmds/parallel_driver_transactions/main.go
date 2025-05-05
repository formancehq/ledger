package main

import (
	"context"
	"log"
	"math/big"

	"github.com/alitto/pond"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/test/antithesis/internal"
)

func main() {
	log.Println("composer: parallel_driver_transactions")

	ctx := context.Background()
	client := internal.NewClient()

	ledger, err := internal.GetRandomLedger(ctx, client)
	if err != nil {
		log.Fatalf("error getting random ledger: %s", err)
	}

	const count = 100

	totalAmount := big.NewInt(0)

	pool := pond.New(10, 10e3)

	for i := 0; i < count; i++ {
		amount := internal.RandomBigInt()
		totalAmount = totalAmount.Add(totalAmount, amount)
		pool.Submit(func() {
			err := internal.RunTx(ctx, client, amount, ledger)
			assert.Sometimes(err == nil, "transaction was committed successfully", internal.Details{
				"ledger": ledger,
			})
		})
	}

	pool.StopAndWait()

	log.Println("composer: parallel_driver_transactions: done")
}
