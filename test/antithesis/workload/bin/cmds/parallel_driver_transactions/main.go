package main

import (
	"context"
	"log"
	"math/big"

	"github.com/alitto/pond"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/test/antithesis/internal"
	"go.uber.org/atomic"
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

	hasError := atomic.NewBool(false)
	totalAmount := big.NewInt(0)

	pool := pond.New(10, 10e3)

	for i := 0; i < count; i++ {
		amount := internal.RandomBigInt()
		totalAmount = totalAmount.Add(totalAmount, amount)
		pool.Submit(func() {
			if !internal.AssertAlwaysErrNil(
				internal.RunTx(ctx, client, amount, ledger),
				"creating transaction from @world to $account always return a nil error",
				internal.Details{
					"ledger": ledger,
				},
			) {
				hasError.CompareAndSwap(false, true)
			}
		})
	}

	pool.StopAndWait()

	cond := !hasError.Load()
	if assert.Always(cond, "all transactions should have been written", internal.Details{}); !cond {
		return
	}

	log.Println("composer: parallel_driver_transactions: done")
}
