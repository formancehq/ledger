package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/alitto/pond"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/test/antithesis/internal"
	"go.uber.org/atomic"
)

func main() {
	ctx := context.Background()
	client := internal.NewClient()

	err := internal.CreateLedger(
		ctx,
		client,
		fmt.Sprintf("ledger-%d", internal.RandomBigInt().Int64()),
	)
	if err != nil {
		assert.Always(err == nil, "ledger should have been created", internal.Details{
			"error": err,
		})
		return
	}

	const count = 1000

	hasError := atomic.NewBool(false)
	totalAmount := big.NewInt(0)

	pool := pond.New(10, 10e3)

	for i := 0; i < count; i++ {
		amount := internal.RandomBigInt()
		totalAmount = totalAmount.Add(totalAmount, amount)
		pool.Submit(func() {
			if !internal.AssertAlwaysErrNil(
				internal.RunTx(ctx, client, amount),
				"creating transaction from @world to $account always return a nil error",
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

	account, err := client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
		Address: "world",
		Expand:  pointer.For("volumes"),
		Ledger:  "default",
	})

	if !internal.AssertAlwaysErrNil(err, "we should be able to query account 'world'") {
		return
	}

	output := account.V2AccountResponse.Data.Volumes["USD/2"].Output

	if !internal.AssertAlways(output != nil, "Expect output of world for USD/2 to be not empty", internal.Details{}) {
		return
	}

	assert.Always(
		output.Cmp(totalAmount) == 0,
		"output of 'world' should match",
		internal.Details{
			"output": output,
		},
	)
}
