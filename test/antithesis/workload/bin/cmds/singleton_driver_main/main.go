package main

import (
	"context"
	"fmt"
	"github.com/alitto/pond"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/test/antithesis/internal"
	"go.uber.org/atomic"
	"math"
	"math/big"
)

func main() {
	ctx := context.Background()
	client := internal.NewClient()

	err := createLedger(ctx, client)
	if err != nil {
		assert.Always(err == nil, "ledger should have been created", internal.Details{
			"error": err,
		})
		return
	}

	const count = 1000

	hasError := atomic.NewBool(false)
	totalAmount := big.NewInt(0)

	pool := pond.New(10, 10000)

	for i := 0; i < count; i++ {
		amount := internal.RandomBigInt()
		totalAmount = totalAmount.Add(totalAmount, amount)
		pool.Submit(func() {
			if !internal.AssertAlwaysErrNil(
				runTx(ctx, client, amount),
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

func createLedger(ctx context.Context, client *client.Formance) error {

	_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		Ledger: "default",
	})

	if assert.Always(err == nil, "ledger should have been created", internal.Details{
		"error": fmt.Sprintf("%+v\n", err),
	}); err != nil {
		return err
	}

	return nil
}

func runTx(ctx context.Context, client *client.Formance, amount *big.Int) error {
	orderID := fmt.Sprint(int64(math.Abs(float64(random.GetRandom()))))
	dest := fmt.Sprintf("orders:%s", orderID)

	_, err := client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		V2PostTransaction: components.V2PostTransaction{
			Postings: []components.V2Posting{{
				Amount:      amount,
				Asset:       "USD/2",
				Destination: dest,
				Source:      "world",
			}},
		},
		Ledger: "default",
	})
	return err
}
