package cmd

import (
	"context"
	"fmt"
	"github.com/alitto/pond"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/stack/libs/go-libs/pointer"
	"go.uber.org/atomic"
	"math"
	"math/big"
)

func runWorkload(ctx context.Context, client *client.Formance) {
	err := createLedger(ctx, client)
	if err != nil {
		assert.Always(err == nil, "ledger should have been created", Details{
			"error": err,
		})
		return
	}

	lifecycle.SetupComplete(Details{
		"Ledger": "Available",
	})

	const count = 1000

	hasError := atomic.NewBool(false)
	totalAmount := big.NewInt(0)
	idSeq := NewIDSeq()

	pool := pond.New(20, 10000)

	fmt.Printf("Insert %d transactions...\r\n", count)
	for i := 0; i < count; i++ {
		amount := randomBigInt()
		totalAmount = totalAmount.Add(totalAmount, amount)
		pool.Submit(func() {
			id, err := runTx(ctx, client, amount)

			if err != nil {
				hasError.CompareAndSwap(false, true)
				return
			}

			idSeq.Register(id)
		})
	}

	pool.StopAndWait()

	//err = idSeq.Check()
	//assert.Always(err == nil, "IDSeq check should pass", Details{
	//	"count": idSeq.Count,
	//	"sum":   idSeq.Sum,
	//})
	//
	//if err != nil {
	//	fmt.Println("Error checking ID sequence", err)
	//	hasError.CompareAndSwap(false, true)
	//	os.Exit(1)
	//	return
	//}

	cond := !hasError.Load()
	if assert.Always(cond, "all transactions should have been written", Details{
		"error": fmt.Sprintf("%+v\n", err),
	}); !cond {
		return
	}

	fmt.Println("Checking balance of 'world'...")
	account, err := client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
		Address: "world",
		Expand:  pointer.For("volumes"),
		Ledger:  "default",
	})

	cond = err == nil
	if assert.Always(cond, "we should be able to query account 'world'", Details{
		"error": fmt.Sprintf("%+v\n", err),
	}); !cond {
		return
	}

	output := account.V2AccountResponse.Data.Volumes["USD/2"].Output

	cond = output != nil
	if assert.Always(cond, "Expect output of world for USD/2 to be not empty", Details{}); !cond {
		return
	}
	fmt.Printf("Expect output of world to be %s and got %d\r\n", totalAmount, output)
	assert.Always(
		output.Cmp(totalAmount) == 0,
		"output of 'world' should match",
		Details{
			"output": output,
		},
	)
}

func runTx(ctx context.Context, client *client.Formance, amount *big.Int) (*big.Int, error) {
	orderID := fmt.Sprint(int64(math.Abs(float64(random.GetRandom()))))
	dest := fmt.Sprintf("orders:%s", orderID)

	res, err := client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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

	assert.Always(
		err == nil,
		"creating transaction from @world to $account always return a nil error",
		Details{
			"error": fmt.Sprintf("%+v\n", err),
		},
	)

	return res.V2CreateTransactionResponse.Data.ID, err
}
