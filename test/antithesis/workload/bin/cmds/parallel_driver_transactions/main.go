package main

import (
	"context"
	"log"
	"math/big"

	"github.com/alitto/pond"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/test/antithesis/internal"
)

func main() {
	log.Println("composer: parallel_driver_transactions")

	ctx := context.Background()
	client := internal.NewClient()

	ledger, err := internal.GetRandomLedger(ctx, client)
	if err != nil {
		ledger = "default"
		log.Printf("error getting random ledger: %s", err)
	}

	const count = 100

	totalAmount := big.NewInt(0)

	pool := pond.New(10, 10e3)

	for i := 0; i < count; i++ {
		amount := internal.RandomBigInt()
		totalAmount = totalAmount.Add(totalAmount, amount)
		pool.Submit(func() {
			res, err := RunTx(ctx, client, Transaction(), ledger)
			assert.Sometimes(err == nil, "transaction was committed successfully", internal.Details{
				"ledger": ledger,
			})
			assert.Always(!internal.IsServerError(res.GetHTTPMeta()), "no internal server error when committing transaction", internal.Details{
				"ledger": ledger,
				"error":  err,
			})
		})
	}

	pool.StopAndWait()

	log.Println("composer: parallel_driver_transactions: done")
}

type Postings []components.V2Posting

func RunTx(
	ctx context.Context,
	client *client.Formance,
	postings Postings,
	ledger string,
) (*operations.V2CreateTransactionResponse, error) {
	res, err := client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger: ledger,
		V2PostTransaction: components.V2PostTransaction{
			Postings: postings,
		},
	})

	return res, err
}

func Transaction() []components.V2Posting {
	postings := []components.V2Posting{}

	postings = append(postings, components.V2Posting{
		Amount:      big.NewInt(100),
		Asset:       "USD/2",
		Destination: "orders:1234",
		Source:      "world",
	})

	return postings
}

func Sequence() []Postings {
	postings := []Postings{}

	for i := 0; i < 10; i++ {
		postings = append(postings, Transaction())
	}

	return postings
}
