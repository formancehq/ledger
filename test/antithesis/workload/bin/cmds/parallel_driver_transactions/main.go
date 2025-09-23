package main

import (
	"context"
	"errors"
	"log"

	"github.com/alitto/pond"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/client/models/sdkerrors"
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
		return
	}

	const count = 100

	pool := pond.New(10, 10e3)

	for range count {
		pool.Submit(func() {
			_, err := RunTx(ctx, client, RandomPostings(), ledger)
			assert.Sometimes(err == nil, "transaction was committed successfully", internal.Details{
				"ledger": ledger,
			})
			if err != nil {
				var e *sdkerrors.V2ErrorResponse
				assert.Always(errors.As(err, &e) && e.ErrorCode == components.V2ErrorsEnumInternal, "no internal server error when committing transaction", internal.Details{
					"ledger": ledger,
					"error":  err,
				})
			}
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

func RandomPostings() []components.V2Posting {
	postings := []components.V2Posting{}

	for range random.GetRandom()%20+1 {
		source := internal.GetRandomAddress()
		destination := internal.GetRandomAddress()
		amount := internal.RandomBigInt()
		asset := random.RandomChoice([]string{"USD/2", "EUR/2", "COIN"})

		postings = append(postings, components.V2Posting{
			Amount:      amount,
			Asset:       asset,
			Destination: destination,
			Source:      source,
		})
	}

	return postings
}

func Sequence() []Postings {
	postings := []Postings{}

	for i := 0; i < 10; i++ {
		postings = append(postings, RandomPostings())
	}

	return postings
}
