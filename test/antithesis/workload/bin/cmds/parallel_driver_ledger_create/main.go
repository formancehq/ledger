package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/test/antithesis/internal"
)

func main() {
	log.Println("composer: parallel_driver_ledger_create")
	ctx := context.Background()
	client := internal.NewClient()
	id := rand.Intn(1e6)
	ledger := fmt.Sprintf("ledger-%d", id)

	res, err := internal.CreateLedger(
		ctx,
		client,
		ledger,
		ledger,
	)

	assert.Sometimes(err == nil, "ledger should have been created properly", internal.Details{
		"error": err,
	})

	assert.Always(
		!internal.IsServerError(res.GetHTTPMeta()),
		"no internal server error when creating ledger",
		internal.Details{
			"error": err,
		},
	)

	log.Println("composer: parallel_driver_ledger_create: done")
}
