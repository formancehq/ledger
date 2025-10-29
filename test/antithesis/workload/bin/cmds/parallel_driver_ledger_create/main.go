package main

import (
	"context"
	"fmt"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger/test/antithesis/internal"
)

func main() {
	log.Println("composer: parallel_driver_ledger_create")
	ctx := context.Background()
	client := internal.NewClient()
	id := random.GetRandom()%1e6
	ledger := fmt.Sprintf("ledger-%d", id)

	_, err := internal.CreateLedger(
		ctx,
		client,
		ledger,
		ledger,
	)

	assert.Sometimes(err == nil, "ledger should have been created properly", internal.Details{
		"error": err,
	})

	log.Println("composer: parallel_driver_ledger_create: done")
}
