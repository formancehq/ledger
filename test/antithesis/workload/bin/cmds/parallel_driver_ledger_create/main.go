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

	err := internal.CreateLedger(
		ctx,
		client,
		ledger,
		ledger,
	)
	if err != nil {
		assert.Always(err == nil, "ledger should have been created properly", internal.Details{
			"error": err,
		})
		return
	}

	log.Println("composer: parallel_driver_ledger_create: done")
}
