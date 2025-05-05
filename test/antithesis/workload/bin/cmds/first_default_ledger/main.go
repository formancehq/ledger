package main

import (
	"context"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/test/antithesis/internal"
)

func main() {
	log.Println("composer: first_default_ledger")

	ctx := context.Background()
	client := internal.NewClient()
	ledger := "default"

	err := internal.CreateLedger(
		ctx,
		client,
		ledger,
	)
	if err != nil {
		assert.Always(err == nil, "ledger should have been created properly", internal.Details{
			"error": err,
		})
		return
	}

	log.Println("composer: first_default_ledger: done")
}
