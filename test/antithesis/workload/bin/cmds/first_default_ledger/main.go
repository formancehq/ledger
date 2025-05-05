package main

import (
	"context"
	"log"

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
		ledger,
	)
	if err != nil {
		log.Fatalf("error creating ledger %s: %s", ledger, err)
	}

	log.Println("composer: first_default_ledger: done")
}
