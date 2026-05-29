package main

import (
	"context"
	"log"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: first_default_ledger")

	ctx := context.Background()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Fatalf("error creating client: %s", err)
	}
	defer conn.Close()

	err = internal.CreateLedger(ctx, client, "default")
	if err != nil {
		log.Fatalf("error creating ledger default: %s", err)
	}

	log.Println("composer: first_default_ledger: done")
}
