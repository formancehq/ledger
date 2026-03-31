package main

import (
	"context"
	"fmt"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: parallel_driver_ledger_create")

	ctx := context.Background()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	id := random.GetRandom() % 1e6
	ledger := fmt.Sprintf("ledger-%d", id)

	err = internal.CreateLedger(ctx, client, ledger)
	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to create ledger", internal.Details{"error": err})

	log.Println("composer: parallel_driver_ledger_create: done")
}
