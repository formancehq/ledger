package main

import (
	"fmt"
	"log"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: parallel_driver_ledger_create")

	ctx, cancel := internal.DriverContext()
	defer cancel()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	id := internal.Rand().Uint64() % 1e6
	ledger := fmt.Sprintf("ledger-%d", id)

	// CreateLedger already emits the canonical "should be able to create ledger"
	// Sometimes assertion with the proper IsTransient classification.
	_ = internal.CreateLedger(ctx, client, ledger)

	log.Println("composer: parallel_driver_ledger_create: done")
}
