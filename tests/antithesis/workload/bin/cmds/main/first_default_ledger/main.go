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
	if err := internal.SeedPITScopeFixture(ctx, client); err != nil {
		log.Fatalf("error creating point-in-time scope fixture: %s", err)
	}
	if err := internal.SeedPITConvergenceFixture(ctx, client); err != nil {
		log.Fatalf("error creating point-in-time convergence fixture: %s", err)
	}
	if err := internal.SeedPITDualAxisFixture(ctx, client); err != nil {
		log.Fatalf("error creating point-in-time dual-axis fixture: %s", err)
	}

	log.Println("composer: first_default_ledger: done")
}
