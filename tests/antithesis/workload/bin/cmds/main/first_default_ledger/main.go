package main

import (
	"context"
	"log"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: first_default_ledger")

	if err := run(); err != nil {
		log.Fatalf("composer: first_default_ledger: %s", err)
	}

	log.Println("composer: first_default_ledger: done")
}

func run() error {
	client, conn, err := internal.NewClient()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	return internal.CreateLedger(context.Background(), client, "default")
}
