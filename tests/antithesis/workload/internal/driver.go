package internal

import (
	"context"
	"log"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// RunDriver is the common boilerplate for parallel drivers:
// connect, pick a random ledger, run fn once.
func RunDriver(name string, fn func(ctx context.Context, client servicepb.BucketServiceClient, ledger string)) {
	log.Printf("composer: %s", name)

	ctx := context.Background()
	client, conn, err := NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	ledger, err := GetRandomLedger(ctx, client)
	if err != nil {
		return
	}

	fn(ctx, client, ledger)

	log.Printf("composer: %s: done", name)
}
