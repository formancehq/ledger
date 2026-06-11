package internal

import (
	"context"
	"log"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// driverTimeout bounds a single driver execution. Retries under fault
// injection stay well below this, so hitting the deadline means the SUT hung
// (e.g. a deadlocked node) — without it the run would stall silently forever.
const driverTimeout = 10 * time.Minute

// RunDriver is the common boilerplate for parallel drivers:
// connect, pick a random ledger, run fn once.
func RunDriver(name string, fn func(ctx context.Context, client servicepb.BucketServiceClient, ledger string)) {
	log.Printf("composer: %s", name)

	ctx, cancel := context.WithTimeout(context.Background(), driverTimeout)
	defer cancel()

	// Deferred so the timeout is reported on every return path, including an
	// early return when ledger selection itself blocked until the deadline.
	defer func() {
		if ctx.Err() != nil {
			log.Printf("composer: %s: timed out after %s — possible SUT hang", name, driverTimeout)
		}
	}()

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

	if ctx.Err() == nil {
		log.Printf("composer: %s: done", name)
	}
}
