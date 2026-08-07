package internal

import (
	"context"
	"log"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// driverTimeout bounds a single parallel driver execution. Retries under fault
// injection stay well below this, so hitting the deadline means the SUT hung
// (e.g. a deadlocked node) — without it the run would stall silently forever.
const driverTimeout = 10 * time.Minute

// singletonDriverTimeout bounds singleton drivers that are not constrained by
// the shortest platform run. It is sized as a fail-safe above the worst-case
// rolling-restart path on a 7-node cluster: 7 × (pod-gone 60s + pod-ready
// 5min + voters 5min) ≈ 77 min. Operational commands that Test Composer must
// observe completing in a 20-minute run use PlatformSingletonContext instead.
const singletonDriverTimeout = 90 * time.Minute

// platformSingletonTimeout leaves enough room in the shortest supported
// Antithesis run for Test Composer to observe command completion and schedule
// another singleton. Destructive scenarios with long per-step timeouts must
// use this bound instead of the 90-minute hang fail-safe.
const platformSingletonTimeout = 12 * time.Minute

// SingletonContext returns a fresh context with the singleton deadline.
// Singletons that run an explicit `main()` (instead of going through
// RunDriver) should use this so a hang surfaces as a timed-out context
// rather than waiting on the composer kill.
func SingletonContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), singletonDriverTimeout)
}

// PlatformSingletonContext bounds a singleton below the 20-minute platform
// run. It is intended for operational scenarios whose internal recovery waits
// could otherwise outlive the entire test.
func PlatformSingletonContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), platformSingletonTimeout)
}

// DriverContext returns a fresh context with the parallel-driver deadline.
// Parallel drivers that own their `main()` (drivers that don't go through
// RunDriver) should use this for the same reason as SingletonContext.
func DriverContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), driverTimeout)
}

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
