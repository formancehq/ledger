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

// singletonDriverTimeout bounds a singleton driver execution. Sized as a
// fail-safe well above the worst-case sum of per-step deadlines:
//   - scaling_structured: 4 cycles × (cooldown 60s + WaitForVoters 10min +
//     stable window 20s) ≈ 45 min
//   - rolling_restart on a 7-node cluster: 7 × (pod-gone 60s + pod-ready
//     5min + voters 5min) ≈ 77 min
//   - quorum_recovery: scale-down 8min + recovery + scale-up 15min ≈ 30 min
//
// Hitting this deadline means the SUT actually hung; Antithesis's
// composer-level kill is a blunter backstop that does not propagate a
// clean cancellation to the RPCs.
const singletonDriverTimeout = 90 * time.Minute

// SingletonContext returns a fresh context with the singleton deadline.
// Singletons that run an explicit `main()` (instead of going through
// RunDriver) should use this so a hang surfaces as a timed-out context
// rather than waiting on the composer kill.
func SingletonContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), singletonDriverTimeout)
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
