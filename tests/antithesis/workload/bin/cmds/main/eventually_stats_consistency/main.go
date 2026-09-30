// eventually_stats_consistency checks asynchronous usage at a fixed source
// horizon. The composer stops other drivers and faults before eventually_ runs.
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	lifecycleCtx, stopLifecycle := internal.SingletonContext()
	defer stopLifecycle()
	ctx, cancel := context.WithTimeout(lifecycleCtx, 5*time.Minute)
	defer cancel()
	// Startup ID discovery is best-effort; an absent first slot must not spend
	// the whole oracle budget before per-attempt source selection can run.
	dialCtx, stopDiscovery := context.WithTimeout(ctx, 5*time.Second)
	conns, err := internal.DialPerNode(dialCtx)
	stopDiscovery()
	connected := err == nil && len(conns) > 0
	assert.Always(connected, "stats convergence oracle connects to replicas", internal.Details{"replicas": len(conns), "error": err})
	if !connected {
		log.Printf("stats convergence: cannot connect: %v", err)
		return
	}
	defer conns.Close()
	// A maintenance driver may have been terminated after enabling the
	// cluster-wide write gate. The parallel pool has stopped by this point.
	// Clear that gate before the oracle creates a write witness.
	// Barrier is itself write-gated, so source selection cannot precede this
	// reset. Try each configured node directly; a follower forwards Apply.
	err = errors.New("no replica candidates")
	for _, conn := range conns {
		resetCtx, stopReset := context.WithTimeout(ctx, 10*time.Second)
		_, err = conn.Bucket.Apply(resetCtx, servicepb.UnsignedApplyRequest("", actions.SetMaintenanceModeAction(false)))
		stopReset()
		if err == nil || ctx.Err() != nil {
			break
		}
	}
	assert.Always(err == nil, "stats oracle clears inherited maintenance mode", internal.Details{"error": err})
	if err != nil {
		log.Printf("stats convergence: cannot clear maintenance mode: %v", err)
		return
	}
	report := runOracle(ctx, conns, oracleConfig{
		Attempts: 3, ConvergenceWindow: 60 * time.Second, Wait: pollDelay,
	})
	for index, attempt := range report {
		log.Printf("stats convergence: attempt=%d qualified=%t horizon=%d error=%s replicas=%+v", index+1, attempt.Qualified, attempt.Horizon, attempt.Error, attempt.Observations)
	}
	last := report[len(report)-1]
	// This separate liveness verdict reports an unqualified oracle, never bad
	// counters. Retain all prior replica diagnostics even if the final fence fails.
	assert.Always(last.Qualified, "stats oracle obtains a qualified observation within its bounded retry budget", internal.Details{"attempts": report})
	if !last.Qualified {
		return
	}
	for _, got := range last.Observations {
		details := internal.Details{"node": got.Node, "sourceIndex": last.Horizon, "witness": last.Witness, "expected": last.Expected, "observation": got}
		assert.Always(got.Converged, "usage counters converge exactly at a fixed source horizon", details)
		if got.Converged {
			assert.Reachable("stats usage convergence verified", details)
		}
	}
}

func pollDelay(ctx context.Context) error {
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
