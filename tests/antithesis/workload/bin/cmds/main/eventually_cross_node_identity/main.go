// Command eventually_cross_node_identity compares every current replica's
// durable business state after faults stop. Local sentinel checks are necessary
// but cannot detect two internally consistent replicas that disagree.
package main

import (
	"context"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	lifecycle, stop := internal.SingletonContext()
	defer stop()
	ctx, cancel := context.WithTimeout(lifecycle, 5*time.Minute)
	defer cancel()
	dialCtx, stopDiscovery := context.WithTimeout(ctx, 5*time.Second)
	conns, err := internal.DialPerNode(dialCtx)
	stopDiscovery()
	assert.Always(err == nil && len(conns) > 0, "cross-node identity oracle connects to replicas", internal.Details{"error": err, "replicas": len(conns)})
	if err != nil || len(conns) == 0 {
		return
	}
	defer conns.Close()
	reports := runOracle(ctx, conns, oracleConfig{Attempts: 3, ConvergenceWindow: time.Minute, CallTimeout: 5 * time.Second, Wait: pollDelay})
	for _, report := range reports {
		log.Printf("cross-node identity: horizon=%d qualified=%t error=%s replicas=%+v", report.Horizon, report.Qualified, report.Error, report.Observations)
	}
	publishReports(reports)
}

// A failed qualification is a recovery/observation failure, never a corruption
// verdict. No identity assertion is emitted until both final fences pass.
func publishReports(reports []attemptReport) {
	qualified := len(reports) > 0 && reports[len(reports)-1].Qualified
	assert.Always(qualified, "cross-node identity oracle obtains a qualified all-member observation", internal.Details{"attempts": reports})
	if !qualified {
		return
	}
	last := reports[len(reports)-1]
	assert.Sometimes(true, "cross-node oracle reaches quiescence", internal.Details{"index": last.Horizon})
	assert.Sometimes(len(last.Observations) >= 2, "at least two nodes converge to a common applied index", internal.Details{"index": last.Horizon, "readyNodes": len(last.Observations), "totalNodes": len(last.Members)})
	assert.Reachable("all current replicas reach the same durable identity horizon", internal.Details{"index": last.Horizon, "members": last.Members})
	for _, got := range last.Observations {
		assert.Always(got.State.Audit.TailMatches && (got.State.Audit.Count == 0 || got.State.Audit.TipPresent), "complete audit listing and point tip agree at a common applied index", internal.Details{"index": last.Horizon, "observation": got})
	}
	if len(last.Observations) < 2 {
		// A single member is a valid topology, but cannot exercise a cross-node
		// comparison. Do not turn it into successful identity coverage.
		return
	}
	ref := last.Observations[0]
	var accountCount uint64
	for _, ledger := range ref.State.Ledgers {
		accountCount += ledger.Accounts
	}
	allEqual := true
	for _, got := range last.Observations[1:] {
		details := internal.Details{"index": last.Horizon, "reference": ref, "observation": got}
		accountsEqual := equalAccounts(ref.State, got.State)
		assert.Always(accountsEqual, "account state is identical across nodes at a common applied index", details)
		auditEqual := ref.State.Audit == got.State.Audit
		assert.Always(auditEqual, "audit hash is identical across nodes at the same sequence", details)
		assert.Always(ref.State.Audit.Version == got.State.Audit.Version, "audit hash version is identical across nodes at the same sequence", details)
		allEqual = allEqual && accountsEqual && auditEqual
	}
	if allEqual && accountCount > 0 && ref.State.Audit.Count > 0 && ref.State.Audit.TailMatches && ref.State.Audit.TipPresent {
		assert.Reachable("cross-node complete account and audit identity verified", internal.Details{"index": last.Horizon, "observations": last.Observations})
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
