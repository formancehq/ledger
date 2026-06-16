package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	antirandom "github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Probes the #327 lost-wakeup class end to end: a linearizable read issued on
// a quiescent cluster (no concurrent writes, no fault injection) must
// complete. Any unrelated write would rescue a stranded WaitForApplied waiter
// and mask the hang, hence eventually_ — fault injection is stopped by the
// platform, other commands are terminated, and the timeline branch ends after
// this check, so the idle window between the write and the probe read is
// genuinely write-free.
const (
	stabilizeTimeout = 90 * time.Second
	probeTimeout     = 30 * time.Second
	writeAttempts    = 3
)

func main() {
	log.Println("composer: eventually_quiesce_read")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)

		return
	}
	defer conn.Close()

	// Stabilize: containers may still be recovering right after faults stop.
	ledger := waitForLedger(ctx, client)
	if ledger == "" {
		log.Println("cluster did not stabilize, skipping probe")

		return
	}

	// One acked write, to a destination we can read back. Bounded retries:
	// the system may still be settling even after reads succeed.
	dest := fmt.Sprintf("users:%d", internal.Rand().Uint64()%internal.UserAccountCount)
	if !writeProbeTransaction(ctx, client, ledger, dest) {
		return
	}

	// Quiesce: idle window with zero writes. Menu {2, 5, 8}s — long enough
	// for the cluster to go fully idle after the write applies.
	idle := antirandom.RandomChoice([]int{2, 5, 8})
	time.Sleep(time.Duration(idle) * time.Second)

	// Probe: linearizable read (default consistency) under its own deadline,
	// so DeadlineExceeded unambiguously means the read hung.
	probeCtx, cancelProbe := context.WithTimeout(ctx, probeTimeout)
	defer cancelProbe()

	start := time.Now()
	_, readErr := client.GetAccount(probeCtx, &servicepb.GetAccountRequest{
		Ledger:  ledger,
		Address: dest,
	})
	elapsed := time.Since(start)

	completed := readErr == nil || !errors.Is(probeCtx.Err(), context.DeadlineExceeded)
	details := internal.Details{
		"ledger":      ledger,
		"address":     dest,
		"idleSeconds": idle,
		"elapsedMs":   elapsed.Milliseconds(),
		"error":       fmt.Sprintf("%v", readErr),
	}

	assert.Always(completed, "linearizable read completes on a quiescent cluster", details)

	if readErr == nil {
		assert.Reachable("quiescent linearizable read returned data", details)
	}
}

// waitForLedger polls until a ledger is listable or the stabilize window ends.
func waitForLedger(ctx context.Context, client servicepb.BucketServiceClient) string {
	stabilizeCtx, cancel := context.WithTimeout(ctx, stabilizeTimeout)
	defer cancel()

	for stabilizeCtx.Err() == nil {
		ledger, err := internal.GetRandomLedger(stabilizeCtx, client)
		if err == nil {
			return ledger
		}

		time.Sleep(time.Second)
	}

	return ""
}

// writeProbeTransaction commits one world→dest posting and reports whether it
// was acknowledged. Failures are logged, not asserted: write availability is
// covered by other drivers, and a skipped probe is preferable to a false
// finding while the cluster is still recovering.
func writeProbeTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger, dest string) bool {
	for attempt := range writeAttempts {
		writeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		_, err := client.Apply(writeCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{
									commonpb.NewPosting("world", dest, "COIN", big.NewInt(1)),
								},
								Force: true,
							},
						}},
					},
				},
			}),
		})
		cancel()

		if err == nil {
			return true
		}

		log.Printf("probe write attempt %d failed: %s", attempt+1, err)
		time.Sleep(2 * time.Second)
	}

	log.Println("probe write never succeeded, skipping probe")

	return false
}
