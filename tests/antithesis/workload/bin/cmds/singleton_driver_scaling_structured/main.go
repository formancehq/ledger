// singleton_driver_scaling_structured exercises deterministic scale-up /
// scale-down cycles, with stability windows between each move. Unlike the
// random-target scaling driver and the rapid-fire scaling_chaos driver, this
// one walks a fixed sequence: 3 → 5 → 3 → 7 → 3 (repeat). Between every move
// it re-reads a sentinel transaction committed before the first move and
// asserts post-commit volumes on a small fresh transaction. The goal is to
// catch regressions where scaling succeeds at the Raft level but breaks
// read paths, cache state, or volume accounting on the new node.
package main

import (
	"context"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"k8s.io/client-go/dynamic"
)

const (
	sentinelLedger        = "sentinel-scaling-structured"
	convergenceTimeout    = 10 * time.Minute
	stableWindow          = 20 * time.Second
	cooldownBetweenRounds = 60 * time.Second
)

// scalingCycle is the deterministic replica sequence. Each value MUST be odd
// (Raft quorum) and present in internal.OddReplicas.
var scalingCycle = []int64{5, 3, 7, 3}

func main() {
	log.Println("composer: singleton_driver_scaling_structured")

	ctx := context.Background()

	dynClient, err := internal.NewK8sClient()
	if err != nil {
		log.Printf("cannot build k8s client: %s", err)
		return
	}

	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("cannot create ledger gRPC client: %s", err)
		return
	}
	defer conn.Close()

	clusterClient := clusterpb.NewClusterServiceClient(conn)
	lsClient := dynClient.Resource(internal.LedgerServiceGVR).Namespace(internal.LedgerServiceNamespace())

	if err := internal.CreateLedger(ctx, client, sentinelLedger); err != nil && !internal.IsTransient(err) {
		log.Printf("cannot create sentinel ledger: %s", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(cooldownBetweenRounds):
		}

		runCycle(ctx, lsClient, clusterClient, client)
	}
}

func runCycle(ctx context.Context, lsClient dynamic.ResourceInterface, clusterClient clusterpb.ClusterServiceClient, client servicepb.BucketServiceClient) {
	// Capture a sentinel commit before scaling; it must survive every move.
	sentinel, err := internal.PreCommitSentinel(ctx, client, sentinelLedger)
	if err != nil {
		if !internal.IsTransient(err) {
			log.Printf("structured-scaling: cannot precommit sentinel: %s", err)
		}
		return
	}

	for _, target := range scalingCycle {
		details := internal.Details{
			"target":   target,
			"sentinel": sentinel.TxID,
		}

		err := internal.PatchReplicas(ctx, lsClient, internal.LedgerServiceName, target)
		assert.Sometimes(err == nil, "structured scaling patch should succeed", details.With(internal.Details{"error": err}))
		if err != nil {
			log.Printf("structured-scaling: patch failed: %s", err)
			return
		}

		log.Printf("structured-scaling: patched replicas=%d, waiting for convergence", target)
		if !internal.WaitForVoters(ctx, clusterClient, target, convergenceTimeout, details) {
			// WaitForVoters already emits Sometimes — bail out for this cycle.
			return
		}

		// Stable window: sentinel must still be readable and a fresh tx must
		// commit with consistent post-commit volumes.
		select {
		case <-ctx.Done():
			return
		case <-time.After(stableWindow):
		}

		sentinel.Verify(ctx, client, "after_scale_to_"+stringify(target))
		verifyFreshCommit(ctx, client, details)
	}
}

func verifyFreshCommit(ctx context.Context, client servicepb.BucketServiceClient, details internal.Details) {
	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: sentinelLedger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings:      []*commonpb.Posting{commonpb.NewPosting("world", "scaling:check", "COIN", internal.RandomBigInt())},
							Force:         true,
							ExpandVolumes: true,
						},
					}},
				},
			},
		}},
	})
	assert.Sometimes(err == nil || internal.IsTransient(err), "fresh commit during stable window should succeed", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}
	tx := internal.ExtractCreatedTransaction(resp)
	if tx == nil {
		return
	}
	internal.CheckPostCommitVolumes(tx.PostCommitVolumes, details)
}

func stringify(v int64) string {
	switch v {
	case 3:
		return "3"
	case 5:
		return "5"
	case 7:
		return "7"
	}
	return "other"
}
