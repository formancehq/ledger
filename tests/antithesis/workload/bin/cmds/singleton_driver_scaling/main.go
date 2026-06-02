package main

import (
	"context"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"k8s.io/client-go/dynamic"
)

// maxStep is the maximum number of replicas to add or remove in a single
// scaling operation. With OrderedReady StatefulSets, each new node must join
// sequentially; large jumps (e.g. 3→7) take too long under fault injection.
const maxStep int64 = 2

// convergenceTimeout is the maximum time to wait for the Raft cluster to
// reach the expected voter count after a scaling patch.
const convergenceTimeout = 10 * time.Minute

func main() {
	log.Println("composer: singleton_driver_scaling")

	ctx := context.Background()

	dynClient, err := internal.NewK8sClient()
	if err != nil {
		log.Printf("cannot build k8s client: %s", err)
		return
	}

	_, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("cannot create ledger gRPC client: %s", err)
		return
	}
	defer conn.Close()

	clusterClient := clusterpb.NewClusterServiceClient(conn)
	lsClient := dynClient.Resource(internal.LedgerServiceGVR).Namespace(internal.LedgerServiceNamespace())

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}

		scale(ctx, lsClient, clusterClient)
	}
}

func scale(ctx context.Context, lsClient dynamic.ResourceInterface, clusterClient clusterpb.ClusterServiceClient) {
	target := internal.OddReplicas[internal.Rand().Intn(len(internal.OddReplicas))]

	currentReplicas, err := internal.GetCurrentReplicas(ctx, lsClient, "ledger")
	if err != nil {
		log.Printf("scaling: cannot get LedgerService: %s", err)
		return
	}

	if currentReplicas == target {
		log.Printf("scaling: already at %d replicas, skipping", target)
		return
	}

	// Scale in steps of maxStep to avoid long convergence times.
	for currentReplicas != target {
		next := nextStep(currentReplicas, target)

		details := internal.Details{
			"ledgerService":   "ledger",
			"currentReplicas": currentReplicas,
			"stepTarget":      next,
			"finalTarget":     target,
		}

		log.Printf("scaling: %d → %d (final target %d)", currentReplicas, next, target)

		err = internal.PatchReplicas(ctx, lsClient, "ledger", next)
		assert.Sometimes(err == nil, "should be able to patch LedgerService replicas", details.With(internal.Details{"error": err}))

		if err != nil {
			log.Printf("scaling: patch failed: %s", err)
			return
		}

		assert.Reachable("scaling patch applied", details)

		if !internal.WaitForVoters(ctx, clusterClient, next, convergenceTimeout, details) {
			return
		}

		currentReplicas = next
	}
}

// nextStep computes the next replica count towards target, clamping the
// change to maxStep and ensuring the result is odd (valid for Raft quorum).
func nextStep(current, target int64) int64 {
	if target > current {
		next := min(current+maxStep, target)
		if next%2 == 0 {
			next++
		}

		return min(next, target)
	}

	next := max(current-maxStep, target)
	if next%2 == 0 {
		next--
	}

	return max(next, 3)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}
