// singleton_driver_scaling_chaos exercises chaotic scaling patterns where
// replica count changes are fired without waiting for the cluster to stabilise.
// This simulates an operator mistake or conflicting automation: scale-up then
// immediately scale-down (or vice-versa) before the Raft cluster has converged.
//
// The driver works in two alternating phases:
//  1. Chaos phase: rapidly patch the Cluster replica count 2-4 times
//     with random odd values [3,5,7], sleeping only a few seconds between
//     patches — not enough time for the operator to finish reconciliation.
//  2. Recovery phase: stop patching and wait for the cluster to converge to
//     whatever the last-applied replica count was. Assert that it eventually
//     does (correctness) but allow generous time (liveness under chaos).
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"k8s.io/client-go/dynamic"
)

// recoveryTimeout is how long we wait for the cluster to converge after
// the chaos phase. This must be generous: the operator needs to handle
// partial scale-ups, learners that never got promoted, and pods in
// CrashLoopBackOff — all under Antithesis fault injection.
const recoveryTimeout = 15 * time.Minute

func main() {
	log.Println("composer: singleton_driver_scaling_chaos")

	ctx, cancel := internal.SingletonContext()
	defer cancel()
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
	lsClient := dynClient.Resource(internal.ClusterGVR).Namespace(internal.ClusterNamespace())

	for {
		select {
		case <-ctx.Done():
			return
		// Wait a bit between chaos rounds so the cluster has some quiet time.
		case <-time.After(60 * time.Second):
		}

		chaosRound(ctx, lsClient, clusterClient)
	}
}

func chaosRound(ctx context.Context, lsClient dynamic.ResourceInterface, clusterClient clusterpb.ClusterServiceClient) {
	r := internal.Rand()

	// Pick 2-4 rapid-fire scaling changes.
	steps := 2 + r.Intn(3)

	log.Printf("chaos-scaling: starting round with %d rapid patches", steps)

	var lastTarget int64

	for i := range steps {
		target := internal.OddReplicas[r.Intn(len(internal.OddReplicas))]
		lastTarget = target

		details := internal.Details{
			"step":   fmt.Sprintf("%d/%d", i+1, steps),
			"target": target,
		}

		log.Printf("chaos-scaling: step %d/%d → %d replicas (fire-and-forget)", i+1, steps, target)

		err := internal.PatchReplicas(ctx, lsClient, "ledger", target)
		assert.Sometimes(err == nil, "chaos scaling patch should succeed", details.With(internal.Details{"error": err}))

		if err != nil {
			log.Printf("chaos-scaling: patch failed: %s", err)
			return
		}

		assert.Reachable("chaos scaling patch applied", details)

		// Short pause — not enough for full convergence.
		// The operator will see a new desired state while still reconciling the previous one.
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(3+r.Intn(8)) * time.Second):
		}
	}

	// Recovery phase: the cluster should eventually converge to lastTarget.
	log.Printf("chaos-scaling: recovery phase — waiting for convergence to %d replicas", lastTarget)

	details := internal.Details{
		"finalTarget": lastTarget,
		"chaosSteps":  steps,
	}

	converged := internal.WaitForVoters(ctx, clusterClient, lastTarget, recoveryTimeout, details)

	assert.Sometimes(converged, "cluster should recover after chaotic scaling", details)

	if converged {
		log.Printf("chaos-scaling: cluster recovered (%d voters)", lastTarget)
	} else {
		log.Printf("chaos-scaling: cluster did NOT converge to %d voters within timeout", lastTarget)
	}
}
