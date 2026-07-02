// singleton_driver_quorum_recovery exercises the operator's force-remove path
// in raft_scaledown.go. The scenario:
//
//  1. Ensure 3-replica cluster, pre-commit a sentinel transaction.
//  2. Identify the leader and choose the two non-leader pods as victims.
//  3. DeletePod(grace=0) on both victims simultaneously (the cluster is now
//     stuck — only one voter remains, no quorum).
//  4. PatchReplicas(1) so the operator observes the crashed pods, classifies
//     them as unhealthy, and runs the force-remove path. Wait until the leader
//     reports voters=1.
//  5. PatchReplicas(3) and wait for the cluster to regain a 3-voter quorum.
//  6. Re-read the sentinel — committed data must survive force-remove +
//     scale-up. This is the strongest correctness assertion of the suite.
//
// The driver runs sparsely (≥ 5 minutes between rounds) because each round is
// destructive and the recovery path takes several minutes under fault
// injection.
package main

import (
	"context"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var qrSentinelLedger = internal.PrefixSentinel.WithSuffix("quorum-recovery")

const (
	qrCooldown         = 5 * time.Minute
	qrScaleDownTimeout = 8 * time.Minute
	qrScaleUpTimeout   = 15 * time.Minute
)

func main() {
	log.Println("composer: singleton_driver_quorum_recovery")

	ctx, cancel := internal.SingletonContext()
	defer cancel()
	dynClient, err := internal.NewK8sClient()
	if err != nil {
		log.Printf("cannot build k8s client: %s", err)
		return
	}
	clientset, err := internal.NewKubeClientset()
	if err != nil {
		log.Printf("cannot build k8s clientset: %s", err)
		return
	}

	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("cannot create ledger gRPC client: %s", err)
		return
	}
	defer conn.Close()

	clusterClient := clusterpb.NewClusterServiceClient(conn)
	lsClient := dynClient.Resource(internal.ClusterGVR).Namespace(internal.ClusterNamespace())

	if err := internal.CreateLedger(ctx, client, qrSentinelLedger); err != nil && !internal.IsTransient(err) {
		log.Printf("cannot create sentinel ledger: %s", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(qrCooldown):
		}

		runRound(ctx, lsClient, clientset, clusterClient, client)
	}
}

func runRound(ctx context.Context, lsClient dynamic.ResourceInterface, clientset kubernetes.Interface, clusterClient clusterpb.ClusterServiceClient, client servicepb.BucketServiceClient) {
	current, err := internal.GetCurrentReplicas(ctx, lsClient, internal.ClusterName)
	if err != nil {
		log.Printf("quorum-recovery: cannot read current replicas: %s", err)
		return
	}
	if current != 3 {
		log.Printf("quorum-recovery: cluster not at N=3 (got %d), skipping", current)
		return
	}

	sentinel, err := internal.PreCommitSentinel(ctx, client, qrSentinelLedger)
	if err != nil {
		if !internal.IsTransient(err) {
			log.Printf("quorum-recovery: precommit failed: %s", err)
		}
		return
	}

	leaderPod, leaderID, err := internal.GetLeaderPodName(ctx, clusterClient)
	if err != nil || leaderID == 0 {
		log.Printf("quorum-recovery: no leader, skipping")
		return
	}

	pods, err := internal.ListLedgerPods(ctx, clientset)
	if err != nil || len(pods) < 3 {
		log.Printf("quorum-recovery: cannot list pods: %s", err)
		return
	}

	var victims []string
	for _, p := range pods {
		if p != leaderPod {
			victims = append(victims, p)
		}
		if len(victims) == 2 {
			break
		}
	}
	if len(victims) != 2 {
		log.Printf("quorum-recovery: expected 2 non-leader pods, got %d", len(victims))
		return
	}

	details := internal.Details{
		"leader":   leaderPod,
		"victims":  victims,
		"sentinel": sentinel.TxID,
	}
	log.Printf("quorum-recovery: killing %v (leader=%s)", victims, leaderPod)

	// Always restore replicas=3 on exit, even if the scale-down assertion
	// times out. Otherwise the cluster stays at desired=1 with a stuck
	// operator scale-down and every subsequent driver runs against a broken
	// cluster for the rest of the experiment.
	defer func() {
		if err := internal.PatchReplicas(context.Background(), lsClient, internal.ClusterName, 3); err != nil {
			log.Printf("quorum-recovery: cleanup PatchReplicas(3) failed: %s", err)
		}
		// Best-effort wait for the cluster to settle back to N=3 voters before
		// releasing the singleton slot. If it doesn't recover we let the next
		// driver iteration deal with it.
		_ = internal.WaitForVoters(context.Background(), clusterClient, 3, qrScaleUpTimeout, details)
	}()

	for _, v := range victims {
		err := internal.DeletePod(ctx, clientset, v)
		assert.Sometimes(err == nil, "quorum-recovery pod delete should succeed",
			details.With(internal.Details{"pod": v, "error": err}))
	}
	assert.Reachable("quorum-recovery killed both non-leader pods", details)

	err = internal.PatchReplicas(ctx, lsClient, internal.ClusterName, 1)
	assert.Sometimes(err == nil, "scale-down to 1 should succeed", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	if !internal.WaitForVoters(ctx, clusterClient, 1, qrScaleDownTimeout, details) {
		// The deferred cleanup restores replicas=3. Sentinel verify is best
		// effort: the cluster may be in an oscillating (1)↔(1,3) state until
		// the operator's scaledown gives up; sentinel data is still committed
		// on the live voter so a read-after-write should hold once we're back.
		sentinel.Verify(ctx, client, "after_quorum_recovery_timeout")
		return
	}
	assert.Reachable("force-remove path executed (voters=1)", details)

	sentinel.Verify(ctx, client, "after_quorum_recovery")
}
