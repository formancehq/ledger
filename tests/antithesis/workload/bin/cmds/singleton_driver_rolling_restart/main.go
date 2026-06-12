// singleton_driver_rolling_restart performs a coordinated rolling restart of
// the ledger StatefulSet. For each pod from the highest ordinal down to 0:
//  1. If the pod hosts the Raft leader, transfer leadership to a non-leader
//     voter first (avoids the natural election delay on hard-delete).
//  2. Hard-delete the pod (grace=0) — the StatefulSet recreates it with the
//     same name but a new UID.
//  3. Wait for the new pod to be Ready and for the cluster voter count to
//     return to its original value.
//
// A sentinel transaction is committed before the sweep and re-read after; a
// background tx burst runs throughout the restart and is asserted to
// `Sometimes` commit (we want at least one write to land mid-restart, proving
// the cluster keeps accepting writes during a single-pod outage).
package main

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"k8s.io/client-go/kubernetes"
)

const (
	rrSentinelLedger      = "sentinel-rolling-restart"
	rrPodGoneTimeout      = 60 * time.Second
	rrPodReadyTimeout     = 5 * time.Minute
	rrVotersTimeout       = 5 * time.Minute
	rrCooldownPerPod      = 15 * time.Second
	rrCooldownBetweenRuns = 90 * time.Second
)

func main() {
	log.Println("composer: singleton_driver_rolling_restart")

	ctx := context.Background()

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

	if err := internal.CreateLedger(ctx, client, rrSentinelLedger); err != nil && !internal.IsTransient(err) {
		log.Printf("cannot create sentinel ledger: %s", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(rrCooldownBetweenRuns):
		}

		runSweep(ctx, clientset, clusterClient, client)
	}
}

func runSweep(ctx context.Context, clientset kubernetes.Interface, clusterClient clusterpb.ClusterServiceClient, client servicepb.BucketServiceClient) {
	pods, err := internal.ListLedgerPods(ctx, clientset)
	if err != nil {
		log.Printf("rolling-restart: list pods failed: %s", err)
		return
	}
	if len(pods) == 0 {
		return
	}
	expected := int64(len(pods))

	sentinel, err := internal.PreCommitSentinel(ctx, client, rrSentinelLedger)
	if err != nil {
		if !internal.IsTransient(err) {
			log.Printf("rolling-restart: precommit failed: %s", err)
		}
		return
	}

	// Background tx burst: fire small commits throughout the sweep.
	burstCtx, cancelBurst := context.WithCancel(ctx)
	defer cancelBurst()
	var committedDuringRestart atomic.Int64
	go writeBurst(burstCtx, client, &committedDuringRestart)

	// Highest ordinal down to 0 — matches kubernetes StatefulSet rolling-update direction.
	for i := len(pods) - 1; i >= 0; i-- {
		pod := pods[i]
		details := internal.Details{
			"pod":      pod,
			"ordinal":  internal.PodOrdinal(pod),
			"sentinel": sentinel.TxID,
		}

		if err := transferAwayFrom(ctx, clusterClient, pod, details); err != nil {
			log.Printf("rolling-restart: %s: transfer skipped: %s", pod, err)
		}

		uid, err := internal.GetPodUID(ctx, clientset, pod)
		if err != nil {
			log.Printf("rolling-restart: get UID %s failed: %s", pod, err)
			continue
		}

		err = internal.DeletePod(ctx, clientset, pod)
		assert.Sometimes(err == nil, "rolling-restart pod delete should succeed", details.With(internal.Details{"error": err}))
		if err != nil {
			continue
		}
		assert.Reachable("rolling-restart deleted pod", details)

		if !internal.WaitForPodGone(ctx, clientset, pod, uid, rrPodGoneTimeout) {
			log.Printf("rolling-restart: %s did not disappear within %s", pod, rrPodGoneTimeout)
			continue
		}
		ready := internal.WaitForPodReady(ctx, clientset, pod, rrPodReadyTimeout)
		assert.Sometimes(ready, "restarted pod should reach Ready", details)
		if !ready {
			continue
		}
		if !internal.WaitForVoters(ctx, clusterClient, expected, rrVotersTimeout, details) {
			// WaitForVoters already emitted a Sometimes on timeout.
			continue
		}
		assert.Reachable("voter count returned to expected after pod restart", details)

		select {
		case <-ctx.Done():
			return
		case <-time.After(rrCooldownPerPod):
		}
	}

	cancelBurst()
	assert.Sometimes(committedDuringRestart.Load() > 0,
		"at least one write must commit during the rolling restart",
		internal.Details{"committed": committedDuringRestart.Load()})

	sentinel.Verify(ctx, client, "after_rolling_restart_sweep")
}

// transferAwayFrom transfers leadership to a non-leader voter when the pod
// hosts the current leader. Returns nil if the pod is not the leader.
func transferAwayFrom(ctx context.Context, clusterClient clusterpb.ClusterServiceClient, pod string, details internal.Details) error {
	leaderPod, leaderID, err := internal.GetLeaderPodName(ctx, clusterClient)
	if err != nil || leaderID == 0 {
		return err
	}
	if leaderPod != pod {
		return nil
	}
	target, err := internal.GetNonLeaderVoter(ctx, clusterClient)
	if err != nil || target == 0 {
		return err
	}
	_, err = clusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{Transferee: target})
	if err == nil {
		assert.Reachable("rolling-restart transferred leadership before pod delete", details.With(internal.Details{"to": target}))
	}
	return err
}

func writeBurst(ctx context.Context, client servicepb.BucketServiceClient, committed *atomic.Int64) {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: rrSentinelLedger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Postings: []*commonpb.Posting{commonpb.NewPosting("world", "burst:rr", "COIN", internal.RandomBigInt())},
								Force:    true,
							},
						}},
					},
				},
			}},
		})
		if err == nil {
			committed.Add(1)
		}
	}
}
