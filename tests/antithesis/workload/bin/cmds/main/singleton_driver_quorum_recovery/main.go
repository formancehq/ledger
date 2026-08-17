// singleton_driver_quorum_recovery exercises the operator's force-remove path
// in raft_scaledown.go. The scenario:
//
//  1. Ensure a stable 3-voter cluster, pre-commit a sentinel transaction and
//     an isolated one-posting PIT oracle; require exact PIT from every voter.
//  2. Identify the leader and choose the two non-leader pods as victims.
//  3. DeletePod(grace=0) on both victims and PatchReplicas(1). While Kubernetes
//     exposes only the original leader but its local Raft membership remains
//     at 3 voters, issue one default-consistency PIT call with configured gRPC
//     retries disabled. Pre/post membership samples and a SUT acknowledgement
//     keep any transparent pre-processing retry from crossing the interval.
//  4. Require a narrow fail-closed result and an authenticated SUT header
//     proving the request stopped at ReadIndexAndWait.
//  5. Wait for the operator force-remove path to report voters=1, then verify
//     the committed sentinel survived.
//  6. PatchReplicas(3), require two equal membership snapshots, and require an
//     exact PIT result from every voter ID in that stable snapshot.
//
// The driver runs sparsely (≥ 5 minutes between rounds) because each round is
// destructive and the recovery path takes several minutes under fault
// injection.
package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"sort"
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
	qrCooldown          = 5 * time.Minute
	qrScaleDownTimeout  = 8 * time.Minute
	qrScaleUpTimeout    = 15 * time.Minute
	qrPITReadyTimeout   = 90 * time.Second
	qrPITProbeTimeout   = 4 * time.Second
	qrMembershipTimeout = 6 * time.Second
	qrPITPollInterval   = time.Second
	qrPodGoneTimeout    = 30 * time.Second

	// qrConfChangeLatencyBudget is the "should be fast" threshold for the
	// force-remove ConfChange to bring voters down to 1 after scale-down.
	// Healthy runs converge in a handful of WaitForVoters poll cycles (5s
	// each); the EN-1043 regression pushed this past 47s in one antithesis
	// run. 30s is chosen to sit comfortably between the two, so a
	// Sometimes assertion catches a regression without flaking on operator
	// polling variance.
	qrConfChangeLatencyBudget = 30 * time.Second
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
	fixture, err := internal.PrepareLinearizablePITFixture(ctx, client)
	if err != nil {
		if !internal.IsTransient(err) {
			log.Printf("quorum-recovery: cannot prepare PIT fixture: %s", err)
		}

		return
	}

	leaderPod, leaderID, err := internal.GetLeaderPodName(ctx, clusterClient)
	if err != nil || leaderID == 0 {
		log.Printf("quorum-recovery: no leader, skipping")
		return
	}
	stableLeaderID, voterIDs, stable := waitForStableVoterSnapshot(ctx, clusterClient, 3, qrPITReadyTimeout)
	if !stable || stableLeaderID != leaderID {
		log.Printf("quorum-recovery: voter membership did not stabilize on leader %d", leaderID)

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

	perNode, err := internal.DialPerNode(ctx, false)
	if err != nil {
		log.Printf("quorum-recovery: cannot dial no-retry per-node clients: %s", err)

		return
	}
	defer perNode.Close()
	leaderConn := perNodeByID(perNode, leaderID)
	if leaderConn == nil {
		log.Printf("quorum-recovery: no direct connection resolved for leader %d", leaderID)

		return
	}
	preFaultReady, preFaultDetails := waitForPITExactOnEveryNode(ctx, perNode, fixture, voterIDs, qrPITReadyTimeout)
	if !preFaultReady {
		log.Printf("quorum-recovery: PIT fixture did not converge before fault: %v", preFaultDetails)

		return
	}

	details := internal.Details{
		"leader":           leaderPod,
		"leader_id":        leaderID,
		"victims":          victims,
		"sentinel":         sentinel.TxID,
		"pit_ledger":       fixture.Ledger,
		"pit_ledger_id":    fixture.LedgerID,
		"pit_min_log":      fixture.MinLogSequence,
		"pit_axis":         fixture.Request.GetHistoricalBalance().GetTemporality().String(),
		"pit_requested_at": fixture.Request.GetHistoricalBalance().GetAt().GetData(),
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

	onlyLeaderPod := waitForOnlyLedgerPod(ctx, clientset, leaderPod, qrPodGoneTimeout)
	failClosedReached := false
	coverageDetails := details.With(internal.Details{
		"fault_window_reached": false,
	})
	if onlyLeaderPod && directNodeStillHasVoters(ctx, leaderConn, 3) {
		probeID := fmt.Sprintf("pit-quorum-%d-%016x", leaderID, internal.Rand().Uint64())
		probeCtx, cancel := context.WithTimeout(ctx, qrPITProbeTimeout)
		view, barrierReached, probeErr := internal.CheckLinearizablePIT(probeCtx, leaderConn.Bucket, fixture, probeID)
		cancel()
		if ctx.Err() != nil {
			return
		}

		membershipStayedAtThree := directNodeStillHasVoters(ctx, leaderConn, 3)
		failClosed := view == nil && internal.IsLinearizablePITPartitionTransient(probeErr)
		probeDetails := details.With(internal.Details{
			"probe_id":                probeID,
			"error":                   probeErr,
			"barrier_reached":         barrierReached,
			"membership_stayed_three": membershipStayedAtThree,
			"fault_window_reached":    membershipStayedAtThree,
		})
		coverageDetails = probeDetails
		if view != nil {
			probeDetails["returned_log_watermark"] = view.GetLogWatermark()
		}
		if membershipStayedAtThree {
			assert.Always(
				failClosed,
				"pit: quorum-isolated default PIT never serves local history",
				probeDetails,
			)
			failClosedReached = failClosed && barrierReached
		}
	} else {
		log.Printf("quorum-recovery: missed three-voter no-quorum PIT probe window")
	}
	assert.Sometimes(
		failClosedReached,
		"pit: default PIT reaches a partition-induced fail-closed outcome",
		coverageDetails,
	)

	// EN-1043 regression sentinel: measure how long it takes for the
	// operator's force-remove ConfChange to actually reduce the voter
	// count to 1. When the transport silently drops MsgApp carrying the
	// ConfChange (channels full, no Unreachable emitted), etcd/raft
	// keeps optimistically retrying and recovery stalls ~47s in the
	// worst antithesis observation (2026-06-08). With Unreachable
	// emitted on drop, Raft transitions the dead peers to StateProbe
	// and the ConfChange applies on the next heartbeat.
	scaleDownStart := time.Now()

	if !internal.WaitForVoters(ctx, clusterClient, 1, qrScaleDownTimeout, details) {
		// The deferred cleanup restores replicas=3. Sentinel verify is best
		// effort: the cluster may be in an oscillating (1)↔(1,3) state until
		// the operator's scaledown gives up; sentinel data is still committed
		// on the live voter so a read-after-write should hold once we're back.
		sentinel.Verify(ctx, client, "after_quorum_recovery_timeout")
		return
	}
	assert.Reachable("force-remove path executed (voters=1)", details)

	// EN-1043: healthy runs should converge well under qrConfChangeLatencyBudget.
	// WaitForVoters polls every 5s, so the observed elapsed time is bounded
	// below by ~5s; the buggy behavior pushes it past 47s.
	elapsed := time.Since(scaleDownStart)
	assert.Sometimes(elapsed < qrConfChangeLatencyBudget,
		"force-remove ConfChange applies within latency budget after scale-down (EN-1043)",
		details.With(internal.Details{"elapsed": elapsed.String(), "budget": qrConfChangeLatencyBudget.String()}))

	sentinel.Verify(ctx, client, "after_quorum_recovery")

	err = internal.PatchReplicas(ctx, lsClient, internal.ClusterName, 3)
	assert.Sometimes(err == nil, "scale-up to 3 should succeed", details.With(internal.Details{"error": err}))
	if err != nil || !internal.WaitForVoters(ctx, clusterClient, 3, qrScaleUpTimeout, details) {
		return
	}

	recovered, recoveryDetails := waitForPITRecoveryOnStableVoters(
		ctx,
		clusterClient,
		fixture,
		qrPITReadyTimeout,
	)
	assert.Sometimes(
		recovered,
		"pit: default linearizable PIT recovers exactly after Raft partition",
		details.With(recoveryDetails),
	)
	if recovered {
		sentinel.Verify(ctx, client, "after_quorum_recovery_scale_up")
	}
}

func perNodeByID(conns internal.PerNodeConns, nodeID uint32) *internal.PerNodeConn {
	for _, conn := range conns {
		if conn.NodeID == nodeID {
			return conn
		}
	}

	return nil
}

func directNodeStillHasVoters(ctx context.Context, conn *internal.PerNodeConn, expected int) bool {
	probeCtx, cancel := context.WithTimeout(ctx, qrMembershipTimeout)
	defer cancel()

	state, err := conn.Cluster.GetClusterState(probeCtx, &clusterpb.GetClusterStateRequest{NodeId: conn.NodeID})
	if err != nil {
		return false
	}

	voters := 0
	for _, peer := range state.GetNodes() {
		if peer.GetSuffrage() == "Voter" {
			voters++
		}
	}

	return voters == expected
}

func waitForOnlyLedgerPod(
	ctx context.Context,
	clientset kubernetes.Interface,
	expectedPod string,
	timeout time.Duration,
) bool {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for waitCtx.Err() == nil {
		pods, err := internal.ListLedgerPods(waitCtx, clientset)
		if err == nil && len(pods) == 1 && pods[0] == expectedPod {
			return true
		}

		select {
		case <-waitCtx.Done():
			return false
		case <-time.After(qrPITPollInterval):
		}
	}

	return false
}

func waitForPITExactOnEveryNode(
	ctx context.Context,
	conns internal.PerNodeConns,
	fixture *internal.LinearizablePITFixture,
	expectedVoterIDs []uint32,
	timeout time.Duration,
) (bool, internal.Details) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	last := internal.Details{
		"expected_voters": slices.Clone(expectedVoterIDs),
		"resolved_nodes":  len(conns),
	}
	for waitCtx.Err() == nil {
		byID := make(map[uint32]*internal.PerNodeConn, len(conns))
		allExact := true
		for _, conn := range conns {
			if conn.NodeID == 0 {
				last[conn.Addr] = "unresolved node ID"

				continue
			}
			if _, duplicate := byID[conn.NodeID]; duplicate {
				allExact = false
				last[conn.Addr] = fmt.Sprintf("duplicate node ID %d", conn.NodeID)

				continue
			}
			byID[conn.NodeID] = conn
		}

		for _, voterID := range expectedVoterIDs {
			conn := byID[voterID]
			if conn == nil {
				allExact = false
				last[fmt.Sprintf("voter_%d", voterID)] = "no direct connection"

				continue
			}

			probeCtx, probeCancel := context.WithTimeout(waitCtx, qrPITProbeTimeout)
			view, _, err := internal.CheckLinearizablePIT(probeCtx, conn.Bucket, fixture, "")
			probeCancel()
			if err != nil {
				allExact = false
				last[conn.Addr] = err.Error()

				continue
			}
			last[conn.Addr] = fmt.Sprintf("exact at log watermark %d", view.GetLogWatermark())
		}
		if allExact && len(expectedVoterIDs) > 0 {
			return true, last
		}

		select {
		case <-waitCtx.Done():
			return false, last
		case <-time.After(qrPITPollInterval):
		}
	}

	return false, last
}

func waitForStableVoterSnapshot(
	ctx context.Context,
	clusterClient clusterpb.ClusterServiceClient,
	expectedVoters int,
	timeout time.Duration,
) (uint32, []uint32, bool) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		previousLeader uint32
		previousVoters []uint32
		havePrevious   bool
	)
	for waitCtx.Err() == nil {
		probeCtx, probeCancel := context.WithTimeout(waitCtx, qrMembershipTimeout)
		state, err := clusterClient.GetClusterState(probeCtx, &clusterpb.GetClusterStateRequest{})
		probeCancel()
		if err == nil && state.GetLeader() != 0 {
			voters := make([]uint32, 0, len(state.GetNodes()))
			for _, peer := range state.GetNodes() {
				if peer.GetSuffrage() == "Voter" {
					voters = append(voters, peer.GetId())
				}
			}
			sort.Slice(voters, func(left, right int) bool { return voters[left] < voters[right] })
			if len(voters) == expectedVoters &&
				havePrevious &&
				state.GetLeader() == previousLeader &&
				slices.Equal(voters, previousVoters) {
				return state.GetLeader(), voters, true
			}
			previousLeader = state.GetLeader()
			previousVoters = voters
			havePrevious = len(voters) == expectedVoters
		} else {
			havePrevious = false
		}

		select {
		case <-waitCtx.Done():
			return 0, nil, false
		case <-time.After(qrPITPollInterval):
		}
	}

	return 0, nil, false
}

func waitForPITRecoveryOnStableVoters(
	ctx context.Context,
	clusterClient clusterpb.ClusterServiceClient,
	fixture *internal.LinearizablePITFixture,
	timeout time.Duration,
) (bool, internal.Details) {
	recoveryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	details := internal.Details{}
	for recoveryCtx.Err() == nil {
		leaderBefore, votersBefore, stableBefore := waitForStableVoterSnapshot(
			recoveryCtx,
			clusterClient,
			3,
			timeout,
		)
		details["membership_before_stable"] = stableBefore
		details["leader_before"] = leaderBefore
		details["voters_before"] = votersBefore
		if !stableBefore {
			return false, details
		}

		conns, err := internal.DialPerNode(recoveryCtx, false)
		if err != nil {
			details["dial_error"] = err.Error()
		} else {
			exact, exactDetails := waitForPITExactOnEveryNode(
				recoveryCtx,
				conns,
				fixture,
				votersBefore,
				timeout,
			)
			conns.Close()
			details = details.With(exactDetails)
			if exact {
				leaderAfter, votersAfter, stableAfter := waitForStableVoterSnapshot(
					recoveryCtx,
					clusterClient,
					3,
					timeout,
				)
				details["membership_after_stable"] = stableAfter
				details["leader_after"] = leaderAfter
				details["voters_after"] = votersAfter
				if stableAfter && leaderAfter == leaderBefore && slices.Equal(votersAfter, votersBefore) {
					return true, details
				}
			}
		}

		select {
		case <-recoveryCtx.Done():
			return false, details
		case <-time.After(qrPITPollInterval):
		}
	}

	return false, details
}
