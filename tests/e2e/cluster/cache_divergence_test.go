//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This test aggressively reproduces the cache divergence race condition:
//
// The race: during rotateLocked, currentGeneration is stored atomically BEFORE
// the gen0/gen1 ShardedMaps are rotated. A concurrent CheckCache (from the
// admission goroutine) can see the new generation but pre-rotation data,
// returning CacheNeedsTouch for a key about to be purged — when CacheMiss
// (full preload) was correct.
//
// Strategy: maximize concurrent CheckCache calls during rotations by:
//   - Very low cache rotation threshold (3) → rotation every 3 entries
//   - Multiple goroutines sending transactions in parallel → many concurrent admissions
//   - Multiple kill/restart cycles → forces catchup with stale cache
//   - Sentinel mode → panics on volume imbalance (fail-fast)
var _ = Describe("Cache divergence under chaos", func() {
	const (
		countInstances = 3
		ledgerName     = "chaos-ledger"
		parallelism    = 4  // concurrent transaction senders
		txPerWorker    = 20 // transactions per worker per phase
		killCycles     = 3  // number of kill/restart cycles
	)

	Context("with aggressive cache rotation and parallel transactions", Ordered, func() {
		var (
			ctx     context.Context
			servers []*testutil.ServiceWithClient
		)

		BeforeAll(func() {
			ctx, servers, _, _ = testutil.SetupMultiNodeCluster(
				countInstances,
				testutil.TestRaftBasePort,
				testutil.TestServiceBasePort,
				testutil.TestHTTPBasePort,
				testutil.TestGatewayBasePort,
				testutil.WithCacheRotationThreshold(3),
				testutil.WithSentinelMode(),
			)
		})

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("should maintain volume consistency across kill/restart cycles with parallel load", func() {
			// Step 1: Create ledger
			_, err := servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// Step 2: Initial parallel load to populate cache
			sendParallelTransactions(ctx, servers[0].Client, ledgerName, parallelism, txPerWorker, 0, "COIN")

			for cycle := 0; cycle < killCycles; cycle++ {
				By(fmt.Sprintf("Kill/restart cycle %d/%d", cycle+1, killCycles))

				// Pick a different follower each cycle
				followerIdx := (cycle % (countInstances - 1)) + 1

				// Verify consistency before kill
				verifyVolumesConsistent(ctx, servers, ledgerName)

				// Kill the follower
				By(fmt.Sprintf("  Killing node %d", followerIdx+1))
				testutil.StopNode(ctx, servers[followerIdx])

				// Blast transactions while follower is down
				// This refreshes volumes in the leader's cache that the follower will miss
				By("  Sending transactions while follower is down")
				sendParallelTransactions(ctx, servers[0].Client, ledgerName, parallelism, txPerWorker, cycle*1000, "COIN")
				sendParallelTransactions(ctx, servers[0].Client, ledgerName, parallelism, txPerWorker, cycle*1000, "USD")
				sendParallelTransactions(ctx, servers[0].Client, ledgerName, parallelism, txPerWorker, cycle*1000, "EUR")

				// Restart the follower — it must catch up through multiple rotations
				By(fmt.Sprintf("  Restarting node %d", followerIdx+1))
				testutil.RestartNode(ctx, servers[followerIdx])

				// Wait for follower to rejoin
				Eventually(func(g Gomega) {
					state, err := servers[followerIdx].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
					g.Expect(err).To(Succeed())
					g.Expect(state.GetLeader()).NotTo(BeZero())
				}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

				// Now send MORE transactions through the leader while follower catches up
				// These proposals may use CacheGuaranteed for volumes the follower
				// doesn't have — this is where the race manifests
				By("  Sending transactions after restart (race window)")
				sendParallelTransactions(ctx, servers[0].Client, ledgerName, parallelism, txPerWorker, cycle*1000+500, "COIN")
			}

			// Final consistency check across ALL nodes. verifyVolumesConsistent
			// polls until the nodes converge, so no fixed settle delay is needed.
			By("Final volume consistency check")
			verifyVolumesConsistent(ctx, servers, ledgerName)
		})
	})
})

// sendParallelTransactions sends transactions from multiple goroutines concurrently.
// Each goroutine sends txCount transactions to different accounts.
// accountOffset ensures different cycles use overlapping but distinct account sets.
func sendParallelTransactions(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	ledgerName string,
	workers, txPerWorker, accountOffset int,
	asset string,
) {
	var wg sync.WaitGroup

	var failures atomic.Int64

	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < txPerWorker; i++ {
				account := fmt.Sprintf("user:%d", accountOffset+workerID*txPerWorker+i)

				_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(
					ledgerName,
					[]*commonpb.Posting{
						actions.NewPosting("world", account, big.NewInt(100), asset),
					},
					nil,
				)))
				if err != nil {
					failures.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()

	// Some failures are OK (node might be catching up), but not too many
	ExpectWithOffset(1, failures.Load()).To(BeNumerically("<", int64(workers*txPerWorker/2)),
		"Too many transaction failures (%d/%d)", failures.Load(), workers*txPerWorker)
}

// verifyVolumesConsistent checks that all running nodes report the same
// aggregated volumes for the given ledger.
func verifyVolumesConsistent(ctx context.Context, servers []*testutil.ServiceWithClient, ledgerName string) {
	type volumeSnapshot struct {
		nodeID  uint32
		volumes map[string]string // "account/asset" → "input:output"
	}

	// Poll the full collect-and-compare until the nodes converge, rather than
	// snapshotting once after a fixed settle delay. Nodes may still be applying
	// when the check starts (especially right after a restart cycle).
	EventuallyWithOffset(1, func(g Gomega) {
		var snapshots []volumeSnapshot

		for _, srv := range servers {
			if srv.GRPCConn == nil {
				continue
			}

			accounts, err := actions.ListAllAccounts(ctx, srv.Client, ledgerName)
			g.Expect(err).To(Succeed(), "Node %d failed to list accounts", srv.NodeID)

			snap := volumeSnapshot{
				nodeID:  srv.NodeID,
				volumes: make(map[string]string),
			}

			for _, acct := range accounts {
				for asset, vol := range acct.GetVolumes() {
					key := fmt.Sprintf("%s/%s", acct.GetAddress(), asset)
					snap.volumes[key] = fmt.Sprintf("%s:%s", vol.GetInput(), vol.GetOutput())
				}
			}

			snapshots = append(snapshots, snap)
		}

		g.Expect(len(snapshots)).To(BeNumerically(">=", 2),
			"Need at least 2 reachable nodes to compare")

		ref := snapshots[0]
		for _, other := range snapshots[1:] {
			for key, refVol := range ref.volumes {
				otherVol, exists := other.volumes[key]
				g.Expect(exists).To(BeTrue(),
					"Node %d missing volume %s (present on node %d with %s)",
					other.nodeID, key, ref.nodeID, refVol)
				g.Expect(otherVol).To(Equal(refVol),
					"Volume mismatch for %s: node %d has %s, node %d has %s",
					key, ref.nodeID, refVol, other.nodeID, otherVol)
			}
		}
	}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
}
