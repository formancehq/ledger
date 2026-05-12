//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// rollingUpdateThreshold performs a rolling upgrade of all nodes to a new threshold.
func rollingUpdateThreshold(
	ctx context.Context,
	servers []*testutil.ServiceWithClient,
	leaderID *uint64,
	newThreshold uint64,
) {
	lid := *leaderID

	for i := range len(servers) {
		nodeID := uint64(i + 1)
		if nodeID == lid {
			continue
		}

		newInstruments := append(servers[i].Service.Instruments, testserver.WithCacheRotationThreshold(newThreshold))
		testutil.RestartNodeWithInstruments(ctx, servers[i], newInstruments)

		Eventually(servers[i]).
			WithTimeout(30 * time.Second).
			WithPolling(500 * time.Millisecond).
			Should(BeFollower())
	}

	var targetID uint64
	for i := range len(servers) {
		if uint64(i+1) != lid {
			targetID = uint64(i + 1)
			break
		}
	}

	resp, err := servers[lid-1].ClusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
		Transferee: uint32(targetID),
	})
	Expect(err).To(Succeed())
	Expect(resp.NewLeader).To(Equal(uint32(targetID)))
	*leaderID = targetID

	for i := range len(servers) {
		Eventually(func(g Gomega) uint64 {
			state, err := servers[i].ClusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{NodeId: servers[i].NodeID})
			g.Expect(err).To(Succeed())
			return uint64(state.Leader)
		}).Should(Equal(targetID))
	}

	for i := range len(servers) {
		Eventually(func(g Gomega) uint64 {
			state, err := servers[i].ClusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{NodeId: servers[i].NodeID})
			g.Expect(err).To(Succeed())
			return state.GetClusterConfig().GetRotationThreshold()
		}).
			WithTimeout(10 * time.Second).
			WithPolling(500 * time.Millisecond).
			Should(Equal(newThreshold))
	}

	oldLeaderIdx := int(lid - 1)
	newInstruments := append(servers[oldLeaderIdx].Service.Instruments, testserver.WithCacheRotationThreshold(newThreshold))
	testutil.RestartNodeWithInstruments(ctx, servers[oldLeaderIdx], newInstruments)

	Eventually(servers[oldLeaderIdx]).
		WithTimeout(30 * time.Second).
		WithPolling(500 * time.Millisecond).
		Should(HaveALeader(leaderID))
}

func expectThreshold(ctx context.Context, servers []*testutil.ServiceWithClient, expected uint64) {
	for i, srv := range servers {
		state, err := srv.ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: srv.NodeID})
		Expect(err).To(Succeed())
		Expect(state.GetClusterConfig().GetRotationThreshold()).To(Equal(expected), fmt.Sprintf("node %d", i+1))
	}
}

func createTxs(ctx context.Context, client servicepb.BucketServiceClient, ledger string, n int, amount int64) {
	for i := 0; i < n; i++ {
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledger, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(amount), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())
	}
}

func expectVolume(ctx context.Context, client servicepb.BucketServiceClient, ledger string, expectedInput string) {
	Eventually(func(g Gomega) {
		account, err := actions.GetAccount(ctx, client, ledger, "bank")
		g.Expect(err).To(Succeed())
		g.Expect(account.Volumes["USD"].Input).To(Equal(expectedInput))
	}).WithTimeout(30 * time.Second).WithPolling(500 * time.Millisecond).Should(Succeed())
}

// expectVolumeAllNodes verifies volume from all nodes.
func expectVolumeAllNodes(ctx context.Context, servers []*testutil.ServiceWithClient, ledger string, expectedInput string) {
	for i, srv := range servers {
		Eventually(func(g Gomega) {
			account, err := actions.GetAccount(ctx, srv.Client, ledger, "bank")
			g.Expect(err).To(Succeed())
			g.Expect(account.Volumes["USD"].Input).To(Equal(expectedInput))
		}).
			WithTimeout(30 * time.Second).
			WithPolling(500 * time.Millisecond).
			Should(Succeed(), fmt.Sprintf("node %d should have bank=%s", i+1, expectedInput))
	}
}

var _ = Describe("Rolling cluster config update", Ordered, func() {
	const countInstances = 3

	var (
		ctx      context.Context
		servers  []*testutil.ServiceWithClient
		leaderID *uint64
	)

	BeforeAll(func() {
		ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(
			countInstances, testutil.TestRaftBasePort, testutil.TestServiceBasePort, testutil.TestHTTPBasePort, testutil.TestGatewayBasePort,
		)
	})

	AfterAll(func() { testutil.StopServers(ctx, servers) })

	It("should start with default threshold", func() {
		expectThreshold(ctx, servers, 1000)
	})

	// --- 1st rolling update: 1000 → 10 (even generation skip, small K for rotations) ---

	It("should decrease threshold, trigger rotations, and preserve volumes", func() {
		client := servers[*leaderID-1].Client

		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.CreateLedgerAction("test", nil)},
		})
		Expect(err).To(Succeed())

		createTxs(ctx, client, "test", 5, 100)
		expectVolume(ctx, client, "test", "500")

		rollingUpdateThreshold(ctx, servers, leaderID, 10)

		// Volumes survive cache reset on all nodes
		expectVolumeAllNodes(ctx, servers, "test", "500")

		// 30 tx trigger ~3 rotations with K=10
		createTxs(ctx, servers[*leaderID-1].Client, "test", 30, 100)
		expectVolume(ctx, servers[*leaderID-1].Client, "test", "3500")
	})

	// --- 2nd rolling update: 10 → 20 (cache epoch must be persisted correctly) ---

	It("should handle a second rolling update and preserve volumes", func() {
		rollingUpdateThreshold(ctx, servers, leaderID, 20)

		// All nodes must agree on the volume after 2 rolling updates
		expectVolumeAllNodes(ctx, servers, "test", "3500")

		// 50 tx trigger rotations with K=20
		createTxs(ctx, servers[*leaderID-1].Client, "test", 50, 100)
		expectVolumeAllNodes(ctx, servers, "test", "8500")
	})

	// --- 3rd rolling update: 20 → 15 (another decrease, verifies epoch keeps incrementing) ---

	It("should handle a third rolling update", func() {
		rollingUpdateThreshold(ctx, servers, leaderID, 15)

		expectVolumeAllNodes(ctx, servers, "test", "8500")

		createTxs(ctx, servers[*leaderID-1].Client, "test", 20, 100)
		expectVolumeAllNodes(ctx, servers, "test", "10500")
	})

	// --- No-op: 15 → 15 (no cache reset, no epoch bump) ---

	It("should be a no-op when threshold unchanged", func() {
		rollingUpdateThreshold(ctx, servers, leaderID, 15)

		// No cache reset expected — volumes intact, operations immediate
		expectVolumeAllNodes(ctx, servers, "test", "10500")

		createTxs(ctx, servers[*leaderID-1].Client, "test", 5, 100)
		expectVolumeAllNodes(ctx, servers, "test", "11000")
	})

	// --- New ledger after multiple config changes ---

	It("should handle new ledgers after multiple config changes", func() {
		client := servers[*leaderID-1].Client
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.CreateLedgerAction("post-change", nil)},
		})
		Expect(err).To(Succeed())

		createTxs(ctx, client, "post-change", 20, 50)
		expectVolume(ctx, client, "post-change", "1000")

		// Original ledger untouched
		expectVolume(ctx, client, "test", "11000")
	})

})
