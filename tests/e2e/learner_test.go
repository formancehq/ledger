//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// waitForLearner polls the cluster state on the leader until the given node appears as a learner.
func waitForLearner(clusterClient clusterpb.ClusterServiceClient, leaderID uint64, learnerNodeID uint32) {
	Eventually(func(g Gomega) {
		state, err := clusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{
			NodeId: uint32(leaderID),
		})
		g.Expect(err).To(Succeed())

		var found bool
		for _, nodeInfo := range state.Nodes {
			if nodeInfo.Id == learnerNodeID {
				g.Expect(nodeInfo.Suffrage).To(Equal("Learner"))
				found = true
			}
		}
		g.Expect(found).To(BeTrue(), fmt.Sprintf("Node %d should appear as a learner", learnerNodeID))
	}).Should(Succeed())
}

var _ = Describe("Learner node", func() {
	const (
		countInstances      = 3
		nodeRaftBasePort    = 6400
		nodeServiceBasePort = 8400
		nodeHTTPBasePort    = 9400
		gatewayBasePort     = 6500
	)

	Context("When adding a learner via AddLearner RPC", Ordered, func() {
		var (
			ctx      context.Context
			servers  []*serviceWithClient
			leaderID *uint64
		)

		BeforeAll(func() {
			ctx, servers, _, leaderID = setupMultiNodeCluster(
				countInstances, nodeRaftBasePort, nodeServiceBasePort, nodeHTTPBasePort, gatewayBasePort,
			)

			_, err := servers[*leaderID-1].clusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId:         4,
				RaftAddress:    "127.0.0.1:16400",
				ServiceAddress: "127.0.0.1:18400",
			})
			Expect(err).To(Succeed())

			waitForLearner(servers[*leaderID-1].clusterClient, *leaderID, 4)
		})

		AfterAll(func() {
			stopServers(ctx, servers)
		})

		It("should show the learner in cluster status", func() {
			lid := *leaderID
			state, err := servers[lid-1].clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
				NodeId: uint32(lid),
			})
			Expect(err).To(Succeed())

			var learnerFound bool
			for _, nodeInfo := range state.Nodes {
				if nodeInfo.Id == 4 {
					Expect(nodeInfo.Suffrage).To(Equal("Learner"))
					learnerFound = true
				}
			}
			Expect(learnerFound).To(BeTrue(), "Node 4 should appear as a learner in cluster state")
		})

		It("should have 4 nodes total (3 voters + 1 learner)", func() {
			lid := *leaderID
			state, err := servers[lid-1].clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
				NodeId: uint32(lid),
			})
			Expect(err).To(Succeed())

			var (
				voterCount   int
				learnerCount int
			)
			for _, nodeInfo := range state.Nodes {
				switch nodeInfo.Suffrage {
				case "Voter":
					voterCount++
				case "Learner":
					learnerCount++
				}
			}
			Expect(voterCount).To(Equal(3), "should have 3 voters")
			Expect(learnerCount).To(Equal(1), "should have 1 learner")
		})

		It("should reject leadership transfer to the learner", func() {
			lid := *leaderID
			_, err := servers[lid-1].clusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
				Transferee: 4,
			})
			Expect(err).To(HaveOccurred())
		})

		It("should continue to accept transactions with a learner in configuration", func() {
			lid := *leaderID
			ledgerName := "learner-test-ledger"

			_, err := servers[lid-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			for i := range 5 {
				_, err := servers[lid-1].client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("user-%d", i), big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			followerID := ((lid + 1) % countInstances) + 1
			Eventually(func(g Gomega) {
				ledgers, err := getAllLedgersInfo(ctx, servers[followerID-1].client)
				g.Expect(err).To(Succeed())
				_, found := ledgers[ledgerName]
				g.Expect(found).To(BeTrue())
			}).Should(Succeed())
		})

		It("should allow leadership transfer to a voter", func() {
			lid := *leaderID
			followerID := ((lid + 1) % countInstances) + 1

			Eventually(func() error {
				_, err := servers[lid-1].clusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
					Transferee: uint32(followerID),
				})
				return err
			}, "10s").Should(Succeed())

			Eventually(servers[followerID-1]).Should(HaveALeader(nil))
		})
	})

	Context("Learner edge cases", Ordered, func() {
		var (
			ctx      context.Context
			servers  []*serviceWithClient
			leaderID *uint64
		)

		BeforeAll(func() {
			ctx, servers, _, leaderID = setupMultiNodeCluster(
				countInstances, nodeRaftBasePort, nodeServiceBasePort, nodeHTTPBasePort, gatewayBasePort,
			)
		})

		AfterAll(func() {
			stopServers(ctx, servers)
		})

		It("should reject duplicate add-learner", func() {
			lid := *leaderID
			_, err := servers[lid-1].clusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId:         4,
				RaftAddress:    "127.0.0.1:16400",
				ServiceAddress: "127.0.0.1:18400",
			})
			Expect(err).To(Succeed())

			waitForLearner(servers[lid-1].clusterClient, lid, 4)

			_, err = servers[lid-1].clusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId:         4,
				RaftAddress:    "127.0.0.1:16400",
				ServiceAddress: "127.0.0.1:18400",
			})
			Expect(err).To(HaveOccurred())
		})

		It("should forward add-learner from follower to leader", func() {
			lid := *leaderID
			followerID := ((lid + 1) % countInstances) + 1

			_, err := servers[followerID-1].clusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId:         5,
				RaftAddress:    "127.0.0.1:16500",
				ServiceAddress: "127.0.0.1:18500",
			})
			Expect(err).To(Succeed())

			waitForLearner(servers[lid-1].clusterClient, lid, 5)
		})
	})
})
