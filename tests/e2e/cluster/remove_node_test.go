//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
)

// waitForNodeRemoved polls the cluster state on the leader until the given node is no longer present.
func waitForNodeRemoved(clusterClient clusterpb.ClusterServiceClient, leaderID uint64, removedNodeID uint32) {
	Eventually(func(g Gomega) {
		state, err := clusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{
			NodeId: uint32(leaderID),
		})
		g.Expect(err).To(Succeed())

		for _, nodeInfo := range state.Nodes {
			g.Expect(nodeInfo.Id).NotTo(Equal(removedNodeID),
				fmt.Sprintf("Node %d should no longer appear in cluster state", removedNodeID))
		}
	}).Should(Succeed())
}

var _ = Describe("Remove node", func() {
	const countInstances = 3

	Context("When removing a follower voter via RemoveNode RPC", Ordered, func() {
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

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("should remove a follower from the cluster", func() {
			lid := *leaderID
			// Pick a follower to remove
			followerID := uint64(((lid) % countInstances) + 1)

			_, err := servers[lid-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
				NodeId: followerID,
			})
			Expect(err).To(Succeed())

			waitForNodeRemoved(servers[lid-1].ClusterClient, lid, uint32(followerID))
		})

		It("should have 2 nodes after removal", func() {
			lid := *leaderID
			state, err := servers[lid-1].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
				NodeId: uint32(lid),
			})
			Expect(err).To(Succeed())
			Expect(state.Nodes).To(HaveLen(2), "should have 2 nodes after removal")
		})

		It("should continue to accept transactions after removal", func() {
			lid := *leaderID
			ledgerName := "remove-voter-test"

			_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			for i := range 3 {
				_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", fmt.Sprintf("user:%d", i), big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}
		})
	})

	Context("When removing a learner via RemoveNode RPC", Ordered, func() {
		var (
			ctx      context.Context
			servers  []*testutil.ServiceWithClient
			leaderID *uint64
		)

		BeforeAll(func() {
			ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(
				countInstances, testutil.TestRaftBasePort, testutil.TestServiceBasePort, testutil.TestHTTPBasePort, testutil.TestGatewayBasePort,
			)

			// Add a learner node (phantom, not a real process)
			_, err := servers[*leaderID-1].ClusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId:         4,
				RaftAddress:    "127.0.0.1:19000",
				ServiceAddress: "127.0.0.1:19100",
			})
			Expect(err).To(Succeed())

			waitForLearner(servers[*leaderID-1].ClusterClient, *leaderID, 4)
		})

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("should remove the learner from the cluster", func() {
			lid := *leaderID
			_, err := servers[lid-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
				NodeId: 4,
			})
			Expect(err).To(Succeed())

			waitForNodeRemoved(servers[lid-1].ClusterClient, lid, 4)
		})

		It("should have 3 voters and 0 learners after learner removal", func() {
			lid := *leaderID
			state, err := servers[lid-1].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
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
			Expect(learnerCount).To(Equal(0), "should have 0 learners")
		})
	})

	Context("When force-removing a stopped follower", Ordered, func() {
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

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("should force-remove a stopped follower from the cluster", func() {
			lid := *leaderID
			// Pick a follower to stop and force-remove
			followerIdx := int(((lid) % countInstances))
			followerID := uint64(servers[followerIdx].NodeID)

			// Stop the follower
			testutil.StopNode(ctx, servers[followerIdx])

			// Force-remove the stopped follower via the leader
			_, err := servers[lid-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
				NodeId: followerID,
				Force:  true,
			})
			Expect(err).To(Succeed())

			waitForNodeRemoved(servers[lid-1].ClusterClient, lid, uint32(followerID))
		})

		It("should continue to accept transactions with remaining 2 nodes", func() {
			lid := *leaderID
			ledgerName := "force-remove-test"

			_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			for i := range 3 {
				_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", fmt.Sprintf("user:%d", i), big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}
		})
	})

	Context("When force-removing causes quorum restoration", Ordered, func() {
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

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("should restore quorum by force-removing both stopped followers", func() {
			lid := *leaderID

			// Stop both followers
			var followerIDs []uint64
			for _, srv := range servers {
				if uint64(srv.NodeID) != lid {
					followerIDs = append(followerIDs, uint64(srv.NodeID))
					testutil.StopNode(ctx, srv)
				}
			}
			Expect(followerIDs).To(HaveLen(2))

			// Force-remove both stopped followers
			for _, fid := range followerIDs {
				_, err := servers[lid-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
					NodeId: fid,
					Force:  true,
				})
				Expect(err).To(Succeed())
			}

			// Verify leader can operate as single-node cluster
			Eventually(func(g Gomega) {
				state, err := servers[lid-1].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
					NodeId: uint32(lid),
				})
				g.Expect(err).To(Succeed())
				g.Expect(state.Nodes).To(HaveLen(1))
			}).Should(Succeed())
		})

		It("should accept writes as a single-node cluster", func() {
			lid := *leaderID
			ledgerName := "quorum-restore-test"

			_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(500), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})
	})

	Context("ForceRemoveNode edge cases", Ordered, func() {
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

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("should reject force-removing the leader itself", func() {
			lid := *leaderID
			_, err := servers[lid-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
				NodeId: lid,
				Force:  true,
			})
			Expect(err).To(HaveOccurred())
		})

		It("should reject force-removing a non-existent node", func() {
			lid := *leaderID
			_, err := servers[lid-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
				NodeId: 99,
				Force:  true,
			})
			Expect(err).To(HaveOccurred())
		})

		It("should reject force-removing with zero node ID", func() {
			lid := *leaderID
			_, err := servers[lid-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
				NodeId: 0,
				Force:  true,
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Context("RemoveNode edge cases", Ordered, func() {
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

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("should reject removing the leader itself", func() {
			lid := *leaderID
			_, err := servers[lid-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
				NodeId: lid,
			})
			Expect(err).To(HaveOccurred())
		})

		It("should reject removing a node that is not in the cluster", func() {
			lid := *leaderID
			_, err := servers[lid-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
				NodeId: 99,
			})
			Expect(err).To(HaveOccurred())
		})

		It("should reject removing with zero node ID", func() {
			lid := *leaderID
			_, err := servers[lid-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
				NodeId: 0,
			})
			Expect(err).To(HaveOccurred())
		})

		It("should forward remove-node from follower to leader", func() {
			lid := *leaderID

			// Add a learner to remove
			_, err := servers[lid-1].ClusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId:         6,
				RaftAddress:    "127.0.0.1:19002",
				ServiceAddress: "127.0.0.1:19102",
			})
			Expect(err).To(Succeed())
			waitForLearner(servers[lid-1].ClusterClient, lid, 6)

			// Send remove request via a follower
			followerID := ((lid) % countInstances) + 1
			_, err = servers[followerID-1].ClusterClient.RemoveNode(ctx, &clusterpb.RemoveNodeRequest{
				NodeId: 6,
			})
			Expect(err).To(Succeed())

			waitForNodeRemoved(servers[lid-1].ClusterClient, lid, 6)
		})
	})
})
