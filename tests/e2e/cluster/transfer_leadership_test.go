//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

var _ = Describe("Leadership transfer", Ordered, func() {
	var (
		ctx      context.Context
		servers  []*testutil.ServiceWithClient
		leaderID *uint64
	)
	const countInstances = 3

	BeforeAll(func() {
		ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(
			countInstances, testutil.TestRaftBasePort, testutil.TestServiceBasePort, testutil.TestHTTPBasePort, testutil.TestGatewayBasePort,
			testutil.WithGateway(),
			testutil.WithTickInterval(50*time.Millisecond),
		)
	})

	AfterAll(func() {
		testutil.StopServers(ctx, servers)
	})

	It("should transfer leadership to a follower", func() {
		lid := *leaderID
		targetID := (lid % countInstances) + 1
		GinkgoLogr.Info(fmt.Sprintf("Transferring leadership from node %d to node %d", lid, targetID))

		resp, err := servers[lid-1].ClusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: uint32(targetID),
		})
		Expect(err).To(Succeed())
		Expect(resp.NewLeader).To(Equal(uint32(targetID)))

		// Verify all nodes see the new leader
		for i := range countInstances {
			Eventually(func(g Gomega) uint64 {
				state, err := servers[i].ClusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{
					NodeId: servers[i].NodeID,
				})
				g.Expect(err).To(Succeed())
				return uint64(state.Leader)
			}).Should(Equal(targetID), fmt.Sprintf("node %d should see node %d as leader", i+1, targetID))
		}

		// Update leaderID for subsequent tests
		*leaderID = targetID
	})

	It("should transfer leadership when requested from a follower", func() {
		lid := *leaderID
		followerID := (lid % countInstances) + 1
		targetID := ((lid + 1) % countInstances) + 1

		GinkgoLogr.Info(fmt.Sprintf("Requesting transfer via follower %d, target node %d (leader is %d)", followerID, targetID, lid))

		resp, err := servers[followerID-1].ClusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: uint32(targetID),
		})
		Expect(err).To(Succeed())
		Expect(resp.NewLeader).To(Equal(uint32(targetID)))

		*leaderID = targetID
	})

	It("should continue operating after leadership transfer", func() {
		lid := *leaderID

		// Create a ledger before transfer
		_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction("transfer-test", nil)),
		})
		Expect(err).To(Succeed())

		// Transfer leadership
		targetID := (lid % countInstances) + 1
		_, err = servers[lid-1].ClusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: uint32(targetID),
		})
		Expect(err).To(Succeed())

		// Wait for all nodes to see new leader
		for i := range countInstances {
			Eventually(func(g Gomega) uint64 {
				state, err := servers[i].ClusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{
					NodeId: servers[i].NodeID,
				})
				g.Expect(err).To(Succeed())
				return uint64(state.Leader)
			}).Should(Equal(targetID))
		}

		// Create transactions through the new leader
		for i := 0; i < 3; i++ {
			_, err := servers[targetID-1].Client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction("transfer-test", []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
		}

		// Verify data is accessible from the old leader (now follower)
		ledger, err := servers[lid-1].Client.GetLedger(ctx, &servicepb.GetLedgerRequest{
			Ledger: "transfer-test",
		})
		Expect(err).To(Succeed())
		Expect(ledger.Name).To(Equal("transfer-test"))

		*leaderID = targetID
	})

	It("should reject transfer to an unknown node", func() {
		lid := *leaderID
		_, err := servers[lid-1].ClusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: 99,
		})
		Expect(err).To(HaveOccurred())
	})

	It("should reject transfer with zero node ID", func() {
		lid := *leaderID
		_, err := servers[lid-1].ClusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: 0,
		})
		Expect(err).To(HaveOccurred())
	})

	// This test MUST be last as it stops a node
	It("should transfer leadership automatically when leader stops gracefully", func() {
		lid := *leaderID

		// Create a ledger and some transactions so the cluster is active
		_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction("auto-transfer-test", nil)),
		})
		Expect(err).To(Succeed())

		for i := 0; i < 3; i++ {
			_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction("auto-transfer-test", []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
		}

		// Stop only the leader gracefully — this should trigger automatic leadership transfer
		oldLeaderID := lid
		GinkgoLogr.Info(fmt.Sprintf("Stopping leader node %d gracefully", oldLeaderID))

		stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		Expect(servers[oldLeaderID-1].Service.Stop(stopCtx)).To(Succeed())

		// Pick a follower to check for new leader
		followerIdx := int(oldLeaderID) % countInstances
		var newLeaderID uint64
		Eventually(func(g Gomega) bool {
			state, err := servers[followerIdx].ClusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{
				NodeId: servers[followerIdx].NodeID,
			})
			g.Expect(err).To(Succeed())
			newLeaderID = uint64(state.Leader)
			return newLeaderID != 0 && newLeaderID != oldLeaderID
		}).Should(BeTrue(), "a new leader should be elected after the old leader stops")

		// Verify the cluster continues to function: create transactions via the new leader
		for i := 0; i < 3; i++ {
			_, err := servers[newLeaderID-1].Client.Apply(ctx, &servicepb.ApplyRequest{
				Envelopes: servicepb.UnsignedEnvelopes(
					actions.CreateTransactionAction("auto-transfer-test", []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
					}, nil, nil),
				),
			})
			Expect(err).To(Succeed())
		}
	})
})
