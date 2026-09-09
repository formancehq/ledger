//go:build e2e

package cluster

import (
	"context"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

var _ = DescribeTable("Rejected learner registration routing preserves active peer routing and identity",
	func(administrative bool) {
		ctx, servers, _, leaderID := testutil.SetupMultiNodeCluster(3)
		lid := *leaderID
		targetID := lid%3 + 1
		leader := servers[lid-1]

		var original *clusterpb.NodeInfo
		var leaderRaftAddress string
		Eventually(func(g Gomega) {
			state, err := leader.ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: uint32(lid)})
			g.Expect(err).To(Succeed())
			g.Expect(state.GetLeader()).To(Equal(uint32(lid)))
			original = nil
			for _, peer := range state.GetNodes() {
				if peer.GetId() == uint32(lid) {
					leaderRaftAddress = peer.GetRaftAddress()
				}
				if peer.GetId() == uint32(targetID) {
					original = peer
				}
			}
			g.Expect(original).NotTo(BeNil())
			g.Expect(original.GetProgress().GetMatch()).To(BeNumerically(">", 0), "reach stale-progress admission, not zero-progress refresh")
			g.Expect(original.GetServiceAddress()).NotTo(BeEmpty())
			g.Expect(leaderRaftAddress).NotTo(BeEmpty())
		}).Within(10 * time.Second).Should(Succeed())

		bootstrapConn, err := grpc.NewClient(leaderRaftAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).To(Succeed())
		DeferCleanup(func() { Expect(bootstrapConn.Close()).To(Succeed()) })
		bootstrapClient := clusterbootstrappb.NewClusterBootstrapServiceClient(bootstrapConn)
		bootstrapCtx := metadata.AppendToOutgoingContext(ctx, node.MetadataKeyClusterID, "test-cluster")
		peers, err := bootstrapClient.GetPeers(bootstrapCtx, &clusterbootstrappb.GetPeersRequest{})
		Expect(err).To(Succeed())
		var originalPeer *clusterbootstrappb.PeerInfo
		for _, peer := range peers.GetPeers() {
			if peer.GetId() == targetID {
				originalPeer = peer
			}
		}
		Expect(originalPeer).NotTo(BeNil())
		Expect(originalPeer.GetInstanceId()).To(HaveLen(16))
		newIdentity := slices.Clone(originalPeer.GetInstanceId())
		newIdentity[0] ^= 0xff
		deadRaftAddress := testserver.AllocateDeadAddress()
		deadServiceAddress := testserver.AllocateDeadAddress()

		if administrative {
			By("rejecting administrative AddLearner with an uncommitted new incarnation")
			_, err = leader.ClusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId: targetID, RaftAddress: deadRaftAddress,
				ServiceAddress: deadServiceAddress, InstanceId: newIdentity,
			})
		} else {
			By("rejecting boot JoinAsLearner with the same stale replication progress")
			_, err = bootstrapClient.JoinAsLearner(bootstrapCtx, &clusterbootstrappb.JoinAsLearnerRequest{
				NodeId: targetID, RaftAddress: deadRaftAddress,
				ServiceAddress: deadServiceAddress, InstanceId: newIdentity,
			})
		}
		Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))
		if administrative {
			Expect(err.Error()).To(ContainSubstring("stale raft progress"))
		} else {
			Expect(err.Error()).To(ContainSubstring("local WAL cannot satisfy the leader's known match index"))
		}

		// GetClusterState reads addresses from the actual transport pools;
		// GetPeers independently reads the committed membership cache. A
		// cached identity alone cannot detect pre-admission pool rewiring.
		state, err := leader.ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: uint32(lid)})
		Expect(err).To(Succeed())
		found := false
		for _, peer := range state.GetNodes() {
			if peer.GetId() == uint32(targetID) {
				Expect(peer.GetServiceAddress()).To(Equal(original.GetServiceAddress()))
				Expect(peer.GetRaftAddress()).To(Equal(original.GetRaftAddress()))
				found = true
			}
		}
		Expect(found).To(BeTrue())
		peers, err = bootstrapClient.GetPeers(bootstrapCtx, &clusterbootstrappb.GetPeersRequest{})
		Expect(err).To(Succeed())
		found = false
		for _, peer := range peers.GetPeers() {
			if peer.GetId() == targetID {
				Expect(peer.GetInstanceId()).To(Equal(originalPeer.GetInstanceId()))
				Expect(peer.GetRaftAddress()).To(Equal(originalPeer.GetRaftAddress()))
				Expect(peer.GetServiceAddress()).To(Equal(originalPeer.GetServiceAddress()))
				found = true
			}
		}
		Expect(found).To(BeTrue())

		// Exercise the retained service connection, routing via the leader
		// to the specific follower rather than using a direct client.
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		forwarded, err := leader.ClusterClient.GetClusterState(probeCtx, &clusterpb.GetClusterStateRequest{NodeId: uint32(targetID)})
		cancel()
		Expect(err).To(Succeed())
		Expect(forwarded.GetLocalNode()).To(Equal(uint32(targetID)))
	},
	Entry("after administrative AddLearner stale-progress rejection", true),
	Entry("after boot JoinAsLearner stale-progress rejection", false),
)
