//go:build e2e && s3

package cluster

import (
	"context"
	"slices"
	"time"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// A restored cluster retains active capability policy, but its cluster-local
// membership evidence is discarded. Wait for the new leader to commit its own
// support before testing capability-dependent writes against restored data.
func waitForRestoredCapabilityEvidence(ctx context.Context, clusterID string, clusterClient clusterpb.ClusterServiceClient) {
	var leaderAddress string
	var leaderID uint64
	Eventually(func(g Gomega) {
		state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		g.Expect(err).To(Succeed())
		g.Expect(state.GetLeader()).NotTo(BeZero())
		leaderID = state.GetLeader()
		for _, peer := range state.GetNodes() {
			if peer.GetId() == state.GetLeader() {
				leaderAddress = peer.GetRaftAddress()
				return
			}
		}
		g.Expect(false).To(BeTrue(), "leader must have a Raft address")
	}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

	conn, err := grpc.NewClient(leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).To(Succeed())
	defer func() { Expect(conn.Close()).To(Succeed()) }()
	bootstrapClient := clusterbootstrappb.NewClusterBootstrapServiceClient(conn)
	bootstrapCtx := metadata.AppendToOutgoingContext(ctx, node.MetadataKeyClusterID, clusterID)
	Eventually(func(g Gomega) {
		peers, err := bootstrapClient.GetPeers(bootstrapCtx, &clusterbootstrappb.GetPeersRequest{})
		g.Expect(err).To(Succeed())
		for _, peer := range peers.GetPeers() {
			if peer.GetId() == leaderID {
				g.Expect(slices.Contains(peer.GetCapabilities(), uint32(capabilities.StrictTransactionAccountMetadata))).To(BeTrue(), "restored leader must commit capability 1 evidence")
				return
			}
		}
		g.Expect(false).To(BeTrue(), "restored leader must appear in membership")
	}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())
}
