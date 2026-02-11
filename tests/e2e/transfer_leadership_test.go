//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	cmdserver "github.com/formancehq/ledger-v3-poc/cmd/server"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Leadership transfer", func() {
	var (
		ctx      context.Context
		servers  []*serviceWithClient
		gateway  *testserver.Gateway
		leaderID uint64
	)
	const (
		countInstances      = 3
		gatewayBasePort     = 6300 // Different from raft_test.go to avoid port conflicts
		nodeRaftBasePort    = 6100
		nodeServiceBasePort = 8100
		nodeHTTPBasePort    = 9100
	)

	BeforeEach(func() {
		ctx = logging.TestingContext()

		gatewayPorts := make([]int, countInstances)
		nodeRaftAddresses := make([]string, countInstances)
		for i := range countInstances {
			gatewayPorts[i] = gatewayBasePort + i
			nodeRaftAddresses[i] = fmt.Sprintf("127.0.0.1:%d", nodeRaftBasePort+i)
		}

		var err error
		gateway, err = testserver.NewGateway(logging.FromContext(ctx), gatewayPorts, nodeRaftAddresses)
		Expect(err).To(Succeed())

		Expect(gateway.Start(ctx)).To(Succeed())
		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(gateway.Stop(stopCtx)).To(Succeed())
		})

		servers = make([]*serviceWithClient, 0, countInstances)
		for i := range countInstances {
			walTmpDir := GinkgoT().TempDir()
			dataTmpDir := GinkgoT().TempDir()
			DeferCleanup(func() {
				Expect(os.RemoveAll(walTmpDir)).To(Succeed())
				Expect(os.RemoveAll(dataTmpDir)).To(Succeed())
			})

			server := testservice.New(cmdserver.NewRunCommand,
				testservice.WithInstruments(
					testservice.DebugInstrumentation(debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					testserver.WithNodeID(i+1),
					testserver.WithHTTPPort(nodeHTTPBasePort+i),
					testserver.WithWalDir(walTmpDir),
					testserver.WithDataDir(dataTmpDir),
					testserver.WithRaftPort(nodeRaftBasePort+i),
					testserver.WithGRPCPort(nodeServiceBasePort+i),
					testserver.WithSnapshotThreshold(10),
					testserver.WithRaftCompactionMargin(1),
					testserver.WithDebug(os.Getenv("DEBUG") == "true"),
					testserver.WithRaftTickInterval(100*time.Millisecond),
					testserver.WithRaftHeartbeatTick(1),
					testserver.WithRaftElectionTick(20),
					testserver.WithPeers(func() []node.Peer {
						ret := make([]node.Peer, 0, countInstances-1)
						for j := range countInstances {
							if i == j {
								continue
							}
							ret = append(ret, node.Peer{
								ID:             uint64(j + 1),
								Address:        fmt.Sprintf("127.0.0.1:%d", gatewayBasePort+j),
								ServiceAddress: fmt.Sprintf("127.0.0.1:%d", nodeServiceBasePort+j),
							})
						}
						return ret
					}()...),
				),
			)
			Expect(server.Start(ctx)).To(Succeed())

			grpcClient, clusterClient, grpcConn, err := newGRPCClient(nodeServiceBasePort + i)
			Expect(err).To(Succeed())
			DeferCleanup(func() {
				_ = grpcConn.Close()
			})

			servers = append(servers, &serviceWithClient{
				service:       server,
				client:        grpcClient,
				clusterClient: clusterClient,
				grpcConn:      grpcConn,
				walDir:        walTmpDir,
				dataDir:       dataTmpDir,
				grpcPort:      nodeServiceBasePort + i,
				nodeID:        uint32(i + 1),
			})
		}
		Eventually(servers[0]).To(HaveALeader(&leaderID))
	})

	AfterEach(func() {
		for i, server := range servers {
			By(fmt.Sprintf("Stopping node %d", i+1), func() {
				stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				DeferCleanup(cancel)
				Expect(server.service.Stop(stopCtx)).To(Succeed())
			})
		}
	})

	It("should transfer leadership to a follower", func() {
		// Pick a follower as the transfer target
		targetID := ((leaderID) % countInstances) + 1
		GinkgoLogr.Info(fmt.Sprintf("Transferring leadership from node %d to node %d", leaderID, targetID))

		resp, err := servers[leaderID-1].clusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: uint32(targetID),
		})
		Expect(err).To(Succeed())
		Expect(resp.NewLeader).To(Equal(uint32(targetID)))

		// Verify all nodes see the new leader
		for i := range countInstances {
			Eventually(func(g Gomega) uint64 {
				state, err := servers[i].clusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{
					NodeId: servers[i].nodeID,
				})
				g.Expect(err).To(Succeed())
				return uint64(state.Leader)
			}).Should(Equal(targetID), fmt.Sprintf("node %d should see node %d as leader", i+1, targetID))
		}
	})

	It("should transfer leadership when requested from a follower", func() {
		// Pick a follower to send the request FROM
		followerID := ((leaderID) % countInstances) + 1
		// Pick a different follower as the transfer TARGET
		targetID := ((leaderID + 1) % countInstances) + 1

		GinkgoLogr.Info(fmt.Sprintf("Requesting transfer via follower %d, target node %d (leader is %d)", followerID, targetID, leaderID))

		resp, err := servers[followerID-1].clusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: uint32(targetID),
		})
		Expect(err).To(Succeed())
		Expect(resp.NewLeader).To(Equal(uint32(targetID)))
	})

	It("should continue operating after leadership transfer", func() {
		// Create a ledger before transfer
		_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{createLedgerAction("transfer-test", nil)},
		})
		Expect(err).To(Succeed())

		// Transfer leadership
		targetID := ((leaderID) % countInstances) + 1
		_, err = servers[leaderID-1].clusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: uint32(targetID),
		})
		Expect(err).To(Succeed())

		// Wait for all nodes to see new leader
		for i := range countInstances {
			Eventually(func(g Gomega) uint64 {
				state, err := servers[i].clusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{
					NodeId: servers[i].nodeID,
				})
				g.Expect(err).To(Succeed())
				return uint64(state.Leader)
			}).Should(Equal(targetID))
		}

		// Create transactions through the new leader
		for i := 0; i < 3; i++ {
			_, err := servers[targetID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction("transfer-test", []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		}

		// Verify data is accessible from the old leader (now follower)
		ledger, err := servers[leaderID-1].client.GetLedger(ctx, &servicepb.GetLedgerRequest{
			Ledger: "transfer-test",
		})
		Expect(err).To(Succeed())
		Expect(ledger.Name).To(Equal("transfer-test"))
	})

	It("should reject transfer to an unknown node", func() {
		_, err := servers[leaderID-1].clusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: 99, // Non-existent node
		})
		Expect(err).To(HaveOccurred())
	})

	It("should reject transfer with zero node ID", func() {
		_, err := servers[leaderID-1].clusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
			Transferee: 0,
		})
		Expect(err).To(HaveOccurred())
	})

	It("should transfer leadership automatically when leader stops gracefully", func() {
		// Create a ledger and some transactions so the cluster is active
		_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{createLedgerAction("auto-transfer-test", nil)},
		})
		Expect(err).To(Succeed())

		for i := 0; i < 3; i++ {
			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction("auto-transfer-test", []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		}

		// Stop only the leader gracefully — this should trigger automatic leadership transfer
		oldLeaderID := leaderID
		GinkgoLogr.Info(fmt.Sprintf("Stopping leader node %d gracefully", oldLeaderID))

		stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		Expect(servers[oldLeaderID-1].service.Stop(stopCtx)).To(Succeed())

		// Pick a follower to check for new leader
		followerIdx := int(oldLeaderID) % countInstances // 0-indexed follower
		var newLeaderID uint64
		Eventually(func(g Gomega) bool {
			state, err := servers[followerIdx].clusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{
				NodeId: servers[followerIdx].nodeID,
			})
			g.Expect(err).To(Succeed())
			newLeaderID = uint64(state.Leader)
			return newLeaderID != 0 && newLeaderID != oldLeaderID
		}).Should(BeTrue(), "a new leader should be elected after the old leader stops")

		// Verify the cluster continues to function: create transactions via the new leader
		for i := 0; i < 3; i++ {
			_, err := servers[newLeaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction("auto-transfer-test", []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		}
	})
})
