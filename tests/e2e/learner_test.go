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
	var (
		ctx      context.Context
		servers  []*serviceWithClient
		leaderID uint64
	)
	const (
		countInstances      = 3
		nodeRaftBasePort    = 6400
		nodeServiceBasePort = 8400
		nodeHTTPBasePort    = 9400
	)

	BeforeEach(func() {
		ctx = logging.TestingContext()

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
					testserver.WithRaftTickInterval(10*time.Millisecond),
					testserver.WithRaftHeartbeatTick(1),
					testserver.WithRaftElectionTick(10),
					testserver.WithPeers(func() []node.Peer {
						ret := make([]node.Peer, 0, countInstances-1)
						for j := range countInstances {
							if i == j {
								continue
							}
							ret = append(ret, node.Peer{
								ID:             uint64(j + 1),
								Address:        fmt.Sprintf("127.0.0.1:%d", nodeRaftBasePort+j),
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

	Context("When adding a learner via AddLearner RPC", func() {
		BeforeEach(func() {
			_, err := servers[leaderID-1].clusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId:         4,
				RaftAddress:    "127.0.0.1:16400",
				ServiceAddress: "127.0.0.1:18400",
			})
			Expect(err).To(Succeed())

			// Wait for the ConfChange to be committed so all subsequent tests see the learner
			waitForLearner(servers[leaderID-1].clusterClient, leaderID, 4)
		})

		It("should show the learner in cluster status", func() {
			state, err := servers[leaderID-1].clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
				NodeId: uint32(leaderID),
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
			state, err := servers[leaderID-1].clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
				NodeId: uint32(leaderID),
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
			_, err := servers[leaderID-1].clusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
				Transferee: 4,
			})
			Expect(err).To(HaveOccurred())
		})

		It("should continue to accept transactions with a learner in configuration", func() {
			ledgerName := "learner-test-ledger"

			// Create a ledger
			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions through the leader
			for i := range 5 {
				_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("user-%d", i), big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Verify a follower can read the transactions (cluster is healthy)
			followerID := ((leaderID + 1) % countInstances) + 1
			Eventually(func(g Gomega) {
				ledgers, err := getAllLedgersInfo(ctx, servers[followerID-1].client)
				g.Expect(err).To(Succeed())
				_, found := ledgers[ledgerName]
				g.Expect(found).To(BeTrue())
			}).Should(Succeed())
		})

		It("should allow leadership transfer to a voter", func() {
			// Pick a follower (voter) as transferee
			followerID := ((leaderID + 1) % countInstances) + 1

			Eventually(func() error {
				_, err := servers[leaderID-1].clusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
					Transferee: uint32(followerID),
				})
				return err
			}, 10*time.Second).Should(Succeed())

			// Verify the new leader
			Eventually(servers[followerID-1]).Should(HaveALeader(nil))
		})
	})

	Context("When adding a learner that already exists", func() {
		BeforeEach(func() {
			_, err := servers[leaderID-1].clusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId:         4,
				RaftAddress:    "127.0.0.1:16400",
				ServiceAddress: "127.0.0.1:18400",
			})
			Expect(err).To(Succeed())

			// Wait for the ConfChange to be committed before trying the duplicate
			waitForLearner(servers[leaderID-1].clusterClient, leaderID, 4)
		})

		It("should reject duplicate add-learner", func() {
			_, err := servers[leaderID-1].clusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId:         4,
				RaftAddress:    "127.0.0.1:16400",
				ServiceAddress: "127.0.0.1:18400",
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When adding a learner via a follower (forwarding)", func() {
		It("should forward the request to the leader and succeed", func() {
			// Pick a follower
			followerID := ((leaderID + 1) % countInstances) + 1

			_, err := servers[followerID-1].clusterClient.AddLearner(ctx, &clusterpb.AddLearnerRequest{
				NodeId:         5,
				RaftAddress:    "127.0.0.1:16500",
				ServiceAddress: "127.0.0.1:18500",
			})
			Expect(err).To(Succeed())

			// Verify the learner appears in cluster state
			waitForLearner(servers[leaderID-1].clusterClient, leaderID, 5)
		})
	})
})
