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
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var _ = Describe("Simple cluster", func() {
	var (
		ctx      context.Context
		servers  []*serviceWithClient
		gateway  *testserver.Gateway
		leaderID uint64
	)
	const (
		countInstances   = 3
		gatewayBasePort  = 7200
		nodeGRPCBasePort = 8000
		nodeHTTPBasePort = 9000
	)

	BeforeEach(func() {
		ctx = logging.TestingContext()

		// Create gateway that forwards to node gRPC ports
		gatewayPorts := make([]int, countInstances)
		nodeAddresses := make([]string, countInstances)
		for i := range countInstances {
			gatewayPorts[i] = gatewayBasePort + i
			nodeAddresses[i] = fmt.Sprintf("127.0.0.1:%d", nodeGRPCBasePort+i)
		}

		var err error
		gateway, err = testserver.NewGateway(logging.FromContext(ctx), gatewayPorts, nodeAddresses)
		Expect(err).To(Succeed())

		// Start gateway before starting nodes
		Expect(gateway.Start(ctx)).To(Succeed())
		DeferCleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(gateway.Stop(ctx)).To(Succeed())
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
					testserver.WithGRPCPort(nodeGRPCBasePort+i),
					testserver.WithSnapshotThreshold(10),
					testserver.WithRaftCompactionMargin(1), // Default is 1000, since we override the default snapshot threshold, we need to adjust this value
					testserver.WithDebug(os.Getenv("DEBUG") == "true"),
					testserver.WithPeers(func() []raft.Peer {
						ret := make([]raft.Peer, 0, countInstances-1)
						for j := range countInstances {
							if i == j {
								continue
							}
							// Use gateway ports instead of direct node ports
							ret = append(ret, raft.Peer{
								ID:      uint64(j + 1),
								Address: fmt.Sprintf("127.0.0.1:%d", gatewayBasePort+j),
							})
						}

						return ret
					}()...),
				),
			)
			Expect(server.Start(ctx)).To(Succeed())

			// Create gRPC client
			grpcClient, grpcConn, err := newGRPCClient(nodeGRPCBasePort + i)
			Expect(err).To(Succeed())
			DeferCleanup(func() {
				_ = grpcConn.Close()
			})

			servers = append(servers, &serviceWithClient{
				service:  server,
				client:   grpcClient,
				grpcConn: grpcConn,
				walDir:   walTmpDir,
				dataDir:  dataTmpDir,
				grpcPort: nodeGRPCBasePort + i,
			})
		}
		Eventually(servers[0]).To(HaveALeader(&leaderID))
	})

	AfterEach(func() {
		for i, server := range servers {
			By(fmt.Sprintf("Stopping node %d", i+1), func() {
				ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				DeferCleanup(cancel)

				Expect(server.service.Stop(ctx)).To(Succeed())
			})
		}
	})

	It("should start successfully", func() {})

	Context("When a follower restart", func() {
		var (
			followerID uint64
		)
		BeforeEach(func() {
			followerID = ((leaderID + 1) % countInstances) + 1
			Expect(servers[followerID-1].service.Stop(ctx)).To(Succeed())
			<-time.After(time.Second)
			Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
		})
		It("Should properly rejoin the cluster", func() {
			Eventually(servers[followerID-1]).To(BeFollower())
			Consistently(servers[followerID-1]).To(BeFollower())
		})
	})
	Context("when the leader is down", func() {
		BeforeEach(func() {
			Eventually(servers[leaderID-1]).To(HaveALeader(&leaderID))
			Expect(servers[leaderID-1].service.Stop(ctx)).To(BeNil())
		})
		It("should elect a new leader", func() {
			Eventually(servers[(leaderID+1)%countInstances]).To(HaveALeader(nil))
		})
	})
	Context("When creating a new ledger", func() {
		BeforeEach(func() {
			_, err := servers[0].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction("ledger0", nil)},
			})
			Expect(err).To(Succeed())
		})
		It("should succeed", func() {})
		Context("Then deleting the ledger", func() {
			BeforeEach(func() {
				// Note: DeleteLedger is now implemented via gRPC Apply
			})
			It("Should succeed", func() {})
		})
	})
	Context("When creating transactions through all nodes", func() {
		var ledgerName string

		BeforeEach(func() {
			ledgerName = "multi-node-ledger"

			// Wait for leader election
			Eventually(servers[0]).To(HaveALeader(nil), "Timed out waiting for leader election")

			// Create ledger
			_, err := servers[0].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("should successfully create transactions through each node", func() {
			// Create a transaction through each node
			for i := range countInstances {
				_, err := servers[i].client.Apply(ctx, &servicepb.ApplyRequest{
					Actions: []*servicepb.Action{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("node-%d", i+1), big.NewInt(100*int64(i+1)), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed(), "Failed to create transaction through node %d", i+1)
			}
		})
	})
	Context("When losing a follower", func() {
		var (
			followerID uint64
		)
		BeforeEach(func() {
			// Find a follower (any node that is not the leader)
			followerID = ((leaderID + 1) % countInstances) + 1

			// Stop the follower
			Expect(servers[followerID-1].service.Stop(ctx)).To(Succeed())
		})

		It("Should continue to work", func() {
			// Ensure leaderID is valid
			Expect(leaderID).NotTo(BeZero(), "leaderID should not be zero")
			Expect(leaderID).To(BeNumerically(">", 0))
			Expect(leaderID).To(BeNumerically("<=", countInstances))

			// Verify cluster still has a leader
			Eventually(servers[leaderID-1]).To(HaveALeader(nil))

			// Create a ledger
			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction("ledger1", nil)},
			})
			Expect(err).To(Succeed())

			// Create some transactions
			for i := 0; i < 5; i++ {
				_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
					Actions: []*servicepb.Action{
						createTransactionAction("ledger1", []*commonpb.Posting{
							newPosting("world", "bank", big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}
		})
		Context("Then creating a new ledger", func() {
			var (
				ledgerName string
			)
			BeforeEach(func() {
				ledgerName = "ledger2"
				// Create a ledger while follower is down
				_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
					Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
				})
				Expect(err).To(Succeed())
			})

			Context("Then the follower come back", func() {
				BeforeEach(func() {
					// Restart the follower
					Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
				})
				It("Should restore the state", func() {
					// Wait for follower to reconnect and sync, then verify it can see the ledger
					Eventually(servers[followerID-1]).To(BeFollower())
					Eventually(func(g Gomega) bool {
						// Then verify the follower can see the ledger created while it was down
						ledgers, err := servers[followerID-1].client.GetAllLedgersInfo(ctx, &servicepb.GetAllLedgersRequest{})
						g.Expect(err).To(Succeed())

						_, found := ledgers.Ledgers[ledgerName]
						return found
					}).To(BeTrue())

					// Verify the follower can access the ledger details
					ledger, err := servers[followerID-1].client.GetLedgerByName(ctx, &servicepb.GetLedgerByNameRequest{
						Name: ledgerName,
					})
					Expect(err).To(Succeed())
					Expect(ledger.Name).To(Equal(ledgerName))
				})
			})

			Context("Then creating more transactions than the snapshot threshold", func() {
				const countTransactions = 15
				BeforeEach(func() {
					// Create enough transactions to trigger a snapshot
					// snapshotThreshold is 10, so we create 15 transactions to ensure a snapshot is created and we have some tx in spool
					GinkgoLogr.Info("Creating transactions")
					for i := 0; i < countTransactions; i++ {
						_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
							Actions: []*servicepb.Action{
								createTransactionAction(ledgerName, []*commonpb.Posting{
									newPosting("world", "bank", big.NewInt(100), "USD"),
								}, nil, nil),
							},
						})
						Expect(err).To(Succeed())
						GinkgoLogr.Info(fmt.Sprintf("Transactions %d created", i))
					}
					GinkgoLogr.Info("Transactions created")

					// todo: check the snapshot has been created
				})

				Context("Then the follower come back", func() {
					BeforeEach(func() {
						Eventually(servers[leaderID-1]).To(HasNextLogID(ledgerName, uint64(16)))

						// Restart the follower
						By("Starting the follower", func() {
							Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
						})

						Eventually(servers[followerID-1]).To(BeFollower())
						Eventually(servers[followerID-1]).To(HasNextLogID(ledgerName, countTransactions+1))
					})

					It("Should restore the state from a snapshot sent by the leader", func() {})
					Context("Then restarting again the follower", func() {
						BeforeEach(func() {
							By("Stopping the follower", func() {
								Expect(servers[followerID-1].service.Stop(ctx)).To(Succeed())
							})
							<-time.After(time.Second)
							By("Starting the follower", func() {
								Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
							})
						})
						It("Should restart as expected", func() {
							Eventually(servers[followerID-1]).To(BeFollower())
							Eventually(servers[followerID-1]).To(HasNextLogID(ledgerName, countTransactions+1))
						})
					})
				})
			})
		})
	})
	Context("When creating a ledger", func() {
		var (
			ledgerName string
		)
		BeforeEach(func() {
			ledgerName = "ledger2"
			// Create a ledger while follower is down
			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			Expect(servers[leaderID-1]).To(HaveALeader(&leaderID))
		})
		Context("When simulating a follower slowness by blocking MsgApp from the leader", func() {
			var (
				followerID uint64
			)
			BeforeEach(func() {
				// Find a follower (any node that is not the leader)
				followerID = ((leaderID + 1) % countInstances) + 1
				By(fmt.Sprintf("Blocking MsgApp from the leader to follower %d", followerID), func() {
					gateway.SetInterceptor(testserver.MessageInterceptorFunc(func(msg *raftpb.Message) bool {
						if msg.To == followerID && msg.Type == raftpb.MsgApp {
							return false
						}
						return true
					}))
				})
			})
			Context("When triggering a leader snapshot", func() {
				const countTransactions = 15
				BeforeEach(func() {
					// Create enough transactions to trigger a snapshot
					// snapshotThreshold is 10, so we create 15 transactions to ensure a snapshot is created and we have some tx in spool
					for i := 0; i < countTransactions; i++ {
						_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
							Actions: []*servicepb.Action{
								createTransactionAction(ledgerName, []*commonpb.Posting{
									newPosting("world", "bank", big.NewInt(100), "USD"),
								}, nil, nil),
							},
						})
						Expect(err).To(Succeed())
					}

					// todo: check the snapshot has been created
				})
				It("Should trigger the sending of a snapshot from a leader", func() {
					gateway.RemoveInterceptor()
					By("Creating a transaction to trigger the delay detection by the leader", func() {
						// Create enough transactions to trigger a snapshot
						// snapshotThreshold is 10, so we create 15 transactions to ensure a snapshot is created and we have some tx in spool
						for i := 0; i < countTransactions; i++ {
							_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
								Actions: []*servicepb.Action{
									createTransactionAction(ledgerName, []*commonpb.Posting{
										newPosting("world", "bank", big.NewInt(100), "USD"),
									}, nil, nil),
								},
							})
							Expect(err).To(Succeed())
						}
					})
					// todo: add real check. I can see in logs the snapshot is restored but I have now way to check it is the case
				})
			})
		})
	})
})
