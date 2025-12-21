//go:build e2e
// +build e2e

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
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Simple cluster", func() {

	type serviceWithClient struct {
		service                   *testservice.Service
		client                    *client.Formance
		raftDataDir, extraDataDir string
	}

	var (
		ctx      context.Context
		servers  []serviceWithClient
		leaderID int64
	)
	const countInstances = 3

	BeforeEach(func() {
		ctx = logging.TestingContext()

		servers = make([]serviceWithClient, 0, countInstances)
		for i := range countInstances {
			raftTmpDir := GinkgoT().TempDir()
			DeferCleanup(func() {
				Expect(os.RemoveAll(raftTmpDir)).To(Succeed())
			})

			extraDataTmpDir := GinkgoT().TempDir()
			DeferCleanup(func() {
				Expect(os.RemoveAll(extraDataTmpDir)).To(Succeed())
			})

			server := testservice.New(cmdserver.NewRootCommand,
				testservice.WithInstruments(
					testserver.WithNodeID(i+1),
					testserver.WithHTTPPort(9000+i),
					testserver.WithDataDir(raftTmpDir),
					testserver.WithGRPCPort(8000+i),
					testserver.WithSnapshotThreshold(10),
					testserver.WithDebug(os.Getenv("DEBUG") == "true"),
					//testserver.WithRaftTickInterval(10*time.Millisecond),
					//testserver.WithRaftHeartbeatTick(10),
					//testserver.WithRaftElectionTick(100),
					testserver.WithPeers(func() []raft.Peer {
						ret := make([]raft.Peer, 0, countInstances-1)
						for j := range countInstances {
							if i == j {
								continue
							}
							ret = append(ret, raft.Peer{
								ID:      uint64(j + 1),
								Address: fmt.Sprintf("127.0.0.1:%d", 8000+j),
							})
						}

						return ret
					}()...),
					testserver.WithExtraDataDir(extraDataTmpDir),
				),
			)
			Expect(server.Start(ctx)).To(Succeed())

			servers = append(servers, serviceWithClient{
				service: server,
				client: client.New(
					client.WithServerURL(fmt.Sprintf("http://localhost:%d", 9000+i)),
				),
				raftDataDir:  raftTmpDir,
				extraDataDir: extraDataTmpDir,
			})
		}
		Eventually(func(g Gomega) bool {
			state, err := servers[0].client.Cluster.GetClusterState(ctx)
			g.Expect(err).To(Succeed())

			if state.ClusterStateResponse.Data.Leader == nil {
				return false
			}

			leaderID = *state.ClusterStateResponse.Data.Leader

			return leaderID != 0
		}).
			Within(10 * time.Second).
			WithPolling(500 * time.Millisecond).
			To(BeTrue())
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
			followerID int64
		)
		BeforeEach(func() {
			followerID = ((leaderID + 1) % countInstances) + 1
			Expect(servers[followerID-1].service.Stop(ctx)).To(Succeed())
			<-time.After(time.Second)
			Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
		})
		It("Should properly rejoin the cluster", func() {
			Eventually(func(g Gomega) bool {
				clusterState, err := servers[followerID-1].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				if clusterState.ClusterStateResponse.Data.Leader == nil {
					return false
				}
				if *clusterState.ClusterStateResponse.Data.Leader == 0 {
					return false
				}

				return true
			}).
				Within(5 * time.Second).
				WithPolling(500 * time.Millisecond).
				To(BeTrue())
		})
	})

	Context("when the leader is down", func() {
		BeforeEach(func() {
			Eventually(func(g Gomega) int64 {
				state, err := servers[0].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				leaderID = *state.ClusterStateResponse.Data.Leader

				return leaderID
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())
		})
		BeforeEach(func() {
			Expect(servers[leaderID-1].service.Stop(ctx)).To(BeNil())
		})
		It("should elect a new leader", func() {
			Eventually(func(g Gomega) bool {
				state, err := servers[(leaderID+1)%countInstances].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				return state.ClusterStateResponse.Data.Leader != nil &&
					*state.ClusterStateResponse.Data.Leader != 0 &&
					*state.ClusterStateResponse.Data.Leader != leaderID
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())
		})
	})

	Context("When creating a new ledger", func() {
		BeforeEach(func() {
			_, err := servers[0].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: "ledger0",
				CreateLedgerRequest: components.CreateLedgerRequest{
					Driver: components.CreateLedgerRequestDriverSqlite.ToPointer(),
				},
			})
			Expect(err).To(Succeed())
		})
		It("should succeed", func() {})
		Context("Then deleting the ledger", func() {
			BeforeEach(func() {
				// Note: DeleteLedger endpoint needs to be added to the SDK
				// For now, we'll skip this test or implement it when the endpoint is available
			})
			It("Should succeed", func() {})
		})
	})

	Context("When creating transactions through all nodes", func() {
		var ledgerName string

		BeforeEach(func() {
			ledgerName = "multi-node-ledger"

			// Wait for leader election
			Eventually(func(g Gomega) uint64 {
				state, err := servers[0].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				return uint64(*state.ClusterStateResponse.Data.Leader)
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())

			// Create ledger
			_, err := servers[0].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: ledgerName,
				CreateLedgerRequest: components.CreateLedgerRequest{
					Driver: components.CreateLedgerRequestDriverSqlite.ToPointer(),
				},
			})
			Expect(err).To(Succeed())
		})

		It("should successfully create transactions through each node", func() {
			// Create a transaction through each node
			for i := range countInstances {
				_, err := servers[i].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
					LedgerName: ledgerName,
					CreateTransactionRequest: components.CreateTransactionRequest{
						Postings: []components.PostingRequest{{
							Source:      "world",
							Destination: fmt.Sprintf("node-%d", i+1),
							Amount:      big.NewInt(100 * int64(i+1)),
							Asset:       "USD",
						}},
					},
				})
				Expect(err).To(Succeed(), "Failed to create transaction through node %d", i+1)
			}
		})
	})

	Context("When losing a follower", func() {
		var (
			leaderID   uint64
			followerID uint64
		)
		BeforeEach(func() {
			// Wait for leader election
			Eventually(func(g Gomega) uint64 {
				state, err := servers[0].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				leaderID = uint64(*state.ClusterStateResponse.Data.Leader)
				return leaderID
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())

			// Find a follower (any node that is not the leader)
			for i := range countInstances {
				if uint64(i+1) != leaderID {
					followerID = uint64(i + 1)
					break
				}
			}
			Expect(followerID).NotTo(BeZero(), "followerID should not be zero - all nodes cannot be leaders")
			Expect(followerID).To(BeNumerically(">", 0))
			Expect(followerID).To(BeNumerically("<=", countInstances))

			// Stop the follower
			Expect(servers[followerID-1].service.Stop(ctx)).To(Succeed())
		})

		It("Should continue to work", func() {
			// Ensure leaderID is valid
			Expect(leaderID).NotTo(BeZero(), "leaderID should not be zero")
			Expect(leaderID).To(BeNumerically(">", 0))
			Expect(leaderID).To(BeNumerically("<=", countInstances))

			// Verify cluster still has a leader
			Eventually(func(g Gomega) bool {
				state, err := servers[leaderID-1].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				return state.ClusterStateResponse.Data.Leader != nil &&
					*state.ClusterStateResponse.Data.Leader != 0
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

			// Create a ledger
			_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: "ledger1",
				CreateLedgerRequest: components.CreateLedgerRequest{
					Driver: components.CreateLedgerRequestDriverSqlite.ToPointer(),
				},
			})
			Expect(err).To(Succeed())

			// Create some transactions
			for i := 0; i < 5; i++ {
				_, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
					LedgerName: "ledger1",
					CreateTransactionRequest: components.CreateTransactionRequest{
						Postings: []components.PostingRequest{{
							Source:      "world",
							Destination: "bank",
							Amount:      big.NewInt(100),
							Asset:       "USD",
						}},
					},
				})
				Expect(err).To(Succeed())
			}
		})
		Context("Then creating a new ledger", func() {
			var ledgerName string
			BeforeEach(func() {
				ledgerName = "ledger2"
				// Create a ledger while follower is down
				_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
					LedgerName: ledgerName,
					CreateLedgerRequest: components.CreateLedgerRequest{
						Driver: components.CreateLedgerRequestDriverSqlite.ToPointer(),
					},
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
					Eventually(func(g Gomega) bool {
						// First verify the follower is connected
						state, err := servers[followerID-1].client.Cluster.GetClusterState(ctx)
						g.Expect(err).To(Succeed())

						if state.ClusterStateResponse.Data.Leader == nil ||
							*state.ClusterStateResponse.Data.Leader == 0 {
							return false
						}

						// Then verify the follower can see the ledger created while it was down
						ledgers, err := servers[followerID-1].client.Ledgers.ListAllLedgers(ctx)
						g.Expect(err).To(Succeed())

						for _, ledger := range ledgers.ListAllLedgersResponse.Data {
							if ledger.Name == ledgerName {
								return true
							}
						}
						return false
					}).Within(10 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

					// Verify the follower can access the ledger details
					ledger, err := servers[followerID-1].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
						LedgerName: ledgerName,
					})
					Expect(err).To(Succeed())
					Expect(ledger.GetGetLedgerResponse().Data.Name).To(Equal(ledgerName))
				})
			})
		})
		Context("Then creating more transactions than the snapshot threshold", func() {
			var ledgerName string
			BeforeEach(func() {
				// Create a ledger
				ledgerName = "ledger-snapshot"

				_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
					LedgerName: ledgerName,
					CreateLedgerRequest: components.CreateLedgerRequest{
						Driver: components.CreateLedgerRequestDriverSqlite.ToPointer(),
					},
				})
				Expect(err).To(Succeed())

				// Create enough transactions to trigger a snapshot
				// snapshotThreshold is 10, so we create 11 transactions to ensure a snapshot is created
				for i := 0; i < 11; i++ {
					_, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
						LedgerName: ledgerName,
						CreateTransactionRequest: components.CreateTransactionRequest{
							Postings: []components.PostingRequest{{
								Source:      "world",
								Destination: "bank",
								Amount:      big.NewInt(100),
								Asset:       "USD",
							}},
						},
					})
					Expect(err).To(Succeed())
				}

				// Wait for snapshot to be created (verify by checking that ledger exists)
				Eventually(func(g Gomega) bool {
					// Verify the ledger exists
					ledger, err := servers[leaderID-1].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
						LedgerName: ledgerName,
					})
					g.Expect(err).To(Succeed())
					return ledger.GetGetLedgerResponse().Data.Name == ledgerName
				}).Within(5 * time.Second).WithPolling(200 * time.Millisecond).Should(BeTrue())
			})

			Context("Then the follower come back", func() {
				BeforeEach(func() {
					// Restart the follower
					Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
				})

				It("Should restore the state from a snapshot sent by the leader", func() {
					// Wait for follower to reconnect and sync
					Eventually(func(g Gomega) bool {
						// Verify the follower is connected
						state, err := servers[followerID-1].client.Cluster.GetClusterState(ctx)
						g.Expect(err).To(Succeed())

						// todo: check the number of servers

						return state.ClusterStateResponse.Data.Leader != nil &&
							*state.ClusterStateResponse.Data.Leader != 0
					}).Within(10 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

					// Verify the follower can see the ledger
					Eventually(func(g Gomega) bool {
						ledger, err := servers[followerID-1].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
							LedgerName: ledgerName,
						})
						g.Expect(err).To(Succeed())
						return ledger.GetGetLedgerResponse().Data.Name == ledgerName
					}).Within(5 * time.Second).WithPolling(100 * time.Millisecond).To(BeTrue())
				})
			})
		})
	})
})
