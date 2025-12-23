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
			Eventually(func(g Gomega) error {
				clusterState, err := servers[followerID-1].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				if clusterState.ClusterStateResponse.Data.Leader == nil {
					return fmt.Errorf("leader is nil")
				}
				if *clusterState.ClusterStateResponse.Data.Leader != leaderID {
					return fmt.Errorf("expected leader to be %d, got %d", leaderID, *clusterState.ClusterStateResponse.Data.Leader)
				}
				// The node should not trigger an election
				if *clusterState.ClusterStateResponse.Data.Leader == clusterState.ClusterStateResponse.Data.LocalNode {
					return fmt.Errorf("expected leader to be different from local node")
				}

				return nil
			}).
				Within(10 * time.Second).
				WithPolling(500 * time.Millisecond).
				To(BeNil())

			Consistently(func(g Gomega) error {
				clusterState, err := servers[followerID-1].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				if clusterState.ClusterStateResponse.Data.Leader == nil {
					return fmt.Errorf("leader is nil")
				}
				if *clusterState.ClusterStateResponse.Data.Leader != leaderID {
					return fmt.Errorf("expected leader to be %d, got %d", leaderID, *clusterState.ClusterStateResponse.Data.Leader)
				}

				return nil
			}).
				Within(2 * time.Second).
				WithPolling(500 * time.Millisecond).
				To(BeNil())
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
			}).
				Within(5 * time.Second).
				WithPolling(500 * time.Millisecond).
				To(BeTrue())
		})
	})
	Context("When creating a new ledger", func() {
		BeforeEach(func() {
			_, err := servers[0].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: "ledger0",
				CreateLedgerRequest: components.CreateLedgerRequest{
					Driver: components.CreateLedgerRequestDriverSqliteMattn.ToPointer(),
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
					Driver: components.CreateLedgerRequestDriverSqliteMattn.ToPointer(),
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
			followerID int64
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
					Driver: components.CreateLedgerRequestDriverSqliteMattn.ToPointer(),
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
			var (
				ledgerName string
			)
			BeforeEach(func() {
				ledgerName = "ledger2"
				// Create a ledger while follower is down
				_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
					LedgerName: ledgerName,
					CreateLedgerRequest: components.CreateLedgerRequest{
						Driver: components.CreateLedgerRequestDriverSqliteMattn.ToPointer(),
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

			Context("Then creating more transactions than the snapshot threshold", func() {
				BeforeEach(func() {
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

					// todo: check the snapshot has been created
				})

				Context("Then the follower come back", func() {
					var (
						leaderState *operations.GetLedgerRaftStateResponse
						err         error
					)
					BeforeEach(func() {
						Eventually(func(g Gomega) error {
							leaderState, err = servers[leaderID-1].client.Ledgers.GetLedgerRaftState(ctx, operations.GetLedgerRaftStateRequest{
								LedgerName: ledgerName,
							})
							g.Expect(err).To(Succeed())

							if leaderState.LedgerClusterStateResponse.Data.InnerState.LastLogID == nil {
								return fmt.Errorf("last log id is nil")
							}
							if *leaderState.LedgerClusterStateResponse.Data.InnerState.LastLogID != 11 {
								return fmt.Errorf("all transactions not committed, expected last log id to be 11, got %d", *leaderState.LedgerClusterStateResponse.Data.InnerState.LastLogID)
							}

							return nil
						}).Within(10 * time.Second).WithPolling(500 * time.Millisecond).To(BeNil())

						// Restart the follower
						By("Starting the follower", func() {
							Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
						})

						Eventually(func(g Gomega) error {
							clusterState, err := servers[followerID-1].client.Cluster.GetClusterState(ctx)
							g.Expect(err).To(Succeed())

							if clusterState.ClusterStateResponse.Data.Leader == nil {
								return fmt.Errorf("leader is nil")
							}
							if *clusterState.ClusterStateResponse.Data.Leader != leaderID {
								return fmt.Errorf("expected leader to be %d, got %d", leaderID, *clusterState.ClusterStateResponse.Data.Leader)
							}
							// The node should not trigger an election
							if *clusterState.ClusterStateResponse.Data.Leader == clusterState.ClusterStateResponse.Data.LocalNode {
								return fmt.Errorf("expected leader to be different from local node")
							}

							return nil
						}).
							Within(10 * time.Second).
							WithPolling(500 * time.Millisecond).
							To(BeNil())
					})

					It("Should restore the state from a snapshot sent by the leader", func() {
						// Wait for follower to reconnect and sync
						Eventually(func(g Gomega) error {
							clusterState, err := servers[followerID-1].client.Ledgers.GetLedgerRaftState(ctx, operations.GetLedgerRaftStateRequest{
								LedgerName: ledgerName,
							})
							g.Expect(err).To(Succeed())

							if clusterState.LedgerClusterStateResponse.Data.Leader == nil {
								return fmt.Errorf("leader is nil")
							}
							if *clusterState.LedgerClusterStateResponse.Data.Leader != *leaderState.LedgerClusterStateResponse.Data.Leader {
								return fmt.Errorf("expected leader to be %d, got %d", leaderID, *clusterState.LedgerClusterStateResponse.Data.Leader)
							}
							// The node should not trigger an election
							if *clusterState.LedgerClusterStateResponse.Data.Leader == clusterState.LedgerClusterStateResponse.Data.LocalNode {
								return fmt.Errorf("expected leader to be different from local node")
							}

							if *clusterState.LedgerClusterStateResponse.Data.InnerState.LastLogID !=
								*leaderState.LedgerClusterStateResponse.Data.InnerState.LastLogID {
								return fmt.Errorf("expected last log id to be %d, got %d",
									*leaderState.LedgerClusterStateResponse.Data.InnerState.LastLogID,
									*clusterState.LedgerClusterStateResponse.Data.InnerState.LastLogID,
								)
							}

							return nil
						}).
							Within(10 * time.Second).
							WithPolling(500 * time.Millisecond).
							To(BeNil())
					})
				})
			})
		})
	})
})
