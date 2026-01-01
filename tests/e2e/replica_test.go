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
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

type serviceWithClient struct {
	service     *testservice.Service
	client      *client.Formance
	raftDataDir string
}

var _ = Describe("Simple cluster", func() {
	var (
		ctx      context.Context
		servers  []*serviceWithClient
		leaderID uint64
	)
	const countInstances = 3

	BeforeEach(func() {
		ctx = logging.TestingContext()

		servers = make([]*serviceWithClient, 0, countInstances)
		for i := range countInstances {
			raftTmpDir := GinkgoT().TempDir()
			DeferCleanup(func() {
				Expect(os.RemoveAll(raftTmpDir)).To(Succeed())
			})

			server := testservice.New(cmdserver.NewRootCommand,
				testservice.WithInstruments(
					testserver.WithNodeID(i+1),
					testserver.WithHTTPPort(9000+i),
					testserver.WithDataDir(raftTmpDir),
					testserver.WithGRPCPort(8000+i),
					testserver.WithSnapshotThreshold(10),
					testserver.WithRaftCompactionMargin(1), // Default is 1000, since we override the default snapshot threshold, we need to adjust this value
					//testserver.WithRaftTickInterval(10*time.Millisecond),
					//testserver.WithRaftHeartbeatTick(10),
					//testserver.WithRaftElectionTick(100),
					testserver.WithDebug(os.Getenv("DEBUG") == "true"),
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
				),
			)
			Expect(server.Start(ctx)).To(Succeed())

			servers = append(servers, &serviceWithClient{
				service: server,
				client: client.New(
					client.WithServerURL(fmt.Sprintf("http://localhost:%d", 9000+i)),
				),
				raftDataDir: raftTmpDir,
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
			Eventually(func(g Gomega) uint64 {
				state, err := servers[0].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				leaderID = uint64(*state.ClusterStateResponse.Data.Leader)

				return leaderID
			}).NotTo(BeZero())
		})
		BeforeEach(func() {
			Expect(servers[leaderID-1].service.Stop(ctx)).To(BeNil())
		})
		It("should elect a new leader", func() {
			Eventually(servers[(leaderID+1)%countInstances]).To(HaveALeader(nil))
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
			Eventually(servers[0]).To(HaveALeader(nil), "Timed out waiting for leader election")

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
					Eventually(servers[followerID-1]).To(BeFollower())
					Eventually(func(g Gomega) bool {
						// Then verify the follower can see the ledger created while it was down
						ledgers, err := servers[followerID-1].client.Ledgers.ListAllLedgers(ctx)
						g.Expect(err).To(Succeed())

						for _, ledger := range ledgers.ListAllLedgersResponse.Data {
							if ledger.Name == ledgerName {
								return true
							}
						}
						return false
					}).To(BeTrue())

					// Verify the follower can access the ledger details
					ledger, err := servers[followerID-1].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
						LedgerName: ledgerName,
					})
					Expect(err).To(Succeed())
					Expect(ledger.GetGetLedgerResponse().Data.Name).To(Equal(ledgerName))
				})
			})

			Context("Then creating more transactions than the snapshot threshold", func() {
				const countTransactions = 15
				BeforeEach(func() {
					// Create enough transactions to trigger a snapshot
					// snapshotThreshold is 10, so we create 15 transactions to ensure a snapshot is created and we have some tx in spool
					for i := 0; i < countTransactions; i++ {
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
					BeforeEach(func() {
						Eventually(servers[leaderID-1]).To(HasLastLog(ledgerName, uint64(15)))

						// Restart the follower
						By("Starting the follower", func() {
							Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
						})

						Eventually(servers[followerID-1]).To(BeFollower())
						Eventually(servers[followerID-1]).To(BeLedgerFollower(ledgerName))
						Eventually(servers[followerID-1]).To(HasLastLog(ledgerName, countTransactions))
					})

					It("Should restore the state from a snapshot sent by the leader", func() {})
					Context("Then restarting again the follower", func() {
						BeforeEach(func() {
							By("Stopping the follower", func() {
								By("Stopping the follower", func() {
									Expect(servers[followerID-1].service.Stop(ctx)).To(Succeed())
								})
								<-time.After(time.Second)
								By("Starting the follower", func() {
									Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
								})
							})
						})
						It("Should restart as expected", func() {
							Eventually(servers[followerID-1]).To(BeFollower())
							Eventually(servers[followerID-1]).To(BeLedgerFollower(ledgerName))
							Eventually(servers[followerID-1]).To(HasLastLog(ledgerName, countTransactions))
						})
					})
				})
			})
		})
	})
})

type beLedgerFollowerMatcher struct {
	ledgerName string
}

func (matcher beLedgerFollowerMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.Ledgers.GetLedgerRaftState(context.Background(), operations.GetLedgerRaftStateRequest{
		LedgerName: matcher.ledgerName,
	})
	if err != nil {
		return false, err
	}

	if clusterState.LedgerClusterStateResponse.Data.Leader == nil {
		return false, nil
	}
	return *clusterState.LedgerClusterStateResponse.Data.Leader !=
		clusterState.LedgerClusterStateResponse.Data.LocalNode, nil
}

func (matcher beLedgerFollowerMatcher) FailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected node to be a follower for ledger '%s'", matcher.ledgerName)
}

func (matcher beLedgerFollowerMatcher) NegatedFailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected node not to be a follower for ledger '%s'", matcher.ledgerName)
}

func BeLedgerFollower(ledgerName string) types.GomegaMatcher {
	return beLedgerFollowerMatcher{
		ledgerName: ledgerName,
	}
}

var _ types.GomegaMatcher = (*beLedgerFollowerMatcher)(nil)

type beFollowerMatcher struct{}

func (matcher beFollowerMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.Cluster.GetClusterState(context.Background())
	if err != nil {
		return false, err
	}

	if clusterState.ClusterStateResponse.Data.Leader == nil {
		return false, nil
	}
	return *clusterState.ClusterStateResponse.Data.Leader !=
		clusterState.ClusterStateResponse.Data.LocalNode, nil
}

func (matcher beFollowerMatcher) FailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected node to be a follower")
}

func (matcher beFollowerMatcher) NegatedFailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected node not to be a follower")
}

func BeFollower() types.GomegaMatcher {
	return beFollowerMatcher{}
}

var _ types.GomegaMatcher = (*beFollowerMatcher)(nil)

type hasLastLogMatcher struct {
	ledgerName      string
	expectedLastLog uint64
	observedLastLog uint64
}

func (matcher *hasLastLogMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.Ledgers.GetLedgerRaftState(context.Background(), operations.GetLedgerRaftStateRequest{
		LedgerName: matcher.ledgerName,
	})
	if err != nil {
		return false, err
	}

	matcher.observedLastLog = uint64(clusterState.LedgerClusterStateResponse.Data.InnerState.LastLogID)

	if matcher.observedLastLog > matcher.expectedLastLog {
		return false, fmt.Errorf("last log %d is greater than expected %d", matcher.observedLastLog, matcher.expectedLastLog)
	}

	return matcher.observedLastLog == matcher.expectedLastLog, nil
}

func (matcher *hasLastLogMatcher) FailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected %s to have last log %d, got %d", matcher.ledgerName, matcher.expectedLastLog, matcher.observedLastLog)
}

func (matcher *hasLastLogMatcher) NegatedFailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected %s not to have last log %d", matcher.ledgerName, matcher.expectedLastLog)
}

func HasLastLog(ledgerName string, lastLog uint64) types.GomegaMatcher {
	return &hasLastLogMatcher{
		ledgerName:      ledgerName,
		expectedLastLog: lastLog,
	}
}

var _ types.GomegaMatcher = (*hasLastLogMatcher)(nil)

type haveALeaderMatcher struct {
	fetchTo *uint64
}

func (h haveALeaderMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.Cluster.GetClusterState(context.Background())
	if err != nil {
		return false, err
	}

	if clusterState.ClusterStateResponse.Data.Leader == nil {
		return false, nil
	}

	leaderID := uint64(*clusterState.ClusterStateResponse.Data.Leader)
	if h.fetchTo != nil {
		*h.fetchTo = leaderID
	}

	return leaderID != 0, nil
}

func (h haveALeaderMatcher) FailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected cluster to have a leader")
}

func (h haveALeaderMatcher) NegatedFailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected cluster not to have a leader")
}

func HaveALeader(fetchTo *uint64) types.GomegaMatcher {
	return haveALeaderMatcher{
		fetchTo: fetchTo,
	}
}

var _ types.GomegaMatcher = (*haveALeaderMatcher)(nil)
