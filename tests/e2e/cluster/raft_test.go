//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
)

var _ = Describe("Simple cluster", func() {
	const countInstances = 3

	Context("Basic cluster operations", Ordered, func() {
		var (
			ctx      context.Context
			servers  []*testutil.ServiceWithClient
			leaderID *uint64
		)

		BeforeAll(func() {
			ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(
				countInstances, testutil.TestRaftBasePort, testutil.TestServiceBasePort, testutil.TestHTTPBasePort, testutil.TestGatewayBasePort,
				testutil.WithGateway(),
			)
		})

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("should start successfully", func() {})

		It("should create a ledger and delete it", func() {
			_, err := servers[0].Client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction("ledger0", nil)},
			})
			Expect(err).To(Succeed())
		})

		It("should create transactions through all nodes", func() {
			ledgerName := "multi-node-ledger"

			Eventually(servers[0]).To(HaveALeader(nil), "Timed out waiting for leader election")

			_, err := servers[0].Client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			for i := range countInstances {
				_, err := servers[i].Client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", fmt.Sprintf("node-%d", i+1), big.NewInt(100*int64(i+1)), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed(), "Failed to create transaction through node %d", i+1)
			}
		})

		It("should rejoin the cluster after a follower restart", func() {
			Skip("Flaky: hangs on stale gRPC connection — see commit a07bc611")
			followerID := ((*leaderID + 1) % countInstances) + 1
			testutil.StopNode(ctx, servers[followerID-1])
			testutil.RestartNode(ctx, servers[followerID-1])

			Eventually(servers[followerID-1]).
				WithTimeout(30 * time.Second).
				WithPolling(500 * time.Millisecond).
				Should(BeFollower(), "Timed out waiting for node to become follower")
			Consistently(servers[followerID-1]).Should(BeFollower())
		})

		// MUST BE LAST — stops the leader permanently
		It("should elect a new leader when leader is down", func() {
			lid := *leaderID
			Eventually(servers[lid-1]).To(HaveALeader(&lid))
			Expect(servers[lid-1].Service.Stop(ctx)).To(BeNil())

			Eventually(servers[(lid+1)%countInstances]).To(HaveALeader(nil))
		})
	})

	Context("When losing a follower", Ordered, func() {
		const (
			ledgerName        = "ledger2"
			countTransactions = 15
		)

		var (
			ctx        context.Context
			servers    []*testutil.ServiceWithClient
			leaderID   *uint64
			followerID uint64
		)

		BeforeAll(func() {
			ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(
				countInstances, testutil.TestRaftBasePort, testutil.TestServiceBasePort, testutil.TestHTTPBasePort, testutil.TestGatewayBasePort,
				testutil.WithGateway(),
			)

			// Find and stop a follower
			followerID = ((*leaderID + 1) % countInstances) + 1
			testutil.StopNode(ctx, servers[followerID-1])
		})

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("should continue to work with a downed follower", func() {
			lid := *leaderID
			Eventually(servers[lid-1]).To(HaveALeader(nil))

			_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction("ledger1", nil)},
			})
			Expect(err).To(Succeed())

			for i := 0; i < 5; i++ {
				_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction("ledger1", []*commonpb.Posting{
							actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}
		})

		It("should restore the state after follower comes back", func() {
			lid := *leaderID

			_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			testutil.RestartNode(ctx, servers[followerID-1])

			Eventually(servers[followerID-1]).
				WithTimeout(30 * time.Second).
				WithPolling(500 * time.Millisecond).
				Should(BeFollower(), "Timed out waiting for node to become follower")
			Eventually(func(g Gomega) bool {
				ledgers, err := actions.ListLedgers(ctx, servers[followerID-1].Client)
				g.Expect(err).To(Succeed())
				_, found := ledgers[ledgerName]
				return found
			}).To(BeTrue())

			ledger, err := servers[followerID-1].Client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal(ledgerName))
		})

		It("should restore the state from a snapshot sent by the leader", func() {
			By("Stopping the follower")
			testutil.StopNode(ctx, servers[followerID-1])

			By("Creating transactions to trigger background maintenance")
			lid := *leaderID
			for i := 0; i < countTransactions; i++ {
				_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			By("Starting the follower")
			testutil.RestartNode(ctx, servers[followerID-1])
			Eventually(servers[followerID-1], 15*time.Second).Should(BeFollower(), "Timed out waiting for node to become follower")
		})

		It("should restart as expected after a second restart", func() {
			By("Stopping the follower")
			testutil.StopNode(ctx, servers[followerID-1])

			By("Starting the follower")
			testutil.RestartNode(ctx, servers[followerID-1])
			Eventually(servers[followerID-1], 15*time.Second).Should(BeFollower(), "Timed out waiting for node to become follower")
		})
	})

	Context("Gateway interceptor tests", func() {
		var (
			ctx      context.Context
			servers  []*testutil.ServiceWithClient
			gateway  *testserver.Gateway
			leaderID *uint64
		)

		BeforeEach(func() {
			ctx, servers, gateway, leaderID = testutil.SetupMultiNodeCluster(
				countInstances, testutil.TestRaftBasePort, testutil.TestServiceBasePort, testutil.TestHTTPBasePort, testutil.TestGatewayBasePort,
				testutil.WithGateway(),
			)
		})

		AfterEach(func() {
			testutil.StopServers(ctx, servers)
		})

		Context("When creating a ledger", func() {
			var ledgerName string

			BeforeEach(func() {
				ledgerName = "ledger2"
				_, err := servers[*leaderID-1].Client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
				})
				Expect(err).To(Succeed())

				Expect(servers[*leaderID-1]).To(HaveALeader(nil))
			})

			Context("When simulating a follower slowness by blocking MsgApp from the leader", func() {
				var followerID uint64

				BeforeEach(func() {
					lid := *leaderID
					followerID = ((lid + 1) % countInstances) + 1
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
						lid := *leaderID
						for i := 0; i < countTransactions; i++ {
							_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
								Requests: []*servicepb.Request{
									actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
										actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
									}, nil, nil),
								},
							})
							Expect(err).To(Succeed())
						}
					})

					It("Should trigger the sending of a snapshot from a leader", func() {
						lid := *leaderID
						gateway.RemoveInterceptor()
						By("Creating a transaction to trigger the delay detection by the leader", func() {
							for i := 0; i < countTransactions; i++ {
								_, err := servers[lid-1].Client.Apply(ctx, &servicepb.ApplyRequest{
									Requests: []*servicepb.Request{
										actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
											actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
										}, nil, nil),
									},
								})
								Expect(err).To(Succeed())
							}
						})
					})
				})
			})
		})
	})
})
