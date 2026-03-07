//go:build e2e

package e2e

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
)

var _ = Describe("Simple cluster", func() {
	const countInstances = 3

	Context("Basic cluster operations", Ordered, func() {
		var (
			ctx      context.Context
			servers  []*serviceWithClient
			leaderID *uint64
		)

		BeforeAll(func() {
			ctx, servers, _, leaderID = setupMultiNodeCluster(
				countInstances, testRaftBasePort, testServiceBasePort, testHTTPBasePort, testGatewayBasePort,
				WithGateway(),
			)
		})

		AfterAll(func() {
			stopServers(ctx, servers)
		})

		It("should start successfully", func() {})

		It("should create a ledger and delete it", func() {
			_, err := servers[0].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction("ledger0", nil)},
			})
			Expect(err).To(Succeed())
		})

		It("should create transactions through all nodes", func() {
			ledgerName := "multi-node-ledger"

			Eventually(servers[0]).To(HaveALeader(nil), "Timed out waiting for leader election")

			_, err := servers[0].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			for i := range countInstances {
				_, err := servers[i].client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("node-%d", i+1), big.NewInt(100*int64(i+1)), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed(), "Failed to create transaction through node %d", i+1)
			}
		})

		It("should rejoin the cluster after a follower restart", func() {
			followerID := ((*leaderID + 1) % countInstances) + 1
			stopNode(ctx, servers[followerID-1])
			restartNode(ctx, servers[followerID-1])

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
			Expect(servers[lid-1].service.Stop(ctx)).To(BeNil())

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
			servers    []*serviceWithClient
			leaderID   *uint64
			followerID uint64
		)

		BeforeAll(func() {
			ctx, servers, _, leaderID = setupMultiNodeCluster(
				countInstances, testRaftBasePort, testServiceBasePort, testHTTPBasePort, testGatewayBasePort,
				WithGateway(),
			)

			// Find and stop a follower
			followerID = ((*leaderID + 1) % countInstances) + 1
			stopNode(ctx, servers[followerID-1])
		})

		AfterAll(func() {
			stopServers(ctx, servers)
		})

		It("should continue to work with a downed follower", func() {
			lid := *leaderID
			Eventually(servers[lid-1]).To(HaveALeader(nil))

			_, err := servers[lid-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction("ledger1", nil)},
			})
			Expect(err).To(Succeed())

			for i := 0; i < 5; i++ {
				_, err := servers[lid-1].client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction("ledger1", []*commonpb.Posting{
							newPosting("world", "bank", big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}
		})

		It("should restore the state after follower comes back", func() {
			lid := *leaderID

			_, err := servers[lid-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			restartNode(ctx, servers[followerID-1])

			Eventually(servers[followerID-1]).
				WithTimeout(30 * time.Second).
				WithPolling(500 * time.Millisecond).
				Should(BeFollower(), "Timed out waiting for node to become follower")
			Eventually(func(g Gomega) bool {
				ledgers, err := listLedgers(ctx, servers[followerID-1].client)
				g.Expect(err).To(Succeed())
				_, found := ledgers[ledgerName]
				return found
			}).To(BeTrue())

			ledger, err := servers[followerID-1].client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal(ledgerName))
		})

		It("should restore the state from a snapshot sent by the leader", func() {
			By("Stopping the follower")
			stopNode(ctx, servers[followerID-1])

			By("Creating transactions past the snapshot threshold")
			lid := *leaderID
			for i := 0; i < countTransactions; i++ {
				_, err := servers[lid-1].client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", "bank", big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			By("Starting the follower")
			restartNode(ctx, servers[followerID-1])
			Eventually(servers[followerID-1], 15*time.Second).Should(BeFollower(), "Timed out waiting for node to become follower")
		})

		It("should restart as expected after a second restart", func() {
			By("Stopping the follower")
			stopNode(ctx, servers[followerID-1])

			By("Starting the follower")
			restartNode(ctx, servers[followerID-1])
			Eventually(servers[followerID-1], 15*time.Second).Should(BeFollower(), "Timed out waiting for node to become follower")
		})
	})

	Context("Gateway interceptor tests", func() {
		var (
			ctx      context.Context
			servers  []*serviceWithClient
			gateway  *testserver.Gateway
			leaderID *uint64
		)

		BeforeEach(func() {
			ctx, servers, gateway, leaderID = setupMultiNodeCluster(
				countInstances, testRaftBasePort, testServiceBasePort, testHTTPBasePort, testGatewayBasePort,
				WithGateway(),
			)
		})

		AfterEach(func() {
			stopServers(ctx, servers)
		})

		Context("When creating a ledger", func() {
			var ledgerName string

			BeforeEach(func() {
				ledgerName = "ledger2"
				_, err := servers[*leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
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
							_, err := servers[lid-1].client.Apply(ctx, &servicepb.ApplyRequest{
								Requests: []*servicepb.Request{
									createTransactionAction(ledgerName, []*commonpb.Posting{
										newPosting("world", "bank", big.NewInt(100), "USD"),
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
								_, err := servers[lid-1].client.Apply(ctx, &servicepb.ApplyRequest{
									Requests: []*servicepb.Request{
										createTransactionAction(ledgerName, []*commonpb.Posting{
											newPosting("world", "bank", big.NewInt(100), "USD"),
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
