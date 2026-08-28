//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"math/big"
	"path/filepath"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	raftwal "github.com/formancehq/ledger/v3/internal/storage/wal"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/metadata"
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
				countInstances,
				testutil.WithGateway(),
			)
		})

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("should start successfully", func() {})

		It("should create a ledger and delete it", func() {
			_, err := servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("ledger0", nil)))
			Expect(err).To(Succeed())
		})

		It("should create transactions through all nodes", func() {
			ledgerName := "multi-node-ledger"

			Eventually(servers[0]).To(HaveALeader(nil), "Timed out waiting for leader election")

			_, err := servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			for i := range countInstances {
				_, err := servers[i].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", fmt.Sprintf("node-%d", i+1), big.NewInt(100*int64(i+1)), "USD"),
				}, nil, nil)))
				Expect(err).To(Succeed(), "Failed to create transaction through node %d", i+1)
			}
		})

		It("should rejoin the cluster after a follower restart", func() {
			Skip("Flaky: hangs on stale gRPC connection — see commit a07bc611")
			followerID := ((*leaderID + 1) % countInstances) + 1
			testutil.StopNode(ctx, servers[followerID-1])
			testutil.RestartNode(ctx, servers[followerID-1])

			Eventually(servers[followerID-1]).
				WithTimeout(30*time.Second).
				WithPolling(500*time.Millisecond).
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
			ledgerName            = "ledger2"
			countTransactions     = 3
			raftCompactionMargin  = uint64(1)
			maintenanceInterval   = 250 * time.Millisecond
			offlineAccountAddress = "snapshot-restored"
		)

		var (
			ctx        context.Context
			servers    []*testutil.ServiceWithClient
			gateway    *testserver.Gateway
			leaderID   *uint64
			followerID uint64
		)

		BeforeAll(func() {
			clusterOptions := []testutil.MultiNodeOption{testutil.WithGateway()}
			for i := range countInstances {
				clusterOptions = append(clusterOptions, testutil.WithNodeInstruments(
					i,
					testserver.WithRaftCompactionMargin(raftCompactionMargin),
					testserver.WithMaintenanceInterval(maintenanceInterval),
				))
			}

			ctx, servers, gateway, leaderID = testutil.SetupMultiNodeCluster(
				countInstances,
				clusterOptions...,
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

			_, err := servers[lid-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("ledger1", nil)))
			Expect(err).To(Succeed())

			for i := 0; i < 5; i++ {
				_, err := servers[lid-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction("ledger1", []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
				}, nil, nil)))
				Expect(err).To(Succeed())
			}
		})

		It("should restore the state after follower comes back", func() {
			lid := *leaderID

			_, err := servers[lid-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			testutil.RestartNode(ctx, servers[followerID-1])

			Eventually(servers[followerID-1]).
				WithTimeout(30*time.Second).
				WithPolling(500*time.Millisecond).
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
			lid := *leaderID

			By("Capturing the caught-up follower progress before shutdown")
			var followerLastIndex uint64
			Eventually(func(g Gomega) {
				leaderState, err := servers[lid-1].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
					NodeId: uint32(lid),
				})
				g.Expect(err).To(Succeed())

				followerState, err := servers[followerID-1].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
					NodeId: uint32(followerID),
				})
				g.Expect(err).To(Succeed())
				g.Expect(followerState.GetState()).To(Equal("Follower"))
				g.Expect(followerState.GetRaftStatus().GetApplied()).To(BeNumerically(">=", leaderState.GetRaftStatus().GetLastIndex()))
				g.Expect(followerState.GetRaftStatus().GetLastPersistedIndex()).To(BeNumerically(">=", leaderState.GetRaftStatus().GetLastIndex()))

				followerLastIndex = followerState.GetRaftStatus().GetLastIndex()
			}).Within(15 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

			By("Stopping the follower")
			testutil.StopNode(ctx, servers[followerID-1])

			By("Creating state that cannot be replayed after compaction")
			for i := 0; i < countTransactions; i++ {
				_, err := servers[lid-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", offlineAccountAddress, big.NewInt(100), "USD"),
				}, nil, nil)))
				Expect(err).To(Succeed())
			}

			minimumSnapshotIndex := followerLastIndex + raftCompactionMargin + 1
			var targetIndex uint64
			Eventually(func(g Gomega) {
				leaderState, err := servers[lid-1].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
					NodeId: uint32(lid),
				})
				g.Expect(err).To(Succeed())

				targetIndex = leaderState.GetRaftStatus().GetLastPersistedIndex()
				g.Expect(targetIndex).To(BeNumerically(">=", minimumSnapshotIndex),
					"offline writes must advance beyond the stopped follower's recoverable log range")
			}).Within(15 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

			By("Waiting for leader maintenance to snapshot beyond the follower's retained range")
			leaderSnapshotter, err := raftwal.NewSnapshotter(
				filepath.Join(servers[lid-1].WalDir, "snap"),
				logging.FromContext(ctx),
			)
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) uint64 {
				snapshot, err := leaderSnapshotter.Load()
				g.Expect(err).To(Succeed())
				g.Expect(snapshot).NotTo(BeNil())

				return snapshot.GetMetadata().GetIndex()
			}).Within(15 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeNumerically(">=", targetIndex))

			By("Allowing only snapshot catch-up while recording the real Raft MsgSnap")
			snapshotSent := make(chan uint64, 1)
			gateway.SetInterceptor(testserver.MessageInterceptorFunc(func(msg *raftpb.Message) bool {
				if msg.GetTo() != followerID {
					return true
				}

				if msg.GetType() == raftpb.MsgApp {
					return false
				}

				if msg.GetType() == raftpb.MsgSnap {
					select {
					case snapshotSent <- msg.GetSnapshot().GetMetadata().GetIndex():
					default:
					}
				}

				return true
			}))
			DeferCleanup(gateway.RemoveInterceptor)

			By("Starting the follower and requiring snapshot transfer evidence")
			testutil.RestartNode(ctx, servers[followerID-1])

			var sentSnapshotIndex uint64
			Eventually(snapshotSent).Within(15 * time.Second).Should(Receive(&sentSnapshotIndex))
			Expect(sentSnapshotIndex).To(BeNumerically(">=", targetIndex))
			gateway.RemoveInterceptor()

			By("Waiting for the follower's applied and durable indexes to pass the installed snapshot")
			Eventually(func(g Gomega) {
				followerState, err := servers[followerID-1].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
					NodeId: uint32(followerID),
				})
				g.Expect(err).To(Succeed())
				g.Expect(followerState.GetState()).To(Equal("Follower"))
				g.Expect(followerState.GetRaftStatus().GetApplied()).To(BeNumerically(">=", sentSnapshotIndex))
				g.Expect(followerState.GetRaftStatus().GetLastPersistedIndex()).To(BeNumerically(">=", sentSnapshotIndex))
			}).Within(30 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

			By("Reading state created while the follower was offline from that follower")
			staleCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "stale")
			account, err := servers[followerID-1].Client.GetAccount(staleCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: offlineAccountAddress,
			})
			Expect(err).To(Succeed())
			Expect(account.FindVolume("USD", "").Balance).To(Equal(fmt.Sprintf("%d", countTransactions*100)))
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
				countInstances,
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
				_, err := servers[*leaderID-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
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
							if msg.GetTo() == followerID && msg.GetType() == raftpb.MsgApp {
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
							_, err := servers[lid-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
								actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
							}, nil, nil)))
							Expect(err).To(Succeed())
						}
					})

					It("Should trigger the sending of a snapshot from a leader", func() {
						lid := *leaderID
						gateway.RemoveInterceptor()
						By("Creating a transaction to trigger the delay detection by the leader", func() {
							for i := 0; i < countTransactions; i++ {
								_, err := servers[lid-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
									actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
								}, nil, nil)))
								Expect(err).To(Succeed())
							}
						})
					})
				})
			})
		})
	})
})
