//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
	raftwal "github.com/formancehq/ledger/v3/internal/storage/wal"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Two voters hold the newest log but restart with their store behind their WAL
// snapshot. The third voter's log is older, so no up-to-date voter grants it a
// vote: the cluster has a leader only if the out-of-sync voters campaign. The
// winner then leads a term whose store it does not have, so it must fetch its
// checkpoint from another peer. An event sink created while their stores were
// behind is inside that checkpoint, and the winner must run it once its store
// is current.
var _ = Describe("Out-of-sync election", func() {
	Context("When the voters holding the newest log restart out of sync", Ordered, func() {
		const (
			countInstances       = 3
			ledgerName           = "out-of-sync-election"
			raftCompactionMargin = uint64(1)
			maintenanceInterval  = 250 * time.Millisecond
		)

		var (
			ctx            context.Context
			servers        []*testutil.ServiceWithClient
			gateway        *testserver.Gateway
			leaderID       *uint64
			sinkReceiver   *httptest.Server
			sinkDeliveries atomic.Int64
		)

		BeforeAll(func() {
			sinkReceiver = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				sinkDeliveries.Add(1)
				w.WriteHeader(http.StatusOK)
			}))

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

			_, err := servers[*leaderID-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		AfterAll(func() {
			gateway.SetPrepareSnapshotInterceptor(nil)
			testutil.StopServers(ctx, servers)
			sinkReceiver.Close()
		})

		clusterState := func(g Gomega, id uint64) *clusterpb.ClusterState {
			state, err := servers[id-1].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
				NodeId: uint64(id),
			})
			g.Expect(err).To(Succeed())

			return state
		}

		syncStatus := func(id uint64) func(g Gomega) string {
			return func(g Gomega) string {
				return clusterState(g, id).GetSyncProgress().GetStatus()
			}
		}

		writeTransactions := func(via uint64, count int) {
			for range count {
				_, err := servers[via-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
				}, nil, nil)))
				Expect(err).To(Succeed())
			}
		}

		// desyncFollower leaves followerID mid-sync: the leader's log is
		// compacted past the follower's last entry while it is down, so it
		// restarts into a MsgSnap, installs the WAL snapshot, and then hangs in
		// the checkpoint fetch the gateway holds. Its store stays behind.
		//
		// running names the nodes still serving; every one of them must have
		// caught up before the follower stops, since the leader needs the
		// remaining voter's acknowledgements for the writes that follow.
		desyncFollower := func(lid, followerID uint64, running []uint64) {
			By(fmt.Sprintf("Stopping follower %d once every node has caught up", followerID))
			var followerLastIndex uint64
			Eventually(func(g Gomega) {
				leaderLastIndex := clusterState(g, lid).GetRaftStatus().GetLastIndex()
				for _, id := range running {
					state := clusterState(g, id)
					g.Expect(state.GetSyncProgress().GetStatus()).To(Equal("normal"), "node %d", id)
					g.Expect(state.GetRaftStatus().GetLastPersistedIndex()).To(BeNumerically(">=", leaderLastIndex), "node %d", id)
				}
				followerLastIndex = clusterState(g, followerID).GetRaftStatus().GetLastIndex()
			}).Within(15 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())
			testutil.StopNode(ctx, servers[followerID-1])

			By("Writing past the follower's retained log range")
			writeTransactions(lid, 3)
			minimumSnapshotIndex := followerLastIndex + raftCompactionMargin + 1
			var targetIndex uint64
			Eventually(func(g Gomega) {
				targetIndex = clusterState(g, lid).GetRaftStatus().GetLastPersistedIndex()
				g.Expect(targetIndex).To(BeNumerically(">=", minimumSnapshotIndex))
			}).Within(15 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

			By("Waiting for the leader to snapshot beyond that range")
			leaderSnapshotter, err := raftwal.NewSnapshotter(
				filepath.Join(servers[lid-1].WalDir, "snap"),
				logging.FromContext(ctx),
			)
			Expect(err).To(Succeed())
			defer func() { Expect(leaderSnapshotter.Close()).To(Succeed()) }()

			Eventually(func(g Gomega) uint64 {
				snapshot, err := leaderSnapshotter.Load()
				g.Expect(err).To(Succeed())
				g.Expect(snapshot).NotTo(BeNil())

				return snapshot.GetMetadata().GetIndex()
			}).Within(15 * time.Second).ProbeEvery(100 * time.Millisecond).Should(BeNumerically(">=", targetIndex))

			By(fmt.Sprintf("Restarting follower %d into a held checkpoint fetch", followerID))
			testutil.RestartNode(ctx, servers[followerID-1])
			Eventually(syncStatus(followerID)).Within(30 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Equal("syncing"))
		}

		It("elects a leader among the out-of-sync voters and resynchronizes every store", func() {
			lid := *leaderID
			followers := []uint64{lid%countInstances + 1, (lid+1)%countInstances + 1}

			gateway.SetPrepareSnapshotInterceptor(func(*snapshotpb.PrepareSnapshotRequest) bool { return false })
			desyncFollower(lid, followers[0], []uint64{lid, followers[0], followers[1]})

			// F1 is already syncing, but F2 is still a checkpointable voter, so
			// this write is eventually allowed once the health poll refreshes the
			// verdict. Both followers append the entry and it
			// reaches their stores only inside the checkpoints they fetch.
			By("Creating an event sink while the followers' stores are behind")
			Eventually(func() error {
				_, err := servers[lid-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", addEventsSinkAction(&commonpb.SinkConfig{
					Name:         "checkpointed-sink",
					Format:       "json",
					BatchSize:    1,
					BatchDelayMs: 1,
					Type: &commonpb.SinkConfig_Http{
						Http: &commonpb.HttpSinkConfig{Endpoint: sinkReceiver.URL},
					},
				})))
				return err
			}).Within(45 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

			desyncFollower(lid, followers[1], []uint64{lid, followers[1]})

			By("Rejecting writes once checkpointable voter quorum is lost")
			Eventually(func() codes.Code {
				_, err := servers[lid-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("recovery-gate-probe", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
				}, nil, nil)))
				return status.Code(err)
				// The recovery gate is refreshed by the periodic leader health poll
				// (30s by default), so allow one complete interval after the second
				// follower becomes non-checkpointable before asserting the verdict.
			}).Within(45 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Equal(codes.Unavailable))

			By("Stopping the leader so the syncing followers elect among themselves")
			var oldLeaderLastIndex uint64
			Eventually(func(g Gomega) {
				oldLeaderLastIndex = clusterState(g, lid).GetRaftStatus().GetLastIndex()
			}).Should(Succeed())
			testutil.StopNode(ctx, servers[lid-1])

			var interimLeader uint64
			Eventually(func(g Gomega) {
				for _, id := range followers {
					if lead := uint64(clusterState(g, id).GetLeader()); lead != 0 {
						interimLeader = lead
					}
				}
				g.Expect(followers).To(ContainElement(interimLeader))
				// The new term's entry is committed on both: their logs are now
				// newer than the stopped leader's.
				g.Expect(clusterState(g, interimLeader).GetRaftStatus().GetCommit()).To(BeNumerically(">", oldLeaderLastIndex))
			}).Within(30 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

			// Both followers stop while their held fetch is pending, so their
			// WAL snapshot stays ahead of their store: they restart out of sync.
			By("Restarting every node together with the checkpoint fetches released")
			for _, id := range followers {
				testutil.StopNode(ctx, servers[id-1])
			}
			gateway.SetPrepareSnapshotInterceptor(nil)

			// A node is ready once its store has caught up, which the
			// out-of-sync voters reach only after electing a leader among
			// themselves and fetching a checkpoint, so every start must be in
			// flight at once. A cluster that never elects never becomes ready.
			started := make(chan struct{})
			var pending sync.WaitGroup
			for _, srv := range servers {
				if srv.NodeID != lid {
					// Join discovery is answered by the leader, and there is
					// none until these very nodes have elected one. Their
					// durable membership already names every peer.
					srv.Service.Instruments = append(srv.Service.Instruments, testserver.WithJoin(""))
				}

				pending.Add(1)
				go func() {
					defer GinkgoRecover()
					defer pending.Done()
					testutil.RestartNode(ctx, srv)
				}()
			}
			go func() {
				pending.Wait()
				close(started)
			}()
			Eventually(started).Within(90 * time.Second).Should(BeClosed())

			By("Requiring the leader to be one of the voters that restarted out of sync")
			var finalLeader uint64
			Eventually(func(g Gomega) {
				finalLeader = uint64(clusterState(g, followers[0]).GetLeader())
				g.Expect(finalLeader).NotTo(BeZero())
			}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
			Expect(followers).To(ContainElement(finalLeader))

			By("Requiring every store, the leader's included, to resynchronize from a peer")
			for _, srv := range servers {
				Eventually(syncStatus(srv.NodeID)).Within(60 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Equal("normal"))
			}

			By("Writing through the recovered cluster")
			delivered := sinkDeliveries.Load()
			writeTransactions(finalLeader, 1)

			By("Requiring the sink inside the checkpoint to deliver from the new leader")
			Eventually(sinkDeliveries.Load).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(BeNumerically(">", delivered))
		})
	})
})
