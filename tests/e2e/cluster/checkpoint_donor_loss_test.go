//go:build e2e

package cluster

import (
	"context"
	"math/big"
	"path/filepath"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

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
)

var _ = Describe("Checkpoint donor loss", func() {
	It("recovers a syncing winner from another voter when its original donor never returns", func() {
		const ledgerName = "checkpoint-donor-loss"
		options := []testutil.MultiNodeOption{testutil.WithGateway()}
		for i := range 3 {
			options = append(options, testutil.WithNodeInstruments(i,
				testserver.WithRaftCompactionMargin(1),
				testserver.WithMaintenanceInterval(250*time.Millisecond),
			))
		}
		ctx, servers, gateway, leaderID := testutil.SetupMultiNodeCluster(3, options...)
		DeferCleanup(func() {
			gateway.SetPrepareSnapshotInterceptor(nil)
			gateway.RemoveInterceptor()
			testutil.StopServers(ctx, servers)
		})

		donorID := *leaderID
		recoveringID := donorID%3 + 1
		survivorID := (donorID+1)%3 + 1
		state := func(g Gomega, id uint64) *clusterpb.ClusterState {
			response, err := servers[id-1].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: id})
			g.Expect(err).To(Succeed())

			return response
		}
		write := func(via uint64, destination string, amount int64) {
			_, err := servers[via-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", destination, big.NewInt(amount), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		}

		_, err := servers[donorID-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())
		write(donorID, "before-checkpoint", 100)

		var stoppedIndex uint64
		Eventually(func(g Gomega) {
			stoppedIndex = state(g, donorID).GetRaftStatus().GetLastIndex()
			for _, srv := range servers {
				g.Expect(state(g, srv.NodeID).GetRaftStatus().GetLastPersistedIndex()).To(BeNumerically(">=", stoppedIndex))
			}
		}).Within(15 * time.Second).Should(Succeed())
		testutil.StopNode(ctx, servers[recoveringID-1])

		By("Compacting beyond the stopped voter's log while another checkpointable voter remains current")
		for range 3 {
			write(donorID, "checkpoint-only", 100)
		}
		var checkpointIndex uint64
		Eventually(func(g Gomega) {
			checkpointIndex = state(g, donorID).GetRaftStatus().GetLastPersistedIndex()
			g.Expect(checkpointIndex).To(BeNumerically(">", stoppedIndex+1))
			g.Expect(state(g, survivorID).GetRaftStatus().GetLastPersistedIndex()).To(BeNumerically(">=", checkpointIndex))
		}).Within(15 * time.Second).Should(Succeed())

		snapshotter, err := raftwal.NewSnapshotter(filepath.Join(servers[donorID-1].WalDir, "snap"), logging.FromContext(ctx))
		Expect(err).To(Succeed())
		defer func() { Expect(snapshotter.Close()).To(Succeed()) }()
		Eventually(func(g Gomega) uint64 {
			snapshot, err := snapshotter.Load()
			g.Expect(err).To(Succeed())
			g.Expect(snapshot).NotTo(BeNil())

			return snapshot.GetMetadata().GetIndex()
		}).Within(15 * time.Second).Should(BeNumerically(">=", checkpointIndex))

		By("Holding the real checkpoint preparation RPC after the recovering voter installs its WAL snapshot")
		preparing := make(chan struct{})
		var prepareOnce sync.Once
		gateway.SetPrepareSnapshotInterceptor(func(*snapshotpb.PrepareSnapshotRequest) bool {
			prepareOnce.Do(func() { close(preparing) })

			return false
		})
		testutil.RestartNode(ctx, servers[recoveringID-1])
		Eventually(preparing).Within(30 * time.Second).Should(BeClosed())
		Eventually(func(g Gomega) string {
			return state(g, recoveringID).GetSyncProgress().GetStatus()
		}).Within(15 * time.Second).Should(Equal("syncing"))

		By("Committing distinguishable business data while the recovering store is still behind")
		write(donorID, "committed-during-sync", 700)
		var committedIndex, originalTerm uint64
		Eventually(func(g Gomega) {
			donor := state(g, donorID).GetRaftStatus()
			committedIndex = donor.GetLastPersistedIndex()
			originalTerm = donor.GetTerm()
			recovering := state(g, recoveringID)
			g.Expect(recovering.GetRaftStatus().GetCommit()).To(BeNumerically(">=", committedIndex))
			g.Expect(recovering.GetRaftStatus().GetLastPersistedIndex()).To(BeNumerically("<", committedIndex))
			g.Expect(state(g, survivorID).GetRaftStatus().GetLastPersistedIndex()).To(BeNumerically(">=", committedIndex))
		}).Within(15 * time.Second).Should(Succeed())

		// Only the recovering voter may initiate an election. The healthy voter
		// still votes and acknowledges entries, so recovery requires the syncing
		// voter to campaign rather than accidentally passing with a healthy leader.
		gateway.SetInterceptor(testserver.MessageInterceptorFunc(func(message *raftpb.Message) bool {
			return message.GetFrom() != survivorID || (message.GetType() != raftpb.MsgPreVote && message.GetType() != raftpb.MsgVote)
		}))
		By("Stopping the original donor permanently and electing the syncing voter")
		testutil.StopNode(ctx, servers[donorID-1])
		Eventually(func(g Gomega) {
			recovering := state(g, recoveringID)
			g.Expect(recovering.GetLeader()).To(Equal(recoveringID))
			g.Expect(recovering.GetRaftStatus().GetTerm()).To(BeNumerically(">", originalTerm))
			g.Expect(recovering.GetRaftStatus().GetCommit()).To(BeNumerically(">", committedIndex))
			g.Expect(recovering.GetSyncProgress().GetStatus()).To(Equal("syncing"))
		}).Within(30 * time.Second).Should(Succeed())

		By("Keeping store-dependent reads gated while checkpoint recovery is held")
		// Disable client retries so the server's explicit sync refusal reaches
		// the assertion instead of becoming an eventual client deadline.
		readClient, _, readConn, err := testutil.NewGRPCClientWithRetry(servers[recoveringID-1].GRPCPort, false)
		Expect(err).To(Succeed())
		DeferCleanup(func() { Expect(readConn.Close()).To(Succeed()) })
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = actions.GetAccount(readCtx, readClient, ledgerName, "checkpoint-only")
		cancel()
		Expect(status.Code(err)).To(Equal(codes.Unavailable))
		Expect(status.Convert(err).Message()).To(Equal("node is syncing"))

		By("Releasing checkpoint recovery with only the other voter available as a donor")
		gateway.SetPrepareSnapshotInterceptor(nil)
		for _, id := range []uint64{recoveringID, survivorID} {
			Eventually(func(g Gomega) {
				current := state(g, id)
				g.Expect(current.GetSyncProgress().GetStatus()).To(Equal("normal"))
				g.Expect(current.GetRaftStatus().GetLastPersistedIndex()).To(BeNumerically(">", committedIndex))
			}).Within(60 * time.Second).Should(Succeed())
		}

		By("Acknowledging a new business write from the recovered winner")
		write(recoveringID, "after-recovery", 900)
		var writtenIndex uint64
		Eventually(func(g Gomega) {
			writtenIndex = state(g, recoveringID).GetRaftStatus().GetLastPersistedIndex()
			g.Expect(state(g, survivorID).GetRaftStatus().GetLastPersistedIndex()).To(BeNumerically(">=", writtenIndex))
		}).Within(15 * time.Second).Should(Succeed())

		// Stale consistency pins these reads to each replica's own store. A
		// routed read through the surviving leader would hide follower data loss.
		localCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "stale")
		for _, id := range []uint64{recoveringID, survivorID} {
			for account, expected := range map[string]string{
				"before-checkpoint":     "100",
				"checkpoint-only":       "300",
				"committed-during-sync": "700",
				"after-recovery":        "900",
			} {
				response, err := actions.GetAccount(localCtx, servers[id-1].Client, ledgerName, account)
				Expect(err).To(Succeed())
				Expect(response.FindVolume("USD", "").GetInput().DecimalString()).To(Equal(expected), "node %d, account %s", id, account)
			}
		}
	})
})
