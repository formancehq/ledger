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

// The leader holds back WAL compaction for a follower it heard from within the
// idle timeout (EN-3429). With a compaction margin of one entry, the leader
// would otherwise compact past a briefly stopped follower and send it a
// snapshot; with the default hold the follower must catch up from the log.
var _ = Describe("Follower log retention", func() {
	It("lets a briefly stopped follower catch up from the leader's log without a snapshot", func() {
		const (
			countInstances        = 3
			ledgerName            = "follower-retention"
			countTransactions     = 5
			raftCompactionMargin  = uint64(1)
			offlineAccountAddress = "retained-log"
		)

		options := []testutil.MultiNodeOption{testutil.WithGateway()}
		for i := range countInstances {
			// Default retention settings: 1 GiB cap, 5 minute idle timeout.
			options = append(options, testutil.WithNodeInstruments(i,
				testserver.WithRaftCompactionMargin(raftCompactionMargin),
				testserver.WithMaintenanceInterval(250*time.Millisecond),
			))
		}
		ctx, servers, gateway, leaderID := testutil.SetupMultiNodeCluster(countInstances, options...)
		DeferCleanup(func() {
			gateway.RemoveInterceptor()
			testutil.StopServers(ctx, servers)
		})

		lid := *leaderID
		followerID := ((lid + 1) % countInstances) + 1
		clusterState := func(id uint64) (*clusterpb.ClusterState, error) {
			stateCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			return servers[id-1].ClusterClient.GetClusterState(stateCtx, &clusterpb.GetClusterStateRequest{NodeId: id})
		}

		_, err := servers[lid-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		By("Capturing the caught-up follower position before shutdown")
		var followerLastIndex uint64
		Eventually(func(g Gomega) {
			leaderState, err := clusterState(lid)
			g.Expect(err).To(Succeed())
			followerState, err := clusterState(followerID)
			g.Expect(err).To(Succeed())
			g.Expect(followerState.GetState()).To(Equal("Follower"))
			g.Expect(followerState.GetRaftStatus().GetApplied()).To(BeNumerically(">=", leaderState.GetRaftStatus().GetLastIndex()))

			followerLastIndex = followerState.GetRaftStatus().GetLastIndex()
		}).Within(15 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

		By("Stopping the follower")
		testutil.StopNode(ctx, servers[followerID-1])

		By("Writing past the compaction margin while the follower is down")
		for range countTransactions {
			_, err := servers[lid-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", offlineAccountAddress, big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		}

		minimumSnapshotIndex := followerLastIndex + raftCompactionMargin + 1
		var targetIndex uint64
		Eventually(func(g Gomega) {
			leaderState, err := clusterState(lid)
			g.Expect(err).To(Succeed())
			targetIndex = leaderState.GetRaftStatus().GetLastPersistedIndex()
			g.Expect(targetIndex).To(BeNumerically(">=", minimumSnapshotIndex),
				"offline writes must advance beyond the compaction margin")
		}).Within(15 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

		By("Waiting for leader maintenance to run past the follower's position")
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

		By("Recording any snapshot the leader sends to the follower")
		snapshotSent := make(chan uint64, 1)
		gateway.SetInterceptor(testserver.MessageInterceptorFunc(func(msg *raftpb.Message) bool {
			if msg.GetTo() == followerID && msg.GetType() == raftpb.MsgSnap {
				select {
				case snapshotSent <- msg.GetSnapshot().GetMetadata().GetIndex():
				default:
				}
			}

			return true
		}))

		By("Restarting the follower and waiting for it to catch up")
		testutil.RestartNode(ctx, servers[followerID-1])
		Eventually(func(g Gomega) {
			followerState, err := clusterState(followerID)
			g.Expect(err).To(Succeed())
			g.Expect(followerState.GetState()).To(Equal("Follower"))
			g.Expect(followerState.GetRaftStatus().GetApplied()).To(BeNumerically(">=", targetIndex))
			g.Expect(followerState.GetRaftStatus().GetLastPersistedIndex()).To(BeNumerically(">=", targetIndex))
		}).Within(30 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

		Expect(snapshotSent).NotTo(Receive(), "the leader must replay the retained log instead of sending a snapshot")

		By("Reading the state written while the follower was down from that follower")
		staleCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "stale")
		account, err := servers[followerID-1].Client.GetAccount(staleCtx, &servicepb.GetAccountRequest{
			Ledger:  ledgerName,
			Address: offlineAccountAddress,
		})
		Expect(err).To(Succeed())
		Expect(account.FindVolume("USD", "").GetBalance().DecimalString()).To(Equal(fmt.Sprintf("%d", countTransactions*100)))
	})
})
